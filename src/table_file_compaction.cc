/************************************************************************
Copyright 2017-2019 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "table_file.h"

#include "bloomfilter.h"
#include "db_mgr.h"
#include "internal_helper.h"
#include "table_mgr.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

void TableFile::releaseDstHandle(void* void_handle) {
    FdbHandle* handle = (FdbHandle*)void_handle;
    delete handle;
}

Status TableFile::compactTo(const std::string& dst_filename,
                            const CompactOptions& options,
                            void*& dst_handle_out)
{
    Status s;
    const DBConfig* db_config = tableMgr->getDbConfig();

    FdbHandle* compact_handle = new FdbHandle(this, db_config, myOpt);
    EP( openFdbHandle(db_config, filename, compact_handle) );

    s = compactToManully( compact_handle,
                          dst_filename,
                          options,
                          dst_handle_out );

    delete compact_handle;
    return s;
}

// Check
//   1) Jungle's own meta section, and
//   2) User defined custom tombstone checking function.
bool TableFile::isFdbDocTombstone(fdb_doc* doc)
{
    const DBConfig* db_config = tableMgr->getDbConfig();

    // Decode meta.
    SizedBuf user_meta_out;
    SizedBuf::Holder h_user_meta_out(user_meta_out); // auto free.

    SizedBuf raw_meta(doc->metalen, doc->meta);;

    InternalMeta i_meta;
    rawMetaToUserMeta(raw_meta, i_meta, user_meta_out);

    if (doc->deleted) i_meta.isTombstone = true;

    // If custom tombstone function exists, call and check it.
    if (db_config->compactionCbFunc) {
        CompactionCbParams params;
        params.rec.kv.key = SizedBuf(doc->keylen, doc->key);
        params.rec.kv.value = SizedBuf(doc->bodylen, doc->body);
        params.rec.meta = SizedBuf(user_meta_out.size, user_meta_out.data);
        params.rec.seqNum = doc->seqnum;

        CompactionCbDecision dec = db_config->compactionCbFunc(params);
        if (dec == CompactionCbDecision::DROP) {
            i_meta.isTombstone = true;
        }
    }
    return i_meta.isTombstone;
}

Status TableFile::compactToManully(FdbHandle* compact_handle,
                                   const std::string& dst_filename,
                                   const CompactOptions& options,
                                   void*& dst_handle_out)
{
    _log_info(myLog, "doing manual compaction");
    Timer tt;
    Status s;

    bool is_last_level = (tableInfo->level == tableMgr->getNumLevels() - 1);
    DBConfig local_config = *(tableMgr->getDbConfig());
    // Set bulk loading true to set WAL-flush-before-commit.
    local_config.bulkLoading = true;

    // Block reuse shouldn't happen during compaction.
    TableFileOptions dst_opt;
    dst_opt.minBlockReuseFileSize = std::numeric_limits<uint64_t>::max();

    FdbHandle* dst_handle = new FdbHandle(this, &local_config, dst_opt);
    dst_handle_out = dst_handle;
    //GcDelete<FdbHandle*> gc_dst(dst_handle);

    EP( openFdbHandle(&local_config, dst_filename, dst_handle) );

    // Create bloom filter for destination file.
    BloomFilter* dst_bf = nullptr;
    if ( local_config.bloomFilterBitsPerUnit > 0.0 ) {
        // Calculate based on WSS.
        uint64_t bf_bitmap_size = myOpt.bloomFilterSize;
        TableStats my_stats;
        getStats(my_stats);
        if (my_stats.workingSetSizeByte) {
            bf_bitmap_size = getBfSizeByWss(&local_config, my_stats.workingSetSizeByte);
        }
        if (!bf_bitmap_size) {
            bf_bitmap_size = getBfSizeByLevel(&local_config, tableInfo->level);
        }
        dst_bf = new BloomFilter(bf_bitmap_size, 3);
    }

    fdb_iterator* itr = nullptr;
    fdb_status fs = FDB_RESULT_SUCCESS;
    fs = fdb_iterator_init( compact_handle->db,
                            &itr,
                            nullptr, 0, nullptr, 0,
                            FDB_ITR_NO_DELETES );
    if (fs != FDB_RESULT_SUCCESS) return Status::MANUAL_COMPACTION_OPEN_FAILED;

    uint64_t cnt = 0;
    uint64_t discards = 0;
    s = Status::OK;

    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    const GlobalConfig* global_config = mgr->getGlobalConfig();
    const GlobalConfig::CompactionThrottlingOptions& t_opt =
        global_config->ctOpt;

    // Flush block cache for every given second.
    Timer sync_timer;
    Timer throttling_timer(t_opt.resolution_ms);

    sync_timer.setDurationMs(local_config.preFlushDirtyInterval_sec * 1000);

    do {
        fdb_doc tmp_doc;
        memset(&tmp_doc, 0x0, sizeof(tmp_doc));

        fdb_doc *ret_doc = &tmp_doc;
        fs = fdb_iterator_get(itr, &ret_doc);
        if (fs != FDB_RESULT_SUCCESS) break;

        // If 1) flag (for not to delete tombstone) is set, OR
        //    2) LSM / level extension mode AND
        //       current level is not the last level,
        // then skip checking whether the given record is tombstone.
        bool is_tombstone_out = false;
        bool check_tombstone = true;
        if (options.preserveTombstone) {
            check_tombstone = false;
        }
        if ( local_config.nextLevelExtension &&
             !is_last_level ) {
            check_tombstone = false;
        }
        if (check_tombstone) {
            is_tombstone_out = isFdbDocTombstone(ret_doc);
        }

        if (is_tombstone_out) {
            // Tombstone.
            discards++;
        } else {
            // WARNING: SHOULD KEEP THE SAME SEQUENCE NUMBER!
            ret_doc->flags = FDB_CUSTOM_SEQNUM;
            fdb_set(dst_handle->db, ret_doc);
            cnt++;

            if (dst_bf) {
                size_t hash_size = ret_doc->keylen;
                if ( local_config.keyLenLimitForHash &&
                     hash_size > local_config.keyLenLimitForHash ) {
                    hash_size = local_config.keyLenLimitForHash;
                }
                dst_bf->set(ret_doc->key, hash_size);
            }
        }

        free(ret_doc->key);
        free(ret_doc->meta);
        free(ret_doc->body);

        if (!tableMgr->isCompactionAllowed()) {
            s = Status::COMPACTION_CANCELLED;
            break;
        }

        if (sync_timer.timeout()) {
            fdb_sync_file(dst_handle->dbFile);
            sync_timer.reset();
            throttling_timer.reset();
        }

        if (d_params.compactionDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.compactionDelayUs);
        }

        // Do throttling, if enabled.
        TableMgr::doCompactionThrottling(t_opt, throttling_timer);

    } while (fdb_iterator_next(itr) == FDB_RESULT_SUCCESS);

    uint64_t elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    uint64_t bf_size = (dst_bf) ? dst_bf->size() : 0;
    _log_info( myLog, "in-place compaction moved %zu live docs, "
               "%zu tombstones, BF size %zu bytes (%zu bits), %zu us, %.1f iops",
               cnt, discards,
               bf_size / 8, bf_size,
               elapsed_us,
               (double)(cnt + discards) * 1000000 / elapsed_us );

    // WARNING: Should be done before commit.
    if (dst_bf) {
        saveBloomFilter(dst_filename + ".bf", dst_bf, true);
        DELETE(dst_bf);
    }
    dst_handle->commit();

    fs = fdb_iterator_close(itr);
    itr = nullptr;

    return s;
}

Status TableFile::mergeCompactTo(const std::string& file_to_merge,
                                 const std::string& dst_filename,
                                 const CompactOptions& options)
{
    Status s;
    fdb_status fs = FDB_RESULT_SUCCESS;
    const DBConfig* db_config = tableMgr->getDbConfig();
    DBConfig dst_config = *db_config;
    dst_config.bulkLoading = true;

    Timer tt;

    // Open ForestDB handles.
    FdbHandle* my_handle = new FdbHandle(this, db_config, myOpt);
    FdbHandle* merge_handle = new FdbHandle(this, db_config, myOpt);

    // Block reuse shouldn't happen during compaction.
    TableFileOptions dst_opt;
    dst_opt.minBlockReuseFileSize = std::numeric_limits<uint64_t>::max();
    FdbHandle* dst_handle = new FdbHandle(this, &dst_config, dst_opt);

    // Create bloom filter for destination file.
    BloomFilter* dst_bf = nullptr;
    if ( db_config->bloomFilterBitsPerUnit > 0.0 ) {
        // Calculate based on WSS if this (origin) table.
        uint64_t bf_bitmap_size = myOpt.bloomFilterSize;
        TableStats my_stats;
        getStats(my_stats);
        if (my_stats.workingSetSizeByte) {
            bf_bitmap_size = getBfSizeByWss(db_config, my_stats.workingSetSizeByte);
        }
        if (!bf_bitmap_size) {
            bf_bitmap_size = getBfSizeByLevel(db_config, tableInfo->level);
        }
        dst_bf = new BloomFilter(bf_bitmap_size, 3);
    }

    // Auto free.
    GcDelete<FdbHandle*> gc_my_handle(my_handle);
    GcDelete<FdbHandle*> gc_merge_handle(merge_handle);
    GcDelete<FdbHandle*> gc_dst_handle(dst_handle);

    EP( openFdbHandle(db_config, filename, my_handle) );
    EP( openFdbHandle(db_config, file_to_merge, merge_handle) );
    EP( openFdbHandle(db_config, dst_filename, dst_handle) );

    // Open iterators.
    fdb_iterator* my_itr = nullptr;
    fdb_iterator* merge_itr = nullptr;

    fs = fdb_iterator_init( my_handle->db, &my_itr,
                            nullptr, 0, nullptr, 0, FDB_ITR_NO_DELETES );
    if (fs != FDB_RESULT_SUCCESS) return Status::MANUAL_COMPACTION_OPEN_FAILED;
    // Auto close.
    GcFunc gc_my_itr( std::bind(fdb_iterator_close, my_itr) );

    fs = fdb_iterator_init( merge_handle->db, &merge_itr,
                            nullptr, 0, nullptr, 0, FDB_ITR_NO_DELETES );
    if (fs != FDB_RESULT_SUCCESS) return Status::MANUAL_COMPACTION_OPEN_FAILED;
    // Auto close.
    GcFunc gc_merge_itr( std::bind(fdb_iterator_close, merge_itr) );

    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    // Flush block cache for every 5 second.
    Timer sync_timer;
    sync_timer.setDurationMs(db_config->preFlushDirtyInterval_sec * 1000);

    uint64_t my_cnt = 0, my_discards = 0,
             merge_cnt = 0, merge_discards = 0;
    uint64_t final_cnt = 0;
    bool my_itr_ended = false;
    bool merge_itr_ended = false;
    fdb_doc* my_doc = nullptr;
    fdb_doc* merge_doc = nullptr;
    do {
        fdb_status my_fs = FDB_RESULT_SUCCESS;
        fdb_status merge_fs = FDB_RESULT_SUCCESS;

        if (!my_doc) my_fs = fdb_iterator_get(my_itr, &my_doc);
        if (!merge_doc) merge_fs = fdb_iterator_get(merge_itr, &merge_doc);
        if ( my_fs    != FDB_RESULT_SUCCESS &&
             merge_fs != FDB_RESULT_SUCCESS ) break;

        int cmp = 0;
        if ( my_doc && !merge_doc) cmp = -1;
        if (!my_doc &&  merge_doc) cmp = 1;
        if (!my_doc && !merge_doc) assert(0);
        if ( my_doc &&  merge_doc) {
            if (db_config->cmpFunc) {
                // Custom cmp mode.
                CustomCmpFunc func = db_config->cmpFunc;
                void* param = db_config->cmpFuncParam;
                cmp = func( my_doc->key,    my_doc->keylen,
                            merge_doc->key, merge_doc->keylen, param );
            } else {
                SizedBuf l(my_doc->keylen, my_doc->key);
                SizedBuf r(merge_doc->keylen, merge_doc->key);
                cmp = SizedBuf::cmp(l, r);
            }
        }

        fdb_doc* doc_choosen = nullptr;

        uint64_t* cnt = &my_cnt;
        uint64_t* discards = &my_discards;

        if (cmp < 0) { // `my_doc < merge_doc`
            doc_choosen = my_doc;
            my_doc = nullptr;

        } else if (cmp > 0) { // `my_doc > merge_doc`
            doc_choosen = merge_doc;
            merge_doc = nullptr;
            cnt = &merge_cnt;
            discards = &merge_discards;

        } else { // `my_doc == merge_doc`
            // We should compare sequence number,
            // and pick fresher one only.
            if (my_doc->seqnum > merge_doc->seqnum) {
                doc_choosen = my_doc;
                fdb_doc_free(merge_doc);
                merge_cnt++;

            // WARNING: The same sequence number should be allowed.
            } else if (my_doc->seqnum <= merge_doc->seqnum) {
                doc_choosen = merge_doc;
                fdb_doc_free(my_doc);
                cnt = &merge_cnt;
                discards = &merge_discards;
                my_cnt++;
            }

            // And also move both cursors.
            my_doc = nullptr;
            merge_doc = nullptr;
        }

        bool is_tombstone_out = false;
        if (!options.preserveTombstone) {
            is_tombstone_out = isFdbDocTombstone(doc_choosen);
        }
        if (is_tombstone_out) {
            // Tombstone.
            (*discards)++;

        } else {
            // WARNING: SHOULD KEEP THE SAME SEQUENCE NUMBER!
            doc_choosen->flags = FDB_CUSTOM_SEQNUM;
            fdb_set(dst_handle->db, doc_choosen);
            (*cnt)++;
            final_cnt++;

            if (dst_bf) {
                size_t hash_size = doc_choosen->keylen;
                if ( db_config->keyLenLimitForHash &&
                     hash_size > db_config->keyLenLimitForHash ) {
                    hash_size = db_config->keyLenLimitForHash;
                }
                dst_bf->set(doc_choosen->key, hash_size);
            }
        }
        fdb_doc_free(doc_choosen);

        // Move iterator of choosen doc.
        if (!my_doc) {
            my_fs = fdb_iterator_next(my_itr);
            if (my_fs != FDB_RESULT_SUCCESS) my_itr_ended = true;
        }
        if (!merge_doc) {
            merge_fs = fdb_iterator_next(merge_itr);
            if (merge_fs != FDB_RESULT_SUCCESS) merge_itr_ended = true;
        }

        if (!tableMgr->isCompactionAllowed()) {
            s = Status::COMPACTION_CANCELLED;
            if (my_doc) fdb_doc_free(my_doc);
            if (merge_doc) fdb_doc_free(merge_doc);
            break;
        }

        if (sync_timer.timeout()) {
            fdb_sync_file(dst_handle->dbFile);
            sync_timer.reset();
        }

        if (d_params.compactionDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.compactionDelayUs);
        }

    // Until both iterators reach end.
    } while ( !my_itr_ended || !merge_itr_ended );

    // WARNING: Should be done before commit.
    if (dst_bf) {
        saveBloomFilter(dst_filename + ".bf", dst_bf, true);
        DELETE(dst_bf);
    }
    dst_handle->commit();

    // Close iterator first.
    gc_my_itr.gcNow();
    gc_merge_itr.gcNow();

    uint64_t elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    uint64_t bf_size = (dst_bf) ? dst_bf->size() : 0;
    _log_info( myLog, "in-place merge compaction "
               "moved %zu live docs %zu tombstones from mine, "
               "%zu live docs %zu tombstones from merge, "
               "%zu docs in new file, BF size %zu byets (%zu bits), "
               "%zu us elapsed, %.1f iops",
               my_cnt, my_discards, merge_cnt, merge_discards,
               final_cnt,
               bf_size / 8, bf_size,
               elapsed_us,
               (double)(my_cnt + my_discards +
                        merge_cnt + merge_discards) * 1000000 / elapsed_us );

    return s;
}

}

