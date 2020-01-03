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

static inline fdb_compact_decision fdb_cb_bridge
                                   ( fdb_file_handle *fhandle,
                                     fdb_compaction_status status,
                                     const char *kv_store_name,
                                     fdb_doc *doc,
                                     uint64_t last_oldfile_offset,
                                     uint64_t last_newfile_offset,
                                     void *ctx)
{
    CompactionCbParams params;
    params.rec.kv.key = SizedBuf(doc->keylen, doc->key);
    params.rec.kv.value = SizedBuf(doc->bodylen, doc->body);
    params.rec.meta = SizedBuf(doc->metalen, doc->meta);
    params.rec.seqNum = doc->seqnum;

    const DBConfig* db_config = (const DBConfig*)(ctx);
    CompactionCbDecision dec = db_config->compactionCbFunc(params);
    if (dec == CompactionCbDecision::DROP) return FDB_CS_DROP_DOC;

    return FDB_CS_KEEP_DOC;
}

static inline void fdb_log_cb(int level,
                              int ec,
                              const char* file,
                              const char* func,
                              size_t line,
                              const char* err_msg,
                              void* ctx)
{
    SimpleLogger* my_log = (SimpleLogger*)ctx;
    my_log->put(level, file, func, line, "[FDB][%d] %s", ec, err_msg);
}

TableFile::FdbHandle::FdbHandle(TableFile* _parent,
                                const DBConfig* db_config,
                                const TableFileOptions& t_file_opt)
    : parent(_parent)
    , dbConfig(db_config)
    , tFileOpt(t_file_opt)
    , dbFile(nullptr)
    , db(nullptr)
    , config(getFdbSettings(db_config))
    , kvsConfig(getKvsSettings())
    {}

TableFile::FdbHandle::~FdbHandle() {
    close();
}

fdb_config TableFile::FdbHandle::getFdbSettings(const DBConfig* db_config) {
    fdb_config config = fdb_get_default_config();

    DBMgr* mgr = DBMgr::getWithoutInit();
    if (mgr) {
        config.buffercache_size = mgr->getGlobalConfig()->fdbCacheSize;
        fdb_set_log_callback_ex_global(fdb_log_cb,
                                       mgr->getLogger());
    }

    if (db_config && db_config->bulkLoading) {
        // Bulk loading mode: enable WAL flush before commit.
        config.wal_flush_before_commit = true;
        config.bulk_load_mode = true;
    } else {
        // Otherwise:
        // Jungle will manually control WAL flushing.
        config.wal_flush_before_commit = false;
        config.bulk_load_mode = false;
    }
    config.do_not_search_wal = true;

    // Disable auto compaction,
    // temporarily enable block reuse.
    config.compaction_threshold = 0;
    if ( db_config->blockReuseFactor &&
         db_config->blockReuseFactor > 100 ) {
        size_t F = db_config->blockReuseFactor;
        // 300% -> 66.6% stale ratio.
        // 333% -> 70% stale ratio.
        config.block_reusing_threshold = (F - 100) * 100 / F;
    } else {
        // Disabled.
        config.block_reusing_threshold = 100;
    }
    config.max_block_reusing_cycle = db_config->maxBlockReuseCycle;
    config.min_block_reuse_filesize = tFileOpt.minBlockReuseFileSize;
    config.seqtree_opt = FDB_SEQTREE_USE;
    config.purging_interval = 60 * 60; // 1 hour.
    config.num_keeping_headers = 10;
    config.do_not_move_to_compacted_file = true;
    //config.enable_reusable_block_reservation = true;

    // If compaction callback function is given, enable it.
    if (db_config->compactionCbFunc) {
        config.compaction_cb = fdb_cb_bridge;
        config.compaction_cb_ctx = (void*)db_config;
        // Callback function will be invoked for every document.
        config.compaction_cb_mask = FDB_CS_MOVE_DOC;
    }

    // We SHOULD have at least one ForestDB background compactor,
    // to do lazy file deletion.
    // NOTE:
    //   We can also disable both compactor and lazy deletion,
    //   but deleting large size file may have bad impact on latency,
    //   as foreground deletion usually happens on close of iterator.
    config.enable_background_compactor = true;
    config.num_compactor_threads = 1;

    config.log_msg_level = 4;
    return config;
}

fdb_kvs_config TableFile::FdbHandle::getKvsSettings() {
    return fdb_get_default_kvs_config();
}

void TableFile::FdbHandle::refreshSettings() {
    config = getFdbSettings(dbConfig);
    kvsConfig = getKvsSettings();
}

Status TableFile::FdbHandle::open(const std::string& filename) {
    fdb_status fs;

    fs = fdb_open(&dbFile, filename.c_str(), &config);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_FILE_FAIL;

    fs = fdb_kvs_open(dbFile, &db, NULL, &kvsConfig);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

    return Status();
}

Status TableFile::FdbHandle::openCustomCmp(const std::string& filename,
                                           fdb_custom_cmp_variable cmp_func,
                                           void* cmp_func_param)
{
    fdb_status fs;

    char* kvs_names[1] = {nullptr};
    fdb_custom_cmp_variable functions[1] = {cmp_func};
    void* user_params[1] = {cmp_func_param};
    fs = fdb_open_custom_cmp(&dbFile, filename.c_str(), &config,
                             1, kvs_names, functions, user_params);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_FILE_FAIL;

    fs = fdb_kvs_open(dbFile, &db, NULL, &kvsConfig);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

    return Status();
}

Status TableFile::FdbHandle::commit() {
    fdb_status fs;
    fs = fdb_commit(dbFile, FDB_COMMIT_MANUAL_WAL_FLUSH);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_COMMIT_FAIL;
    return Status();
}

Status TableFile::FdbHandle::close() {
    fdb_status fs = FDB_RESULT_SUCCESS;
    if (db) {
        fs = fdb_kvs_close(db);
        if (fs != FDB_RESULT_SUCCESS) return Status::FDB_KVS_CLOSE_FAIL;
        db = nullptr;
    }
    if (dbFile) {
        fdb_close(dbFile);
        if (fs != FDB_RESULT_SUCCESS) return Status::FDB_CLOSE_FAIL;
        dbFile = nullptr;
    }
    return Status();
}

TableFile::FdbHandleGuard::FdbHandleGuard( TableFile* _t_file,
                                           FdbHandle* _handle )
    : tFile(_t_file), handle(_handle)
    {}

TableFile::FdbHandleGuard::~FdbHandleGuard() {
    if (handle) tFile->returnHandle(handle);
}


TableFile::TableFile(const TableMgr* table_mgr)
    : myNumber(NOT_INITIALIZED)
    , fOps(nullptr)
    , tableMgr(table_mgr)
    , tableInfo(nullptr)
    , writer(nullptr)
    , bfByKey(nullptr)
    , tlbByKey(nullptr)
    , myLog(nullptr)
{}

TableFile::~TableFile() {
    assert(snapHandles.size() == 0);
    {   std::lock_guard<std::mutex> l(latestSnapshotLock);
        for (Snapshot*& cur_snp: latestSnapshot) {
            // Remaining snapshot's reference counter should be 1,
            // which is referred to by this file.
            // Note that all iterators derived from this file
            // should be closed before calling this destructor.
            assert(cur_snp->refCount == 1);
            fdb_kvs_close(cur_snp->fdbSnap);
            delete cur_snp;
        }
    }
    if (writer) {
        DELETE(writer);
    }
    for (auto& entry: readers) {
        delete entry;
    }
    DELETE(bfByKey);
    DELETE(tlbByKey);
}

std::string TableFile::getTableFileName(const std::string& path,
                                        uint64_t prefix_num,
                                        uint64_t table_file_num)
{
    // Table file name example: table0001_00000001
    //                          table0001_00000002
    //                          ...
    char p_num[16];
    char t_num[16];
    sprintf(p_num, "%04" PRIu64, prefix_num);
    sprintf(t_num, "%08" PRIu64, table_file_num);
    std::string t_filename = path + "/table" + p_num + "_" + t_num;
    return t_filename;
}

TableFile::FdbHandle* TableFile::getIdleHandle() {
    mGuard l(readersLock);
    FdbHandle* ret = nullptr;
    auto entry = readers.begin();
    if (entry == readers.end()) {
        l.unlock();

        ret = new FdbHandle(this, tableMgr->getDbConfig(), myOpt);
        openFdbHandle(tableMgr->getDbConfig(), filename, ret);

        l.lock();
    } else {
        ret = *entry;
        readers.pop_front();
    }
    return ret;
}

void TableFile::returnHandle(FdbHandle* f_handle) {
    mGuard l(readersLock);
    readers.push_front(f_handle);
}

Status TableFile::openFdbHandle(const DBConfig* db_config,
                                const std::string& f_name,
                                FdbHandle* f_handle)
{
    Status s;
    if (db_config->cmpFunc) {
        // Custom cmp mode.
        EP( f_handle->openCustomCmp( f_name,
                                     db_config->cmpFunc,
                                     db_config->cmpFuncParam ) );
    } else {
        EP( f_handle->open(f_name) );
    }
    return Status::OK;
}

uint64_t TableFile::getBfSizeByLevel(const DBConfig* db_config, size_t level) {
    uint64_t MAX_TABLE_SIZE = db_config->getMaxTableSize(level);
    uint64_t bf_bitmap_size = MAX_TABLE_SIZE / 1024 *
                              db_config->bloomFilterBitsPerUnit;
    return bf_bitmap_size;
}

uint64_t TableFile::getBfSizeByWss(const DBConfig* db_config, uint64_t wss) {
    uint64_t bf_bitmap_size = wss / 1024 *
                              db_config->bloomFilterBitsPerUnit;
    return bf_bitmap_size;
}

uint64_t TableFile::getBfSize() const {
    if (!bfByKey) return 0;
    return bfByKey->size();
}

void TableFile::initBooster(size_t level, const DBConfig* db_config) {
    uint64_t limit = tableMgr->getBoosterLimit(level);
    if (!limit) return;
    tlbByKey = new TableLookupBooster( limit, tableMgr, this );
}

Status TableFile::create(size_t level,
                         uint64_t table_number,
                         const std::string& f_name,
                         FileOps* f_ops,
                         const TableFileOptions& opt)
{
    if (writer) return Status::ALREADY_INITIALIZED;

    Status s;
    filename = f_name;
    myNumber = table_number;
    fOps = f_ops;
    myOpt = opt;

    if (fOps->exist(filename)) {
        // Previous file exists, which means that there is a legacy log file.
        // We should overwrite it.
        _log_warn(myLog, "table %s already exists, remove it", filename.c_str());
        fOps->remove(filename);
    }

    const DBConfig* db_config = tableMgr->getDbConfig();

    // Create a ForestDB file.
    writer = new FdbHandle(this, tableMgr->getDbConfig(), myOpt);
    EP( openFdbHandle(db_config, filename, writer) );

    // Bloom filter (LSM mode only).
    if ( db_config->bloomFilterBitsPerUnit > 0.0 &&
         !bfByKey ) {
        uint64_t bf_bitmap_size = myOpt.bloomFilterSize;
        if (!bf_bitmap_size) bf_bitmap_size = getBfSizeByLevel(db_config, level);
        bfByKey = new BloomFilter(bf_bitmap_size, 3);

        // Initial save.
        saveBloomFilter(filename + ".bf", bfByKey, true);
    }

    // Lookup booster.
    initBooster(level, db_config);

    // Initial commit.
    EP( writer->commit() );
    updateSnapshot();

    return Status();
}

Status TableFile::load(size_t level,
                       uint64_t table_number,
                       const std::string& f_name,
                       FileOps* f_ops,
                       const TableFileOptions& opt)
{
    if (writer) return Status::ALREADY_INITIALIZED;
    if (!f_ops->exist(f_name.c_str())) return Status::FILE_NOT_EXIST;

    Status s;
    filename = f_name;
    myNumber = table_number;
    fOps = f_ops;
    myOpt = opt;

    const DBConfig* db_config = tableMgr->getDbConfig();

    // Handle for writer.
    writer = new FdbHandle(this, tableMgr->getDbConfig(), myOpt);
    EP( openFdbHandle(db_config, filename, writer) );

    // Bloom filter (LSM mode only).
    if ( db_config->bloomFilterBitsPerUnit > 0.0 &&
         !bfByKey ) {
        std::string bf_filename = filename + ".bf";
        loadBloomFilter(bf_filename, bfByKey);
    }

    // Lookup booster.
    initBooster(level, db_config);

    // Pre-load snapshot.
    updateSnapshot();

    return Status();
}

Status TableFile::loadBloomFilter(const std::string& filename,
                                  BloomFilter*& bf_out)
{
    // Bloom filter file doesn't exist, just OK.
    if (!fOps->exist(filename)) {
        bf_out = nullptr;
        return Status::OK;
    }

    Status s;
    FileHandle* b_file = nullptr;
    EP( fOps->open(&b_file, filename.c_str()) );

   try {
    size_t file_size = fOps->eof(b_file);
    if (!file_size) throw Status();

    SizedBuf header( sizeof(uint32_t) * 2 );
    SizedBuf::Holder h_header(header);
    SizedBuf buf(file_size - header.size);

    TC( fOps->pread(b_file, header.data, header.size, 0) );
    RwSerializer ss(header);

    //   << Format >>
    // Version          4 bytes
    // Length (X)       4 bytes
    // Bitmap           X bytes
    uint32_t ver = ss.getU32(s);
    (void)ver;
    uint32_t data_size = ss.getU32();
    (void)data_size;
    assert(data_size == buf.size);

    TC( fOps->pread(b_file, buf.data, data_size, header.size) );
    bf_out = new BloomFilter(0, 3);
    // Memory region of `buf` will be moved to Bloom filter.
    bf_out->moveBitmapFrom(buf.data, buf.size);

    EP( fOps->close(b_file) );
    DELETE(b_file);
    return Status::OK;

   } catch (Status s) {
    EP( fOps->close(b_file) );
    DELETE(b_file);
    return Status::OK;
   }
}

Status TableFile::saveBloomFilter(const std::string& filename,
                                  BloomFilter* bf,
                                  bool call_fsync)
{
    if (filename.empty() || !bf || !bf->size()) return Status::OK;

    Status s;
    FileHandle* b_file = nullptr;
    EP( fOps->open(&b_file, filename.c_str()) );

   try {
    size_t data_size = bf->size() / 8;
    SizedBuf buf( sizeof(uint32_t) * 2);
    SizedBuf::Holder h_buf(buf);

    RwSerializer ss(buf);
    ss.putU32(0);
    ss.putU32(data_size);
    TC( fOps->pwrite(b_file, buf.data, buf.size, 0) );
    TC( fOps->pwrite(b_file, bf->getPtr(), data_size, buf.size) );
    if (call_fsync) fOps->fsync(b_file);

    EP( fOps->close(b_file) );
    DELETE(b_file);
    return Status::OK;

   } catch (Status s) {
    EP( fOps->close(b_file) );
    DELETE(b_file);
    return Status::OK;
   }
}

Status TableFile::changeOptions(const TableFileOptions& new_opt) {
    Status s;

    _log_info(myLog, "table %zu_%zu changed minBlockReuseFileSize %zu -> %zu",
              tableMgr->getTableMgrOptions()->prefixNum, myNumber,
              myOpt.minBlockReuseFileSize, new_opt.minBlockReuseFileSize);
    myOpt = new_opt;

    // Close and reopen to apply the new configuration.
    writer->close();
    writer->refreshSettings();
    EP( openFdbHandle(tableMgr->getDbConfig(), filename, writer) );

    return Status();
}

Status TableFile::openSnapshot(DB* snap_handle,
                               const uint64_t checkpoint)
{
    Status s;
    uint64_t snap_seqnum = 0;

    {   mGuard l(chkMapLock);
        auto entry = chkMap.find(checkpoint);
        if (entry == chkMap.end()) {
            // Exact match doesn't exist.
            auto e_max = chkMap.rbegin();
            if ( e_max == chkMap.rend() ||
                 checkpoint > e_max->second ) {
                // Beyond the table's checkpoint.
                // Take the latest marker.
                l.unlock();
                getLatestSnapMarker(snap_seqnum);

            } else {
                // Find greatest one smaller than chk.
                auto entry = chkMap.begin();
                while (entry != chkMap.end()) {
                    if (entry->first <= checkpoint) {
                        snap_seqnum = entry->second;
                    }
                    entry++;
                }
            }
        } else {
            // Exact match exists.
            snap_seqnum = entry->second;
        }
    }
    if (!snap_seqnum) return Status::INVALID_CHECKPOINT;

    FdbHandleGuard g(this, getIdleHandle());
    fdb_kvs_handle* kvs_db = g.handle->db;

    fdb_status fs;
    fdb_kvs_handle* fdbSnap;
    fs = fdb_snapshot_open(kvs_db, &fdbSnap, snap_seqnum);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

    {   mGuard l(snapHandlesLock);
        snapHandles.insert( std::make_pair(snap_handle, fdbSnap) );
    }
    return Status();
}

Status TableFile::closeSnapshot(DB* snap_handle) {
    Status s;
    fdb_kvs_handle* fdb_snap = nullptr;
    {   mGuard l(snapHandlesLock);
        auto entry = snapHandles.find(snap_handle);
        if (entry == snapHandles.end()) return Status::INVALID_SNAPSHOT;
        fdb_snap = entry->second;
        snapHandles.erase(entry);
    }

    fdb_status fs = fdb_kvs_close(fdb_snap);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_CLOSE_FAIL;
    return Status();
}

void TableFile::addCheckpoint(uint64_t chk, uint64_t commit_seqnum) {
    if (tableInfo) {
        _log_info(myLog, "file lv %zu num %zu hash %zu checkpoint %zu %zu",
                  tableInfo->level, tableInfo->number, tableInfo->hashNum,
                  chk, commit_seqnum);
    }
    mGuard l(chkMapLock);
    chkMap.insert( std::make_pair(chk, commit_seqnum) );
}

Status TableFile::setCheckpoint(Record* rec,
                                uint64_t prev_seqnum,
                                std::list<uint64_t>& checkpoints,
                                bool remaining_all)
{
    Status s;
    for (auto& chk_entry: checkpoints) {
        uint64_t chk = chk_entry;
        if ( prev_seqnum == chk ||
             (prev_seqnum < chk && rec && chk < rec->seqNum) ||
             (prev_seqnum <= chk && remaining_all) ) {
            // Commit for the checkpoint.
            fdb_seqnum_t commit_seqnum;
            fdb_get_kvs_seqnum(writer->db, &commit_seqnum);
            EP( writer->commit() );

            addCheckpoint(chk, commit_seqnum);
        }
    }
    return Status();
}

void TableFile::userMetaToRawMeta(const SizedBuf& user_meta,
                                  bool is_tombstone,
                                  SizedBuf& raw_meta_out)
{
    // Add 9 bytes in front:
    //   identifier (1 byte) + version (4 bytes) + flags (4 bytes).

    // NOTE: Even though `user_meta` is empty, we should put 9 bytes.
    if (!raw_meta_out.size) {
        raw_meta_out.alloc(user_meta.size + META_ADD_SIZE);
    }
    RwSerializer rw(raw_meta_out);

    // Put 0x1 as an identifier.
    rw.putU8(0x1);
    // Version 1.
    rw.putU32(0x0);

    // Flags.
    uint32_t flags = 0x0;
    if (is_tombstone) flags |= 0x1;
    rw.putU32(flags);

    // User meta.
    rw.put(user_meta.data, user_meta.size);
}

void TableFile::rawMetaToUserMeta(const SizedBuf& raw_meta,
                                  bool& is_tombstone_out,
                                  SizedBuf& user_meta_out)
{
    if (raw_meta.empty()) return;

    RwSerializer rw(raw_meta);

    // Check identifier.
    uint8_t identifier = rw.getU8();
    if (identifier != 0x1) {
        // No conversion.
        raw_meta.copyTo(user_meta_out);
        return;
    }

    // Version.
    uint32_t version = rw.getU32();
    (void)version; // TODO: version.

    // Flags.
    uint32_t flags = rw.getU32();
    if (flags & 0x1) is_tombstone_out = true;

    // User meta.
    if (raw_meta.size <= META_ADD_SIZE) {
        // Empty user meta.
        return;
    }

    user_meta_out.alloc(raw_meta.size - META_ADD_SIZE);
    rw.get(user_meta_out.data, user_meta_out.size);
}

Status TableFile::setSingle(uint32_t key_hash_val,
                            const Record& rec,
                            uint64_t& offset_out)
{
    fdb_doc doc;
    fdb_status fs;
    fdb_kvs_handle* kvs_db = writer->db;

    memset(&doc, 0x0, sizeof(doc));
    doc.key = rec.kv.key.data;
    doc.keylen = rec.kv.key.size;

    char tmp_buf[512];
    SizedBuf raw_meta_static(rec.meta.size + META_ADD_SIZE, tmp_buf);

    SizedBuf raw_meta_alloc;
    SizedBuf::Holder h_raw_meta(raw_meta_alloc);

    if (rec.meta.size < 500) {
        userMetaToRawMeta(rec.meta, rec.isDel(), raw_meta_static);
        doc.meta = raw_meta_static.data;
        doc.metalen = raw_meta_static.size;

    } else {
        userMetaToRawMeta(rec.meta, rec.isDel(), raw_meta_alloc);
        doc.meta = raw_meta_alloc.data;
        doc.metalen = raw_meta_alloc.size;
    }

    doc.body = rec.kv.value.data;
    doc.bodylen = rec.kv.value.size;
    doc.seqnum = rec.seqNum;
    doc.flags = FDB_CUSTOM_SEQNUM;

    fs = fdb_set(kvs_db, &doc);
    if (fs != FDB_RESULT_SUCCESS) {
        return Status::FDB_SET_FAIL;
    }

    offset_out = doc.offset;

    if (rec.isIns()) {
        // Set bloom filter if exists.
        if (bfByKey) {
            bfByKey->set(rec.kv.key.data, rec.kv.key.size);
        }
    }
    // Put into booster if exists.
    if (tlbByKey) {
        TableLookupBooster::Elem ee( key_hash_val, rec.seqNum, offset_out );
        tlbByKey->setIfNew(ee);
    }

    return Status();
}

Status TableFile::setBatch(std::list<Record*>& batch,
                           std::list<uint64_t>& checkpoints,
                           const SizedBuf& min_key,
                           const SizedBuf& min_key_next_table,
                           uint32_t target_hash,
                           bool bulk_load_mode)
{
    Timer tt;

    uint64_t prev_seqnum = 0;
    size_t num_l0 = tableMgr->getNumL0Partitions();
    size_t set_count = 0;
    size_t del_count = 0;

    for (auto& entry: batch) {
        Record* rec = entry;

        // If hash is given, check hash.
        uint32_t hash_val = getMurmurHash32(rec->kv.key);
        if (target_hash != _SCU32(-1)) {
            size_t key_hash = hash_val % num_l0;
            if (key_hash != target_hash) continue;
        }

        // If range is given, check range:
        //  [min_key, min_key_next_table)
        if ( !min_key.empty() &&
             rec->kv.key < min_key) continue;
        if ( !min_key_next_table.empty() &&
             rec->kv.key >= min_key_next_table ) continue;

        // Append all checkpoints that
        //  `record[n-1] seqnum <= chk < record[n] seqnum`
        setCheckpoint(rec, prev_seqnum, checkpoints);

        if (rec->isCmd()) continue;

        uint64_t offset_out = 0;
        Status s = setSingle(hash_val, *rec, offset_out);
        if (!s) return s;

        if (rec->isDel()) del_count++;
        else set_count++;

        prev_seqnum = rec->seqNum;
    }

    // Set all remaining (record[n] <= chk) checkpoints.
    setCheckpoint(nullptr, prev_seqnum, checkpoints, true);

    // Save bloom filter.
    // WARNING: Writing bloom filter SHOULD BE DONE BEFORE COMMIT.
    Timer tt_bf;
    if (bfByKey) saveBloomFilter(filename + ".bf", bfByKey, true);
    uint64_t bf_elapsed = tt_bf.getUs();

    if (!bulk_load_mode) {
        // Commit and update index node (not in bulk load mode).
        writer->commit();

        // Pre-load & keep the snapshot of latest table file data.
        updateSnapshot();
    }

    SimpleLogger::Levels ll = SimpleLogger::INFO;
    if (tableInfo) {
        if (tableInfo->level) {
            _log_( ll, myLog,
                   "L%zu: file %zu_%zu, set %zu del %zu, %zu us, %zu us",
                   tableInfo->level,
                   tableMgr->getTableMgrOptions()->prefixNum,
                   myNumber,
                   set_count, del_count, tt.getUs(), bf_elapsed );
        } else {
            _log_( ll, myLog,
                   "L%zu: hash %zu, file %zu_%zu, set %zu del %zu, %zu us, %zu us",
                   tableInfo->level,
                   tableInfo->hashNum,
                   tableMgr->getTableMgrOptions()->prefixNum,
                   myNumber,
                   set_count, del_count, tt.getUs(), bf_elapsed );
        }
    } else {
        _log_( ll, myLog,
               "brand new table: file %zu_%zu, set %zu del %zu, %zu us, %zu us",
               tableMgr->getTableMgrOptions()->prefixNum,
               myNumber,
               set_count, del_count, tt.getUs(), bf_elapsed );
    }

    // Bulk load mode: all done here.
    if (bulk_load_mode) return Status();

    {
        // Remove all checkpoints earlier than the oldest seqnum.
        uint64_t oldest_seq = 0;
        getOldestSnapMarker(oldest_seq);

        mGuard l(chkMapLock);
        auto entry = chkMap.begin();
        while (entry != chkMap.end()) {
            if ( entry->first < oldest_seq ||
                 entry->second < oldest_seq ) {
                if (tableInfo) {
                    _log_debug( myLog,
                                "file lv %zu num %zu hash %zu removed "
                                "checkpoint %zu %zu",
                                tableInfo->level, tableInfo->number,
                                tableInfo->hashNum,
                                entry->first, entry->second );
                }
                entry = chkMap.erase(entry);
            } else {
                entry++;
            }
        }
    }

    return Status();
}

Status TableFile::get(DB* snap_handle,
                      Record& rec_io,
                      bool meta_only)
{
    const DBConfig* db_config = tableMgr->getDbConfig();

    // Search bloom filter first if exists.
    if ( bfByKey &&
         db_config->useBloomFilterForGet &&
         !bfByKey->check(rec_io.kv.key.data, rec_io.kv.key.size) ) {
        return Status::KEY_NOT_FOUND;
    }

    fdb_status fs;
    fdb_doc doc_base;
    fdb_doc doc_by_offset;
    memset(&doc_base, 0x0, sizeof(doc_base));
    doc_base.key = rec_io.kv.key.data;
    doc_base.keylen = rec_io.kv.key.size;

    fdb_doc* doc = &doc_base;

    if (snap_handle) {
        // Snapshot (does not use booster).
        fdb_kvs_handle* kvs_db = nullptr;
        {   mGuard l(snapHandlesLock);
            auto entry = snapHandles.find(snap_handle);
            if (entry == snapHandles.end()) return Status::SNAPSHOT_NOT_FOUND;
            kvs_db = entry->second;
        }

        if (meta_only) {
            fs = fdb_get_metaonly(kvs_db, doc);
        } else {
            fs = fdb_get(kvs_db, doc);
        }

    } else {
        // Normal.
        FdbHandleGuard g(this, getIdleHandle());
        fdb_kvs_handle* kvs_db = g.handle->db;

        bool skip_normal_search = false;
        uint32_t key_hash = getMurmurHash32(rec_io.kv.key);
        IF ( !meta_only && tlbByKey ) {
            // Search booster if exists.
            memset(&doc_by_offset, 0x0, sizeof(doc_by_offset));
            Status s = tlbByKey->get( key_hash, doc_by_offset.offset );
            if (!s) break;

            fs = fdb_get_byoffset_raw(kvs_db, &doc_by_offset);
            if (fs != FDB_RESULT_SUCCESS) {
                break;
            }

            if ( rec_io.kv.key == SizedBuf( doc_by_offset.keylen,
                                            doc_by_offset.key ) ) {
                skip_normal_search = true;
                free(doc_by_offset.key);
                doc_by_offset.key = rec_io.kv.key.data;
                doc_by_offset.keylen = rec_io.kv.key.size;
                doc = &doc_by_offset;
            } else {
                free(doc_by_offset.key);
                free(doc_by_offset.meta);
                free(doc_by_offset.body);
            }
        }

        if (!skip_normal_search) {
            if (meta_only) {
                fs = fdb_get_metaonly(kvs_db, doc);
            } else {
                fs = fdb_get(kvs_db, doc);
                if ( fs == FDB_RESULT_SUCCESS && tlbByKey ) {
                    // Put into booster if exists.
                    tlbByKey->setIfNew( TableLookupBooster::Elem
                                        ( key_hash, doc->seqnum, doc->offset ) );
                }
            }
        }
    }
    if (fs != FDB_RESULT_SUCCESS) {
        return Status::KEY_NOT_FOUND;
    }

    rec_io.kv.value.set(doc->bodylen, doc->body);
    rec_io.kv.value.setNeedToFree();

    // Decode meta.
    SizedBuf user_meta_out;
    SizedBuf raw_meta(doc->metalen, doc->meta);;
    SizedBuf::Holder h_raw_meta(raw_meta); // auto free raw meta.
    raw_meta.setNeedToFree();
    bool is_tombstone_out = false;
    rawMetaToUserMeta(raw_meta, is_tombstone_out, user_meta_out);

    user_meta_out.moveTo( rec_io.meta );

    rec_io.seqNum = doc->seqnum;
    rec_io.type = (is_tombstone_out || doc->deleted)
                  ? Record::DELETION
                  : Record::INSERTION;

    return Status();
}

Status TableFile::getByOffset(DB* snap_handle,
                              uint64_t offset,
                              Record& rec_out)
{
    fdb_status fs;
    fdb_doc doc_by_offset;
    memset(&doc_by_offset, 0x0, sizeof(doc_by_offset));
    doc_by_offset.offset = offset;

    fdb_doc* doc = &doc_by_offset;

    if (snap_handle) {
        // Snapshot (does not use booster).
        fdb_kvs_handle* kvs_db = nullptr;
        {   mGuard l(snapHandlesLock);
            auto entry = snapHandles.find(snap_handle);
            if (entry == snapHandles.end()) return Status::SNAPSHOT_NOT_FOUND;
            kvs_db = entry->second;
        }

        fs = fdb_get_byoffset_raw(kvs_db, &doc_by_offset);

    } else {
        // Normal.
        FdbHandleGuard g(this, getIdleHandle());
        fdb_kvs_handle* kvs_db = g.handle->db;

        fs = fdb_get_byoffset_raw(kvs_db, &doc_by_offset);
    }
    if (fs != FDB_RESULT_SUCCESS) {
        return Status::INVALID_OFFSET;
    }

    rec_out.kv.key.set(doc->keylen, doc->key);
    rec_out.kv.key.setNeedToFree();

    rec_out.kv.value.set(doc->bodylen, doc->body);
    rec_out.kv.value.setNeedToFree();

    // Decode meta.
    SizedBuf user_meta_out;
    SizedBuf raw_meta(doc->metalen, doc->meta);;
    SizedBuf::Holder h_raw_meta(raw_meta); // auto free raw meta.
    raw_meta.setNeedToFree();
    bool is_tombstone_out = false;
    rawMetaToUserMeta(raw_meta, is_tombstone_out, user_meta_out);

    user_meta_out.moveTo( rec_out.meta );

    rec_out.seqNum = doc->seqnum;
    rec_out.type = (is_tombstone_out || doc->deleted)
                   ? Record::DELETION
                   : Record::INSERTION;

    return Status();
}

Status TableFile::appendCheckpoints(RwSerializer& file_s)
{
    mGuard l(chkMapLock);
    file_s.putU32(chkMap.size());
    for (auto& entry: chkMap) {
        uint64_t chk = entry.first;
        uint64_t fdb_seq = entry.second;
        file_s.putU64(chk);
        file_s.putU64(fdb_seq);
    }
    return Status();
}

Status TableFile::loadCheckpoints(RwSerializer& file_s)
{
    mGuard l(chkMapLock);
    Status s;
    uint32_t num_chks = file_s.getU32(s);
    for (size_t ii=0; ii<num_chks; ++ii) {
        uint64_t chk = file_s.getU64(s);
        uint64_t fdb_seq = file_s.getU64(s);
        chkMap.insert( std::make_pair(chk, fdb_seq) );
    }
    return Status();
}

Status TableFile::getAvailCheckpoints(std::list<uint64_t>& chk_out) {
    mGuard l(chkMapLock);
    for (auto& entry: chkMap) {
        uint64_t chk_num = entry.first;
        chk_out.push_back(chk_num);
    }
    return Status();
}

Status TableFile::getCheckpointSeqnum(uint64_t chk, uint64_t& seqnum_out) {
    mGuard l(chkMapLock);
    auto entry = chkMap.find(chk);
    if (entry != chkMap.end()) {
        seqnum_out = entry->second;
        return Status();
    }
    return Status::ERROR;
}

Status TableFile::destroySelf() {
    if (fOps->exist(filename.c_str())) {
        // Instead removing it immediately,
        // put it into remove list.
        DBMgr* dbm = DBMgr::getWithoutInit();
        std::string bf_filename = filename + ".bf";
        if (!dbm) {
            fOps->remove(filename.c_str());
            fOps->remove(bf_filename.c_str());
        } else {
            dbm->addFileToRemove(filename);
            dbm->addFileToRemove(bf_filename);
        }
    }
    return Status();
}

Status TableFile::getLatestSnapMarker(uint64_t& last_snap_seqnum) {
    FdbHandleGuard g(this, this->getIdleHandle());
    fdb_file_handle* db_file = g.handle->dbFile;

    // Get last snap marker.
    fdb_snapshot_info_t* markers = nullptr;
    uint64_t num_markers = 0;
    fdb_status fs = fdb_get_all_snap_markers(db_file, &markers, &num_markers);
    if (fs != FDB_RESULT_SUCCESS) return Status::ERROR;
    if (!markers || !num_markers) return Status::SNAPSHOT_NOT_FOUND;

    last_snap_seqnum = markers[0].kvs_markers[0].seqnum;
    fdb_free_snap_markers(markers, num_markers);
    return Status();
}

Status TableFile::getSnapMarkerUpto(uint64_t upto,
                                    uint64_t& snap_seqnum_out)
{
    FdbHandleGuard g(this, this->getIdleHandle());
    fdb_file_handle* db_file = g.handle->dbFile;

    // Get last snap marker.
    fdb_snapshot_info_t* markers = nullptr;
    uint64_t num_markers = 0;
    fdb_status fs = fdb_get_all_snap_markers(db_file, &markers, &num_markers);
    if (fs != FDB_RESULT_SUCCESS) return Status::ERROR;
    if (!markers || !num_markers) return Status::SNAPSHOT_NOT_FOUND;

    snap_seqnum_out = 0;
    for (size_t ii=0; ii<num_markers; ++ii) {
        if (upto >= markers[ii].kvs_markers[0].seqnum) {
            snap_seqnum_out = markers[ii].kvs_markers[0].seqnum;
            break;
        }
    }
    fdb_free_snap_markers(markers, num_markers);
    return Status();
}

Status TableFile::getOldestSnapMarker(uint64_t& oldest_snap_seqnum) {
    FdbHandleGuard g(this, this->getIdleHandle());
    fdb_file_handle* db_file = g.handle->dbFile;

    // Get first snap marker.
    fdb_snapshot_info_t* markers = nullptr;
    uint64_t num_markers = 0;
    fdb_status fs = fdb_get_all_snap_markers(db_file, &markers, &num_markers);
    if (fs != FDB_RESULT_SUCCESS) return Status::ERROR;
    if (!markers || !num_markers) return Status::SNAPSHOT_NOT_FOUND;

    oldest_snap_seqnum = markers[num_markers-1].kvs_markers[0].seqnum;
    fdb_free_snap_markers(markers, num_markers);
    return Status();
}

Status TableFile::getStats(TableStats& stats_out) {
    FdbHandleGuard g(this, this->getIdleHandle());
    fdb_file_handle* db_file = g.handle->dbFile;
    fdb_kvs_handle* kvs_db = g.handle->db;

    fdb_file_info info;
    fdb_status fs = fdb_get_file_info(db_file, &info);
    if (fs != FDB_RESULT_SUCCESS) return Status::ERROR;

    fdb_kvs_info kvs_info;
    fs = fdb_get_kvs_info(kvs_db, &kvs_info);

    stats_out.numKvs = info.doc_count;
    stats_out.workingSetSizeByte = info.space_used;
    stats_out.totalSizeByte = info.file_size;

    // This should be a bug.
    assert(stats_out.workingSetSizeByte < stats_out.totalSizeByte * 2);
    if (stats_out.workingSetSizeByte > stats_out.totalSizeByte * 2) {
        _log_fatal(myLog, "found wrong WSS, %s, %zu / %zu",
                   filename.c_str(),
                   stats_out.workingSetSizeByte,
                   stats_out.totalSizeByte);

        DBMgr* dbm = DBMgr::getWithoutInit();
        if (dbm) {
            _log_fatal(dbm->getLogger(),
                       "found wrong WSS, %s, %zu / %zu",
                       filename.c_str(),
                       stats_out.workingSetSizeByte,
                       stats_out.totalSizeByte);
        }
        // Make it small so as to compact quickly
        stats_out.workingSetSizeByte = stats_out.totalSizeByte / 10;
    }

    stats_out.blockReuseCycle = info.sb_bmp_revnum;

    stats_out.lastSeqnum = kvs_info.last_seqnum;
    stats_out.approxDocCount = kvs_info.doc_count;
    stats_out.approxDelCount = kvs_info.deleted_count;

    return Status();
}

Status TableFile::getMaxKey(SizedBuf& max_key_out) {
    Status s;
    TableFile::Iterator itr;
    EP( itr.init(nullptr, this, SizedBuf(), SizedBuf()) );

 try {
    TC( itr.gotoEnd() );

    Record rec_out;
    Record::Holder h_rec_out(rec_out);
    TC( itr.get(rec_out) );

    rec_out.kv.key.moveTo(max_key_out);
    return Status();

 } catch (Status s) {
    return s;
 }
}

Status TableFile::updateSnapshot() {
    fdb_seqnum_t snap_seqnum = 0;
    getLatestSnapMarker(snap_seqnum);

    FdbHandleGuard g(this, getIdleHandle());
    fdb_kvs_handle* kvs_db = g.handle->db;
    fdb_kvs_handle* snap_handle = nullptr;
    fdb_status fs = fdb_snapshot_open(kvs_db, &snap_handle, snap_seqnum);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

    Snapshot* new_snp = new Snapshot(this, snap_handle, snap_seqnum);

    std::list<Snapshot*> stale_snps;
    {   std::lock_guard<std::mutex> l(latestSnapshotLock);
        auto entry = latestSnapshot.begin();

        // Decrease the reference count of the previously latest one.
        if (entry != latestSnapshot.end()) {
            Snapshot*& latest_snp = *entry;
            latest_snp->refCount--;
        }

        while (entry != latestSnapshot.end()) {
            Snapshot*& cur_snp = *entry;
            if (!cur_snp->refCount) {
                stale_snps.push_back(cur_snp);
                entry = latestSnapshot.erase(entry);
            } else {
                entry++;
            }
        }
        latestSnapshot.push_front(new_snp);
    }

    // Close all stale snapshots (refCount == 0).
    for (Snapshot*& cur_snp: stale_snps) {
        _log_trace(myLog, "delete snapshot %p refcount %zu",
                   cur_snp, cur_snp->refCount);
        fdb_kvs_close(cur_snp->fdbSnap);
        delete cur_snp;
    }
    return Status();
}

Status TableFile::leaseSnapshot(TableFile::Snapshot*& snp_out) {
    std::lock_guard<std::mutex> l(latestSnapshotLock);
    auto entry = latestSnapshot.begin();
    assert(entry != latestSnapshot.end());
    Snapshot* snp = *entry;
    snp->refCount++;
    _log_trace(myLog, "lease snapshot %p refcount %zu",
               snp, snp->refCount);
    snp_out = snp;

    return Status();
}

Status TableFile::returnSnapshot(TableFile::Snapshot* snapshot) {
    std::list<Snapshot*> stale_snps;
    {   std::lock_guard<std::mutex> l(latestSnapshotLock);
        snapshot->refCount--;
        _log_trace(myLog, "return snapshot %p refcount %zu",
                   snapshot, snapshot->refCount);
        auto entry = latestSnapshot.begin();
        while (entry != latestSnapshot.end()) {
            Snapshot*& cur_snp = *entry;
            if (!cur_snp->refCount) {
                stale_snps.push_back(cur_snp);
                entry = latestSnapshot.erase(entry);
            } else {
                entry++;
            }
        }
    }

    // Close all stale snapshots (refCount == 0).
    for (Snapshot*& cur_snp: stale_snps) {
        _log_trace(myLog, "delete snapshot %p refcount %zu",
                   cur_snp, cur_snp->refCount);
        fdb_kvs_close(cur_snp->fdbSnap);
        delete cur_snp;
    }
    return Status();
}

} // namespace jungle

