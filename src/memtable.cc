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

#include "memtable.h"

#include "bloomfilter.h"
#include "crc32.h"
#include "db_mgr.h"
#include "internal_helper.h"
#include "log_file.h"
#include "log_mgr.h"
#include "skiplist.h"

#include "generic_bitmap.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <atomic>
#include <list>
#include <mutex>

namespace jungle {

MemTable::RecNode::RecNode(const SizedBuf& _key) {
    skiplist_init_node(&snode);
    recList = new RecList();
    key.alloc(_key);
    freeKey = true;
}

MemTable::RecNode::RecNode(const SizedBuf* _key)
    : recList(nullptr) {
    // Just keep the pointer (for query purpose).
    skiplist_init_node(&snode);
    key.referTo(*_key);
    freeKey = false;
}

MemTable::RecNode::~RecNode() {
    {   mGuard l(recListLock);
        DELETE(recList);
    }
    if (freeKey) key.free();
    skiplist_free_node(&snode);
}


int MemTable::RecNode::cmp(skiplist_node *a, skiplist_node *b, void *aux) {
    RecNode *aa, *bb;
    aa = _get_entry(a, RecNode, snode);
    bb = _get_entry(b, RecNode, snode);

    return SizedBuf::cmp(aa->key, bb->key);
}

Record* MemTable::RecNode::getLatestRecord( const uint64_t seq_from,
                                            const uint64_t seq_upto )
{
    mGuard l(recListLock);
    Record* rec = nullptr;
    auto entry = recList->rbegin();
    while (entry != recList->rend()) {
        Record* tmp = *entry;
        if (!valid_number(seq_upto) || tmp->seqNum <= seq_upto) {
            if (!valid_number(seq_from) || tmp->seqNum >= seq_from) {
                rec = tmp;
            }
            break;
        }
        entry++;
    }
    return rec;
}

std::list<Record*> MemTable::RecNode::discardRecords(uint64_t seq_begin) {
    mGuard l(recListLock);
    std::list<Record*> ret;

    auto entry = recList->begin();
    while (entry != recList->end()) {
        Record* cur = *entry;
        if (cur->seqNum >= seq_begin) {
            ret.push_back(cur);
            entry = recList->erase(entry);
        } else {
            entry++;
        }
    }
    return ret;
}

uint64_t MemTable::RecNode::getMinSeq() {
    mGuard l(recListLock);
    auto entry = recList->begin();
    if (entry == recList->end()) {
        // Shouldn't happen.
        assert(0);
    }
    Record* rec = *entry;
    return rec->seqNum;
}

bool MemTable::RecNode::validKeyExist( const uint64_t seq_from,
                                       const uint64_t seq_upto,
                                       bool allow_tombstone )
{
    bool valid_key_exist = true;

    if (getMinSeq() > seq_upto) {
        // No record belongs to the current snapshot (iterator),
        valid_key_exist = false;
    }

    if (valid_key_exist) {
        Record* rec = getLatestRecord(seq_from, seq_upto);
        if (!rec) {
            // All records in the list may have
            // higher sequence number than given `chk`.
            // In such a case, `rec` can be `nullptr`.
            valid_key_exist = false;
        } else if ( !allow_tombstone &&
                    !rec->isIns() ) {
            // Record exists, but latest one is not an insert,
            // and doesn't allow tombstone.
            valid_key_exist = false;
        }
    }
    return valid_key_exist;
}

MemTable::RecNodeSeq::RecNodeSeq(Record* _rec)
    : rec(_rec)
{
    skiplist_init_node(&snode);
}

MemTable::RecNodeSeq::~RecNodeSeq() {
    skiplist_free_node(&snode);
}

int MemTable::RecNodeSeq::cmp(skiplist_node *a, skiplist_node *b, void *aux) {
    RecNodeSeq *aa, *bb;
    aa = _get_entry(a, RecNodeSeq , snode);
    bb = _get_entry(b, RecNodeSeq , snode);

    if (aa->rec->seqNum < bb->rec->seqNum) return -1;
    if (aa->rec->seqNum > bb->rec->seqNum) return 1;
    return 0;
}

uint64_t record_flags(Record* rec) {
    // 00 00 00 00 00 00 00 00: normal doc (insertion)
    // 00 00 00 00 00 00 00 01: deletion marker
    // 00 00 00 00 00 00 00 02: other commands
    uint64_t ret = 0x0;
    switch (rec->type) {
    case Record::Type::INSERTION:
        ret = 0x0;
        break;
    case Record::Type::DELETION:
        ret = 0x1;
        break;
    case Record::Type::COMMAND:
        ret = 0x2;
        break;
    }
    return ret;
}

Record::Type get_record_type_from_flags(uint64_t flags) {
    return static_cast<Record::Type>(flags & 0x0f);
}

const uint64_t FLAG_COMPRESSED = 0x10;
void record_flags_set_compressed(uint64_t& flags) {
    flags |= FLAG_COMPRESSED;
}
bool record_flags_is_compressed(const uint64_t flags) {
    return flags & FLAG_COMPRESSED;
}

uint64_t flush_marker_flags() {
    // 01 00 00 00 00 00 00 00: flush marker
    uint64_t ret = 0x01;
    ret = ret << 56; // 7-byte shift
    return ret;
}

uint64_t checkpoint_flags() {
    // 02 00 00 00 00 00 00 00: checkpoint marker
    uint64_t ret = 0x02;
    ret = ret << 56; // 7-byte shift
    return ret;
}

enum FlagType {
    // 00 00 00 00 00 00 00 00: RECORD
    // 01 00 00 00 00 00 00 00: FLUSH_MARKER
    // 02 00 00 00 00 00 00 00: CHECKPOINT
    // fe 00 00 00 00 00 00 00: PADDING
    RECORD          = 0,
    FLUSH_MARKER    = 1,
    CHECKPOINT      = 2,
    // Indicate that the following bytes are just for alignment
    PADDING         = 254,
    UNKNOWN         = 255
};

FlagType identify_type(uint64_t flags) {
    if (flags == PADDING_HEADER_FLAG) return FlagType::PADDING;
    flags = flags >> (8 * 7);
    if      (flags == 0x0 ) return FlagType::RECORD;
    else if (flags == 0x01) return FlagType::FLUSH_MARKER;
    else if (flags == 0x02) return FlagType::CHECKPOINT;
    else if (flags == 0xfe) return FlagType::PADDING;

    return FlagType::UNKNOWN;
}

MemTable::MemTable(const LogFile* log_file)
    : startSeqNum(0)
    , minSeqNum(NOT_INITIALIZED)
    , flushedSeqNum(NOT_INITIALIZED)
    , syncedSeqNum(NOT_INITIALIZED)
    , maxSeqNum(NOT_INITIALIZED)
    , seqNumAlloc(NOT_INITIALIZED)
    , bytesSize(0)
    , checkpointsDirty(false)
    , logFile(log_file)
    , lastInsertedRec(nullptr)
    , increasingOrder(false)
    , myLog(nullptr)
{
    idxByKey = new skiplist_raw();
    skiplist_init(idxByKey, MemTable::cmpKey);

    idxBySeq = new skiplist_raw();
    skiplist_init(idxBySeq, RecNodeSeq::cmp);

    // 1M bits (128KB) for 16384 entries.
    uint64_t bf_bitmap_size =
        logFile->logMgr->getDbConfig()->maxEntriesInLogFile * 64;
    bfByKey = new BloomFilter(bf_bitmap_size, 3);
}

MemTable::~MemTable() {
    if (idxByKey) {
        // Note: due to dedup,
        //       # logs in skiplist <= # total logs.
        skiplist_node* cursor = skiplist_begin(idxByKey);
        while (cursor) {
            RecNode* node = _get_entry(cursor, RecNode, snode);
            cursor = skiplist_next(idxByKey, cursor);

            skiplist_erase_node(idxByKey, &node->snode);
            skiplist_release_node(&node->snode);
            skiplist_wait_for_free(&node->snode);
            delete node;
        }
        skiplist_free(idxByKey);
        delete idxByKey;

        {   mGuard l(staleLogsLock);
            for (auto& entry: staleLogs) {
                Record* rec = entry;
                rec->free();
                delete rec;
            }
        }
    }

    if (idxBySeq) {
        skiplist_node* cursor = skiplist_begin(idxBySeq);
        while (cursor) {
            RecNodeSeq* node = _get_entry(cursor, RecNodeSeq, snode);
            cursor = skiplist_next(idxBySeq, cursor);

            skiplist_erase_node(idxBySeq, &node->snode);
            skiplist_release_node(&node->snode);
            skiplist_wait_for_free(&node->snode);
            node->rec->free();
            delete node->rec;
            delete node;
        }
        skiplist_free(idxBySeq);
        delete idxBySeq;
    }

    if (bfByKey) {
        delete bfByKey;
        bfByKey = nullptr;
    }
}

int MemTable::cmpKey(skiplist_node *a, skiplist_node *b, void *aux) {
    RecNode *aa, *bb;
    aa = _get_entry(a, RecNode, snode);
    bb = _get_entry(b, RecNode, snode);

    CMP_NULL_CHK(aa->key.data, bb->key.data);

    if (aux) {
        // Custom cmp
        MemTable* mt = reinterpret_cast<MemTable*>(aux);
        CustomCmpFunc func = mt->logFile->logMgr->getDbConfig()->cmpFunc;
        void* param = mt->logFile->logMgr->getDbConfig()->cmpFuncParam;
        return func(aa->key.data, aa->key.size,
                    bb->key.data, bb->key.size,
                    param);
    }

    return SizedBuf::cmp(aa->key, bb->key);
}

Status MemTable::getReady() {
    // Set custom cmp function if given.
    if (logFile->logMgr->getDbConfig()->cmpFunc) {
        skiplist_raw_config s_config = skiplist_get_config(idxByKey);
        s_config.aux = (void*)this;
        skiplist_set_config(idxByKey, s_config);
    }
    return Status();
}

Status MemTable::init(const uint64_t start_seq_num) {
    getReady();

    startSeqNum = start_seq_num;
    return Status();
}

Status MemTable::assignSeqNum(Record& rec_local) {
    if (rec_local.seqNum == NOT_INITIALIZED) {
        // Seqnum is not given.
        uint64_t expected = NOT_INITIALIZED;
        uint64_t val = startSeqNum;

        // NOTE: seqnum should start from 1.
        if (!val) val = 1;
        if (seqNumAlloc.compare_exchange_weak(expected, val)) {
            // First insert.
            minSeqNum = val;
            rec_local.seqNum = val;
        } else {
            // NOTE: `memory_order_seq_cst` will be thread-safe.
            uint64_t seq = seqNumAlloc.fetch_add(1, MOSC) + 1;
            rec_local.seqNum = seq;
        }

    } else {
        // Seqnum is given by user.
        uint64_t expected = seqNumAlloc;
        uint64_t val = rec_local.seqNum;
        if (expected != NOT_INITIALIZED &&
            rec_local.seqNum <= expected &&
            !logFile->logMgr->getDbConfig()->allowOverwriteSeqNum) {
            // Overwrite is not allowed, fail.
            return Status::INVALID_SEQNUM;
        }

        if (minSeqNum != NOT_INITIALIZED &&
            rec_local.seqNum < minSeqNum) {
            // Smaller than the min seq num.
            return Status::INVALID_SEQNUM;
        }

        if (seqNumAlloc.compare_exchange_weak(expected, val)) {
            if (expected == NOT_INITIALIZED) {
                // First insert.
                minSeqNum = rec_local.seqNum;
            }
        } else {
            // Other thread interfered.
            if (!logFile->logMgr->getDbConfig()->allowOverwriteSeqNum) {
                // Overwrite is not allowed.
                return Status::INVALID_SEQNUM;
            }
        }
    }
    return Status();
}

Status MemTable::updateMaxSeqNum(const uint64_t seq_num) {
    uint64_t expected = maxSeqNum;
    uint64_t val = seq_num;
    if (expected < seq_num || expected == NOT_INITIALIZED) {
        maxSeqNum.compare_exchange_weak(expected, val);
    } // Otherwise, ignore it.
    return Status();
}

MemTable::RecNode* MemTable::findRecNode(Record* rec) {
    RecNode query(&rec->kv.key);
    skiplist_node* cursor = skiplist_find(idxByKey, &query.snode);
    RecNode* existing = _get_entry(cursor, RecNode, snode);
    return existing;
}

void MemTable::addToByKeyIndex(Record* rec) {
    RecNode* existing_node = nullptr;

    // Try non-duplicate insert first.

    // Create a RecNode as a wrapper
    // both for insertion and deletion (tombstone).
    RecNode* rec_node = new RecNode(rec->kv.key);
    {
        mGuard l(rec_node->recListLock);
        rec_node->recList->push_back(rec);
    }

    int ret = skiplist_insert_nodup(idxByKey, &rec_node->snode);
    if (ret == 0) {
        uint64_t hash_pair[2];
        get_hash_pair(logFile->logMgr->getDbConfig(), rec->kv.key, false, hash_pair);
        bfByKey->set(hash_pair);
        return;
    } else {
        // Already exist, re-find.
        delete rec_node;
        existing_node = findRecNode(rec);
        assert(existing_node);
    }

    // Existing key RecNode: just push back.
    mGuard l(existing_node->recListLock);
    existing_node->recList->push_back(rec);
    skiplist_release_node(&existing_node->snode);
}

Status MemTable::addToBySeqIndex(Record* rec, Record*& prev_rec_out) {
    if ( valid_number(rec->seqNum) && rec->seqNum <= maxSeqNum ) {
        // If the seqnum of `rec` is smaller than the current max,
        // we should search existing one.
        RecNodeSeq query(rec);
        skiplist_node* cursor = skiplist_find(idxBySeq, &query.snode);
        if (cursor) {
            RecNodeSeq* prev = _get_entry(cursor, RecNodeSeq, snode);
            prev_rec_out = prev->rec;
            // If already exist, replace and then return the old one.
            prev->rec = rec;
            skiplist_release_node(&prev->snode);
            return Status();
        }
    }

    // TODO: multi-thread update on the same key?
    RecNodeSeq* new_rec_node = new RecNodeSeq(rec);
    skiplist_insert(idxBySeq, &new_rec_node->snode);
    prev_rec_out = nullptr;

    return Status();
}

Status MemTable::putNewRecord(const Record& _rec) {
    Status s;
    Record rec_local = _rec;
    // If deletion, clear value.
    if (rec_local.isDel()) {
        rec_local.kv.value = SizedBuf();
    }

    s = assignSeqNum(rec_local);
    if (!s) return s;

    // Make a clone
    Record* rec = new Record();
    s = rec->clone(rec_local);
    if (!s) return s;

    // Append into by-seq index
    Record* prev_rec = nullptr;
    addToBySeqIndex(rec, prev_rec);

    if (prev_rec) {
        mGuard l(staleLogsLock);
        staleLogs.push_back(prev_rec);
    }

    // If this is a special command (not ins or del),
    // skip by-key index update.
    if (!rec->isCmd()) {
        addToByKeyIndex(rec);
    }

    if (!lastInsertedRec) {
        increasingOrder = true;
    } else {
        if ( increasingOrder &&
             lastInsertedRec->kv.key > rec->kv.key ) {
            increasingOrder = false;
        }
    }
    lastInsertedRec = rec;

    updateMaxSeqNum(rec_local.seqNum);

    bytesSize += rec->size();
    return Status();
}

Status MemTable::discardDirty(uint64_t seq_begin,
                              bool rollback_seq_counter)
{
    // WARNING:
    //   * Not MT-safe if there are multiple writers.
    //   * MT-safe against other readers.
    if (!idxBySeq || !idxByKey) {
        // skiplists are must.
        return Status::NOT_INITIALIZED;
    }

    Record query_rec;
    RecNodeSeq query(&query_rec);
    query.rec->seqNum = seq_begin;
    skiplist_node* cursor = skiplist_find_greater_or_equal(idxBySeq, &query.snode);
    while (cursor) {
        RecNodeSeq* rec_seq = _get_entry(cursor, RecNodeSeq, snode);

        // Find by key (there can be multiple items).
        RecNode key_query(&rec_seq->rec->kv.key);
        skiplist_node* key_cursor = skiplist_find(idxByKey, &key_query.snode);
        if (key_cursor) {
            RecNode* rec_key = _get_entry(key_cursor, RecNode, snode);
            std::list<Record*> discarded = rec_key->discardRecords(seq_begin);
            skiplist_release_node(key_cursor);

            // NOTE:
            //   Due to the race with reader, we cannot delete the discarded
            //   records now, hence push them to `staleLogs`.
            {   mGuard l(staleLogsLock);
                for (auto& entry: discarded) {
                    staleLogs.push_back(entry);
                }
            }
        }

        cursor = skiplist_next(idxBySeq, &rec_seq->snode);
        skiplist_erase_node(idxBySeq, &rec_seq->snode);
        skiplist_release_node(&rec_seq->snode);
        skiplist_wait_for_free(&rec_seq->snode);
        delete rec_seq;
    }
    if (cursor) {
        skiplist_release_node(cursor);
    }

    return Status();
}

Status MemTable::findRecordBySeq(const uint64_t seq_num,
                                 Record& rec_out)
{
    if ( flushedSeqNum != NOT_INITIALIZED &&
         seq_num <= flushedSeqNum )
        return Status::ALREADY_PURGED;

    Record query_rec;
    RecNodeSeq query(&query_rec);
    query.rec->seqNum = seq_num;

    skiplist_node* cursor = skiplist_find(idxBySeq, &query.snode);
    if (cursor) {
        RecNodeSeq* rec_node = _get_entry(cursor, RecNodeSeq, snode);
        rec_out = *rec_node->rec;
        skiplist_release_node(cursor);
        return Status();
    }

    return Status::SEQNUM_NOT_FOUND;
}

Status MemTable::getRecordByKey(const uint64_t chk,
                                const SizedBuf& key,
                                Record& rec_out,
                                bool allow_flushed_log,
                                bool allow_tombstone)
{
    // Check bloom filter first for fast screening.
    uint64_t hash_pair[2];
    get_hash_pair(logFile->logMgr->getDbConfig(), key, false, hash_pair);
    if (!bfByKey->check(hash_pair)) {
        return Status::KEY_NOT_FOUND;
    }

    return getRecordByKeyInternal( chk, key, rec_out,
                                   allow_flushed_log, allow_tombstone,
                                   SearchOptions::EQUAL );
}

Status MemTable::getNearestRecordByKey(const uint64_t chk,
                                       const SizedBuf& key,
                                       Record& rec_out,
                                       bool allow_flushed_log,
                                       bool allow_tombstone,
                                       SearchOptions s_opt)
{
    // NOTE: We can't use bloom filter
    //       as we should allow non-exact match.
    return getRecordByKeyInternal( chk, key, rec_out,
                                   allow_flushed_log, allow_tombstone,
                                   s_opt );
}

Status MemTable::getRecordByKeyInternal(const uint64_t chk,
                                        const SizedBuf& key,
                                        Record& rec_out,
                                        bool allow_flushed_log,
                                        bool allow_tombstone,
                                        SearchOptions s_opt)
{
    RecNode query(&key);
    skiplist_node* cursor = nullptr;
    switch (s_opt) {
    default:
    case SearchOptions::EQUAL:
        cursor = skiplist_find(idxByKey, &query.snode);
        break;

    case SearchOptions::GREATER:
    case SearchOptions::GREATER_OR_EQUAL:
        cursor = skiplist_find_greater_or_equal(idxByKey, &query.snode);
        break;

    case SearchOptions::SMALLER:
    case SearchOptions::SMALLER_OR_EQUAL:
        cursor = skiplist_find_smaller_or_equal(idxByKey, &query.snode);
        break;
    }
    if (!cursor) return Status::KEY_NOT_FOUND;

    RecNode* node = _get_entry(cursor, RecNode, snode);
    if (!s_opt.isExactMatchAllowed()) {
        // Exact match is not allowed. Move the cursor if the current
        // cursor is pointing to the same key.
        while (SizedBuf::cmp(key, node->key) == 0) {
            skiplist_node* prev_cursor = cursor;
            if (s_opt == SearchOptions::GREATER) {
                cursor = skiplist_next(idxByKey, prev_cursor);
            } else {
                cursor = skiplist_prev(idxByKey, prev_cursor);
            }
            skiplist_release_node(prev_cursor);
            if (!cursor) return Status::KEY_NOT_FOUND;

            node = _get_entry(cursor, RecNode, snode);
        }
    }

    Record* rec_ret = node->getLatestRecord(NOT_INITIALIZED, chk);
    if (!rec_ret) {
        skiplist_release_node(&node->snode);
        return Status::KEY_NOT_FOUND;
    }
    if ( valid_number(flushedSeqNum) &&
         rec_ret->seqNum <= flushedSeqNum ) {
        // Already purged KV pair, go to table.
        if (!allow_flushed_log) {
            skiplist_release_node(&node->snode);
            return Status::KEY_NOT_FOUND;
        } // Tolerate if this is snapshot.
    }

    if ( !allow_tombstone && rec_ret->isDel() ) {
        // Last operation is deletion.
        skiplist_release_node(&node->snode);
        return Status::KEY_NOT_FOUND;
    }
    rec_out = *rec_ret;
    skiplist_release_node(&node->snode);
    return Status();
}

Status MemTable::getRecordsByPrefix(const uint64_t chk,
                                    const SizedBuf& prefix,
                                    SearchCbFunc cb_func,
                                    bool allow_flushed_log,
                                    bool allow_tombstone)
{
    uint64_t hash_pair[2];
    bool used_custom_hash = get_hash_pair( logFile->logMgr->getDbConfig(),
                                           prefix, true, hash_pair );
    if (used_custom_hash) {
        // Unlike point get, if a custom hash is not given,
        // we can't use bloom filter.
        if (!bfByKey->check(hash_pair)) {
            return Status::KEY_NOT_FOUND;
        }
    }

    RecNode query(&prefix);
    skiplist_node* cursor = nullptr;
    cursor = skiplist_find_greater_or_equal(idxByKey, &query.snode);
    if (!cursor) return Status::KEY_NOT_FOUND;

    RecNode* node = _get_entry(cursor, RecNode, snode);
    if (!node->recList) {
        skiplist_release_node(&node->snode);
        return Status::KEY_NOT_FOUND;
    }

    auto is_prefix_match = [&](const SizedBuf& key) -> bool {
        if (key.size < prefix.size) return false;
        return (SizedBuf::cmp( SizedBuf(prefix.size, key.data),
                               prefix ) == 0);
    };

    while ( is_prefix_match(node->key) ) {
        Record* rec_ret = node->getLatestRecord(NOT_INITIALIZED, chk);
        if (!rec_ret) {
            skiplist_release_node(&node->snode);
            return Status::KEY_NOT_FOUND;
        }

        bool found = true;
        // Dummy loop to use `break`.
        do {
            if ( valid_number(flushedSeqNum) &&
                 rec_ret->seqNum <= flushedSeqNum ) {
                // Already purged KV pair, go to the next matching key.
                if (!allow_flushed_log) {
                    found = false;
                    break;
                } // Tolerate if this is snapshot.
            }

            if ( !allow_tombstone && rec_ret->isDel() ) {
                // Last operation is deletion, go to the next matching key.
                found = false;
                break;
            }
        } while (false);

        if (found) {
            SearchCbDecision dec = cb_func({*rec_ret});
            if (dec == SearchCbDecision::STOP) {
                skiplist_release_node(&node->snode);
                return Status::OPERATION_STOPPED;
            }
        }

        cursor = skiplist_next(idxByKey, &node->snode);
        skiplist_release_node(&node->snode);

        if (!cursor) break;
        node = _get_entry(cursor, RecNode, snode);
    }

    if (cursor) {
        skiplist_release_node(cursor);
    }
    return Status();
}

Status MemTable::sync(FileOps* f_ops,
                      FileHandle* fh)
{
    Status s;
    s = f_ops->fsync(fh);
    if (!s) return s;

    return Status();
}

size_t MemTable::getLengthMetaSize(uint64_t flags) {
    if (record_flags_is_compressed(flags)) {
        return sizeof(uint64_t) +       // seq number.
               sizeof(uint32_t) * 4;    // K+M+V+O lengths.
    } else {
        return sizeof(uint64_t) +       // seq number.
               sizeof(uint32_t) * 3;    // K+M+V lengths.
    }
}

Status MemTable::loadRecord(RwSerializer& rws,
                            uint64_t flags,
                            uint64_t& seqnum_out)
{
    Status s;
    Record* rec = new Record();
    rec->type = get_record_type_from_flags(flags);

    const bool COMPRESSION = record_flags_is_compressed(flags);
    const DBConfig* db_config = logFile->logMgr->getDbConfig();
    DB* parent_db = logFile->logMgr->getParentDb();

    const size_t LOCAL_COMP_BUF_SIZE = 4096;
    char local_comp_buf[LOCAL_COMP_BUF_SIZE];

    const size_t LEN_META_SIZE = getLengthMetaSize(flags);
    uint64_t rec_offset = rws.pos();

  try{
    uint32_t crc_len = 0;
    uint8_t len_buf[32];
    // CRC of length meta + length meta + CRC of KMV.
    if (!rws.available(4 + LEN_META_SIZE + 4)) {
        throw Status(Status::INCOMPLETE_LOG);
    }

    TC_( crc_len = rws.getU32(s) );
    TC( rws.get(len_buf, LEN_META_SIZE) );

    uint32_t crc_len_chk = crc32_8(len_buf, LEN_META_SIZE, 0);
    if (crc_len != crc_len_chk) {
        _log_err(myLog, "crc error %x != %x, at %zu",
                 crc_len, crc_len_chk, rws.pos() - sizeof(crc_len));
        throw Status(Status::CHECKSUM_ERROR);
    }

    RwSerializer len_buf_rw(len_buf, 32);
    rec->seqNum = len_buf_rw.getU64(s);
    seqnum_out = rec->seqNum;

    uint32_t k_size = len_buf_rw.getU32(s);
    uint32_t m_size = len_buf_rw.getU32(s);
    uint32_t v_size = len_buf_rw.getU32(s);
    uint32_t o_size = v_size;
    if (COMPRESSION) {
        o_size = len_buf_rw.getU32(s);
    }

    uint32_t crc_data = 0;
    TC_( crc_data = rws.getU32(s) );

    uint32_t crc_data_chk = 0;
    if (k_size) {
        if (!rws.available(k_size)) {
            throw Status(Status::INCOMPLETE_LOG);
        }
        rec->kv.key.alloc(k_size, nullptr);
        TC( rws.get(rec->kv.key.data, k_size) );
        crc_data_chk = crc32_8(rec->kv.key.data, k_size, crc_data_chk);
    }
    if (m_size) {
        if (!rws.available(m_size)) {
            throw Status(Status::INCOMPLETE_LOG);
        }
        rec->meta.alloc(m_size, nullptr);
        TC( rws.get(rec->meta.data, m_size) );
        crc_data_chk = crc32_8(rec->meta.data, m_size, crc_data_chk);
    }
    if (v_size) {
        if (!rws.available(v_size)) {
            throw Status(Status::INCOMPLETE_LOG);
        }

        // If not compressed, `o_size == v_size`.
        rec->kv.value.alloc(o_size);

        if (COMPRESSION) {
            if (!db_config->compOpt.cbDecompress) {
                _log_fatal(myLog, "found compressed record %s, but decompression "
                           "function is not given",
                           rec->kv.key.toReadableString().c_str());
                throw Status(Status::INVALID_CONFIG);
            }

            SizedBuf comp_buf(v_size, local_comp_buf);
            SizedBuf::Holder h_comp_buf(comp_buf);

            if (v_size > LOCAL_COMP_BUF_SIZE) {
                // Size is bigger than local buffer, allocate from heap.
                comp_buf.alloc(v_size);
            }
            TC( rws.get(comp_buf.data, v_size) );
            ssize_t output_len =
                db_config->compOpt.cbDecompress(parent_db, comp_buf, rec->kv.value);
            if (output_len != o_size) {
                _log_fatal(myLog, "decompression failed: %zd, db %s, "
                           "key %s, offset %zu",
                           output_len,
                           parent_db->getPath().c_str(),
                           rec->kv.key.toReadableString().c_str(),
                           rws.pos() - v_size);
                throw Status(Status::DECOMPRESSION_FAILED);
            }
            crc_data_chk = crc32_8(comp_buf.data, v_size, crc_data_chk);

        } else {
            TC( rws.get(rec->kv.value.data, v_size) );
            crc_data_chk = crc32_8(rec->kv.value.data, v_size, crc_data_chk);
        }
    }

    if (crc_data != crc_data_chk) {
        _log_err(myLog, "crc error %x (expected) != %x (actual), "
                 "in the record starting at offset 0x%lx",
                 crc_data, crc_data_chk, rec_offset - 8);
        throw Status(Status::CHECKSUM_ERROR);
    }

    Record* prev_rec = nullptr;
    addToBySeqIndex(rec, prev_rec);
    if (prev_rec) {
        mGuard l(staleLogsLock);
        staleLogs.push_back(prev_rec);
    }

    bytesSize += rec->size();

    if (rec->isCmd()) {
        // If this is a special command (not ins or del),
        // skip by-key index update.
        return Status();
    }
    addToByKeyIndex(rec);

    return Status();

  } catch (Status s) {
    rec->free();
    delete rec;
    return s;
  }
}

Status MemTable::loadFlushMarker(RwSerializer& rws,
                                 uint64_t& synced_seq_num_out)
{
    if (!rws.available(8)) {
        return Status::INCOMPLETE_LOG;
    }

    Status s;
    EP_( synced_seq_num_out = rws.getU64(s) );
    return Status();
}

Status MemTable::loadCheckpoint(RwSerializer& rws)
{
    if (!rws.available(8)) {
        return Status::INCOMPLETE_LOG;
    }

    Status s;
    uint64_t chk_seqnum = 0;
    EP_( chk_seqnum = rws.getU64(s) );

    mGuard l(checkpointsLock);
    auto entry = checkpoints.rbegin();
    if ( entry != checkpoints.rend() &&
         *entry == chk_seqnum ) {
        // Same checkpoint already exists, ignore.
        return Status();
    }
    checkpoints.push_back(chk_seqnum);
    return Status();
}

Status MemTable::load(RwSerializer& rws,
                      uint64_t min_seq,
                      uint64_t flushed_seq,
                      uint64_t synced_seq)
{
    Timer tt;
    getReady();

    flushedSeqNum = flushed_seq;
    minSeqNum = min_seq;
    startSeqNum = min_seq;
    seqNumAlloc = synced_seq;
    maxSeqNum = synced_seq;
    uint64_t padding_start_pos = NOT_INITIALIZED;

    Status s;
    size_t filesize = rws.size();
    size_t last_valid_size = rws.pos();
    uint64_t num_record = 0, num_flush = 0, num_chk = 0;
    uint64_t last_seq = NOT_INITIALIZED;
    std::string m;

    for (; rws.pos() < filesize;) {
        if (!rws.available(8)) {
            s = Status::INCOMPLETE_LOG;
            m = "not enough bytes for flags";
            break;
        }

        uint64_t flags;
        EB_( flags = rws.getU64(s), "failed to load flags" )
        FlagType type = identify_type(flags);
        if (type == FlagType::RECORD) {
            uint64_t seq = 0;
            EB( loadRecord(rws, flags, seq),  "failed to load record" )
            last_seq = seq;
            num_record++;
            last_valid_size = rws.pos();

        } else if (type == FlagType::FLUSH_MARKER) {
            uint64_t marker_seq;
            EB( loadFlushMarker(rws, marker_seq), "failed to load flush marker" )
            num_flush++;
            last_valid_size = rws.pos();

        } else if (type == FlagType::CHECKPOINT) {
            EB( loadCheckpoint(rws), "failed to load checkpoint" )
            num_chk++;
            last_valid_size = rws.pos();

        } else if (type == FlagType::PADDING) {
            // The rest bytes are just for alignment
            m = "hit padding bytes";
            padding_start_pos = last_valid_size;
            break;
        } else {
            s = Status::UNKNOWN_LOG_FLAG;
            break;
        }
    }

    if (NOT_INITIALIZED != last_seq) {
        maxSeqNum = last_seq;
        seqNumAlloc = last_seq;
        syncedSeqNum = last_seq;
    }

    if (NOT_INITIALIZED != last_seq && last_seq < synced_seq) {
        _log_err( myLog,
                  "failed to load memTable for log file %s %ld "
                  "as some log entries are missing. "
                  "File size %zu, last read pos %zu, padding_start_pos %s. "
                  "Loaded last_seq %s, record %ld, flush marker %ld, chk %ld, "
                  "skiplist %zu. Expected min seq %s, synced_seq %s, "
                  "flushed_seq %s. %lu us elapsed, error code %s, message %s",
                  logFile->filename.c_str(), logFile->getLogFileNum(),
                  filesize, rws.pos(), _seq_str(padding_start_pos).c_str(),
                  _seq_str(last_seq).c_str(),
                  num_record,
                  num_flush,
                  num_chk,
                  skiplist_get_size(idxBySeq),
                  _seq_str(min_seq).c_str(),
                  _seq_str(synced_seq).c_str(),
                  _seq_str(flushed_seq).c_str(),
                  tt.getUs(),
                  s.toString().c_str(),
                  m.c_str() );
    } else if (!s) {
        _log_warn( myLog,
                  "MemTable for log file %s %ld loading complete with error. "
                  "File size %zu, last read pos %zu, padding_start_pos %s. "
                  "Loaded last_seq %s, record %ld, flush marker %ld, chk %ld, "
                  "skiplist %zu. Expected min seq %s, synced_seq %s, "
                  "flushed_seq %s. %lu us elapsed, error code %s, message %s",
                  logFile->filename.c_str(), logFile->getLogFileNum(),
                  filesize, rws.pos(), _seq_str(padding_start_pos).c_str(),
                  _seq_str(last_seq).c_str(),
                  num_record,
                  num_flush,
                  num_chk,
                  skiplist_get_size(idxBySeq),
                  _seq_str(min_seq).c_str(),
                  _seq_str(synced_seq).c_str(),
                  _seq_str(flushed_seq).c_str(),
                  tt.getUs(),
                  s.toString().c_str(),
                  m.c_str() );
    } else {
        _log_info( myLog,
                   "MemTable for log file %s %ld loading complete. "
                   "File size %zu, last read pos %zu, padding_start_pos %s. "
                   "Loaded last_seq %s, record %ld, flush marker %ld, chk %ld, "
                   "skiplist %zu. Expected min seq %s, synced_seq %s, "
                   "flushed_seq %s. %lu us elapsed, error code %s, message %s",
                   logFile->filename.c_str(), logFile->getLogFileNum(),
                   filesize, rws.pos(), _seq_str(padding_start_pos).c_str(),
                   _seq_str(last_seq).c_str(),
                   num_record,
                   num_flush,
                   num_chk,
                   skiplist_get_size(idxBySeq),
                   _seq_str(min_seq).c_str(),
                   _seq_str(synced_seq).c_str(),
                   _seq_str(flushed_seq).c_str(),
                   tt.getUs(),
                   s.toString().c_str(),
                   m.c_str() );
    }
    return s;
}

Status MemTable::findOffsetOfSeq(SimpleLogger* logger,
                                 RwSerializer& rws,
                                 uint64_t target_seq,
                                 uint64_t& offset_out,
                                 uint64_t* padding_start_pos_out)
{
    Timer tt;

    if (padding_start_pos_out != nullptr) {
        *padding_start_pos_out = NOT_INITIALIZED;
    }

    Status s;
    size_t filesize = rws.size();
    size_t last_valid_size = rws.pos();
    uint64_t last_offset = 0;
    uint64_t last_seq = 0;
    std::string m;

    for (; rws.pos() < filesize;) {
        if (!rws.available(8)) {
            s = Status::INCOMPLETE_LOG;
            m = "not enough bytes for flags";
            break;
        }

        uint64_t flags;
        EB_( flags = rws.getU64(s), "failed to load flags" )
        FlagType type = identify_type(flags);
        if (type == FlagType::RECORD) {
            // Just read length part and then skip.
            const size_t LEN_META_SIZE = getLengthMetaSize(flags);

            // CRC of length meta + length meta + CRC of KMV.
            if (!rws.available(4 + LEN_META_SIZE + 4)) {
                s = Status::INCOMPLETE_LOG;
                m = "not enough bytes for record";
                break;
            }
            uint32_t crc_len = 0;
            uint8_t len_buf[32];
            EP_( crc_len = rws.getU32(s) );
            EP( rws.get(len_buf, LEN_META_SIZE) );

            uint32_t crc_len_chk = crc32_8(len_buf, LEN_META_SIZE, 0);
            if (crc_len != crc_len_chk) {
                _log_err(logger, "crc error %x != %x", crc_len, crc_len_chk);
                std::stringstream ss;
                ss << "crc error " << crc_len << " != " << crc_len_chk;
                m = ss.str();
                s = Status::CHECKSUM_ERROR;
                break;
            }

            RwSerializer len_buf_rw(len_buf, 32);
            last_seq = len_buf_rw.getU64(s);

            uint32_t k_size = len_buf_rw.getU32(s);
            uint32_t m_size = len_buf_rw.getU32(s);
            uint32_t v_size = len_buf_rw.getU32(s);

            uint32_t crc_data = 0;
            TC_( crc_data = rws.getU32(s) );
            (void)crc_data;

            // Skip.
            if (!rws.available(k_size + m_size + v_size)) {
                s = Status::INCOMPLETE_LOG;
                m = "not enough bytes for content of record";
                break;
            }
            rws.pos( rws.pos() + k_size + m_size + v_size );

            // Seq number check.
            if (last_seq <= target_seq) last_offset = rws.pos();
            last_valid_size = rws.pos();

        } else if (type == FlagType::FLUSH_MARKER) {
            if (!rws.available(8)) {
                s = Status::INCOMPLETE_LOG;
                m = "not enough bytes for flush marker";
                break;
            }
            uint64_t dummy = rws.getU64(s);
            (void)dummy;
            last_valid_size = rws.pos();

        } else if (type == FlagType::CHECKPOINT) {
            if (!rws.available(8)) {
                s = Status::INCOMPLETE_LOG;
                m = "not enough bytes for checkpoint";
                break;
            }
            uint64_t dummy = rws.getU64(s);
            (void)dummy;
            last_valid_size = rws.pos();

        } else if (type == FlagType::PADDING) {
            // The rest bytes are just for alignment
            m = "hit padding bytes";
            if (padding_start_pos_out != nullptr) {
                *padding_start_pos_out = last_valid_size;
            }
            break;
        } else {
            s = Status::UNKNOWN_LOG_FLAG;
            break;
        }
    }

    if (s) {
        // Only return offset on success
        offset_out = last_offset;
        _log_info( logger,
                   "found offset %ld for seqnum %ld, loaded last_seq %s, "
                   "file_size %zu, last read pos %zu, "
                   "%lu us elapsed, error code %s, message %s",
                   last_offset, target_seq,
                   _seq_str(last_seq).c_str(),
                   filesize, rws.pos(),
                   tt.getUs(),
                   s.toString().c_str(),
                   m.c_str() );
    } else {
        _log_err( logger,
                  "failed to find offset for seqnum %ld, last_offset %ld, "
                  "loaded last_seq %s, file_size %zu, last read pos %zu, "
                  "%lu us elapsed, error code %s, message %s",
                  target_seq, last_offset,
                  _seq_str(last_seq).c_str(),
                  filesize, rws.pos(),
                  tt.getUs(),
                  s.toString().c_str(),
                  m.c_str() );
    }

    return s;
}

// MemTable flush: skiplist (memory) -> log file. (disk)
Status MemTable::flush(RwSerializer& rws, uint64_t upto)
{
    if (minSeqNum == NOT_INITIALIZED) {
        // No log in this file. Just do nothing and return OK.
        return Status();
    }

    // Write logs in a mutation order.
    // From `synced seq num` to `max seq num`
    uint64_t seqnum_upto = maxSeqNum;

    // Manually given sequence number limit.
    if (valid_number(upto) && upto < seqnum_upto) {
        seqnum_upto = upto;
    }

    // Flush never happened: start from min seq num
    // Otherwise: start from last sync seq num + 1
    uint64_t ii = (syncedSeqNum.load() != NOT_INITIALIZED)
                  ? syncedSeqNum.load() + 1
                  : minSeqNum.load();

    Status s;
    uint64_t num_flushed = 0;
    uint64_t start_seqnum = NOT_INITIALIZED;
    Record query_rec;
    RecNodeSeq query(&query_rec);
    query.rec->seqNum = ii;
    skiplist_node* cursor = skiplist_find_greater_or_equal(idxBySeq, &query.snode);

    const size_t REC_META_LEN =
        // Flags           CRC for length     seq number
        sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint64_t) +
        // KMV(O) lengths         CRC for KMV
        sizeof(uint32_t) * 4 + sizeof(uint32_t);

    const size_t PAGE_LIMIT =
        DBMgr::getWithoutInit()->getGlobalConfig()->memTableFlushBufferSize;

    DB* parent_db = logFile->logMgr->getParentDb();
    const DBConfig* db_config = logFile->logMgr->getDbConfig();

    // Compression is enabled only when all three callbacks are given.
    const bool COMPRESSION = db_config->compOpt.cbGetMaxSize &&
                             db_config->compOpt.cbCompress &&
                             db_config->compOpt.cbDecompress;
    // Local compression buffer to avoid frequent memory allocation.
    const ssize_t LOCAL_COMP_BUF_SIZE = 4096;
    char local_comp_buf[LOCAL_COMP_BUF_SIZE];

    // Keep extra headroom.
    SizedBuf tmp_buf(PAGE_LIMIT * 2);
    SizedBuf::Holder h_tmp_buf(tmp_buf);
    RwSerializer ss_tmp_buf(tmp_buf);

   try {
    uint8_t len_buf[32];
    memset(len_buf, 0xff, 32);
    while (cursor) {
        RecNodeSeq* rec_node = _get_entry(cursor, RecNodeSeq, snode);
        Record* rec = rec_node->rec;
        if (!valid_number(start_seqnum)) start_seqnum = rec->seqNum;
        if (rec->seqNum > seqnum_upto) break;

        // << Record format >>
        // flags                            8 bytes (first byte: 0x0)
        // CRC32 of lengths                 4 bytes
        // seq numnber                      8 bytes
        // key length (A)                   4 bytes
        // meta length (B)                  4 bytes
        // value length (C)                 4 bytes
        // [ original value length (D)      4 bytes ] only when compressed
        // CRC32 of KMV                     4 bytes
        // key                              A
        // meta                             B
        // value                            C

        if (ss_tmp_buf.pos() + REC_META_LEN > PAGE_LIMIT) {
            // Flush and reset the position.
            TC( rws.put(tmp_buf.data, ss_tmp_buf.pos()) );
            ss_tmp_buf.pos(0);
        }

        ssize_t comp_buf_size = 0; // Output buffer size.
        ssize_t comp_size = 0; // Actual compressed size.

        SizedBuf comp_buf;
        SizedBuf::Holder h_comp_buf(comp_buf);
        // Refer to the local buffer by default.
        comp_buf.referTo( SizedBuf(LOCAL_COMP_BUF_SIZE, local_comp_buf) );

        uint64_t rec_flags = record_flags(rec);
        if (COMPRESSION && rec->type == Record::Type::INSERTION) {
            // If compression is enabled, ask if we compress this record.
            comp_buf_size = db_config->compOpt.cbGetMaxSize(parent_db, *rec);
            if (comp_buf_size > 0) {
                if (comp_buf_size > LOCAL_COMP_BUF_SIZE) {
                    // Bigger than the local buffer size, allocate a new.
                    comp_buf.alloc(comp_buf_size);
                }
                // Do compression.
                comp_size = db_config->compOpt.cbCompress(parent_db, *rec, comp_buf);
                if (comp_size > 0) {
                    // Compression succeeded, set the flag.
                    record_flags_set_compressed(rec_flags);
                } else if (comp_size < 0) {
                    _log_err( myLog, "compression failed: %zd, db %s, key %s",
                              comp_size,
                              parent_db->getPath().c_str(),
                              rec->kv.key.toReadableString().c_str() );
                }
                // Otherwise: if `comp_size == 0`,
                //            that implies cancelling compression.
            }
        }

        TC( ss_tmp_buf.putU64(rec_flags) );

        // Put seqnum + length info to `len_buf`.
        RwSerializer len_buf_s(len_buf, 32);
        len_buf_s.putU64(rec->seqNum);
        len_buf_s.putU32(rec->kv.key.size);
        len_buf_s.putU32(rec->meta.size);
        if (comp_size > 0) {
            len_buf_s.putU32(comp_size);
        }
        len_buf_s.putU32(rec->kv.value.size);
        uint64_t len_buf_size = len_buf_s.pos();

        // Calculate CRC of `len_buf`,
        // and put both CRC and `len_buf` to `tmp_buf`.
        uint32_t crc_len = crc32_8(len_buf, len_buf_size, 0);
        TC( ss_tmp_buf.putU32(crc_len) );
        TC( ss_tmp_buf.put(len_buf, len_buf_size) );

        // Reference buffer to data to be written.
        SizedBuf value_ref;
        if (comp_size > 0) {
            value_ref.set(comp_size, comp_buf.data);
        } else {
            value_ref.referTo(rec->kv.value);
        }

        // Calculate CRC of data, and put the CRC to `tmp_buf`.
        uint32_t crc_data = crc32_8(rec->kv.key.data, rec->kv.key.size, 0);
        crc_data = crc32_8(rec->meta.data, rec->meta.size, crc_data);
        crc_data = crc32_8(value_ref.data, value_ref.size, crc_data);
        TC( ss_tmp_buf.putU32(crc_data) );

        size_t data_len = rec->kv.key.size + rec->kv.value.size + rec->meta.size;
        if (data_len >= PAGE_LIMIT) {
            // Data itself is bigger than a page, flush and bypass buffer.
            TC( rws.put(tmp_buf.data, ss_tmp_buf.pos()) );
            ss_tmp_buf.pos(0);

            // Write to file directly.
            TC( rws.put(rec->kv.key.data, rec->kv.key.size) );
            TC( rws.put(rec->meta.data, rec->meta.size) );
            TC( rws.put(value_ref.data, value_ref.size) );

        } else {
            if (ss_tmp_buf.pos() + data_len > PAGE_LIMIT) {
                // Data itself is not bigger than a page,
                // but the entire data in the buffer will exceed
                // the page size, once we append data.
                // Flush the buffer into file.
                TC( rws.put(tmp_buf.data, ss_tmp_buf.pos()) );
                ss_tmp_buf.pos(0);
            }

            // Write to buffer.
            TC( ss_tmp_buf.put(rec->kv.key.data, rec->kv.key.size) );
            TC( ss_tmp_buf.put(rec->meta.data, rec->meta.size) );
            TC( ss_tmp_buf.put(value_ref.data, value_ref.size) );
        }

        num_flushed++;

        ii = rec->seqNum;
        {   mGuard l(checkpointsLock);
            for (auto& entry: checkpoints) {
                uint64_t chk_seqnum = entry;
                if (chk_seqnum == ii) {
                    TC( appendCheckpointMarker(ss_tmp_buf, chk_seqnum) );
                }
            }
        }

        cursor = skiplist_next(idxBySeq, &rec_node->snode);
        skiplist_release_node(&rec_node->snode);
    }
    if (cursor) skiplist_release_node(cursor);

    {   // In case that only a checkpoint is appended without any new record.
        mGuard l(checkpointsLock);
        auto entry = checkpoints.rbegin();
        if ( checkpointsDirty &&
             entry != checkpoints.rend() &&
             valid_number(syncedSeqNum) &&
             *entry == syncedSeqNum ) {
            TC( appendCheckpointMarker(rws, syncedSeqNum) );
        }
        checkpointsDirty = false;
    }

    // Just in case if remaining data exists.
    if (ss_tmp_buf.pos()) {
        TC( rws.put(tmp_buf.data, ss_tmp_buf.pos()) );
    }

    _log_debug(myLog,
               "MemTable %ld flushed %ld skiplist %zu, "
               "start_seqnum %ld seqnum_upto %ld",
               logFile->getLogFileNum(), num_flushed,
               skiplist_get_size(idxBySeq),
               start_seqnum, seqnum_upto);

    syncedSeqNum = seqnum_upto;
    return Status();

   } catch (Status s) {
    if (cursor) skiplist_release_node(cursor);
    return s;
   }
}

Status MemTable::appendFlushMarker(RwSerializer& rws)
{
    // << Flush marker format >>
    // flags,             8 bytes (first byte: 0x1)
    // synced seq number, 8 bytes
    Status s;
    EP( rws.putU64(flush_marker_flags()) );
    EP( rws.putU64(syncedSeqNum) );
    return Status();
}

Status MemTable::appendCheckpointMarker(RwSerializer& rws,
                                        uint64_t chk_seqnum)
{
    // << Flush marker format >>
    // flags,                   8 bytes (first byte: 0x2)
    // checkpoint seq number,   8 bytes
    Status s;
    EP( rws.putU64(checkpoint_flags()) );
    EP( rws.putU64(chk_seqnum) );
    return Status();
}

Status MemTable::checkpoint(uint64_t& seq_num_out) {
    // No log. Do nothing.
    if (!valid_number(maxSeqNum)) return Status();

    mGuard l(checkpointsLock);
    seq_num_out = maxSeqNum;

    auto last_entry = checkpoints.rbegin();
    if ( last_entry != checkpoints.rend() &&
         *last_entry == maxSeqNum) {
        // Same one already exists, ignore.
        return Status();
    }
    checkpoints.push_back(seq_num_out);
    checkpointsDirty = true;
    return Status();
}

Status MemTable::getLogsToFlush(const uint64_t seq_num,
                                std::list<Record*>& list_out,
                                bool ignore_sync_seqnum)
{
    if ( !ignore_sync_seqnum &&
         syncedSeqNum == NOT_INITIALIZED ) {
        return Status::LOG_NOT_SYNCED;
    }
    if ( flushedSeqNum != NOT_INITIALIZED &&
         seq_num <= flushedSeqNum ) {
        return Status::ALREADY_PURGED;
    }

    uint64_t ii = minSeqNum;
    if (flushedSeqNum != NOT_INITIALIZED) ii = flushedSeqNum + 1;

    // Remove previous records if there is a seq number rollback.
    auto entry = list_out.rbegin();
    while (entry != list_out.rend()) {
        Record* cur_rec = *entry;
        if (cur_rec->seqNum >= ii) {
            _log_err(myLog, "found duplicate seq number across different log files: "
                     "%zu. will use newer one",
                     cur_rec->seqNum);
            list_out.pop_back();
            entry = list_out.rbegin();
        } else {
            break;
        }
    }

    Record query_rec;
    RecNodeSeq query(&query_rec);

    // Check last put seq number for debugging purpose.
    uint64_t last_seq = 0;
    query.rec->seqNum = ii;
    skiplist_node* cursor = skiplist_find_greater_or_equal(idxBySeq, &query.snode);
    while (cursor) {
        RecNodeSeq* rec_node = _get_entry(cursor, RecNodeSeq, snode);
        Record* rec = rec_node->rec;
        if (rec->seqNum > seq_num) break;

        if (rec->seqNum == last_seq) {
            _log_err(myLog, "found duplicate seq number %zu", last_seq);
            assert(0);
        }
        list_out.push_back(rec);
        last_seq = rec->seqNum;

        cursor = skiplist_next(idxBySeq, &rec_node->snode);
        skiplist_release_node(&rec_node->snode);
    }
    if (cursor) skiplist_release_node(cursor);

    return Status();
}

Status MemTable::getCheckpoints(const uint64_t seq_num,
                                std::list<uint64_t>& list_out)
{
    mGuard l(checkpointsLock);
    for (auto& entry: checkpoints) {
        uint64_t chk_seqnum = entry;
        if ( valid_number(flushedSeqNum) &&
             chk_seqnum <= flushedSeqNum ) continue;
        if ( chk_seqnum <= seq_num ||
             !valid_number(seq_num) ) {
            list_out.push_back(chk_seqnum);
        }
    }
    return Status();
}

size_t MemTable::getNumLogs() const {
    return skiplist_get_size(idxBySeq);
}

Status MemTable::forceSeqnum(uint64_t to) {
    if ( valid_number(minSeqNum) &&
         minSeqNum > to ) {
        return Status();
    }

    if ( !valid_number(flushedSeqNum) ||
         !valid_number(syncedSeqNum) ||
         to > flushedSeqNum ||
         to > syncedSeqNum ) {
        seqNumAlloc = to;
        maxSeqNum = to;
        syncedSeqNum = to;
        flushedSeqNum = to;
        if (!valid_number(minSeqNum)) minSeqNum = to;
        _log_warn(myLog, "updated flushed/sync seq numbers to %zu", to);
    }
    return Status();
}

} // namespace jungle

