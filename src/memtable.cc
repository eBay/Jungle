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

#define REC_LEN_SIZE (20)

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

Record* MemTable::RecNode::getLatestRecord(const uint64_t chk) {
    mGuard l(recListLock);
    Record* rec = nullptr;
    auto entry = recList->rbegin();
    while (entry != recList->rend()) {
        Record* tmp = *entry;
        if (!valid_number(chk) || tmp->seqNum <= chk) {
            rec = tmp;
            break;
        }
        entry++;
    }
    return rec;
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

bool MemTable::RecNode::validKeyExist( const uint64_t chk,
                                       bool allow_tombstone )
{
    bool valid_key_exist = true;

    if (getMinSeq() > chk) {
        // No record belongs to the current snapshot (iterator),
        valid_key_exist = false;
    }

    if (valid_key_exist) {
        Record* rec = getLatestRecord(chk);
        assert(rec);
        if ( !allow_tombstone &&
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
    return static_cast<Record::Type>(flags & 0xff);
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
        bfByKey->set(rec->kv.key.data, rec->kv.key.size);
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
                                uint64_t* key_hash,
                                Record& rec_out,
                                bool allow_flushed_log,
                                bool allow_tombstone)
{
    // Check bloom filter first for fast screening.
    if (key_hash) {
        if (!bfByKey->check(key_hash)) return Status::KEY_NOT_FOUND;
    } else {
        if (!bfByKey->check(key.data, key.size)) return Status::KEY_NOT_FOUND;
    }

    RecNode query(&key);
    skiplist_node* cursor = skiplist_find(idxByKey, &query.snode);
    if (!cursor) return Status::KEY_NOT_FOUND;

    RecNode* node = _get_entry(cursor, RecNode, snode);
    Record* rec_ret = node->getLatestRecord(chk);
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

Status MemTable::sync(FileOps* f_ops,
                      FileHandle* fh)
{
    Status s;
    s = f_ops->fsync(fh);
    if (!s) return s;

    return Status();
}

Status MemTable::loadRecord(RwSerializer& rws,
                            uint64_t flags,
                            uint64_t& seqnum_out)
{
    Status s;
    Record* rec = new Record();
    rec->type = get_record_type_from_flags(flags);

  try{
    uint32_t crc_len = 0;
    uint8_t len_buf[32];
    if (!rws.available(4 + REC_LEN_SIZE + 4)) {
        throw Status(Status::INCOMPLETE_LOG);
    }

    TC_( crc_len = rws.getU32(s) );
    TC( rws.get(len_buf, REC_LEN_SIZE) );

    uint32_t crc_len_chk = crc32_8(len_buf, REC_LEN_SIZE, 0);
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
        rec->kv.value.alloc(v_size, nullptr);
        TC( rws.get(rec->kv.value.data, v_size) );
        crc_data_chk = crc32_8(rec->kv.value.data, v_size, crc_data_chk);
    }

    if (crc_data != crc_data_chk) {
        _log_err(myLog, "crc error %x != %x", crc_data, crc_data_chk);
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
            if (!rws.available(4 + REC_LEN_SIZE + 4)) {
                s = Status::INCOMPLETE_LOG;
                m = "not enough bytes for record";
                break;
            }
            uint32_t crc_len = 0;
            uint8_t len_buf[32];
            EP_( crc_len = rws.getU32(s) );
            EP( rws.get(len_buf, REC_LEN_SIZE) );

            uint32_t crc_len_chk = crc32_8(len_buf, REC_LEN_SIZE, 0);
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
Status MemTable::flush(RwSerializer& rws)
{
    if (minSeqNum == NOT_INITIALIZED) {
        // No log in this file. Just do nothing and return OK.
        return Status();
    }

    // Write logs in a mutation order.
    // From `synced seq num` to `max seq num`
    uint64_t seqnum_upto = maxSeqNum;

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
        // KMV lengths         CRC for KMV
        sizeof(uint32_t) * 3 + sizeof(uint32_t);

    const size_t PAGE_LIMIT =
        DBMgr::getWithoutInit()->getGlobalConfig()->memTableFlushBufferSize;

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
        // flags                8 bytes (first byte: 0x0)
        // CRC32 of lengths     4 bytes
        // seq numnber          8 bytes
        // key length (A)       4 bytes
        // meta length (B)      4 bytes
        // value length (C)     4 bytes
        // CRC32 of KMV         4 bytes
        // key                  A
        // meta                 B
        // value                C

        if (ss_tmp_buf.pos() + REC_META_LEN > PAGE_LIMIT) {
            // Flush and reset the position.
            TC( rws.put(tmp_buf.data, ss_tmp_buf.pos()) );
            ss_tmp_buf.pos(0);
        }

        TC( ss_tmp_buf.putU64(record_flags(rec)) );

        // Put seqnum + length info to `len_buf`.
        RwSerializer len_buf_s(len_buf, 32);
        len_buf_s.putU64(rec->seqNum);
        len_buf_s.putU32(rec->kv.key.size);
        len_buf_s.putU32(rec->meta.size);
        len_buf_s.putU32(rec->kv.value.size);
        uint64_t len_buf_size = len_buf_s.pos();

        // Calculate CRC of `len_buf`,
        // and put both CRC and `len_buf` to `tmp_buf`.
        uint32_t crc_len = crc32_8(len_buf, len_buf_size, 0);
        TC( ss_tmp_buf.putU32(crc_len) );
        TC( ss_tmp_buf.put(len_buf, len_buf_size) );

        // Calculate CRC of data, and put the CRC to `tmp_buf`.
        uint32_t crc_data = crc32_8(rec->kv.key.data, rec->kv.key.size, 0);
        crc_data = crc32_8(rec->meta.data, rec->meta.size, crc_data);
        crc_data = crc32_8(rec->kv.value.data, rec->kv.value.size, crc_data);
        TC( ss_tmp_buf.putU32(crc_data) );

        size_t data_len = rec->kv.key.size + rec->kv.value.size + rec->meta.size;
        if (data_len >= PAGE_LIMIT) {
            // Data itself is bigger than a page, flush and bypass buffer.
            TC( rws.put(tmp_buf.data, ss_tmp_buf.pos()) );
            ss_tmp_buf.pos(0);

            // Write to file directly.
            TC( rws.put(rec->kv.key.data, rec->kv.key.size) );
            TC( rws.put(rec->meta.data, rec->meta.size) );
            TC( rws.put(rec->kv.value.data, rec->kv.value.size) );

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
            TC( ss_tmp_buf.put(rec->kv.value.data, rec->kv.value.size) );
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

