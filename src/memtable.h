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

#pragma once

#include "fileops_base.h"
#include "internal_helper.h"
#include "skiplist.h"

#include <libjungle/jungle.h>

#include <atomic>
#include <list>
#include <vector>

class BloomFilter;
class SimpleLogger;

namespace jungle {

class LogFile;
class MemTable {
    friend class LogFile;
private:
    struct RecNode;

public:
    MemTable(const LogFile* log_file);
    ~MemTable();

    static int cmpKey(skiplist_node *a, skiplist_node *b, void *aux);

    Status init(const uint64_t start_seq_num = 0);

    Status assignSeqNum(Record& rec_local);
    Status updateMaxSeqNum(const uint64_t seq_num);
    RecNode* findRecNode(Record* rec);
    void addToByKeyIndex(Record* rec);
    Status addToBySeqIndex(Record* rec, Record*& prev_rec_out);
    Status putNewRecord(const Record& rec);
    // Returns pointer only.
    Status findRecordBySeq(const uint64_t seq_num,
                           Record& rec_out);
    // Returns pointer only.
    Status getRecordByKey(const uint64_t chk,
                          const SizedBuf& key,
                          uint64_t* key_hash,
                          Record& rec_out,
                          bool allow_flushed_log,
                          bool allow_tombstone);
    Status sync(FileOps* f_ops,
                FileHandle* fh);
    Status appendFlushMarker(RwSerializer& rws);
    Status appendCheckpointMarker(RwSerializer& rws,
                                  uint64_t chk_seqnum);
    Status loadRecord(RwSerializer& rws,
                      uint64_t flags,
                      uint64_t& seqnum_out);
    Status loadFlushMarker(RwSerializer& rws,
                           uint64_t& synced_seq_num_out);
    Status loadCheckpoint(RwSerializer& rws);

    Status load(RwSerializer& rws,
                uint64_t min_seq,
                uint64_t flushed_seq,
                uint64_t synced_seq);

    static Status findOffsetOfSeq(SimpleLogger* logger,
                                  RwSerializer& rws,
                                  uint64_t target_seq,
                                  uint64_t& offset_out,
                                  uint64_t* padding_start_pos_out = nullptr);

    Status flush(RwSerializer& rws, uint64_t upto = NOT_INITIALIZED);
    Status checkpoint(uint64_t& seq_num_out);
    Status getLogsToFlush(const uint64_t seq_num,
                          std::list<Record*>& list_out,
                          bool ignore_sync_seqnum);
    // If seq_num == NOT_INITIALIZED, return all.
    Status getCheckpoints(const uint64_t seq_num,
                          std::list<uint64_t>& list_out);

    size_t getNumLogs() const;

    // Next seqnum will start from `to + 1`.
    uint64_t getSeqCounter() const { return seqNumAlloc.load(MOR); }
    Status forceSeqnum(uint64_t to);

    void setLogger(SimpleLogger* logger) { myLog = logger; }

    bool isIncreasingOrder() const { return increasingOrder; }

    // Size of memory table in bytes
    uint64_t size() const { return bytesSize.load(); }

    class Iterator {
    public:
        Iterator();
        ~Iterator();

        enum SeekOption {
            GREATER = 0,
            SMALLER = 1,
        };

        Status init(const MemTable* m_table,
                    const SizedBuf& start_key,
                    const SizedBuf& end_key,
                    const uint64_t seq_upto);
        Status initSN(const MemTable* m_table,
                      const uint64_t min_seq,
                      const uint64_t max_seq);
        Status get(Record& rec_out);
        Status prev(bool allow_tombstone = false);
        Status next(bool allow_tombstone = false);
        Status seek(const SizedBuf& key, SeekOption opt = GREATER);
        Status seekSN(const uint64_t seqnum, SeekOption opt = GREATER);
        Status gotoBegin();
        Status gotoEnd();
        Status close();

    private:
        enum Type {
            BY_KEY = 0,
            BY_SEQ = 1
        } type;
        const MemTable* mTable;
        skiplist_node* cursor;
        uint64_t minSeq;
        uint64_t maxSeq;
        SizedBuf startKey;
        SizedBuf endKey;
        uint64_t seqUpto;
    };

private:
// === TYPES
    static const int INIT_ARRAY_SIZE = 16384;
    static const int MAX_LOGFILE_SIZE = 4*1024*1024;

    struct RecNode {
        using RecList = std::list<Record*>;

        RecNode(const SizedBuf& _key);
        RecNode(const SizedBuf* _key);
        ~RecNode();

        static int cmp(skiplist_node *a, skiplist_node *b, void *aux);
        Record* getLatestRecord(const uint64_t chk);
        uint64_t getMinSeq();
        bool validKeyExist(const uint64_t chk,
                           bool allow_tombstone = false);

        skiplist_node snode;
        SizedBuf key;
        bool freeKey;
        std::mutex recListLock;
        RecList* recList;
    };

    struct RecNodeSeq {
        RecNodeSeq(Record* _rec);
        ~RecNodeSeq();

        static int cmp(skiplist_node *a, skiplist_node *b, void *aux);

        skiplist_node snode;
        // TODO: multi-thread update?
        Record* rec;
    };

// === FUNCTIONS
    static size_t getLengthMetaSize(uint64_t flags);

    Status getReady();

// === VARIABLES
    uint64_t startSeqNum;

    // Earliest log seq number.
    std::atomic<uint64_t> minSeqNum;
    // Last flushed (moved to index table) seq number.
    std::atomic<uint64_t> flushedSeqNum;
    // Last synced (flushed from Mem table into corresponding log file,
    // but not physically durable yet) seq number. If logs are flushed
    // and then synced at once, this value always will be the same as
    // `fsyncedSeqNum`.
    std::atomic<uint64_t> syncedSeqNum;
    // Latest log seq number in the current mem table.
    std::atomic<uint64_t> maxSeqNum;
    // Atomic counter for seq number allocation.
    std::atomic<uint64_t> seqNumAlloc;
    // Atomic counter for byte size of whole memtable
    std::atomic<uint64_t> bytesSize;

    // Index by sequence number.
    skiplist_raw* idxBySeq;

    // Index by key.
    skiplist_raw* idxByKey;
    // Bloom filter for key.
    BloomFilter* bfByKey;

    // Temporary store for stale (overwritten using the same seq num) logs.
    std::mutex staleLogsLock;
    std::list<Record*> staleLogs;

    // Checkpoints.
    std::mutex checkpointsLock;
    std::list<uint64_t> checkpoints;
    bool checkpointsDirty;

    // Parent log file.
    const LogFile* logFile;

    // Pointer to last inserted record.
    Record* lastInsertedRec;

    // `true` if all records have been inserted in key order.
    bool increasingOrder;

    SimpleLogger* myLog;
};

} // namespace jungle

