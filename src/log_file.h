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
#include "memtable.h"

#include <libjungle/jungle.h>

#include <atomic>
#include <list>
#include <string>

class SimpleLogger;

namespace jungle {

// TODO: make this class virtual in the future.
class LogMgr;
class LogFile {
    friend class MemTable;
public:
    LogFile(LogMgr* log_mgr);
    ~LogFile();

    static std::string getLogFileName(const std::string& path,
                                      uint64_t prefix_num,
                                      uint64_t log_file_num);

    Status openFHandle();
    Status closeFHandle();

    void touch();

    bool isExpired();

    uint64_t getLastAcc();

    Status create(const std::string& _filename,
                  FileOps* _f_ops,
                  const uint64_t log_file_num,
                  const uint64_t start_seq_num);
    Status load(const std::string& _filename,
                FileOps* _f_ops,
                uint64_t log_file_num,
                uint64_t min_seq,
                uint64_t flushed_seq,
                uint64_t synced_seq);

    Status loadMemTable();

    Status truncate(uint64_t seq_upto);

    Status assignSeqNum(Record& rec_local);

    Status setSN(const Record& rec);

    // Returns pointer only.
    Status getSN(const uint64_t seq_num, Record& rec_out);

    // Returns pointer only.
    Status get(const uint64_t chk,
               const SizedBuf& key,
               uint64_t* key_hash,
               Record& rec_out,
               bool allow_flushed_log,
               bool allow_tombstone);

    Status flushMemTable(uint64_t upto = NOT_INITIALIZED);

    Status purgeMemTable();

    bool isMemTablePurged() const { return memtablePurged; }

    uint64_t getMemTableSize() const;

    Status sync();

    bool isSynced();

    Status checkpoint(uint64_t& seq_num_out);

    Status getLogsToFlush(const uint64_t seq_num,
                          std::list<Record*>& list_out,
                          bool ignore_sync_seqnum);
    // If seq_num == NOT_INITIALIZED, return all.
    Status getCheckpoints(const uint64_t seq_num,
                          std::list<uint64_t>& list_out);
    Status setSyncedSeqNum(const uint64_t seq_num);
    Status setFlushedSeqNum(const uint64_t seq_num);
    Status updateSeqNumByBulkLoader(const uint64_t seq_num);
    Status destroySelf();

    uint64_t getMinSeqNum() const;
    uint64_t getFlushedSeqNum() const;
    uint64_t getSyncedSeqNum() const;
    uint64_t getMaxSeqNum() const;
    uint64_t getLogFileNum() const { return logFileNum; }

    void setImmutable();
    bool isImmutable() const { return immutable; }
    bool isValidToWrite();
    bool isIncreasingOrder() const;

    uint64_t getSeqCounter() const {
        if (mTable) return mTable->getSeqCounter();
        return NOT_INITIALIZED;
    }

    Status forceSeqnum(uint64_t to) {
        if (mTable) {
            return mTable->forceSeqnum(to);
        }
        // In TTL mode, `mTable` will not exist but that's ok
        // as TTL mode is only available in log-section-only mode.
        return Status::NOT_INITIALIZED;
    }

    void setLogger(SimpleLogger* logger) {
        myLog = logger;
        if (mTable) mTable->setLogger(logger);
    }

    class Iterator {
    public:
        Iterator();
        ~Iterator();

        enum SeekOption {
            GREATER = 0,
            SMALLER = 1,
        };

        Status init(LogFile* l_file,
                    const SizedBuf& start_key,
                    const SizedBuf& end_key,
                    const uint64_t seq_upto);
        Status initSN(LogFile* l_file,
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
        LogFile* lFile;
        MemTable::Iterator mItr;
    };

private:
// === TYPES
    struct PurgedMemTableMeta {
        PurgedMemTableMeta()
            : minSeq(0), maxSeq(0), flushedSeq(0), syncedSeq(0) {}
        uint64_t minSeq;
        uint64_t maxSeq;
        uint64_t flushedSeq;
        uint64_t syncedSeq;
    };

    enum IntegrityTypes {
        UNKNOWN,
        VALID,
        CORRUPTED
    };

// === FUNCTIONS

    bool okToCloseFHandle();

// === VARIABLES

    // Log file number.
    uint64_t logFileNum;

    // File name.
    std::string filename;

    // File operations.
    FileOps* fOps;

    // Handle for this log file.
    FileHandle* fHandle;

    // In-memory index for this log file.
    MemTable* mTable;

    // If true, immutable file.
    std::atomic<bool> immutable;

    // If true, a new checkpoint is added after immutable
    bool coldChk;

    // If file is corrupted, new writes are forbidden.
    IntegrityTypes integrity;

    // Memtable has been purged.
    std::atomic<bool> memtablePurged;

    // Memtable metadata if it is purged.
    PurgedMemTableMeta memTableOnFileMeta;

    // Parent log manager.
    LogMgr* logMgr;

    // Current file size.
    uint64_t fileSize;

    // Timer for last access.
    Timer lastAcc;

    // Logger.
    SimpleLogger* myLog;
};

} // namespace jungle

