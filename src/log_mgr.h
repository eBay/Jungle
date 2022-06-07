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

#include "avltree.h"
#include "fileops_base.h"
#include "internal_helper.h"
#include "log_file.h"
#include "log_manifest.h"
#include "table_mgr.h"

#include <libjungle/jungle.h>

#include <atomic>
#include <map>
#include <mutex>
#include <vector>

class SimpleLogger;

namespace jungle {

class LogMgrOptions {
public:
    LogMgrOptions()
        : fOps(nullptr)
        , fDirectOps(nullptr)
        , prefixNum(0)
        , dbConfig(nullptr)
        , startSeqnum(1)
        {}

    /**
     * DB path.
     */
    std::string path;

    /**
     * Filesystem operations.
     */
    FileOps* fOps;

    /**
     * Filesystem operations if Direct IO is used.
     */
    FileOps* fDirectOps;

    /**
     * KVStore ID.
     */
    uint64_t prefixNum;

    /**
     * KVStore name.
     */
    std::string kvsName;

    /**
     * Pointer to the parent DB handle's config.
     */
    const DBConfig* dbConfig;

    /**
     * Start sequence number if the DB is created for the first time.
     * The first record will be assigned to this sequence number.
     * If not given, sequence number starts from 1.
     */
    uint64_t startSeqnum;
};

// Semaphore that allows only one operation at a time.
struct OpSema {
    OpSema() : enabled(true), grabbed(false) {}
    std::atomic<bool> enabled;
    std::atomic<bool> grabbed;
};

struct OpSemaWrapper {
    OpSemaWrapper(OpSema* _op_sema) : op_sema(_op_sema), acquired(false) {}
    ~OpSemaWrapper() {
        if (acquired) {
            op_sema->grabbed = false;
        }
        op_sema = nullptr;
        acquired = false;
    }

    bool acquire() {
        bool expected = false;
        bool val = true;
        if ( op_sema->enabled &&
             op_sema->grabbed.compare_exchange_weak(expected, val) ) {
            acquired = true;
        }
        return acquired;
    }

    OpSema* op_sema;
    bool acquired;
};

namespace checker { class Checker; }

struct GlobalBatchStatus;
class LogMgr {
    friend class checker::Checker;
    friend class GlobalBatchExecutor;

public:
    LogMgr(DB* parent_db, const LogMgrOptions& lm_opt = LogMgrOptions());

    ~LogMgr();

    Status init(const LogMgrOptions& lm_opt);

    void logMgrSettings();

    Status rollback(uint64_t seq_upto);

    Status removeStaleFiles();

    bool isLogStoreMode() const;

    Status openSnapshot(DB* snap_handle,
                        const uint64_t checkpoint,
                        std::list<LogFileInfo*>*& log_file_list_out);
    Status closeSnapshot(DB* snap_handle);

    void lockWriteMutex();

    void unlockWriteMutex();

    Status setMulti(const std::list<Record>& batch);

    void setVisibleSeqBarrier(uint64_t to);

    void setGlobalBatch(uint64_t fwd_barrier,
                        std::shared_ptr<GlobalBatchStatus> status);

    uint64_t getVisibleSeqBarrier();

    Status checkBatchValidity(const std::list<Record>& batch);

    Status setMultiInternal(const std::list<Record>& batch,
                            uint64_t& max_seq_out);

    Status setSN(const Record& rec);

    // Returns pointer only.
    Status getSN(const uint64_t seq_num, Record& rec_out);

    // Returns pointer only.
    Status get(const uint64_t chk,
               std::list<LogFileInfo*>* l_list,
               const SizedBuf& key,
               Record& rec_out);

    Status getNearest(const uint64_t chk,
                      std::list<LogFileInfo*>* l_list,
                      const SizedBuf& key,
                      Record& rec_out,
                      SearchOptions s_opt);

    Status getPrefix(const uint64_t chk,
                     std::list<LogFileInfo*>* l_list,
                     const SizedBuf& prefix,
                     SearchCbFunc cb_func);

    Status sync(bool call_fsync = true);

    Status syncNoWait(bool call_fsync = true);

    Status discardDirty(uint64_t seq_begin);

protected:
    Status syncInternal(bool call_fsync);

    Status addNewLogFile(LogFileInfoGuard& cur_log_file_info,
                         LogFileInfoGuard& new_log_file_info);

    void adjustThrottling(uint64_t num_records_flushed,
                          double elapsed,
                          const FlushOptions& options,
                          uint64_t ln_to_original,
                          uint64_t ln_to);

    double getSlowestMergeRate(bool include_table_rate = true);

    void adjustThrottlingExtreme();

public:
    Status flush(const FlushOptions& options,
                 const uint64_t seq_num,
                 TableMgr* table_mgr);

    Status doLogReclaim();
    Status doBackgroundLogReclaimIfNecessary();
    uint32_t increaseOpenMemtable() { return numMemtables.fetch_add(1) + 1; }
    uint32_t decreaseOpenMemtable() { return numMemtables.fetch_sub(1) - 1; }
    uint32_t getNumMemtables() const { return numMemtables; }

    Status checkpoint(uint64_t& seq_num_out, bool call_fsync = true);
    Status getAvailCheckpoints(std::list<uint64_t>& chk_out);

    // Return (last flushed seq + 1) to max seq
    Status getAvailSeqRange(uint64_t& min_seq,
                            uint64_t& max_seq);
    Status getMaxSeqNum(uint64_t& seq_num_out);
    Status getMinSeqNum(uint64_t& seq_num_out);
    Status getLastFlushedSeqNum(uint64_t& seq_num_out);
    Status getLastSyncedSeqNum(uint64_t& seq_num_out);

    bool checkTimeToFlush(const GlobalConfig& config);
    Status close();

    Status syncSeqnum(TableMgr* t_mgr);

    void execBackPressure(uint64_t elapsed_us);

    inline const DBConfig* getDbConfig() const { return opt.dbConfig; }

    void setLogger(SimpleLogger* logger) {
        myLog = logger;
        if (mani) mani->setLogger(myLog);
    }

    size_t getNumLogFiles();

    uint64_t getMinLogFileIndex();

    uint64_t getMaxLogFileIndex();

    DB* getParentDb() const { return parentDb; }

    struct Iterator {
    public:
        Iterator();
        ~Iterator();

        enum SeekOption {
            GREATER = 0,
            SMALLER = 1,
        };

        Status init(DB* snap_handle,
                    LogMgr* log_mgr,
                    const SizedBuf& start_key,
                    const SizedBuf& end_key);
        Status initSN(DB* snap_handle,
                      LogMgr* log_mgr,
                      uint64_t min_seq,
                      uint64_t max_seq);
        Status get(Record& rec_out);
        Status prev();
        Status next();
        Status seek(const SizedBuf& key, SeekOption opt = GREATER);
        Status seekSN(const uint64_t seqnum, SeekOption opt = GREATER);
        Status gotoBegin();
        Status gotoEnd();
        Status close();
    private:
        enum Type {
            BY_KEY = 0,
            BY_SEQ = 1
        };
        struct ItrItem {
            ItrItem() : flags(0x0), lInfo(nullptr), lItr(nullptr) {}
            enum Flag {
                none = 0x0,
                no_more_prev = 0x1,
                no_more_next = 0x2,
            };
            inline static int cmpSeq(avl_node *a, avl_node *b, void *aux) {
                ItrItem* aa = _get_entry(a, ItrItem, an);
                ItrItem* bb = _get_entry(b, ItrItem, an);
                if (aa->lastRec.seqNum < bb->lastRec.seqNum) return -1;
                else if (aa->lastRec.seqNum > bb->lastRec.seqNum) return 1;
                return 0;
            }
            inline static int cmpKey(avl_node *a, avl_node *b, void *aux) {
                ItrItem* aa = _get_entry(a, ItrItem, an);
                ItrItem* bb = _get_entry(b, ItrItem, an);

                CMP_NULL_CHK(aa->lastRec.kv.key.data, bb->lastRec.kv.key.data);

                int cmp = 0;
                if (aux) {
                    // Custom cmp mode.
                    LogMgr* lm = reinterpret_cast<LogMgr*>(aux);
                    CustomCmpFunc func = lm->getDbConfig()->cmpFunc;
                    void* param = lm->getDbConfig()->cmpFuncParam;
                    cmp = func(aa->lastRec.kv.key.data, aa->lastRec.kv.key.size,
                               bb->lastRec.kv.key.data, bb->lastRec.kv.key.size,
                               param);
                } else {
                    cmp = SizedBuf::cmp(aa->lastRec.kv.key, bb->lastRec.kv.key);
                }

                // Note: key: ascending, seq: descending order.
                if (cmp == 0) return cmpSeq(b, a, aux);
                return cmp;
            }
            avl_node an;
            uint8_t flags;
            LogFileInfo* lInfo;
            LogFile::Iterator* lItr;
            Record lastRec;
        };

        Status initInternal(DB* snap_handle,
                            LogMgr* log_mgr,
                            uint64_t min_seq,
                            uint64_t max_seq,
                            const SizedBuf& start_key,
                            const SizedBuf& end_key,
                            LogMgr::Iterator::Type _type);
        Status seekInternal(const SizedBuf& key,
                            const uint64_t seqnum,
                            SeekOption opt,
                            bool goto_end = false);
        Status moveToLastValid();

        void addLogFileItr(LogFileInfo* l_info);
        inline int cmpSizedBuf(const SizedBuf& l, const SizedBuf& r);
        inline bool checkValidBySeq(ItrItem* item,
                                    const uint64_t cur_seq,
                                    const bool is_prev = false);
        inline bool checkValidByKey(ItrItem* item,
                                    const SizedBuf& cur_key,
                                    const bool is_prev = false);

        Type type;
        LogMgr* lMgr;
        std::vector<ItrItem*> itrs;
        std::list<LogFileInfo*>* snapLogList;
        uint64_t minSeqSnap;
        uint64_t maxSeqSnap;
        SizedBuf startKey;
        SizedBuf endKey;
        avl_tree curWindow;
        avl_node* windowCursor;
    };

protected:
// === TYPES
    using LogFileList = std::list<LogFileInfo*>;
    using SnapMap = std::unordered_map<DB*, LogFileList*>;

// === VARIABLES
    // Backward pointer to parent DB instance.
    DB* parentDb;

    std::atomic<bool> initialized;
    LogMgrOptions opt;
    LogManifest* mani;

    std::recursive_mutex writeMutex;

    OpSema syncSema;
    std::mutex syncMutex;

    OpSema flushSema;

    OpSema reclaimSema;

    std::mutex addNewLogFileMutex;

    std::mutex sMapLock;
    SnapMap sMap;

    /**
     * IOPS.
     * If non-zero, throttling is enabled based on this number.
     */
    std::atomic<double> throttlingRate;

    /**
     * Timer that remembers the last time `throttlingRate`
     * was updated.
     */
    Timer throttlingRateTimer;

    /**
     * Keep the last flushed time.
     */
    Timer lastFlushTimer;

    /**
     * Interval of last two flushes in ms.
     */
    std::atomic<int64_t> lastFlushIntervalMs;

    /**
     * Number of set calls since the last flush.
     */
    std::atomic<int64_t> numSetRecords;

    /**
     * If non-zero, records up to this number (inclusive) will be
     * visible to user.
     */
    std::atomic<uint64_t> visibleSeqBarrier;

    /**
     * Global batch.
     */
    std::atomic<uint64_t> fwdVisibleSeqBarrier;

    /**
     * If `globalBatchFlag == true`, this status contains the current
     * status of the global batch.
     */
    std::shared_ptr<GlobalBatchStatus> globalBatchStatus;

    /**
     * Protect `globalBatchStatus` in a case when reader tries to read
     * the status while global batch executer attempts to clear it.
     */
    std::mutex globalBatchStatusLock;

    /**
     * Number of memory loaded log files.
     */
    std::atomic<uint32_t> numMemtables;

    /**
     * Logger.
     */
    SimpleLogger* myLog;

    /**
     * Verbose logging control for sync.
     */
    VerboseLog vlSync;
};

} // namespace jungle

