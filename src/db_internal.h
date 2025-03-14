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
#include "db_manifest.h"
#include "db_mgr.h"
#include "event_awaiter.h"
#include "fileops_base.h"
#include "internal_helper.h"
#include "log_mgr.h"
#include "table_mgr.h"

#include <libjungle/jungle.h>

#include <atomic>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using simple_thread_pool::TaskHandle;

class SimpleLogger;

namespace jungle {

class DB::DBInternal {
public:
    static const size_t MAX_CLOSE_RETRY = 100;  // 10 seconds.

    struct ThrottlingStats {
        ThrottlingStats()
            : lastLogFlushRate(0)
            , lastTableFlushRate(0)
            , lastSplitRate(0)
        {
            // 10 seconds.
            lastSplitRateExpiry.setDurationMs(10 * 1000);
            lastTableFlushRateExpiry.setDurationMs(10 * 1000);
        }
        // Unit: IOPS.
        std::atomic<double> lastLogFlushRate;

        // Table compaction will not happen frequently. Need expiry.
        Timer lastTableFlushRateExpiry;
        std::atomic<double> lastTableFlushRate;

        // Split will not happen frequently. Need expiry.
        Timer lastSplitRateExpiry;
        std::atomic<double> lastSplitRate;
    };

    struct Flags {
        Flags()
            : onGoingBgTasks(0)
            , closing(false)
            , rollbackInProgress(false)
            , seqLoading(false)
            {}
        std::atomic<size_t> onGoingBgTasks;
        std::atomic<bool> closing;
        std::atomic<bool> rollbackInProgress;
        std::atomic<bool> seqLoading;
    };

    DBInternal();

    ~DBInternal();

    void destroy();

    // Correct contradict attributes.
    void adjustConfig();

    // Wait for on-going background tasks to be finished (or canceled).
    void waitForBgTasks();

    void incBgTask() {
        flags.onGoingBgTasks.fetch_add(1);
    }

    void decBgTask() {
        assert(flags.onGoingBgTasks.load());
        flags.onGoingBgTasks.fetch_sub(1);
    }

    size_t getNumBgTasks() const {
        return flags.onGoingBgTasks.load();
    }

    void updateOpHistory(size_t amount = 1);

    enum OpType {
        OPTYPE_READ = 0x1,
        OPTYPE_WRITE = 0x2,
        OPTYPE_FLUSH = 0x3,
        OPTYPE_COMPACT = 0x4,
    };
    Status checkHandleValidity(OpType op_type = OpType::OPTYPE_READ);

    // DB directory path (e.g., /foo/bar),
    // not including "/" at the end.
    std::string path;

    // Current DB config.
    DBConfig dbConfig;

    // Backward pointer to parent DBGroup.
    // If NULL, it was directly opened using DB::open() without DBGroup.
    DBGroup* dbGroup;

    // Backward pointer to wrapper of this DB instance.
    DBWrap* wrapper;

    // File ops handle, depends on the platform.
    FileOps* fOps;

    // File ops handle based on direct-io
    FileOps* fDirectOps;

    // DB manifest.
    DBManifest* mani;

    // Sub key-value store ID.
    uint64_t kvsID;

    // Sub key-value store name.
    std::string kvsName;

    // Log manager.
    LogMgr* logMgr;

    // Table manager.
    TableMgr* tableMgr;

    // Throttling statistics.
    ThrottlingStats tStats;

    // Logger.
    SimpleLogger* myLog;

    // Verbose logging control for async flush.
    VerboseLog vlAsyncFlush;

    // Flags.
    Flags flags;

    // Last async flush job, if delayed task is scheduled.
    std::shared_ptr<TaskHandle> asyncFlushJob;
    std::mutex asyncFlushJobLock;
};

class DB::SnapInternal {
public:
    SnapInternal(uint64_t _last_flush, uint64_t _chk_num)
        : lastFlush(_last_flush)
        , chkNum(_chk_num)
        , logList(nullptr)
        , tableList(nullptr)
        {}
    uint64_t lastFlush;
    uint64_t chkNum;
    std::list<LogFileInfo*>* logList;
    std::list<TableInfo*>* tableList;
};

class DBGroup::DBGroupInternal {
public:
    DBGroupInternal()
        : defaultDB(nullptr)
        {}

    std::string path;
    DBConfig config;

    DB* defaultDB;
};

class Iterator::IteratorInternal {
public:
    IteratorInternal(Iterator* _parent)
        : db(nullptr)
        , windowCursor(nullptr)
        , parent(_parent)
    {
        avl_init(&curWindow, nullptr);
    }

    enum Type {
        BY_KEY = 0,
        BY_SEQ = 1
    };
    struct ItrItem {
        ItrItem() : flags(0x0), logItr(nullptr), tableItr(nullptr) {}
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

            // Seqnumber cannot be the same in the iterator.
            // Pick log first and then table.
            if (aa->logItr && !bb->logItr) return -1;
            else if (!aa->logItr && bb->logItr) return 1;

            // Cannot happen. Bug.
            assert(0);
            return 0;
        }
        inline static int cmpKey(avl_node *a, avl_node *b, void *aux) {
            ItrItem* aa = _get_entry(a, ItrItem, an);
            ItrItem* bb = _get_entry(b, ItrItem, an);

            CMP_NULL_CHK(aa->lastRec.kv.key.data, bb->lastRec.kv.key.data);

            int cmp = 0;
            if (aux) {
                // Custom cmp mode.
                DB* db = reinterpret_cast<DB*>(aux);
                CustomCmpFunc func = db->p->dbConfig.cmpFunc;
                void* param = db->p->dbConfig.cmpFuncParam;
                cmp = func(aa->lastRec.kv.key.data, aa->lastRec.kv.key.size,
                           bb->lastRec.kv.key.data, bb->lastRec.kv.key.size,
                           param);
            } else {
                cmp = SizedBuf::cmp(aa->lastRec.kv.key, bb->lastRec.kv.key);
            }

            // NOTE:
            //  key: ascending, seq: descending order.
            //  e.g.)
            //  K1 (seq 5), K1 (seq 2), K2 (seq 3), K2 (seq 1) ..
            if (cmp == 0) return cmpSeq(b, a, aux);
            return cmp;
        }
        avl_node an;
        uint8_t flags;
        LogMgr::Iterator* logItr;
        TableMgr::Iterator* tableItr;

        // WARNING: Jungle's iterator doesn't own the memory of `lastRec`.
        //          It is managed by either log iterator or table iterator.
        Record lastRec;
    };

    Status moveToLastValid();

    Status seekInternal(const SizedBuf& key,
                        const uint64_t seqnum,
                        SeekOption opt,
                        bool goto_end = false);

    inline int cmpSizedBuf(const SizedBuf& l, const SizedBuf& r);

    bool checkValidBySeq(ItrItem* item,
                         const uint64_t cur_seq,
                         const bool is_prev = false);
    bool checkValidByKey(ItrItem* item,
                         const SizedBuf& cur_key,
                         const bool is_prev = false);

    Type type;
    DB* db;
    //LogMgr::Iterator logItr;
    std::vector<ItrItem*> itrs;
    avl_tree curWindow;
    avl_node* windowCursor;
    Iterator* parent;

    // Intolerable error detected. If set, we cannot proceed iteration.
    Status fatalError;
};

struct GlobalBatchStatus {
    enum Status {
        /**
         * Global batch is in progress, dirty items shouldn't be visible.
         */
        INVISIBLE = 0,

        /**
         * Global batch is done, dirty items should be visible.
         * It is an intermediate status that cleaning-up of
         * global batch related stuff are in progress.
         */
        VISIBLE = 1,
    };
    GlobalBatchStatus() : batchId(0), curStatus(INVISIBLE) {}
    uint64_t batchId;
    std::atomic<Status> curStatus;
};

class GlobalBatchExecutor {
public:
    GlobalBatchExecutor() : gbIdCounter(1) {}

    Status execute(const GlobalBatch& g_batch);

private:
    std::atomic<uint64_t> gbIdCounter;
};

} // namespace jungle

