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

#include "internal_helper.h"
#include "skiplist.h"
#include "table_writer.h"
#include "worker_mgr.h"

#include <libjungle/jungle.h>

#include "simple_thread_pool.h"

#include <atomic>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

class SimpleLogger;

namespace jungle {

struct DBWrap {
    DBWrap() : db(nullptr), refCount(0) {
        skiplist_init_node(&snode);
    }
    ~DBWrap() {
        skiplist_free_node(&snode);
    }

    static int cmp(skiplist_node *a, skiplist_node *b, void *aux) {
        DBWrap *aa, *bb;
        aa = _get_entry(a, DBWrap, snode);
        bb = _get_entry(b, DBWrap, snode);

        if (aa->path < bb->path) return -1;
        else if (aa->path > bb->path) return 1;
        else {
            if (aa->kvsName < bb->kvsName) return -1;
            else if (aa->kvsName > bb->kvsName) return 1;
            else return 0;
        }
    }

    DB* db;
    std::atomic<uint64_t> refCount;
    std::string path;
    std::string kvsName;
    skiplist_node snode;
};

static GlobalConfig _default_global_config_;

class FlusherQueue;
class GlobalBatchExecutor;

// Singleton class
class DBMgr {
    friend class Flusher;
    friend class CmdHandler;
    friend class Compactor;
    friend class LogReclaimer;

public:
    static const size_t MAX_OP_HISTORY = 360;   // 1 hour.

    static DBMgr* init(const GlobalConfig& config = _default_global_config_);
    static DBMgr* get();
    static DBMgr* getWithoutInit();
    static void destroy();

    Status addLogReclaimer();

    DB* openExisting(const std::string& path, const std::string& kvs_name);
    Status assignNew(DB* db);

    Status close(DB* db);
    Status closeAll(const std::string& path);

    Status addFileToRemove(const std::string& full_path);
    Status popFileToRemove(std::string& full_path);
    Status forceRemoveFiles();

    void updateGlobalTime();

    uint64_t getGlobalTime() const;

    void updateOpHistory(size_t amount = 1);

    bool determineIdleStatus();

    bool setIdleStatus(bool to);

    bool isIdleTraffic() const;

    WorkerMgr* workerMgr() const { return wMgr; }

    FlusherQueue* flusherQueue() const { return fQueue; }

    TableWriterMgr* tableWriterMgr() const { return twMgr; }

    GlobalBatchExecutor* getGlobalBatchExecutor() const { return gbExecutor; }

    GlobalConfig* getGlobalConfig() { return &gConfig; }

    SimpleLogger* getLogger() const { return myLog; }

    simple_thread_pool::ThreadPoolMgr* getTpMgr() { return &tpMgr; }

    void setDebugParams(const DebugParams& to,
                        size_t effective_time_sec = 3600);

    DebugParams getDebugParams() {
        std::lock_guard<std::mutex> l(debugParamsLock);
        return debugParams;
    }

    bool enableDebugCallbacks(bool new_value) {
        bool old_value = debugCbEnabled.load(MOR);
        debugCbEnabled.store(new_value, MOR);
        return old_value;
    }

    bool isDebugParamEffective() const { return !debugParamsTimer.timeout(); }

    bool isDebugCallbackEffective() const { return debugCbEnabled.load(MOR); }

private:
    DBMgr();

    ~DBMgr();

    void printGlobalConfig();

    void initInternal(const GlobalConfig& config);

    // Singleton instance and lock.
    static std::atomic<DBMgr*> instance;
    static std::mutex instanceLock;

    // Global config.
    GlobalConfig gConfig;

    // Map of <DB path, DB wrap instance> pairs.
    skiplist_raw dbMap;
    std::mutex dbMapLock;

    // Worker manager.
    WorkerMgr* wMgr;

    // Async flush request queue.
    FlusherQueue* fQueue;

    // Pending files to be removed.
    std::list<std::string> filesToRemove;
    std::mutex filesToRemoveLock;

    // Table writers pool manager.
    TableWriterMgr* twMgr;

    GlobalBatchExecutor* gbExecutor;

    // Debugging parameters.
    DebugParams debugParams;
    std::mutex debugParamsLock;
    Timer debugParamsTimer;
    std::atomic<bool> debugCbEnabled;

    // For async timer purpose.
    simple_thread_pool::ThreadPoolMgr tpMgr;

    // Global time (epoch in seconds).
    std::atomic<uint64_t> globalTime;

    // Circular history of the number of operations (an entry: 10 seconds).
    // -1: not initialized, should skip.
    std::vector< std::atomic<int64_t>* > opHistory;

    // `true` if the current traffic to this process is idle.
    std::atomic<bool> idleTraffic;

    // Logger.
    SimpleLogger* myLog;
};

} // namespace jungle



