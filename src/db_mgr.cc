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

#include "db_mgr.h"

#include "cmd_handler.h"
#include "compactor.h"
#include "db_internal.h"
#include "flusher.h"
#include "internal_helper.h"
#include "log_reclaimer.h"

#include <set>

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

std::atomic<DBMgr *> DBMgr::instance(nullptr);
std::mutex DBMgr::instanceLock;

DBMgr* DBMgr::init(const GlobalConfig& config) {
    DBMgr* mgr = instance.load(MOR);
    if (!mgr) {
        std::lock_guard<std::mutex> l(instanceLock);
        mgr = instance.load(MOR);
        if (!mgr) {
            mgr = new DBMgr();
            instance.store(mgr, MOR);

            mgr->initInternal(config);
        }
    }
    return mgr;
}

void DBMgr::printGlobalConfig() {
    if (gConfig.itcOpt.timeWindow_sec) {
        _log_info(myLog, "idle time compaction checking window %zu sec, "
                  "lower than %zu iops, adjusted factor %zu",
                  gConfig.itcOpt.timeWindow_sec,
                  gConfig.itcOpt.iopsThreshold,
                  gConfig.itcOpt.factor);
        if (gConfig.itcOpt.startHour == gConfig.itcOpt.endHour) {
            _log_info(myLog, "idle time compaction will happen regardless of time");
        } else {
            int tz_gap = SimpleLoggerMgr::getTzGap();
            int tz_gap_abs = (tz_gap < 0) ? (tz_gap * -1) : (tz_gap);
            _log_info(myLog, "idle time compaction time: %02zu to %02zu, %c%02d:%02d",
                      gConfig.itcOpt.startHour, gConfig.itcOpt.endHour,
                      (tz_gap >= 0)?'+':'-', tz_gap_abs / 60, tz_gap_abs % 60);
        }

    } else {
        _log_info(myLog, "idle time compaction disabled");
    }

    _log_info(myLog, "compaction throttling resolution %zu ms factor %zu",
              gConfig.ctOpt.resolution_ms,
              gConfig.ctOpt.throttlingFactor);
}

void DBMgr::initInternal(const GlobalConfig& config) {
    updateGlobalTime();

    gConfig = config;

    std::string log_file = gConfig.globalLogPath + "/jungle_global.log";
    myLog = new SimpleLogger(log_file, 1024, 32*1024*1024, 4);
    myLog->setLogLevel(4);
    myLog->setDispLevel(-1);
    myLog->start();

    printGlobalConfig();

    for (size_t ii=0; ii<config.numFlusherThreads; ++ii) {
        std::string t_name = "flusher_" + std::to_string(ii);
        Flusher* flusher = new Flusher(t_name, config);
        if (gConfig.dedicatedSyncOnlyFlusher && ii > 0) {
            flusher->handleSyncOnly = false;
        }
        wMgr->addWorker(flusher);
        flusher->run();
    }
    _log_info(myLog, "%zu flusher threads, sleep time %zu ms",
              config.numFlusherThreads,
              config.flusherSleepDuration_ms);

    for (size_t ii=0; ii<config.numCompactorThreads; ++ii) {
        std::string t_name = "compactor_" + std::to_string(ii);
        Compactor* compactor = new Compactor(t_name, config);
        wMgr->addWorker(compactor);
        compactor->run();
    }
    _log_info(myLog, "%zu compactor threads, sleep time %zu ms",
              config.numCompactorThreads,
              config.compactorSleepDuration_ms);
    _log_info(myLog, "auto sync by flusher: %s, "
              "min number of records to flush: %zu",
              get_on_off_str(config.flusherAutoSync),
              config.flusherMinRecordsToTrigger);

    {
        CmdHandler* cmd_handler = new CmdHandler("cmd_handler", config);
        wMgr->addWorker(cmd_handler);
        cmd_handler->run();
        _log_info(myLog, "initiated cmd handler thread");
    }

    simple_thread_pool::ThreadPoolOptions tp_opt;
    tp_opt.numInitialThreads = 0; // main thread only.
    tpMgr.init(tp_opt);
    _log_info(myLog, "initiated async timer");

    twMgr->init();
}

DBMgr* DBMgr::get() {
    DBMgr* mgr = instance.load(MOR);
    if (!mgr) {
        return init();
    }
    return mgr;
}

DBMgr* DBMgr::getWithoutInit() {
    DBMgr* mgr = instance.load(MOR);
    return mgr;
}

void DBMgr::destroy() {
    std::lock_guard<std::mutex> l(instanceLock);
    DBMgr* mgr = instance.load(MOR);
    if (mgr) {
        for (size_t ii = 0; ii < MAX_OP_HISTORY; ++ii) {
            _log_trace(mgr->myLog, "[%zu] %zd",
                       ii, mgr->opHistory[ii]->load());
        }

        delete mgr;
        instance.store(nullptr, MOR);
    }
}


DBMgr::DBMgr()
    : wMgr(new WorkerMgr())
    , fQueue(new FlusherQueue())
    , twMgr(new TableWriterMgr())
    , gbExecutor(new GlobalBatchExecutor())
    , debugCbEnabled(false)
    , idleTraffic(false)
    , myLog(nullptr)
{
    updateGlobalTime();

    skiplist_init(&dbMap, DBWrap::cmp);

    for (size_t ii = 0; ii < MAX_OP_HISTORY; ++ii) {
        opHistory.push_back( new std::atomic<int64_t>(-1) );
    }
}

DBMgr::~DBMgr() {
    _log_info(myLog, "destroying DB manager");
    {   // Remove all pending files.
        std::lock_guard<std::mutex> l(filesToRemoveLock);
        for (auto& entry: filesToRemove) {
            if (!FileMgr::exist(entry)) continue;
            Timer tt;
            FileMgr::remove(entry);
            _log_info(myLog, "removed pending file %s. %zu us",
                      entry.c_str(), tt.getUs());
        }
        filesToRemove.clear();
    }

    // Should close all workers before closing DBMgr.
    if (wMgr) DELETE(wMgr);
    if (fQueue) DELETE(fQueue);
    if (twMgr) DELETE(twMgr);
    if (gbExecutor) DELETE(gbExecutor);

    skiplist_node* cursor = skiplist_begin(&dbMap);
    while (cursor) {
        DBWrap* db_wrap = _get_entry(cursor, DBWrap, snode);
        cursor = skiplist_next(&dbMap, cursor);
        delete db_wrap;
    }
    skiplist_free(&dbMap);

    tpMgr.shutdown();

    for (auto& entry: opHistory) delete entry;

    DELETE(myLog);
}

Status DBMgr::addLogReclaimer() {
    std::string t_name = "reclaimer_0";
    // Check if it already exists.
    WorkerBase* existing = wMgr->getWorker(t_name);
    if (existing) return Status::ALREADY_EXIST;

    LogReclaimer* reclaimer = new LogReclaimer(t_name, gConfig);
    wMgr->addWorker(reclaimer);
    reclaimer->run();

    _log_info( myLog, "added log file reclaimer thread, "
               "sleep time %zu sec",
               gConfig.logFileReclaimerSleep_sec );

    return Status();
}

DB* DBMgr::openExisting(const std::string& path, const std::string& kvs_name) {
    std::lock_guard<std::mutex> l(dbMapLock);

    DBWrap query;
    query.path = path;
    query.kvsName = kvs_name;

    skiplist_node* cursor = skiplist_find(&dbMap, &query.snode);
    if (!cursor) {
        return nullptr;
    }

    DBWrap* db_wrap = _get_entry(cursor, DBWrap, snode);
    db_wrap->refCount.fetch_add(1, MOR);
    skiplist_release_node(&db_wrap->snode);
    return db_wrap->db;
}

Status DBMgr::assignNew(DB* db) {
    DBWrap* db_wrap = nullptr;

    std::lock_guard<std::mutex> l(dbMapLock);

    DBWrap query;
    query.path = db->p->path;
    query.kvsName = db->p->kvsName;

    skiplist_node* cursor = skiplist_find(&dbMap, &query.snode);
    if (cursor) {
        skiplist_release_node(cursor);
        return Status::ALREADY_EXIST;
    }

    db_wrap = new DBWrap();
    db_wrap->db = db;
    db_wrap->path = db->p->path;
    db_wrap->kvsName = db->p->kvsName;
    db_wrap->refCount.fetch_add(1, MOR);
    skiplist_insert(&dbMap, &db_wrap->snode);

    db->p->wrapper = db_wrap;

    return Status();
}

Status DBMgr::close(DB* db) {
    DBWrap* db_wrap = db->p->wrapper;
    if (!db_wrap)
        return Status();

    if (db_wrap->refCount.load(MOR) == 0)
        return Status::ALREADY_CLOSED;

    uint64_t expected = 1;
    uint64_t val = 0;
    if (db_wrap->refCount.compare_exchange_weak(expected, val)) {
        // Destroy DB handle.
        _log_debug(db->p->myLog, "Destroy DB %p", db);
        std::lock_guard<std::mutex> l(dbMapLock);
        skiplist_erase_node(&dbMap, &db_wrap->snode);
        skiplist_wait_for_free(&db_wrap->snode);
        if (db->p->dbGroup) {
            // This is a default handle of group handle.
            // Delete group handle as well.
            delete db->p->dbGroup;
        }
        db->p->destroy();
        delete db_wrap;
        delete db;
    } else {
        uint64_t ref_c = db_wrap->refCount.fetch_sub(1, MOR);
        _log_debug(db->p->myLog, "Decrease DB %p ref_count = %ld", db, ref_c);
    }

    return Status();
}

Status DBMgr::closeAll(const std::string& path) {
    std::lock_guard<std::mutex> l(dbMapLock);

    // Close & free all handles for the given path.
    DBWrap query;
    query.path = path;

    skiplist_node* cursor = skiplist_find(&dbMap, &query.snode);
    while (cursor) {
        DBWrap* db_wrap = _get_entry(cursor, DBWrap, snode);
        cursor = skiplist_next(&dbMap, cursor);

        if (db_wrap->path == path) {
            skiplist_erase_node(&dbMap, &db_wrap->snode);
            skiplist_release_node(&db_wrap->snode);
            skiplist_wait_for_free(&db_wrap->snode);
            db_wrap->db->p->destroy();
            delete db_wrap->db;
            delete db_wrap;
        } else {
            skiplist_release_node(&db_wrap->snode);
            break;
        }
        skiplist_release_node(&db_wrap->snode);
    }
    if (cursor) skiplist_release_node(cursor);

    return Status();
}

Status DBMgr::addFileToRemove(const std::string& full_path) {
    if (!FileMgr::exist(full_path)) return Status::FILE_NOT_EXIST;

    std::lock_guard<std::mutex> l(filesToRemoveLock);
    filesToRemove.push_back(full_path);

    return Status();
}

Status DBMgr::popFileToRemove(std::string& full_path) {
    std::lock_guard<std::mutex> l(filesToRemoveLock);
    auto entry = filesToRemove.begin();
    if (entry == filesToRemove.end()) return Status::FILE_NOT_EXIST;

    full_path = *entry;
    filesToRemove.pop_front();
    return Status();
}

Status DBMgr::forceRemoveFiles() {
    // Remove all files in queue (blocking).
    std::lock_guard<std::mutex> l(filesToRemoveLock);
    for (const std::string& full_path: filesToRemove) {
        Timer tt;
        FileMgr::remove(full_path);
        _log_info( getLogger(),
                   "force removed pending file %s, %zu us",
                   full_path.c_str(), tt.getUs() );
    }
    filesToRemove.clear();
    return Status();
}

void DBMgr::setDebugParams(const DebugParams& to,
                           size_t effective_time_sec)
{
    std::lock_guard<std::mutex> l(debugParamsLock);
    debugParams = to;
    debugParamsTimer.setDurationMs(effective_time_sec * 1000);
    debugParamsTimer.reset();
    _log_warn(myLog, "new debugging parameters (effective %zu seconds): "
              "compaction delay %zu %zu, "
              "urgent compaction size %zu, "
              "urgent compaction ratio %zu, "
              "urgent compaction writes %zu, "
              "rollback delay %zu",
              effective_time_sec,
              debugParams.compactionDelayUs,
              debugParams.compactionItrScanDelayUs,
              debugParams.urgentCompactionFilesize,
              debugParams.urgentCompactionRatio,
              debugParams.urgentCompactionNumWrites,
              debugParams.rollbackDelayUs);
}

void DBMgr::updateGlobalTime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    globalTime.store(tv.tv_sec, MOR);
    _log_trace(myLog, "updated global time: %zu", tv.tv_sec);
}

uint64_t DBMgr::getGlobalTime() const {
    return globalTime.load(MOR);
}

void DBMgr::updateOpHistory(size_t amount) {
    uint64_t g_time = getGlobalTime();
    uint64_t idx = (g_time / 10) % MAX_OP_HISTORY;
    int64_t prev = opHistory[idx]->load(MOR);
    if ( prev == -1 ) {
        int64_t exp = -1;
        int64_t des = (int64_t)amount;
        if (opHistory[idx]->compare_exchange_weak(exp, des, MOR) ) {
            uint64_t next_idx = (idx + 1) % MAX_OP_HISTORY;
            opHistory[next_idx]->store(-1);
            return;
        }
    }
    opHistory[idx]->fetch_add(amount, MOR);
}

bool DBMgr::determineIdleStatus() {
    if (!gConfig.itcOpt.timeWindow_sec) return false;

    if (gConfig.itcOpt.startHour < gConfig.itcOpt.endHour) {
        // e.g.) 01 - 04: from 01:00:00 to 03:59:59.
        SimpleLoggerMgr::TimeInfo lt = std::chrono::system_clock::now();
        if ( lt.hour < (int)gConfig.itcOpt.startHour ||
             lt.hour >= (int)gConfig.itcOpt.endHour ) {
            return false;
        }
    } else {
        // e.g.) 23 - 01: from 23:00:00 to 23:59:59 and
        //                     00:00:00 to 00:59:59.
        SimpleLoggerMgr::TimeInfo lt = std::chrono::system_clock::now();
        if ( lt.hour > (int)gConfig.itcOpt.endHour &&
             lt.hour < (int)gConfig.itcOpt.startHour ) return false;
    }

    uint64_t g_time = getGlobalTime();
    uint64_t s_idx = (g_time / 10) % MAX_OP_HISTORY;
    size_t count = 0;
    for ( int ii = MAX_OP_HISTORY + s_idx; ii > (int)s_idx; --ii ) {
        int idx = ii % MAX_OP_HISTORY;
        int64_t entry = opHistory[idx]->load(MOR);
        if (entry < 0) continue;
        if (entry / 10 > gConfig.itcOpt.iopsThreshold) return false;

        count++;
        if (count >= 3 && count >= gConfig.itcOpt.timeWindow_sec / 10) break;
    }

    if (count >= 3) {
        // We need at least two slots (including the current one)
        // to make sure no false alarm.
        return true;
    }
    return false;
}

bool DBMgr::setIdleStatus(bool to) {
    bool prev = idleTraffic.load(MOR);
    if (prev != to) {
        return idleTraffic.compare_exchange_weak(prev, to, MOR);
    }
    return false;
}

bool DBMgr::isIdleTraffic() const {
    return idleTraffic.load(MOR);
}

} // namespace jungle

