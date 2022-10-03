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

#include "log_mgr.h"

#include "db_internal.h"
#include "db_mgr.h"
#include "fileops_base.h"
#include "internal_helper.h"
#include "murmurhash3.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <algorithm>
#include <list>
#include <thread>
#include <vector>

namespace jungle {

LogMgr::LogMgr(DB* parent_db, const LogMgrOptions& lm_opt)
    : parentDb(parent_db)
    , initialized(false)
    , opt(lm_opt)
    , mani(nullptr)
    , throttlingRate(0)
    , lastFlushIntervalMs(0)
    , numSetRecords(0)
    , visibleSeqBarrier(0)
    , fwdVisibleSeqBarrier(0)
    , globalBatchStatus(nullptr)
    , numMemtables(0)
    , myLog(nullptr)
    , vlSync(VERBOSE_LOG_SUPPRESS_MS)
    {}

LogMgr::~LogMgr() {
    assert(sMap.size() == 0);
    delete mani;
}

std::string LogMgr::getManifestFilename() const {
    char m_filename[256];
    sprintf(m_filename, "log%04" PRIu64 "_manifest", opt.prefixNum);
    return m_filename;
}

Status LogMgr::init(const LogMgrOptions& lm_opt) {
    if (mani) return Status::ALREADY_INITIALIZED;

    opt = lm_opt;
    syncSema.enabled = true;
    flushSema.enabled = true;
    reclaimSema.enabled = true;

    Status s;
    mani = new LogManifest(this,
                           opt.fOps,
                           opt.dbConfig->directIoOpt.enabled
                               && FileOps::supportDirectIO()
                           ? opt.fDirectOps : opt.fOps);
    if (!mani) return Status::ALLOCATION_FAILURE;
    mani->setLogger(myLog);

    if (getDbConfig()->logSectionOnly) {
        DBMgr* dbm = DBMgr::getWithoutInit();
        GlobalConfig* g_config = (dbm) ? dbm->getGlobalConfig() : nullptr;
        _log_info(myLog,
                  "log-section mode: "
                  "reclaimer period %u seconds, TTL %u seconds",
                  (g_config) ? g_config->logFileReclaimerSleep_sec : 0,
                  getDbConfig()->logFileTtl_sec);
    }

   try {
    std::string m_filename;
    if (opt.dbConfig->customManifestPath.empty()) {
        // Normal open: manifest file in the same path.
        m_filename = opt.path + "/" + getManifestFilename();
    } else {
        // Custom open: manifest file in the custom path.
        m_filename = opt.dbConfig->customManifestPath + "/" + getManifestFilename();
        _log_info(myLog, "open custom manifest: %s", m_filename.c_str());
    }

    if (opt.fOps->exist(m_filename.c_str())) {
        // Manifest file already exists, load it.
        s = mani->load(opt.path, m_filename, opt.prefixNum);
        if (!s) {
            // Error happened, try again using backup file.
            _log_err(myLog, "loading manifest error: %d, try again", s);
            TC(BackupRestore::restore(opt.fOps, m_filename));
            s = mani->load(opt.path, m_filename, opt.prefixNum);
        }
        if (!s) throw s;

    } else {
        // Not exist, initial setup phase.

        // If `m_filename` is from custom path, should return error.
        if (!opt.dbConfig->customManifestPath.empty()) {
            _log_err(myLog, "custom manifest does not exist: %s", m_filename.c_str());
            throw Status(Status::MANIFEST_NOT_EXIST);
        }

        // Create manifest file.
        TC(mani->create(opt.path, m_filename, opt.prefixNum));

        // Get new log file number, and file name
        uint64_t log_num;
        TC(mani->issueLogFileNumber(log_num));
        std::string l_filename =
                LogFile::getLogFileName(opt.path, opt.prefixNum, log_num);

        // Create a log file and add it to manifest.
        LogFile* l_file = new LogFile(this);
        l_file->setLogger(myLog);

       try {
        TC(l_file->create(l_filename,
                          opt.dbConfig->directIoOpt.enabled
                              && FileOps::supportDirectIO()
                          ? opt.fDirectOps : opt.fOps,
                          log_num,
                          opt.startSeqnum - 1));
        if (opt.startSeqnum > 1) {
            // If start seqnum is given on the initialization,
            // set sync/flush seqnum properly for the first log file.
            l_file->updateMaxSeqNum(opt.startSeqnum - 1);
            l_file->setFlushedSeqNum(opt.startSeqnum - 1);
            l_file->setSyncedSeqNum(opt.startSeqnum - 1);
        }
        TC(mani->addNewLogFile(log_num, l_file, 1));

       } catch (Status s) {
        delete l_file;
        throw s;
       }

        // Sync manifest file.
        mani->store(true);
    }
    mani->setLogger(myLog);

    logMgrSettings();

    removeStaleFiles();

    if (mani->getNumLogFiles() == 0) {
        // No valid log file exists. Start from zero.
        _log_warn(myLog, "no valid log file exists, start from 0");
        std::string l_filename =
                LogFile::getLogFileName(opt.path, opt.prefixNum, 0);
        LogFile* l_file = new LogFile(this);
        l_file->setLogger(myLog);

       try {
        TC(l_file->create(l_filename,
                          opt.dbConfig->directIoOpt.enabled
                              && FileOps::supportDirectIO()
                          ? opt.fDirectOps : opt.fOps,
                          0,
                          0));
        TC(mani->addNewLogFile(0, l_file, 1));

       } catch (Status s) {
        delete l_file;
        throw s;
       }
    }

    initialized = true;
    return Status();

   } catch (Status s) {
    _log_err(myLog, "init manifest error: %d", s);
    DELETE(mani);
    return s;
   }
}

void LogMgr::logMgrSettings() {
    DBMgr* mgr = DBMgr::getWithoutInit();
    // NOTE: `DBMgr` may not be initialized if `LogMgr` is being
    //       used by external tool. We can just ignore it.
    if (!mgr) return;

    GlobalConfig* g_conf = mgr->getGlobalConfig();

    _log_info( myLog,
               "initialized log manager, memtable flush buffer %zu, "
               "direct-IO %s, custom hash length function %s",
               g_conf->memTableFlushBufferSize,
               get_on_off_str(opt.dbConfig->directIoOpt.enabled),
               get_on_off_str((bool)opt.dbConfig->customLenForHash) );
}

Status LogMgr::rollback(uint64_t seq_upto) {
    Status s;

    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    // Return error in read-only mode.
    if (getDbConfig()->readOnly) return Status::WRITE_VIOLATION;

    // WARNING:
    //   Both syncing (memtable -> log) and flushing (log -> table)
    //   should be blocked during rollback.

    Timer tt;
    const size_t MAX_RETRY_MS = 1000; // 1 second.
    tt.setDurationMs(MAX_RETRY_MS);

    OpSemaWrapper ow_sync(&syncSema);
    while (!ow_sync.acquire()) {
        if (tt.timeout()) {
            _log_err(myLog, "rollback timeout due to sync");
            return Status::TIMEOUT;
        }
        Timer::sleepMs(10);
    }
    assert(ow_sync.op_sema->enabled);

    OpSemaWrapper ow_flush(&flushSema);
    while (!ow_flush.acquire()) {
        if (tt.timeout()) {
            _log_err(myLog, "rollback timeout due to flush");
            return Status::TIMEOUT;
        }
        Timer::sleepMs(10);
    }
    assert(ow_flush.op_sema->enabled);

    OpSemaWrapper ow_reclaim(&reclaimSema);
    while (!ow_reclaim.acquire()) {
        if (tt.timeout()) {
            _log_err(myLog, "rollback timeout due to reclaim");
            return Status::TIMEOUT;
        }
        Timer::sleepMs(10);
    }
    assert(ow_reclaim.op_sema->enabled);

    _log_info(myLog, "[ROLLBACK] upto %zu", seq_upto);

    // Set rollback flag, it will be reset on exit of this function.
    GcFunc gc_func( [this]() -> void
                    {this->parentDb->p->flags.rollbackInProgress = false;} );
    this->parentDb->p->flags.rollbackInProgress = true;

    // Should sync first.
    EP( syncInternal(false) );

    // Find corresponding log file.
    LogFileInfo* linfo;
    EP( mani->getLogFileInfoBySeq(seq_upto, linfo) );

    LogFileInfoGuard gg(linfo);
    if (gg.empty() || gg.ptr->isRemoved()) return Status::SEQNUM_NOT_FOUND;

    // Truncate that file.
    EP( gg.file()->truncate(seq_upto) );

    if (d_params.rollbackDelayUs) {
        // If debug parameter is given, sleep here.
        Timer::sleepUs(d_params.rollbackDelayUs);
    }

    gg.file()->setImmutable();
    _log_info(myLog, "[ROLLBACK] truncated log file %zu", linfo->logFileNum);

    // Remove all log files after that.
    uint64_t lf_max = 0;
    mani->getMaxLogFileNum(lf_max);
    for (uint64_t ii = linfo->logFileNum + 1; ii <= lf_max; ++ii) {
        // Avoid loading memtable because of this call.
        LogFileInfoGuard ll(mani->getLogFileInfoP(ii, true));

        // Remove file from manifest.
        mani->removeLogFile(ii);
        _log_info(myLog, "[ROLLBACK] removed log file %ld.", ii);

        if (d_params.rollbackDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.rollbackDelayUs);
        }
    }
    mani->rollbackLogFileNumber(linfo->logFileNum);

    // Adjust manifest, and store.
    mani->setLastSyncedLog(linfo->logFileNum);
    mani->store(true);
    _log_info(myLog, "[ROLLBACK] now %zu is the last seqnum", seq_upto);

    DBMgr::get()->forceRemoveFiles();

    // Reload the entire memtable, to purge rolled-back records.
    linfo->setEvicted();

    return Status();
}

Status LogMgr::removeStaleFiles() {
    // Do nothing in read only mode.
    if (getDbConfig()->readOnly) return Status();

    std::vector<std::string> files;
    FileMgr::scan(opt.path, files);

    char p_num[16];
    sprintf(p_num, "%04" PRIu64, opt.prefixNum);
    std::string prefix = "log";
    prefix += p_num;
    prefix += "_";
    size_t prefix_len = prefix.size();

    std::string m_filename = getManifestFilename();

    bool need_mani_sync = false;
    for (auto& entry: files) {
        std::string& ff = entry;
        size_t pos = ff.find(prefix);
        if ( pos != std::string::npos &&
             ff.find(m_filename) == std::string::npos ) {
            // Check if it is in manifest.
            uint64_t log_num = atoi( ff.substr( prefix_len,
                                                ff.size() - prefix_len ).c_str() );
            if (!mani->logFileExist(log_num)) {
                Timer tt;
                opt.fOps->remove(opt.path + "/" + ff);
                need_mani_sync = true;
                _log_warn(myLog, "%s does not exist in manifest, removed. %zu us",
                          ff.c_str(), tt.getUs());
            }
        }
    }

    if (need_mani_sync) {
        // Should sync manifest file.
        mani->store(true);
        _log_info(myLog, "done manifest sync for stale file removal");
    }

    return Status();
}

bool LogMgr::isLogStoreMode() const {
    if ( getDbConfig()->logSectionOnly &&
         getDbConfig()->logFileTtl_sec ) {
        return true;
    }
    return false;
}

Status LogMgr::openSnapshot(DB* snap_handle,
                            const uint64_t checkpoint,
                            std::list<LogFileInfo*>*& log_file_list_out)
{
    Status s;
    LogFileList* l_list = new LogFileList();
    {   mGuard l(sMapLock);
        sMap.insert( std::make_pair(snap_handle, l_list) );
    }

    uint64_t l_num_min;
    uint64_t l_num_max;

   for (;;) {
    // Special case: empty DB (both checkpoint and last flush are zero).
    if ( !snap_handle->sn->chkNum &&
         !snap_handle->sn->lastFlush ) {
        // Skip this phase, let the list empty.
        break;
    }

    // Get log file numbers.
    EP( mani->getMinLogFileNum(l_num_min) );
    EP( mani->getLogFileNumBySeq(checkpoint, l_num_max) );

    bool log_store_mode = getDbConfig()->logSectionOnly;
    bool retry_init = false;
    for (uint64_t ii = l_num_min; ii <= l_num_max; ++ii) {
        LogFileInfo* l_info = nullptr;
        if (log_store_mode) {
            s = mani->getLogFileInfoSnapshot(ii, l_info);
        } else {
            s = mani->getLogFileInfo(ii, l_info);
        }

        if (!s || l_info->isRemoved()) {
            // `l_num_min` is invalid. Retry.
            // Free all resources.
            for (auto& entry: *l_list) {
                LogFileInfo* item = entry;
                if (log_store_mode) {
                    item->doneForSnapshot();
                } else {
                    item->done();
                }
            }
            if (s) {
                if (log_store_mode) {
                    l_info->doneForSnapshot();
                } else {
                    l_info->done();
                }
            }

            retry_init = true;
            break;
        }
        l_list->push_back(l_info);
    }
    if (retry_init) continue;
    break;
   }

    log_file_list_out = l_list;
    return Status();
}

Status LogMgr::closeSnapshot(DB* snap_handle) {
    Status s;
    bool log_store_mode = getDbConfig()->logSectionOnly;
    LogFileList* l_list = nullptr;
    {   mGuard l(sMapLock);
        auto entry = sMap.find(snap_handle);
        assert(entry != sMap.end());
        l_list = entry->second;
        sMap.erase(entry);
    }

    for (auto& entry: *l_list) {
        LogFileInfo* info = entry;
        if (log_store_mode) {
            info->doneForSnapshot();
        } else {
            info->done();
        }
    }
    delete l_list;
    return Status();
}

Status LogMgr::cloneManifest(DB* snap_handle,
                             const uint64_t checkpoint,
                             const std::string& dst_path,
                             std::list<LogFileInfo*>*& log_file_list_out)
{
    Status s;
    EP( openSnapshot(snap_handle, checkpoint, log_file_list_out) );

    // Clone manifest file only when opening snapshot succeeds.
    EP( mani->clone(dst_path) );

    return Status();
}

Status LogMgr::addNewLogFile(LogFileInfoGuard& cur_log_file_info,
                             LogFileInfoGuard& new_log_file_info)
{
    std::unique_lock<std::mutex> ll(addNewLogFileMutex);
    Status s;

    // Log file is full. Add one more file.
    uint64_t new_log_num = 0;
    uint64_t max_log_num = 0;
    s = mani->getMaxLogFileNum(max_log_num);
    if (s) new_log_num = max_log_num + 1;

    uint64_t log_file_num = cur_log_file_info.ptr->logFileNum;
    if (max_log_num == log_file_num) {
        // WARNING:
        //   We should not make this file immutable here, as there can be
        //   other writer thread that pushes new updates to this file, until
        //   the max log file number is updated below.

        std::string l_filename =
                LogFile::getLogFileName(opt.path, opt.prefixNum, new_log_num);
        LogFile* l_new_file = new LogFile(this);
        l_new_file->setLogger(myLog);
        uint64_t start_seqnum = cur_log_file_info->file->getMaxSeqNum() + 1;
        l_new_file->create( l_filename,
                            opt.dbConfig->directIoOpt.enabled
                                && FileOps::supportDirectIO()
                            ? opt.fDirectOps : opt.fOps,
                            new_log_num,
                            start_seqnum );
        mani->addNewLogFile(new_log_num, l_new_file, start_seqnum);
        s = mani->setMaxLogFileNum(log_file_num, new_log_num);
        assert(s);

        ll.unlock();
        _log_info(myLog, "moved to a new log file %ld, start seq %s",
                  new_log_num, _seq_str(start_seqnum).c_str());

        // Sync manifest file.
        mani->store(false);

    } else {
        // Otherwise, other thread already added a new log file.
        ll.unlock();
    }

    LogFileInfo* lf_info = nullptr;
    do {
        mani->getMaxLogFileNum(new_log_num);
        lf_info = mani->getLogFileInfoP(new_log_num);
    } while (!lf_info || lf_info->isRemoved());

    new_log_file_info = LogFileInfoGuard(lf_info);

    return Status();
}

void LogMgr::execBackPressure(uint64_t elapsed_us) {
    if (throttlingRate > 0) {
        DBMgr* mgr = DBMgr::getWithoutInit();
        GlobalConfig* g_config = mgr->getGlobalConfig();

        // Throttling.
        double exp_us = 1000000.0 / throttlingRate.load();

        size_t effective_time_ms =
            std::min( lastFlushIntervalMs.load(),
                      (int64_t)THROTTLING_EFFECTIVE_TIME_MS );
        size_t num_log_files = mani->getNumLogFiles();
        size_t log_files_limit = g_config->flusherMinLogFilesToTrigger * 2;
        if (num_log_files > log_files_limit) {
            effective_time_ms *= (num_log_files - log_files_limit);
        }

        uint64_t throttle_age_ms = throttlingRateTimer.getUs() / 1000;
        if ( effective_time_ms &&
             throttle_age_ms < effective_time_ms ) {
            // Should consider age.
            exp_us *= (effective_time_ms - throttle_age_ms);
            exp_us /= effective_time_ms;

            double cur_us = elapsed_us;
            if ( exp_us > cur_us ) {
                // Throttle incoming writes.
                double remaining_us = exp_us - cur_us;
                if (remaining_us > 1.0) {
                    Timer::sleepUs((uint64_t)remaining_us);
                }
            }
        }
    }
}

Status LogMgr::setSN(const Record& rec) {
    Timer tt;

    Status s;
    uint64_t log_file_num = 0;
    uint64_t max_log_file_num = 0;
    bool overwrite = false;

    if (parentDb) parentDb->p->updateOpHistory();

    // Return error in read-only mode.
    if (getDbConfig()->readOnly) return Status::WRITE_VIOLATION;

    // seqnum should start from 1.
    if ( rec.seqNum == 0 ) return Status::INVALID_SEQNUM;

    // All writes will be serialized, except for throttling part.
    std::unique_lock<std::recursive_mutex> wm(writeMutex);

    // Get latest log file.
    LogFileInfo* lf_info = nullptr;
    do {
        EP(mani->getMaxLogFileNum(max_log_file_num));
        log_file_num = max_log_file_num;

        if ( valid_number(rec.seqNum) &&
             getDbConfig()->allowOverwriteSeqNum ) {
            // May overwrite existing seqnum, get corresponding log file.
            // NOTE:
            //   We should ignore max seqnum checking here,
            //   as seq numbers may not be consecutive.
            s = mani->getLogFileNumBySeq( rec.seqNum,
                                          log_file_num,
                                          false,
                                          true );
            if (s) overwrite = true;
            // If not exist, use the latest file.
        }
        lf_info = mani->getLogFileInfoP(log_file_num);

    } while (!lf_info || lf_info->isRemoved());
    LogFileInfoGuard g_li(lf_info);

    // If 1) this file is not writable, AND
    //    2) 1) overwrite is not allowed, OR
    //       2) overwrite is allowed, but not overwriting.
    if ( !g_li->file->isValidToWrite() &&
         ( !getDbConfig()->allowOverwriteSeqNum ||
           !overwrite ) ) {
        addNewLogFile(g_li, g_li);

        DBMgr* dbm = DBMgr::getWithoutInit();
        if (dbm && dbm->isDebugCallbackEffective()) {
            DebugParams dp = dbm->getDebugParams();
            if (dp.addNewLogFileCb) {
                DebugParams::GenericCbParams p;
                dp.addNewLogFileCb(p);
            }
        }
    }

    EP(g_li->file->setSN(rec));

    if (g_li->file->isImmutable()) {
        // Overwrote immutable file,
        // reset sync/flush seq num if necessary.
        uint64_t last_synced_log = 0;
        s = mani->getLastSyncedLog(last_synced_log);
        if (s && log_file_num < last_synced_log) {
            mani->setLastSyncedLog(log_file_num);
        }
        uint64_t last_flushed_log = 0;
        s = mani->getLastFlushedLog(last_flushed_log);
        if (s && log_file_num < last_flushed_log) {
            mani->setLastFlushedLog(log_file_num);
        }
    }

    wm.unlock();
    execBackPressure(tt.getUs());

    numSetRecords.fetch_add(1);
    return Status();
}

void LogMgr::lockWriteMutex() {
    writeMutex.lock();
}

void LogMgr::unlockWriteMutex() {
    writeMutex.unlock();
}

Status LogMgr::setMulti(const std::list<Record>& batch) {
    Timer tt;
    Status s;

    if (parentDb) parentDb->p->updateOpHistory();

    // Return error in read-only mode.
    if (getDbConfig()->readOnly) return Status::WRITE_VIOLATION;

    // `batch` should contain at least one record.
    if (!batch.size()) return Status::EMPTY_BATCH;

    // All writes will be serialized, except for throttling part.
    std::unique_lock<std::recursive_mutex> wm(writeMutex); // -----------------

    uint64_t max_seq = 0;
    getMaxSeqNum(max_seq); // empty log should be tolerated.
    setVisibleSeqBarrier(max_seq);
    GcFunc gc_vs( [&]{
        setVisibleSeqBarrier(0);
    } );

    EP( checkBatchValidity(batch) );
    uint64_t dummy64 = 0;
    EP( setMultiInternal(batch, dummy64) );

    gc_vs.gcNow();
    wm.unlock(); // -----------------------------------------------------------

    execBackPressure(tt.getUs());

    return Status();
}

void LogMgr::setVisibleSeqBarrier(uint64_t to) {
    visibleSeqBarrier.store(to, MOR);
}

void LogMgr::setGlobalBatch(uint64_t fwd_barrier,
                            std::shared_ptr<GlobalBatchStatus> status)
{
    {   std::lock_guard<std::mutex> l(globalBatchStatusLock);
        globalBatchStatus = status;
    }
    fwdVisibleSeqBarrier.store(fwd_barrier, MOR);
}

uint64_t LogMgr::getVisibleSeqBarrier() {
    // WARNING: We should cache the barrier as it may change over time.
    uint64_t fwd_barrier = fwdVisibleSeqBarrier.load(MOR);
    if (fwd_barrier) {
        std::shared_ptr<GlobalBatchStatus> status_ptr;
        {   std::lock_guard<std::mutex> l(globalBatchStatusLock);
            status_ptr = globalBatchStatus;
        }

        if ( status_ptr &&
             status_ptr->curStatus.load(MOR) == GlobalBatchStatus::VISIBLE ) {
            // Global batch is done, but the task is doing clean-up.
            // Dirty items should be visible,
            return fwd_barrier;
        }
        // Otherwise,
        // 1) Global batch is in progres, OR
        // 2) The entire process of global batch is done,
        // then the same as the normal case.
    }
    return visibleSeqBarrier.load(MOR);
}

Status LogMgr::checkBatchValidity(const std::list<Record>& batch)
{
    Status s;
    const Record& first_rec = *batch.begin();

    if ( valid_number(first_rec.seqNum) &&
         getDbConfig()->allowOverwriteSeqNum ) {
        // Batch set doesn't allow overwriting existing seq numbers.
        uint64_t dummy_u64 = 0;
        s = mani->getLogFileNumBySeq(first_rec.seqNum, dummy_u64);
        if (s) return Status::INVALID_SEQNUM;
    }

    // Check given sequence numbers.
    // It should be
    //   1) all NOT_INITIALIZED or
    //   2) increasing order greater than the current max.
    if (!valid_number(first_rec.seqNum)) {
        for (auto& entry: batch) {
            const Record& rr = entry;
            if (valid_number(rr.seqNum)) return Status::INVALID_SEQNUM;
        }
    } else {
        uint64_t prev_seq = 0;
        getMaxSeqNum(prev_seq); // At least bigger than max seq.
        for (auto& entry: batch) {
            const Record& rr = entry;
            if ( !valid_number(rr.seqNum) ||
                 rr.seqNum <= prev_seq ) {
                return Status::INVALID_SEQNUM;
            }
            prev_seq = rr.seqNum;
        }
    }
    return Status();
}

Status LogMgr::setMultiInternal(const std::list<Record>& batch,
                                uint64_t& max_seq_out)
{
    Status s;
    DBMgr* dbm = DBMgr::getWithoutInit();

    // Get latest log file.
    uint64_t max_log_file_num = 0;
    LogFileInfo* lf_info = nullptr;
    do {
        EP(mani->getMaxLogFileNum(max_log_file_num));
        lf_info = mani->getLogFileInfoP(max_log_file_num);
    } while (!lf_info || lf_info->isRemoved());
    LogFileInfoGuard g_li(lf_info);

    for (auto& entry: batch) {
        const Record& rr = entry;
        EP( g_li->file->setSN(rr) );
        // WARNING:
        //   During an atomic batch, we will not move to next
        //   log file to make rollback easier.
    }
    max_seq_out = g_li->file->getMaxSeqNum();

    if (dbm && dbm->isDebugCallbackEffective()) {
        DebugParams dp = dbm->getDebugParams();
        if (dp.newLogBatchCb) {
            dp.newLogBatchCb(DebugParams::GenericCbParams());
        }
    }
    numSetRecords.fetch_add(batch.size());
    return Status();
}

Status LogMgr::getSN(const uint64_t seq_num, Record& rec_out) {
    Status s;
    LogFileInfo* linfo;

    if (parentDb) parentDb->p->updateOpHistory();

    EP( mani->getLogFileInfoBySeq(seq_num, linfo) );
    LogFileInfoGuard gg(linfo);
    if (gg.empty() || gg.ptr->isRemoved()) {
        return Status::KEY_NOT_FOUND;
    }

    uint64_t max_seq = gg->file->getMaxSeqNum();
    uint64_t visible_seq_barrier = getVisibleSeqBarrier();
    if ( seq_num > max_seq ||
         ( visible_seq_barrier &&
           seq_num > visible_seq_barrier ) ) {
        return Status::KEY_NOT_FOUND;
    }

    Record rec_out_local;
    EP( gg->file->getSN(seq_num, rec_out_local) );
    rec_out_local.copyTo(rec_out);
    return Status();
}

Status LogMgr::get(const uint64_t chk,
                   std::list<LogFileInfo*>* l_list,
                   const SizedBuf& key,
                   Record& rec_out)
{
    Status s;
    uint64_t min_log_num, max_log_num;
    const DBConfig* db_config = getDbConfig();

    if (parentDb) parentDb->p->updateOpHistory();

    Record rec_out_local;
    uint64_t chk_local = chk;
    if (valid_number(chk)) {
        // Snapshot: beyond the last flushed log.
        assert(l_list);
        auto entry = l_list->rbegin();
        while (entry != l_list->rend()) {
            LogFileInfo* l_info = *entry;
            s = l_info->file->get(chk_local, key, rec_out_local, true, true);
            if (s) {
                rec_out_local.copyTo(rec_out);
                return s;
            }
            entry++;
        }
    } else {
        // Normal: from the last flushed log.
        EP( mani->getLastFlushedLog(min_log_num) );
        EP( mani->getMaxLogFileNum(max_log_num) );

        if (db_config->logSectionOnly) {
            // Log only mode: searching skiplist one-by-one.
            for (int64_t ii = max_log_num; ii >= (int64_t)min_log_num; --ii) {
                LogFileInfoGuard li(mani->getLogFileInfoP(ii));
                if (li.empty() || li.ptr->isRemoved()) continue;
                s = li->file->get(chk_local, key, rec_out_local, false, true);
                if (s) {
                    rec_out_local.copyTo(rec_out);
                    return s;
                }
            }

        } else {
            // Get whole list and then find,
            // to reduce skiplist overhead.
            std::vector<LogFileInfo*> l_files;
            EP( mani->getLogFileInfoRange(min_log_num, max_log_num, l_files) );
            size_t num = l_files.size();
            if (!num) return Status::KEY_NOT_FOUND;

            bool found = false;
            for (int ii = num-1; ii>=0; --ii) {
                LogFileInfo* l_info = l_files[ii];
                if (l_info->isRemoved()) continue;

                // Reserve max seq number as a temporary snapshot.
                chk_local = l_info->file->getMaxSeqNum();

                // WARNING: Fetching `visibleSeqBarrier` should be done AFTER
                //          fetching max seq number.
                uint64_t visible_seq_barrier = getVisibleSeqBarrier();
                if (visible_seq_barrier) {
                    chk_local = std::min(chk_local, visible_seq_barrier);
                }

                s = l_info->file->get(chk_local, key, rec_out_local, false, true);
                if (s) {
                    rec_out_local.copyTo(rec_out);
                    found = true;
                    break;
                }
            }

            for (LogFileInfo* ll: l_files) ll->done();
            if (found) return Status();
        }
    }
    return Status::KEY_NOT_FOUND;
}

Status LogMgr::getNearest(const uint64_t chk,
                          std::list<LogFileInfo*>* l_list,
                          const SizedBuf& key,
                          Record& rec_out,
                          SearchOptions s_opt)
{
    Status s;
    uint64_t min_log_num, max_log_num;

    if (parentDb) parentDb->p->updateOpHistory();

    Record cur_nearest;
    CustomCmpFunc custom_cmp = getDbConfig()->cmpFunc;
    auto cmp_func = [&](const SizedBuf& a, const SizedBuf& b) -> int {
        if (custom_cmp) {
            return custom_cmp( a.data, a.size, b.data, b.size,
                               getDbConfig()->cmpFuncParam );
        } else {
            return SizedBuf::cmp(a, b);
        }
    };

    auto update_cur_nearest = [&](const Record& rec_out) {
        bool update_cur_nearest = false;
        if (cur_nearest.kv.key.empty()) {
            update_cur_nearest = true;
        } else {
            if (s_opt.isGreater()) {
                update_cur_nearest = (cmp_func(rec_out.kv.key, cur_nearest.kv.key) < 0);
            } else if (s_opt.isSmaller()) {
                update_cur_nearest = (cmp_func(cur_nearest.kv.key, rec_out.kv.key) < 0);
            }
        }
        if (update_cur_nearest) {
            cur_nearest = rec_out;
        }
    };

    // NOTE:
    //   Since it is log and we scan file backward,
    //   we should return once we see the first exact match.
    Record rec_out_local;
    uint64_t chk_local = chk;
    if (valid_number(chk)) {
        // Snapshot: beyond the last flushed log.
        assert(l_list);
        auto entry = l_list->rbegin();
        while (entry != l_list->rend()) {
            LogFileInfo* l_info = *entry;
            s = l_info->file->getNearest(chk_local, key, rec_out_local,
                                         true, true, s_opt);
            if (s) {
                update_cur_nearest(rec_out_local);
                if (cmp_func(key, cur_nearest.kv.key) == 0) {
                    rec_out_local.copyTo(rec_out);
                    return s;
                }
            }
            entry++;
        }
    } else {
        // Normal: from the last flushed log.
        EP( mani->getLastFlushedLog(min_log_num) );
        EP( mani->getMaxLogFileNum(max_log_num) );

        if (getDbConfig()->logSectionOnly) {
            // Log only mode: searching skiplist one-by-one.
            for (int64_t ii = max_log_num; ii >= (int64_t)min_log_num; --ii) {
                LogFileInfoGuard li(mani->getLogFileInfoP(ii));
                if (li.empty() || li.ptr->isRemoved()) continue;
                s = li->file->getNearest(chk_local, key, rec_out_local,
                                         false, true, s_opt);
                if (s) {
                    update_cur_nearest(rec_out_local);
                    if (cmp_func(key, cur_nearest.kv.key) == 0) {
                        rec_out_local.copyTo(rec_out);
                        return s;
                    }
                }
            }

        } else {
            // Get whole list and then find,
            // to reduce skiplist overhead.
            std::vector<LogFileInfo*> l_files;
            EP( mani->getLogFileInfoRange(min_log_num, max_log_num, l_files) );
            size_t num = l_files.size();
            if (!num) return Status::KEY_NOT_FOUND;

            bool found = false;
            for (int ii = num-1; ii>=0; --ii) {
                LogFileInfo* l_info = l_files[ii];
                if (l_info->isRemoved()) continue;

                // Reserve max seq number as a temporary snapshot.
                chk_local = l_info->file->getMaxSeqNum();

                // WARNING: Fetching `visibleSeqBarrier` should be done AFTER
                //          fetching max seq number.
                uint64_t visible_seq_barrier = getVisibleSeqBarrier();
                if (visible_seq_barrier) {
                    chk_local = std::min(chk_local, visible_seq_barrier);
                }

                s = l_info->file->getNearest(chk_local, key, rec_out_local,
                                             false, true, s_opt);
                if (s) {
                    update_cur_nearest(rec_out_local);
                    if ( s_opt.isExactMatchAllowed() &&
                         cmp_func(key, cur_nearest.kv.key) == 0 ) {
                        // If `equal` is allowed, then return the
                        // first found exact match.
                        rec_out_local.copyTo(rec_out);
                        found = true;
                        break;
                    }
                }
            }

            for (LogFileInfo* ll: l_files) ll->done();
            if (found) return Status();
        }
    }

    if (cur_nearest.kv.key.empty()) {
        return Status::KEY_NOT_FOUND;
    }

    // Just in case.
    if (s_opt.isGreater()) {
        if (cmp_func(cur_nearest.kv.key, key) < 0) return Status::KEY_NOT_FOUND;
    } else if (s_opt.isSmaller()) {
        if (cmp_func(key, cur_nearest.kv.key) < 0) return Status::KEY_NOT_FOUND;
    }

    cur_nearest.copyTo(rec_out);
    return Status();
}

Status LogMgr::getPrefix(const uint64_t chk,
                         std::list<LogFileInfo*>* l_list,
                         const SizedBuf& prefix,
                         SearchCbFunc cb_func)
{
    Status s;
    uint64_t min_log_num, max_log_num;
    const DBConfig* db_config = getDbConfig();

    if (parentDb) parentDb->p->updateOpHistory();

    uint64_t chk_local = chk;
    if (valid_number(chk)) {
        // Snapshot: beyond the last flushed log.
        assert(l_list);
        auto entry = l_list->rbegin();
        while (entry != l_list->rend()) {
            LogFileInfo* l_info = *entry;
            s = l_info->file->getPrefix(chk_local, prefix, cb_func, true, true);
            if (s == Status::OPERATION_STOPPED) return s;
            entry++;
        }
    } else {
        // Normal: from the last flushed log.
        EP( mani->getLastFlushedLog(min_log_num) );
        EP( mani->getMaxLogFileNum(max_log_num) );

        if (db_config->logSectionOnly) {
            // Log only mode: searching skiplist one-by-one.
            for (int64_t ii = max_log_num; ii >= (int64_t)min_log_num; --ii) {
                LogFileInfoGuard li(mani->getLogFileInfoP(ii));
                if (li.empty() || li.ptr->isRemoved()) continue;
                s = li->file->getPrefix(chk_local, prefix, cb_func, false, true);
                if (s == Status::OPERATION_STOPPED) return s;
            }

        } else {
            // Get whole list and then find,
            // to reduce skiplist overhead.
            std::vector<LogFileInfo*> l_files;
            EP( mani->getLogFileInfoRange(min_log_num, max_log_num, l_files) );
            size_t num = l_files.size();
            if (!num) return Status::KEY_NOT_FOUND;

            bool stopped = false;
            for (int ii = num-1; ii>=0; --ii) {
                LogFileInfo* l_info = l_files[ii];
                if (l_info->isRemoved()) continue;

                // Reserve max seq number as a temporary snapshot.
                chk_local = l_info->file->getMaxSeqNum();

                // WARNING: Fetching `visibleSeqBarrier` should be done AFTER
                //          fetching max seq number.
                uint64_t visible_seq_barrier = getVisibleSeqBarrier();
                if (visible_seq_barrier) {
                    chk_local = std::min(chk_local, visible_seq_barrier);
                }

                s = l_info->file->getPrefix(chk_local, prefix, cb_func, false, true);
                if (s == Status::OPERATION_STOPPED) {
                    stopped = true;
                    break;
                }
            }

            for (LogFileInfo* ll: l_files) ll->done();
            if (stopped) return Status::OPERATION_STOPPED;
        }
    }

    return Status::OK;
}

Status LogMgr::sync(bool call_fsync) {
    std::lock_guard<std::mutex> l(syncMutex);
    return syncNoWait(call_fsync);
}

Status LogMgr::syncNoWait(bool call_fsync) {
    // Return error in read-only mode.
    if (getDbConfig()->readOnly) return Status::WRITE_VIOLATION;

    // Only one sync operation at a time.
    OpSemaWrapper ow(&syncSema);
    if (!ow.acquire()) {
        _log_debug(myLog, "Sync failed. Other thread is working on it.");
        return Status::OPERATION_IN_PROGRESS;
    }
    assert(ow.op_sema->enabled);
    return syncInternal(call_fsync);
}

Status LogMgr::syncInternal(bool call_fsync) {
    Status s;
    uint64_t ln_from, ln_to;
    s = mani->getMaxLogFileNum(ln_to);
    if (!s) {
        // No log, do nothing.
        return Status();
    }
    s = mani->getLastSyncedLog(ln_from);
    if (!s) {
        // Checkpointing (memtable -> logs) never happend.
        // Start from the first log file.
        EP( mani->getMinLogFileNum(ln_from) );
    }

    // Selective logging based on timer, to avoid verbose messages.
    int num_suppressed = 0;
    SimpleLogger::Levels log_level = vlSync.checkPrint(num_suppressed)
                                     ? SimpleLogger::INFO
                                     : SimpleLogger::DEBUG;
    num_suppressed = (myLog && myLog->getLogLevel() >= SimpleLogger::DEBUG)
                     ? 0 : num_suppressed;

    if (ln_from + 2 <= ln_to) {
        // Big sync (across 3 log files), leave log message.
        log_level = SimpleLogger::INFO;
    }
    _log_(log_level, myLog, "sync log file %zu - %zu (fsync = %s), "
          "%d suppressed messages",
          ln_from, ln_to, call_fsync ? "true" : "false",
          num_suppressed);

    uint64_t last_synced_log = ln_from;
    uint64_t seq_barrier = getVisibleSeqBarrier();
    for (uint64_t ii=ln_from; ii<=ln_to; ++ii) {
        // Write log file first
        LogFileInfoGuard li(mani->getLogFileInfoP(ii));
        if (li.empty() || li.ptr->isRemoved()) continue;

        uint64_t before_sync = li->file->getSyncedSeqNum();
        EP( li->file->flushMemTable( seq_barrier ? seq_barrier : NOT_INITIALIZED ) );
        uint64_t after_sync = li->file->getSyncedSeqNum();
        _log_( log_level, myLog, "synced log file %zu, min seq %s, "
               "flush seq %s, sync seq %s -> %s, max seq %s",
               ii,
               _seq_str( li->file->getMinSeqNum() ).c_str(),
               _seq_str( li->file->getFlushedSeqNum() ).c_str(),
               _seq_str( before_sync ).c_str(),
               _seq_str( after_sync ).c_str(),
               _seq_str( li->file->getMaxSeqNum() ).c_str() );
        if (call_fsync) {
            EP( li->file->sync() );
        }
        if (valid_number(after_sync)) {
            last_synced_log = ii;
        }
        // Set this file immutable, if it is not the last file.
        if (ii < ln_to) {
            li->file->setImmutable();
        }
    }

    // Sync up manifest file next
    mani->setLastSyncedLog(last_synced_log);
    EP( mani->store(call_fsync) );
    _log_(log_level, myLog, "updated log manifest file.");

    return Status();
}

Status LogMgr::discardDirty(uint64_t seq_begin) {
    // Return error in read-only mode.
    if (getDbConfig()->readOnly) return Status::WRITE_VIOLATION;

    // Should not race with sync.
    OpSemaWrapper ow(&syncSema);
    if (!ow.acquire()) {
        _log_debug(myLog, "discard failed. Other thread is working on it.");
        return Status::OPERATION_IN_PROGRESS;
    }
    assert(ow.op_sema->enabled);

    Status s;
    uint64_t ln_from, ln_to;
    s = mani->getMaxLogFileNum(ln_to);
    if (!s) {
        // No log, do nothing.
        return Status();
    }

    uint64_t last_synced_seq = 0;
    getLastSyncedSeqNum(last_synced_seq);
    if (!valid_number(last_synced_seq)) {
        // Nothing has been synced yet.
        last_synced_seq = 0;
    }

    EP( mani->getLogFileNumBySeq(seq_begin, ln_from) );
    _log_info(myLog, "discard logs from %zu, file from %zu to %zu",
              seq_begin, ln_from, ln_to);

    for (uint64_t ii=ln_from; ii<=ln_to; ++ii) {
        // Write log file first
        LogFileInfoGuard li(mani->getLogFileInfoP(ii));
        if (li.empty() || li.ptr->isRemoved()) continue;

        li->file->discardDirty(seq_begin);
        _log_info( myLog, "discarded logs from log file %zu, min seq %s, "
                   "flush seq %s, sync seq %s, max seq %s",
                   ii,
                   _seq_str( li->file->getMinSeqNum() ).c_str(),
                   _seq_str( li->file->getFlushedSeqNum() ).c_str(),
                   _seq_str( li->file->getSyncedSeqNum() ).c_str(),
                   _seq_str( li->file->getMaxSeqNum() ).c_str() );
    }
    return Status();
}

Status LogMgr::flush(const FlushOptions& options,
                     const uint64_t seq_num,
                     TableMgr* table_mgr)
{
    if (!seq_num) {
        // Zero sequence number is not allowed.
        return Status::INVALID_SEQNUM;
    }

    OpSemaWrapper ow(&flushSema);
    if (!ow.acquire()) {
        _log_debug(myLog, "Flush skipped. Other thread is working on it.");
        return Status::OPERATION_IN_PROGRESS;
    }
    assert(ow.op_sema->enabled);

    Status s;
    Timer tt;

    // Grab all logs and pass them to table manager
    uint64_t ln_from, ln_to, ln_to_original;
    mani->getLastFlushedLog(ln_from);
    if (options.beyondLastSync) {
        // Upto the latest log.
        mani->getMaxLogFileNum(ln_to);
    } else {
        // Upto the last synced log.
        mani->getLastSyncedLog(ln_to);
    }

    if (ln_to == NOT_INITIALIZED) {
        // Sync (memtable -> logs) never happend, cannot flush.
        return Status::LOG_NOT_SYNCED;
    }
    if (ln_from == NOT_INITIALIZED) {
        // Flush (logs -> tables) never happend.
        // Flush from the first log file.
        EP( mani->getMinLogFileNum(ln_from) );
    }

    ln_to_original = ln_to;
    if ( options.numFilesLimit &&
         ln_to - ln_from + 1 > options.numFilesLimit ) {
        ln_to = ln_from + options.numFilesLimit - 1;
    }

    uint64_t seq_num_local = seq_num;
    uint64_t seq_barrier = getVisibleSeqBarrier();
    if (seq_barrier && seq_barrier < seq_num_local) {
        seq_num_local = seq_barrier;
    }

    if (seq_num_local == NOT_INITIALIZED) {
        // Purge all synced (checkpointed) logs.
        LogFileInfoGuard ll(mani->getLogFileInfoP(ln_to, true));
        if (options.beyondLastSync) {
            seq_num_local = ll->file->getMaxSeqNum();
        } else {
            seq_num_local = ll->file->getSyncedSeqNum();
        }

    } else {
        // Not all logs, need to adjust `ln_to`.
        seq_num_local = seq_num;
        // If in purge only mode, don't need to load mem table.
        mani->getLogFileNumBySeq(seq_num_local, ln_to, options.purgeOnly);
    }
    _log_debug(myLog, "Given seq upto %s, actual seq upto %ld",
               _seq_str(seq_num).c_str(), seq_num_local);

    {   // Compare given seq with the last flushed seq.
        LogFileInfoGuard ll(mani->getLogFileInfoP(ln_from, true));
        uint64_t last_flushed_seq = ll->file->getFlushedSeqNum();
        if (valid_number(last_flushed_seq) && last_flushed_seq >= seq_num) {
            // Already flushed. Do nothing.
            return Status::ALREADY_FLUSHED;
        }
    }

    uint64_t num_records_flushed = 0;
    if (!options.purgeOnly) {
        std::list<Record*> records;
        std::list<uint64_t> checkpoints;
        bool increasing_order = true;

        for (uint64_t ii = ln_from; ii <= ln_to; ++ii) {
            LogFileInfoGuard ll(mani->getLogFileInfoP(ii));
            s = ll->file->getLogsToFlush( seq_num_local,
                                          records,
                                          options.beyondLastSync );
            if (!s) _log_warn(myLog, "s: %d", s);
            ll->file->getCheckpoints(seq_num_local, checkpoints);
            increasing_order = increasing_order && ll->file->isIncreasingOrder();
        }
        num_records_flushed = records.size();
        _log_info( myLog, "Gather records from log files %ld -- %ld, %zu records.",
                   ln_from, ln_to, num_records_flushed );
        if (increasing_order) {
            _log_info( myLog, "INCREASING ORDER, set sequantial loading flag" );
        }
        parentDb->p->flags.seqLoading = increasing_order;

        if (records.size()) {
#if 0
            // NOTE:
            //   Sorting records in a key order will not help,
            //   because there is a seq-index in table file.
            //   Sorting records by key makes them random for seq-index,
            //   thus there is no benefit.
            //
            //   Need to re-visit this logic later.
            const DBConfig* db_config = getDbConfig();
            if (!increasing_order && db_config->preFlushDirtySize) {
                auto less_records = [](const Record* ll, const Record* rr) -> bool {
                    return (ll->kv.key < rr->kv.key);
                };
                Timer sort_timer;
                records.sort(less_records);
                _log_info(myLog, "sorting %zu records took %zu us",
                          records.size(), sort_timer.getUs());
                // FIXME:
                //   Sorting `records` discards checkpoint info.
                checkpoints.clear();
            }
#endif
            EP( table_mgr->setBatch(records, checkpoints) );
            EP( table_mgr->storeManifest() );
            _log_debug(myLog, "Updated table files.");
        } else {
            // WARNING:
            //   Even if `records` is empty, we SHOULD proceed
            //   as we need to purge log files properly.
        }

        // Set flush log & seq number.
        for (uint64_t ii = ln_from; ii <= ln_to; ++ii) {
            LogFileInfoGuard ll(mani->getLogFileInfoP(ii));
            if (options.beyondLastSync) {
                ll->file->setSyncedSeqNum(seq_num_local);
            }
            EP( ll->file->setFlushedSeqNum(seq_num_local) );
        }

    } else {
        // Purge only mode: set flush seq number of the last file.
        // WARNING: Should avoid loading memtable because of this call.
        LogFileInfoGuard ll(mani->getLogFileInfoP(ln_to, true));
        EP( ll->file->setFlushedSeqNum(seq_num_local) );
    }

    _log_info(myLog,
              "Flush done, seq upto %s, actual seq upto %lu, "
              "log from %lu to %lu",
              _seq_str(seq_num).c_str(), seq_num_local, ln_from, ln_to);

    if (options.beyondLastSync) {
        mani->setLastSyncedLog(ln_to);
    }
    mani->setLastFlushedLog(ln_to);
    // Remove log file except for ln_to.
    for (uint64_t ii = ln_from; ii < ln_to; ++ii) {
        // Avoid loading memtable because of this call.
        LogFileInfoGuard ll(mani->getLogFileInfoP(ii, true));

        // If it is not immutable yet (happens when we flush without sync),
        // set it now.
        if (!ll->file->isImmutable()) {
            ll->file->setImmutable();
        }

        // Remove file from manifest.
        mani->removeLogFile(ii);
        _log_info(myLog, "Removed log file %ld.", ii);
    }

    // Store log & table manifest file.
    EP( mani->store(true) );
    _log_debug(myLog, "Updated log manifest file.");

    if (num_records_flushed) {
        adjustThrottling(num_records_flushed, tt.getSec(),
                         options, ln_to_original, ln_to);
    }

    return s;
}

void LogMgr::adjustThrottling(uint64_t num_records_flushed,
                              double elapsed,
                              const FlushOptions& options,
                              uint64_t ln_to_original,
                              uint64_t ln_to)
{
    uint64_t flush_time_gap_us = lastFlushTimer.getUs();
    lastFlushTimer.reset();
    lastFlushIntervalMs = flush_time_gap_us / 1000;

    int64_t num_set_records = numSetRecords.load();

    double incoming_rate =
        flush_time_gap_us
        ? (double)num_set_records * 1000000 / flush_time_gap_us
        : 0;
    double log_flush_rate =
        elapsed ? num_records_flushed / elapsed : 0;

    if (parentDb) {
        parentDb->p->tStats.lastLogFlushRate = log_flush_rate;
    }
    double slowest_speed = getSlowestMergeRate(false);

    const DBConfig* db_config = getDbConfig();
    size_t num_log_files = getNumLogFiles();
    _log_info( myLog, "numFilesLimit %zu, num log files %zu, "
               "num records flushed %zu, num set records %zd, "
               "incoming rate %.1f iops, flush rate %.1f iops, "
               "slowest rate %.1f iops, last flush interval %zu ms, "
               "num log files limit soft %zu hard %zu",
               options.numFilesLimit,
               num_log_files,
               num_records_flushed,
               num_set_records,
               incoming_rate,
               log_flush_rate,
               slowest_speed,
               lastFlushIntervalMs.load(),
               db_config->throttlingNumLogFilesSoft,
               db_config->throttlingNumLogFilesHard );
    bool enable_throttling = false;
    bool too_many_logs = false;

    if (num_log_files > db_config->throttlingNumLogFilesHard) {
        enable_throttling = true;
        too_many_logs = true;
    }

    if ( num_log_files > db_config->throttlingNumLogFilesSoft &&
         slowest_speed > 0 &&
         num_records_flushed > db_config->throttlingThreshold &&
         incoming_rate > slowest_speed ) {
        enable_throttling = true;
    }

    if (enable_throttling) {
        throttlingRate.store( slowest_speed );
        if (too_many_logs) {
            adjustThrottlingExtreme();
        }
        _log_info(myLog,
            "enable write throttling, # records flushed %zu, %s"
            "new throttling rate %.1f ops/sec (%.1f us)",
            num_records_flushed,
            (too_many_logs) ? "too many logs, " : "",
            throttlingRate.load(),
            1000000.0 / throttlingRate.load());

    } else {
        // If # records waiting for being flushed is less than a threshold,
        // cancel the throttling.
        throttlingRate = 0;
        _log_info(myLog,
            "cancel write throttling, # records flushed %zu, %.3f sec",
            num_records_flushed, elapsed);
    }
    throttlingRateTimer.reset();

    if (numSetRecords >= (int64_t)num_records_flushed) {
        numSetRecords.fetch_sub(num_records_flushed);
    } else {
        numSetRecords.store(0);
    }
}

double LogMgr::getSlowestMergeRate(bool include_table_rate) {
    if (!parentDb) return 0;

    // Pick the smallest non-zero rate.
    std::set<double> rates;
    const DB::DBInternal::ThrottlingStats& t_stats = parentDb->p->tStats;
    if ( t_stats.lastLogFlushRate.load() ) {
        rates.insert( t_stats.lastLogFlushRate.load() );
    }

    if (include_table_rate) {
        if ( t_stats.lastTableFlushRate.load() &&
             !t_stats.lastTableFlushRateExpiry.timeout() ) {
            rates.insert( t_stats.lastTableFlushRate.load() );
        }
        if ( t_stats.lastSplitRate.load() &&
             !t_stats.lastSplitRateExpiry.timeout() ) {
            rates.insert( t_stats.lastSplitRate.load() );
        }
    }

    if (!rates.size()) return 0;
    return *rates.begin();
}

void LogMgr::adjustThrottlingExtreme() {
    if (getDbConfig()->logSectionOnly) return;

    size_t num_logs = getNumLogFiles();
    if (num_logs <= 128) return;

    size_t factor = num_logs - 128;
    double target = 10000.0 / factor;

    if (throttlingRate == 0) {
        throttlingRate.store(target);
    } else {
        throttlingRate = std::min( target, throttlingRate.load() );
    }
    throttlingRateTimer.reset();
}

Status LogMgr::doLogReclaim() {
    OpSemaWrapper ow(&reclaimSema);
    if (!ow.acquire()) {
        _log_debug(myLog, "Reclaim skipped. Other thread is working on it.");
        return Status::OPERATION_IN_PROGRESS;
    }
    assert(ow.op_sema->enabled);

    mani->reclaimExpiredLogFiles();
    return Status();
}

Status LogMgr::doBackgroundLogReclaimIfNecessary() {
    const DBConfig* db_config = getDbConfig();

    // Only in log store mode.
    if (!db_config->logSectionOnly) return Status();

    if (numMemtables > db_config->maxKeepingMemtables) {
        DBMgr* db_mgr = DBMgr::getWithoutInit();
        if (!db_mgr) {
            return Status::NOT_INITIALIZED;
        }
        _log_info(myLog, "trigger immediate log reclaim, %zu > %zu",
                  numMemtables.load(),
                  getDbConfig()->maxKeepingMemtables);
        return db_mgr->workerMgr()->invokeWorker("reclaimer");
    }
    return Status();
}

Status LogMgr::checkpoint(uint64_t& seq_num_out, bool call_fsync) {
    Status s;
    uint64_t log_file_num = 0;

   for(;;) {
    // Get latest log file.
    EP(mani->getMaxLogFileNum(log_file_num));
    LogFileInfoGuard g_li(mani->getLogFileInfoP(log_file_num));
    if (g_li.empty() || g_li.ptr->isRemoved()) continue;

    // If this file is already immutable: force append.
    EP(g_li->file->checkpoint(seq_num_out));
    break;
   }
    // Durable sync all.
    s = sync(call_fsync);
    // Tolerate race condition, report error for all the others.
    if (!s && s != Status::OPERATION_IN_PROGRESS) return s;

    return Status();
}

Status LogMgr::getAvailCheckpoints(std::list<uint64_t>& chk_out) {
    Status s;
    uint64_t ln_flushed;
    uint64_t ln_max;

   for (;;) {
    EP(mani->getLastFlushedLog(ln_flushed));
    EP(mani->getMaxLogFileNum(ln_max));

    // Check every file.
    for (uint64_t ii=ln_flushed; ii<=ln_max; ++ii) {
        LogFileInfoGuard g(mani->getLogFileInfoP(ii));
        if (g.empty() || g.ptr->isRemoved()) continue; // skip this file.

        g.file()->getCheckpoints(NOT_INITIALIZED, chk_out);
    }
    break;
   }
    return Status();
}


Status LogMgr::getAvailSeqRange(uint64_t& min_seq,
                                uint64_t& max_seq)
{
    Status s;
    uint64_t ln_flush;

   for (;;) {
    EP( mani->getLastFlushedLog(ln_flush) );
    LogFileInfoGuard li_flush( mani->getLogFileInfoP(ln_flush, true) );
    if (li_flush.empty() || li_flush.ptr->isRemoved()) continue;

    min_seq = li_flush->file->getFlushedSeqNum();
    if (min_seq == NOT_INITIALIZED) {
        // Purge never happened.
        min_seq = 0;
    } else {
        // Available seq number starts from purge seq + 1.
        min_seq++;
    }
    break;
   }

    // Note: if max file doesn't exist, it means there is no log.
    //       Return failure in that case.
    uint64_t ln_max = NOT_INITIALIZED;
   for (;;) {
    EP( mani->getMaxLogFileNum(ln_max) );
    LogFileInfoGuard li_max(mani->getLogFileInfoP(ln_max, true));
    if (li_max.empty() || li_max.ptr->isRemoved()) continue;
    max_seq = li_max->file->getMaxSeqNum();
    break;
   }

    return Status();
}

Status LogMgr::getMaxSeqNum(uint64_t& seq_num_out) {
    Status s;
    uint64_t ln_max = 0;
    uint64_t max_seq = NOT_INITIALIZED;
    const size_t MAX_TRY = 16;

    uint64_t visible_seq_barrier = getVisibleSeqBarrier();
    if (visible_seq_barrier) {
        seq_num_out = visible_seq_barrier;
        return Status();
    }

   for (size_t num_tries = 0; num_tries < MAX_TRY; num_tries++) {
    EP( mani->getMaxLogFileNum(ln_max) );

    bool succ = false;
    for (int64_t cur_idx = ln_max; cur_idx >= 0; --cur_idx) {
        LogFileInfoGuard li(mani->getLogFileInfoP(cur_idx, true));
        if (li.empty() || li.ptr->isRemoved()) {
            break;
        }
        max_seq = li->file->getMaxSeqNum();
        if (max_seq != NOT_INITIALIZED) {
            succ = true;
            break;
        }
    }
    if (succ) break;
   }

    if (max_seq == NOT_INITIALIZED) {
        return Status::LOG_NOT_EXIST;
    }
    seq_num_out = max_seq;
    return Status();
}

Status LogMgr::getMinSeqNum(uint64_t& seq_num_out) {
    Status s;
    uint64_t ln_min = 0;
    uint64_t min_seq = NOT_INITIALIZED;

    LogFileInfo* lf_info = nullptr;

   for (;;) {
    EP(mani->getLastFlushedLog(ln_min));
    // WARNING: Should avoid file loading due to this call.
    lf_info = mani->getLogFileInfoP(ln_min, true);
    if (!lf_info || lf_info->isRemoved()) continue;
    min_seq = lf_info->file->getFlushedSeqNum();
    break;
   }

    LogFileInfoGuard li(lf_info);

    if (valid_number(min_seq)) {
        if (min_seq == li->file->getMaxSeqNum()) {
            LogFileInfo* next_file;
            // WARNING: Should avoid file loading due to this call.
            s = mani->getLogFileInfo(ln_min+1, next_file, true);
            if (!s) {
                // Next file doesn't exist,
                // means that there is no record in log section.
                return Status::LOG_NOT_EXIST;
            }
            next_file->done();
        }
        // Min seq: last flush + 1
        min_seq++;
    } else {
        // Nothing has been flushed yet. Get min seq.
        min_seq = li->file->getMinSeqNum();
        if (!valid_number(min_seq)) {
            uint64_t ln_max = 0;
            EP( mani->getMaxLogFileNum(ln_max) );
            if (ln_max <= ln_min) {
                return Status::LOG_NOT_EXIST;
            }
            // If there are more log file, we should scan.
            for (size_t ii = ln_min+1; ii <= ln_max; ++ii) {
                LogFileInfoGuard ll(mani->getLogFileInfoP(ii, true));
                if (!ll.ptr || ll.ptr->isRemoved()) continue;

                min_seq = ll.ptr->file->getFlushedSeqNum();
                if (valid_number(min_seq)) break;

                min_seq = ll.ptr->file->getMinSeqNum();
                if (valid_number(min_seq)) break;
            }
            if (!valid_number(min_seq)) {
                // All log files don't have meaningful data.
                return Status::LOG_NOT_EXIST;
            }
        }
    }
    seq_num_out = min_seq;
    return Status();
}

Status LogMgr::getLastFlushedSeqNum(uint64_t& seq_num_out) {
    Status s;
    uint64_t ln_flush = 0;
    uint64_t flush_seq = NOT_INITIALIZED;
    const size_t MAX_TRY = 16;

   for (size_t num_tries=0; num_tries < MAX_TRY; ++num_tries) {
    EP(mani->getLastFlushedLog(ln_flush));
    LogFileInfoGuard li(mani->getLogFileInfoP(ln_flush, true));
    if (li.empty() || li.ptr->isRemoved()) continue;
    flush_seq = li->file->getFlushedSeqNum();
    break;
   }

    if (!valid_number(flush_seq)) {
        // Nothing has been flushed yet.
        return Status::INVALID_SEQNUM;
    }
    seq_num_out = flush_seq;
    return Status();
}

Status LogMgr::getLastSyncedSeqNum(uint64_t& seq_num_out) {
    Status s;
    uint64_t ln_sync = 0;
    uint64_t sync_seq = NOT_INITIALIZED;
    const size_t MAX_TRY = 16;

   for (size_t num_tries=0; num_tries < MAX_TRY; ++num_tries) {
    EP(mani->getLastSyncedLog(ln_sync));
    LogFileInfoGuard li(mani->getLogFileInfoP(ln_sync, true));
    if (li.empty() || li.ptr->isRemoved()) continue;
    sync_seq = li->file->getSyncedSeqNum();
    if (!valid_number(sync_seq)) {
        // This should be a bug.
        _log_err( myLog, "log file %zu returned invalid seq number, "
                  "evicted %d removed %d memtable purge %d",
                  li.ptr->logFileNum,
                  li.ptr->evicted.load(),
                  li.ptr->removed.load(),
                  li->file->isMemTablePurged() );
        assert(0);
    }
    break;
   }

    if (!valid_number(sync_seq)) {
        // Nothing has been flushed yet.
        return Status::INVALID_SEQNUM;
    }
    seq_num_out = sync_seq;
    return Status();
}


bool LogMgr::checkTimeToFlush(const GlobalConfig& config) {
    Status s;
    uint64_t l_last_flush = 0;
    uint64_t l_max = 0;
    uint64_t seq_last_flush = NOT_INITIALIZED;
    uint64_t seq_max = NOT_INITIALIZED;

    if (getDbConfig()->readOnly) return false;
    if (syncSema.grabbed) return false;
    if (flushSema.grabbed) return false;
    if (getDbConfig()->logSectionOnly) return false;

    const size_t MAX_TRY = 10;
    size_t num_try = 0;
    for (num_try = 0; num_try < MAX_TRY; ++num_try) {
        s = mani->getMaxLogFileNum(l_max);
        if (!s) return false;

        LogFileInfoGuard g_max(mani->getLogFileInfoP(l_max, true));
        if (g_max.empty() || g_max.ptr->isRemoved()) continue;

        seq_max = g_max->file->getMaxSeqNum();
        break;
    }
    if (num_try >= MAX_TRY) return false;

    for (num_try = 0; num_try < MAX_TRY; ++num_try) {
        s = mani->getLastFlushedLog(l_last_flush);
        if (!s) l_last_flush = 0;

        LogFileInfoGuard g_flush(mani->getLogFileInfoP(l_last_flush, true));
        if (g_flush.empty() || g_flush.ptr->isRemoved()) continue;

        seq_last_flush = g_flush->file->getFlushedSeqNum();
        break;
    }
    if (num_try >= MAX_TRY) return false;

    if (seq_last_flush == NOT_INITIALIZED) seq_last_flush = 0;
    if (seq_max == NOT_INITIALIZED) return false;

    // If seq number gap exceeds the limit.
    if (seq_max > seq_last_flush + config.flusherMinRecordsToTrigger) {
        return true;
    }
    // If the number of log files exceeds the limit.
    if (l_max > l_last_flush + config.flusherMinLogFilesToTrigger) {
        return true;
    }

    return false;
}

Status LogMgr::close() {
    if (!initialized) return Status();

    // If sync() or flush() is running,
    // wait until they finish their jobs.
    OpSemaWrapper op_sync(&syncSema);
    _log_info(myLog, "Wait for on-going sync operation.");

    uint64_t ticks = 0;
    while (!op_sync.acquire()) {
        ticks++;
        Timer::sleepMs(1);
    }
    syncSema.enabled = false;
    _log_info(myLog, "Disabled syncing for %p, %zu ticks", this, ticks);

    if (!getDbConfig()->readOnly) {
        // Last sync before close (not in read-only mode).
        syncInternal(false);
        _log_info(myLog, "Last sync done");
    } else {
        _log_info(myLog, "read-only mode: skip the last sync");
    }

    OpSemaWrapper op_flush(&flushSema);
    _log_info(myLog, "Wait for on-going flush operation.");
    ticks = 0;
    while (!op_flush.acquire()) {
        ticks++;
        Timer::sleepMs(1);
    }

    flushSema.enabled = false;
    _log_info(myLog, "Disabled flushing for %p, %zu ticks", this, ticks);

    OpSemaWrapper op_reclaim(&reclaimSema);
    _log_info(myLog, "Wait for on-going log reclaim operation.");
    ticks = 0;
    while (!op_reclaim.acquire()) {
        ticks++;
        Timer::sleepMs(1);
    }

    reclaimSema.enabled = false;
    _log_info(myLog, "Disabled reclaiming for %p, %zu ticks", this, ticks);

    initialized = false;
    return Status();
}

Status LogMgr::syncSeqnum(TableMgr* t_mgr) {
    // WARNING:
    //   This function will be called on opening DB only,
    //   assuming that the DB is NOT activated yet.

    uint64_t last_seqnum = NOT_INITIALIZED;
    t_mgr->getLastSeqnum(last_seqnum);

    // If tables do not exist, do nothing.
    if (!valid_number(last_seqnum)) return Status();

    uint64_t min_log_file = 0;
    uint64_t max_log_file = 0;
    Status s;
    s = mani->getMinLogFileNum(min_log_file);
    // Log section is empty, do nothing.
    if (!s) return Status();
    s = mani->getMaxLogFileNum(max_log_file);

    for (size_t ii=min_log_file; ii<=max_log_file; ++ii) {
        LogFileInfoGuard ll( mani->getLogFileInfoP(ii) );
        uint64_t min_seq = ll.file()->getMinSeqNum();
        uint64_t flushed_seq = ll.file()->getFlushedSeqNum();
        uint64_t seq_counter = ll.file()->getSeqCounter();
        _log_info(myLog, "log file %zu, min seq %s flushed seq %s seq counter %s "
                  "table seq %s",
                  ii,
                  _seq_str(min_seq).c_str(),
                  _seq_str(flushed_seq).c_str(),
                  _seq_str(seq_counter).c_str(),
                  _seq_str(last_seqnum).c_str());
        // WARNING:
        //   We should not force set flushed seq number.
        //   If crash happens in the middle of flushing,
        //   we should re-flush them, instead of force-setting
        //   the last flushed number (it causes data loss).
        //ll.file()->forceSeqnum(last_seqnum);
    }

    return s;
}

size_t LogMgr::getNumLogFiles() {
    if (!initialized || !mani) return 0;
    return mani->getNumLogFiles();
}

uint64_t LogMgr::getMinLogFileIndex() {
    if (!initialized || !mani) return NOT_INITIALIZED;
    uint64_t log_file_num_out = NOT_INITIALIZED;
    mani->getMinLogFileNum(log_file_num_out);
    return log_file_num_out;
}

uint64_t LogMgr::getMaxLogFileIndex() {
    if (!initialized || !mani) return NOT_INITIALIZED;
    uint64_t log_file_num_out = NOT_INITIALIZED;
    mani->getMaxLogFileNum(log_file_num_out);
    return log_file_num_out;
}

} // namespace jungle

