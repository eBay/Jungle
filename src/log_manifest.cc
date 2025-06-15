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

#include "log_manifest.h"

#include "crc32.h"
#include "db_mgr.h"
#include "event_awaiter.h"
#include "internal_helper.h"
#include "log_mgr.h"
#include "skiplist.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <algorithm>
#include <sstream>

namespace jungle {

static uint8_t LOGMANI_FOOTER[8] = {0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xab, 0xcd};
static uint32_t LOGMANI_VERSION = 0x1;

void LogManifest::reclaimExpiredLogFiles() {
    uint64_t last_synced_log_file_num;
    Status s = getLastSyncedLog(last_synced_log_file_num);
    if (!s) return;

    std::stringstream size_message;
    uint64_t total_memtable_size = 0;
    std::vector<uint64_t> live_file_acc;
    skiplist_node* begin = skiplist_begin(&logFiles);
    skiplist_node* cursor = begin;
    while (cursor) {
        LogFileInfo* info = _get_entry(cursor, LogFileInfo, snode);
        info->grab();

        // Collect memory usage info
        if ( !info->file->isMemTablePurged() ) {
            uint64_t size = info->file->getMemTableSize();
            if ( size > 0 ) {
                size_message << ", logfile_" << info->logFileNum << ": " << size;
                total_memtable_size += size;
            }
        }

        // NOTE:
        //   Need to keep the first log file, as it may be frequently
        //   accessed by getting min seqnum or something like that.
        if ( cursor != begin &&
             info->logFileNum < last_synced_log_file_num &&
             !info->isEvicted() &&
             !info->isRemoved() &&
             info->file->isImmutable() &&
             !info->file->isMemTablePurged() &&
             info->file->isExpired() ) {
            _log_info(myLog, "will purge memtable of log file %zu, "
                      "last access %zu us ago, ref count %zu",
                      info->logFileNum,
                      info->file->getLastAcc(),
                      info->getRefCount());
            info->setEvicted();
        }

        if ( !info->isEvicted() &&
             !info->isRemoved() &&
             info->file->isImmutable() &&
             !info->file->isMemTablePurged() ) {
            live_file_acc.push_back(info->file->getLastAcc());
        }

        cursor = skiplist_next(&logFiles, cursor);
        skiplist_release_node(&info->snode);

        info->done();
    }
    if (cursor) skiplist_release_node(cursor);

    // Log memory usage info
    thread_local uint64_t last_total_log_size = 0;
    if ( total_memtable_size != last_total_log_size ) {
        _log_info(myLog, "memtable memory usage info, total %lu bytes, "
                  "%zu tables%s",
                  total_memtable_size,
                  live_file_acc.size(),
                  size_message.str().c_str());
        last_total_log_size = total_memtable_size;
    }

    // TODO:
    //   What if MemTable loading is too fast and occupies huge memory
    //   before next reclaimer wake-up?
    uint32_t limit = logMgr->getDbConfig()->maxKeepingMemtables;
    if ( limit && live_file_acc.size() > limit ) {

        // Too many MemTables are in memory, do urgent reclaiming.
        size_t num_files_to_purge = live_file_acc.size() - limit;
        std::sort(live_file_acc.begin(), live_file_acc.end());

        _log_info( myLog, "num memtable %zu exceeds limit %zu, "
                   "last access %zu us ago",
                   live_file_acc.size(),
                   limit,
                   live_file_acc[limit - 1] );

        size_t num_purged = 0;
        begin = skiplist_begin(&logFiles);
        cursor = begin;
        while (cursor) {
            LogFileInfo* info = _get_entry(cursor, LogFileInfo, snode);
            info->grab();

            if ( cursor != begin &&
                 info->logFileNum < last_synced_log_file_num &&
                 !info->isEvicted() &&
                 !info->isRemoved() &&
                 info->file->isImmutable() &&
                 !info->file->isMemTablePurged() &&
                 info->file->getLastAcc() > live_file_acc[limit - 1] ) {
                _log_info(myLog, "will purge memtable of log file %zu (urgent), "
                          "last access %zu us ago, ref count %zu",
                          info->logFileNum,
                          info->file->getLastAcc(),
                          info->getRefCount());
                info->setEvicted();
                num_purged++;
            }

            cursor = skiplist_next(&logFiles, cursor);
            skiplist_release_node(&info->snode);

            info->done();

            if (num_purged >= num_files_to_purge) break;
        }
        if (cursor) skiplist_release_node(cursor);
    }
}

LogManifest::LogManifest(LogMgr* log_mgr, FileOps* _f_ops, FileOps* _f_l_ops)
    : fOps(_f_ops)
    , fLogOps(_f_l_ops)
    , mFile(nullptr)
    , mBackupFile(nullptr)
    , lastFlushedLog(NOT_INITIALIZED)
    , lastSyncedLog(NOT_INITIALIZED)
    , maxLogFileNum(NOT_INITIALIZED)
    , cachedManifest(32768)
    , lenCachedManifest(0)
    , fullBackupRequired(true)
    , logMgr(log_mgr)
    , myLog(nullptr)
{
    skiplist_init(&logFiles, LogFileInfo::cmp);
    skiplist_init(&logFilesBySeq, LogFileInfo::cmpBySeq);
}

LogManifest::~LogManifest() {
    // Should join reclaimer first, before releasing skiplist.
    // It will be safe as this destructor will be invoked after
    // LogMgr::close() is done.

    if (mFile) {
        delete mFile;
    }

    if (mBackupFile) {
        delete mBackupFile;
    }
    // NOTE: Skip `logFilesBySeq` as they share the actual memory.
    skiplist_node* cursor = skiplist_begin(&logFiles);
    while (cursor) {
        LogFileInfo* info = _get_entry(cursor, LogFileInfo, snode);
        LogFile* log_file = info->file;
        cursor = skiplist_next(&logFiles, cursor);

        delete log_file;
        delete info;
    }
    skiplist_free(&logFiles);
    skiplist_free(&logFilesBySeq);

    if (!cachedManifest.empty()) {
        cachedManifest.free();
    }
    lenCachedManifest = 0;
}

bool LogManifest::isLogReclaimerActive() {
    return logMgr->isLogStoreMode();
}

void LogManifest::spawnReclaimer() {
    if ( isLogReclaimerActive() ) {
        DBMgr* dbm = DBMgr::getWithoutInit();
        if (!dbm) return;
        Status s = dbm->addLogReclaimer();
        if (s) {
            _log_info(myLog, "initiated log reclaimer");
        } else {
            _log_info(myLog, "log reclaimer already exists");
        }
    }
}

Status LogManifest::create(const std::string& path,
                           const std::string& filename,
                           const uint64_t prefix_num)
{
    if (!fOps) return Status::NOT_INITIALIZED;
    if (fOps->exist(filename.c_str())) return Status::ALREADY_EXIST;
    if (filename.empty()) return Status::INVALID_PARAMETERS;

    dirPath = path;
    mFileName = filename;

    // Create a new file.
    Status s;
    EP( fOps->open(&mFile, mFileName.c_str()) );

    // Store initial data.
    EP( store(true) );

    spawnReclaimer();
    return Status();
}

Status LogManifest::load(const std::string& path,
                         const std::string& filename,
                         const uint64_t prefix_num)
{
    if (!fOps) return Status::NOT_INITIALIZED;
    if (mFile) return Status::ALREADY_LOADED;
    if (filename.empty()) return Status::INVALID_PARAMETERS;

    dirPath = path;
    mFileName = filename;
    prefixNum = prefix_num;

    Status s;
    Timer tt;
    const DBConfig* db_config = logMgr->getDbConfig();

    EP( fOps->open(&mFile, mFileName.c_str()) );

   try {
    // File should be bigger than 16 bytes (FOOTER + version + CRC32).
    size_t file_size = fOps->eof(mFile);
    if (file_size < 16) throw Status(Status::FILE_CORRUPTION);

    // Footer check
    RwSerializer ss(fOps, mFile);
    uint8_t chk_footer[8];
    ss.pos(file_size - 16);
    ss.get(chk_footer, 8);
    if (memcmp(LOGMANI_FOOTER, chk_footer, 8) != 0) {
        throw Status(Status::FILE_CORRUPTION);
    }

    // Version
    uint32_t ver_file = ss.getU32(s);
    (void)ver_file;

    // CRC check
    uint32_t crc_file = ss.getU32(s);

    SizedBuf chk_buf(file_size - 4);
    SizedBuf::Holder h_chk_buf(chk_buf);
    ss.pos(0);
    ss.get(chk_buf.data, chk_buf.size);
    uint32_t crc_local = crc32_8(chk_buf.data, chk_buf.size, 0);
    if (crc_local != crc_file) throw Status(Status::CHECKSUM_ERROR);

    ss.pos(0);
    maxLogFileNum.store(ss.getU64(s), MOR);
    lastFlushedLog.store(ss.getU64(s), MOR);
    lastSyncedLog.store(ss.getU64(s), MOR);
    uint32_t num_log_files = ss.getU32(s);
    _log_info(myLog,
              "max log file num %ld, last flush %ld, last sync %ld, "
              "num log files %zu",
              maxLogFileNum.load(), lastFlushedLog.load(),
              lastSyncedLog.load(), num_log_files);

    uint64_t last_synced_seq = NOT_INITIALIZED;

    bool first_file_read = false;
    for (uint32_t ii=0; ii < num_log_files; ++ii) {
        LogFile* l_file = new LogFile(logMgr);
        l_file->setLogger(myLog);

        uint64_t l_file_num = ss.getU64(s);
        std::string l_filename =
                LogFile::getLogFileName(dirPath, prefixNum, l_file_num);

        uint64_t min_seq = ss.getU64(s);
        uint64_t purged_seq = ss.getU64(s);
        uint64_t synced_seq = ss.getU64(s);

        bool invalid_log = false;
        if ( db_config->logSectionOnly &&
             db_config->truncateInconsecutiveLogs &&
             valid_number(min_seq) ) {
            // Log-only mode, validity check.
            if ( valid_number(synced_seq) &&
                 min_seq > synced_seq ) {
                // This cannot happen, probably caused by
                // abnormal shutdown.
                _log_warn( myLog, "min seq %s > synced seq %s, break",
                           _seq_str(min_seq).c_str(),
                           _seq_str(synced_seq).c_str() );
                invalid_log = true;
            }
            if ( valid_number(last_synced_seq) &&
                 min_seq != last_synced_seq + 1 ) {
                // Inconsecutive sequence number,
                // probably caused by abnormal shutdown and then
                // re-open.
                _log_warn( myLog, "min seq %s and last synced seq %s "
                           "are inconsecutive, break",
                           _seq_str(min_seq).c_str(),
                           _seq_str(last_synced_seq).c_str() );
                invalid_log = true;
            }
        }

        // WARNING:
        //   If the last log file is empty and manifest file is not properly
        //   updated, we should remove the log file. If not, next DB open
        //   will remove all log files after that empty log file, due to
        //   incorrect manifest entry (min_seq = last_sync + 1) for that log file.
        if ( db_config->logSectionOnly &&
             db_config->truncateInconsecutiveLogs &&
             ii + 1 == num_log_files &&
             !valid_number(min_seq) ) {
            _log_warn( myLog, "the last log file %zu is empty and manifest entry "
                       "does not match: min seq %s, "
                       "synced seq %s. will remove it.",
                       ii,
                       _seq_str(min_seq).c_str(),
                       _seq_str(synced_seq).c_str() );
            invalid_log = true;
        }

        if (invalid_log) {
            delete l_file;
            if (l_file_num) {
                maxLogFileNum.store(l_file_num-1, MOR);
                lastSyncedLog.store(l_file_num-1, MOR);
                _log_warn(myLog, "adjusted max log file num %zu, "
                          "last synced log file num %zu",
                          maxLogFileNum.load(),
                          lastSyncedLog.load());
            }
            break;
        }

        if (valid_number(synced_seq)) {
            last_synced_seq = synced_seq;
        }
        if (!valid_number(min_seq) && valid_number(last_synced_seq)) {
            min_seq = last_synced_seq + 1;
        }
        if (!valid_number(synced_seq) && valid_number(last_synced_seq)) {
            synced_seq = last_synced_seq;
        }

        if (l_file_num < lastFlushedLog) {
            // If the log file number is less than last flushed log,
            // it is not valid.
            _log_warn(myLog, "log file %zu is less than last flushed log %zu, "
                      "skip it",
                      l_file_num, lastFlushedLog.load());
            delete l_file;
            continue;
        }

        s = l_file->load(l_filename, fLogOps, l_file_num,
                         min_seq, purged_seq, synced_seq);
        if (!s) {
            _s_warn(myLog) << "log file " << l_file_num << " read error: " << s;
            if ( s == Status::FILE_NOT_EXIST ||
                 s == Status::CHECKSUM_ERROR ) {
                if ( !first_file_read &&
                     ii + 1 < num_log_files ) {
                    // If this is the first log file, and there are
                    // more log files to read, we can tolerate this error.
                    _log_warn(myLog, "index %zu (out of %zu) log number %zu is "
                              "the first log yet, skip it",
                              ii, num_log_files, l_file_num);
                    lastFlushedLog = NOT_INITIALIZED;
                    lastSyncedLog = NOT_INITIALIZED;
                    delete l_file;
                    continue;
                }

                // Log file in the middle or the last one.
                _s_warn(myLog) << "something wrong happened, stop loading here";
                delete l_file;
                if ( getNumLogFiles() ) {
                    if (l_file_num) {
                        maxLogFileNum.store(l_file_num-1, MOR);
                        lastSyncedLog.store(l_file_num-1, MOR);
                        _log_warn(myLog, "adjusted max log file num %zu, "
                                  "last synced log file num %zu",
                                  maxLogFileNum.load(),
                                  lastSyncedLog.load());
                    }
                    break;
                }
                // Otherwise, there is no valid log file. Should create one,
                // and set its number to max file number.
                l_file = new LogFile(logMgr);
                l_file_num = maxLogFileNum;

                // WARNING: WE SHOULD RESET LAST FLUSHED/SYNCED FILE NUMBER.
                lastFlushedLog = NOT_INITIALIZED;
                lastSyncedLog = NOT_INITIALIZED;

                std::string l_filename =
                    LogFile::getLogFileName(dirPath, prefixNum, l_file_num);
                // NOTE: `start_seq_num` will be re-synced with tables
                //       after table loading is done. So it is safe to
                //       set it to 0 here.
                l_file->create(l_filename, fLogOps, l_file_num, 0);

                _log_warn(myLog, "no log file is found due to previous crash, "
                          "created new log file %zu", l_file_num);

                // Make it escape loop.
                ii = num_log_files;
            }
            // Otherwise: tolerate.
        }
        first_file_read = true;

        _log_info( myLog,
                   "log %ld, min seq %s, last flush %s, last sync %s",
                   l_file_num,
                   _seq_str(min_seq).c_str(),
                   _seq_str(purged_seq).c_str(),
                   _seq_str(synced_seq).c_str() );

        addNewLogFile(l_file_num, l_file, min_seq);
        if ( !valid_number(lastSyncedLog) ||
             lastSyncedLog < l_file_num ) {
            lastSyncedLog.store(l_file_num);
        }
    }

    _log_info(myLog, "loading manifest & log files done: %lu us, "
              "flushed %s synced %s max %s",
              tt.getUs(),
              _seq_str(lastFlushedLog).c_str(),
              _seq_str(lastSyncedLog).c_str(),
              _seq_str(maxLogFileNum).c_str() );

    spawnReclaimer();
    return Status();

   } catch (Status s) {
    // Error happened, close file.
    fOps->close(mFile);
    DELETE(mFile);
    return s;
   }
}

Status LogManifest::clone(const std::string& dst_path) {
    if (mFileName.empty() || !fOps) return Status::NOT_INITIALIZED;

    std::unique_lock<std::mutex> l(mFileWriteLock);
    std::string src_file = mFileName;
    std::string dst_file = dst_path + "/" + logMgr->getManifestFilename();
    return BackupRestore::copyFile(fOps, src_file, dst_file, true);
}

Status LogManifest::store(bool call_fsync) {
    if (mFileName.empty() || !fOps) return Status::NOT_INITIALIZED;

    if (call_fsync || logMgr->getDbConfig()->serializeMultiThreadedLogFlush) {
        // `fsync` is required, or serialize option is on:
        // calls by multiple threads should be serialized.
        std::unique_lock<std::mutex> l(mFileWriteLock);
        return storeInternal(call_fsync);
    } else {
        // `fsync` is not a must: try lock and return without blocking.
        // (currently invoked by `addNewLogFile()` only).
        std::unique_lock<std::mutex> l(mFileWriteLock, std::try_to_lock);
        if (!l.owns_lock()) {
            return Status::OPERATION_IN_PROGRESS;
        }
        return storeInternal(call_fsync);
    }
}

Status LogManifest::storeInternal(bool call_fsync) {
    Status s;

    SizedBuf mani_buf(4096);
    SizedBuf::Holder h_mani_buf(mani_buf);
    // Resizable serializer.
    RwSerializer ss(&mani_buf);

    //   << Log manifest file format >>
    // Latest log file number,       8 bytes
    // Last flushed log file number, 8 bytes
    // Last synced log file number,  8 bytes
    // Number of log files,          4 bytes
    uint32_t num_log_files = skiplist_get_size(&logFiles);
    ss.putU64(maxLogFileNum);
    ss.putU64(lastFlushedLog);
    ss.putU64(lastSyncedLog);
    ss.putU32(num_log_files);
    _log_debug(myLog,
               "max log file num %ld, last flush %ld, last sync %ld, "
               "num log files %zu",
               maxLogFileNum.load(), lastFlushedLog.load(),
               lastSyncedLog.load(), num_log_files);

    skiplist_node* cursor = skiplist_begin(&logFiles);
    while (cursor) {
        //   << Log info entry format >>
        // Log file number,         8 bytes
        // Min seq number,          8 bytes
        // Last flushed seq number, 8 bytes
        // Last synced seq number,  8 bytes
        LogFileInfo* info = _get_entry(cursor, LogFileInfo, snode);
        LogFile* l_file = info->file;

        // WARNING: We should grab `info` due to below
        //          seq number retrievals. Otherwise, there can
        //          be a possibility of heap-use-after-free if
        //          this file is being evicted by the reclaimer.
        info->grab(false);

        ss.putU64(info->logFileNum);
        ss.putU64(l_file->getMinSeqNum());
        ss.putU64(l_file->getFlushedSeqNum());
        ss.putU64(l_file->getSyncedSeqNum());
        _log_trace(myLog,
                   "log %ld, min seq %ld, last flush %ld, last sync %ld",
                   info->logFileNum, l_file->getMinSeqNum(),
                   l_file->getFlushedSeqNum(), l_file->getSyncedSeqNum());
        info->done();

        cursor = skiplist_next(&logFiles, cursor);
        skiplist_release_node(&info->snode);
    }
    if (cursor) skiplist_release_node(cursor);

    ss.put(LOGMANI_FOOTER, 8);

    // Version
    ss.putU32(LOGMANI_VERSION);

    // CRC32
    uint32_t crc_val = crc32_8(mani_buf.data, ss.pos(), 0);

    ss.putU32(crc_val);

    // We will skip the common data and write different part only,
    // to reduce disk IOs (assuming that memory comparison is faster than disk write).
    uint32_t first_diff_pos = 0;
    bool need_truncate = !lenCachedManifest || lenCachedManifest > ss.pos();

    if (lenCachedManifest) {
        uint32_t min_len = std::min(cachedManifest.size, (uint32_t)ss.pos());

        // FIXME: It will be a bit faster if we compare 8-byte chunk.
        for (size_t ii = 0; ii < min_len; ++ii) {
            if (cachedManifest.data[ii] != mani_buf.data[ii]) {
                first_diff_pos = ii;
                break;
            }
        }
    }

    if (ss.pos() > first_diff_pos) {
        EP( fOps->pwrite( mFile,
                          mani_buf.data + first_diff_pos,
                          ss.pos() - first_diff_pos,
                          first_diff_pos ) );
    }
    _log_trace(myLog, "new buffer size %zu, cached %zu, first diff %zu",
               ss.pos(), lenCachedManifest, first_diff_pos);

    while (ss.pos() > cachedManifest.size) {
        size_t new_size = cachedManifest.size * 2;
        cachedManifest.free();
        cachedManifest.alloc(new_size);
        _log_info(myLog, "buffer for log manifest cache increased %zu", new_size);
    }

    memcpy(cachedManifest.data, mani_buf.data, ss.pos());
    lenCachedManifest = ss.pos();

    // Should truncate tail.
    if (need_truncate) {
        fOps->ftruncate(mFile, ss.pos());
    }

    bool backup_done = false;
    if (call_fsync) {
        s = fOps->fsync(mFile);

        if (s) {
            // WARNING:
            //   We should update backup file only when the original manifest
            //   file is synced. If not, there can be a possibility that
            //   both files are corrupted at the same time.

            // After success, make a backup file one more time,
            // using the latest data.
            // Same as above, tolerate backup failure.
            Status s_backup = BackupRestore::backup
                              ( fOps, &mBackupFile, mFileName, mani_buf,
                                ss.pos(),
                                fullBackupRequired ? 0 : first_diff_pos,
                                call_fsync );
            if (s_backup) {
                backup_done = true;
            }
        }
    }

    // If backup file is not synced with `cachedManifest` this time,
    // we should set this value to `true` so as to write the
    // entire date next time.
    fullBackupRequired = !backup_done;
    return s;
}

Status LogManifest::issueLogFileNumber(uint64_t& new_log_file_number) {
    uint64_t expected = NOT_INITIALIZED;
    uint64_t val = 0;
    if (maxLogFileNum.compare_exchange_weak(expected, val)) {
        // The first log file, number 0.
    } else {
        // Otherwise: current max + 1.
        do {
            expected = maxLogFileNum;
            val = maxLogFileNum + 1;
        } while (!maxLogFileNum.compare_exchange_weak(expected, val));
    }
    new_log_file_number = val;
    return Status();
}

Status LogManifest::rollbackLogFileNumber(uint64_t to) {
    maxLogFileNum = to;
    return Status();
}

bool LogManifest::logFileExist(const uint64_t log_num) {
    LogFileInfo query(log_num);
    skiplist_node* cursor = skiplist_find(&logFiles, &query.snode);
    if (!cursor) {
        return false;
    }
    skiplist_release_node(cursor);
    return true;
}

Status LogManifest::getLogFileInfo(const uint64_t log_num,
                                   LogFileInfo*& info_out,
                                   bool force_not_load_memtable)
{
    LogFileInfo query(log_num);
    skiplist_node* cursor = skiplist_find(&logFiles, &query.snode);
    if (!cursor) {
        return Status::LOG_FILE_NOT_FOUND;
    }
    info_out = _get_entry(cursor, LogFileInfo, snode);
    if (force_not_load_memtable) {
        info_out->grab();
    } else {
        info_out->grab(isLogReclaimerActive());
    }
    skiplist_release_node(cursor);
    return Status();
}

Status LogManifest::getLogFileInfoSnapshot(const uint64_t log_num,
                                       LogFileInfo*& info_out)
{
    LogFileInfo query(log_num);
    skiplist_node* cursor = skiplist_find(&logFiles, &query.snode);
    if (!cursor) {
        return Status::LOG_FILE_NOT_FOUND;
    }
    info_out = _get_entry(cursor, LogFileInfo, snode);
    info_out->grabForSnapshot();

    skiplist_release_node(cursor);
    return Status();
}

Status LogManifest::getLogFileInfoRange(const uint64_t s_log_inc,
                                        const uint64_t e_log_inc,
                                        std::vector<LogFileInfo*>& info_out,
                                        bool force_not_load_memtable)
{
    LogFileInfo query(s_log_inc);
    skiplist_node* cursor =
        skiplist_find_greater_or_equal(&logFiles, &query.snode);
    if (!cursor) {
        return Status::LOG_FILE_NOT_FOUND;
    }

    while (cursor) {
        LogFileInfo* l_info = _get_entry(cursor, LogFileInfo, snode);
        if (force_not_load_memtable) {
            l_info->grab();
        } else {
            l_info->grab(isLogReclaimerActive());
        }
        info_out.push_back(l_info);

        if (l_info->logFileNum >= e_log_inc) {
            cursor = nullptr;
        } else {
            cursor = skiplist_next(&logFiles, &l_info->snode);
        }
        skiplist_release_node(&l_info->snode);
    }
    return Status();
}

Status LogManifest::getLogFileInfoBySeq(const uint64_t seq_num,
                                        LogFileInfo*& info_out,
                                        bool force_not_load_memtable,
                                        bool allow_non_exact_match)
{
    LogFileInfo query(0);
    query.startSeq = seq_num;
    skiplist_node* cursor = skiplist_find_smaller_or_equal
                            ( &logFilesBySeq, &query.snodeBySeq );
    if (!cursor) return Status::LOG_FILE_NOT_FOUND;

    LogFileInfo* info = _get_entry(cursor, LogFileInfo, snodeBySeq);
    LogFile* file = info->file;

    DBMgr* mgr = DBMgr::getWithoutInit();
    if (mgr && mgr->isDebugCallbackEffective()) {
        DebugParams d_params = mgr->getDebugParams();
        if (d_params.getLogFileInfoBySeqCb) {
            DebugParams::GenericCbParams p;
            d_params.getLogFileInfoBySeqCb(p);
        }
    }

    uint64_t file_min_seq = file->getMinSeqNum();
    if (valid_number(file_min_seq) && file_min_seq > seq_num) {
        // WARNING: This can happen for the first log file,
        //          if user uses custom seqnum which is bigger than
        //          the expected seqnum.
        skiplist_release_node(cursor);
        return Status::LOG_FILE_NOT_FOUND;
    }

    if ( file->getMaxSeqNum() < seq_num ) {
        // Given seq number is bigger than this file's max seq number.
        // * If this file is not the last file, follow
        //   `allow_non_exact_match` flag.
        // * If it is the last file, we should return LOG_FILE_NOT_FOUND,
        //   as the given seq number is will be the biggest one (so that
        //   not an overwrite).
        skiplist_node* next_cursor = skiplist_next(&logFilesBySeq, cursor);
        bool is_last_file = (next_cursor == nullptr);
        if (next_cursor) {
            skiplist_release_node(next_cursor);
        }
        if (is_last_file || !allow_non_exact_match) {
            skiplist_release_node(cursor);
            return Status::LOG_FILE_NOT_FOUND;
        }
    }

    info_out = info;
    if (force_not_load_memtable) {
        info_out->grab();
    } else {
        info_out->grab(isLogReclaimerActive());
    }
    skiplist_release_node(cursor);
    return Status();
}

LogFileInfo* LogManifest::getLogFileInfoP(uint64_t log_num,
                                          bool force_not_load_memtable) {
    LogFileInfo* ret = nullptr;
    Status s = getLogFileInfo(log_num, ret, force_not_load_memtable);
    if (!s) return nullptr;
    return ret;
}

Status LogManifest::addNewLogFile(uint64_t log_num,
                                  LogFile* log_file,
                                  uint64_t start_seqnum)
{
    LogFileInfo* info = new LogFileInfo(log_num);
    if (!info) return Status::ALLOCATION_FAILURE;

    if (log_file->isMemTablePurged()) info->evicted = true;
    info->file = log_file;
    info->startSeq = start_seqnum;
    skiplist_insert(&logFilesBySeq, &info->snodeBySeq);
    skiplist_insert(&logFiles, &info->snode);

    return Status();
}

Status LogManifest::removeLogFile(uint64_t log_num) {
    LogFileInfo query(log_num);
    skiplist_node* cursor = skiplist_find(&logFiles, &query.snode);
    if (!cursor) {
        return Status::LOG_FILE_NOT_FOUND;
    }

    LogFileInfo* info = _get_entry(cursor, LogFileInfo, snode);

    // NOTE: the last done() call will kill itself (suicide).
    info->setRemoved();

    skiplist_erase_node(&logFiles, &info->snode);
    skiplist_release_node(&info->snode);
    skiplist_wait_for_free(&info->snode);

    skiplist_erase_node(&logFilesBySeq, &info->snodeBySeq);
    skiplist_wait_for_free(&info->snodeBySeq);

    return Status();
}

Status LogManifest::getLogFileNumBySeq(const uint64_t seqnum,
                                       uint64_t& log_file_num_out,
                                       bool force_not_load_memtable,
                                       bool ignore_max_seq_num)
{
    LogFileInfo* info;
    Status s;
    EP( getLogFileInfoBySeq( seqnum,
                             info,
                             force_not_load_memtable,
                             ignore_max_seq_num ) );
    LogFileInfoGuard gg(info);
    if (!info->file) return Status::NOT_INITIALIZED;
    log_file_num_out = info->file->getLogFileNum();
    return Status();
}

Status LogManifest::getMaxLogFileNum(uint64_t& log_file_num_out) {
    uint64_t max_log_file_num = maxLogFileNum.load(MOR);
    if  (max_log_file_num == NOT_INITIALIZED)
        return Status::NOT_INITIALIZED;

    log_file_num_out = max_log_file_num;
    return Status();
}

Status LogManifest::setMaxLogFileNum(uint64_t cur_num, uint64_t new_num) {
    if (maxLogFileNum.compare_exchange_weak(cur_num, new_num)) {
        return Status();
    }
    return Status::ERROR;
}


Status LogManifest::getMinLogFileNum(uint64_t& log_file_num_out) {
    skiplist_node* cursor = skiplist_begin(&logFiles);
    if (!cursor) {
        return Status::NOT_INITIALIZED;
    }
    LogFileInfo* info = _get_entry(cursor, LogFileInfo, snode);
    log_file_num_out = info->logFileNum;
    skiplist_release_node(cursor);
    return Status();
}

Status LogManifest::getLastFlushedLog(uint64_t& last_flushed_log) {
    if (lastFlushedLog == NOT_INITIALIZED) {
        // Flush never happened yet, return the min log file number.
        // If no log file exists, return error.
        Status s;
        s = getMinLogFileNum(last_flushed_log);
        if (!s) last_flushed_log = NOT_INITIALIZED;
        return s;
    }
    last_flushed_log = lastFlushedLog;
    return Status();
}

Status LogManifest::getLastSyncedLog(uint64_t& last_synced_log) {
    last_synced_log = lastSyncedLog;
    if (lastSyncedLog == NOT_INITIALIZED) {
        return Status::NOT_INITIALIZED;
    }

    return Status();
}

size_t LogManifest::getNumLogFiles() {
    return skiplist_get_size(&logFiles);
}


} // namespace jungle

