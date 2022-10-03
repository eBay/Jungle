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
#include "log_file.h"
#include "skiplist.h"

#include <libjungle/jungle.h>

#include <atomic>
#include <list>
#include <map>
#include <string>
#include <thread>

class SimpleLogger;

namespace jungle {

struct LogFileInfo {
    LogFileInfo() {
        LogFileInfo(0);
    }
    LogFileInfo(uint64_t l_file_num)
        : logFileNum(l_file_num)
        , startSeq(0)
        , file(nullptr)
        , refCount(0)
        , snapCount(0)
        , removed(false)
        , evicted(false) {
        skiplist_init_node(&snodeBySeq);
        skiplist_init_node(&snode);
    }
    ~LogFileInfo() {
        skiplist_free_node(&snodeBySeq);
        skiplist_free_node(&snode);
        assert(refCount == 0);
    }

    static int cmp(skiplist_node *a, skiplist_node *b, void *aux) {
        LogFileInfo *aa, *bb;
        aa = _get_entry(a, LogFileInfo, snode);
        bb = _get_entry(b, LogFileInfo, snode);

        if (aa->logFileNum < bb->logFileNum) return -1;
        if (aa->logFileNum > bb->logFileNum) return 1;
        return 0;
    }

    static int cmpBySeq(skiplist_node *a, skiplist_node *b, void *aux) {
        LogFileInfo *aa, *bb;
        aa = _get_entry(a, LogFileInfo, snodeBySeq);
        bb = _get_entry(b, LogFileInfo, snodeBySeq);

        if (aa->startSeq < bb->startSeq) return -1;
        if (aa->startSeq > bb->startSeq) return 1;
        return 0;
    }

    void grab(bool load_memtable_if_needed = false) {
        refCount.fetch_add(1);

        // WARNING:
        //   We should put `lock_guard` here (after increasing
        //   `refCount` and before the if-clause). If not, Memtable
        //   purging can happen while the caller of this (`grab`)
        //   function is doing something with the Memtable.
        //
        //   e.g.) T1 (calls `done`), T2 (calls `grab`)
        //     T1: decrease `refCount` -> it becomes 0.
        //     T1: grab `evictionLock`, `refCount` is still 0.
        //         Start purging.
        //        --- switch ---
        //     T2: increase `refCount` by 1.
        //         `load_memtable_if_needed = false`.
        //     T2: `LogFile::mTable` is still not `nullptr`, or
        //         `memtablePurged` is `false` yet.
        //         Do something with `mTable`.
        //        --- switch ---
        //     T1: Delete `mTable`.
        //        --- crash ---
        //
        // FIXME: Need to address this issue so that we can move
        //        `lock_guard` inside the if-clause.
        std::lock_guard<std::mutex> l(evictionLock);
        if (load_memtable_if_needed) {
            if (file->isMemTablePurged()) {
                file->loadMemTable();
                evicted = false;
            }
        }
    }

    void grabForSnapshot() {
        snapCount.fetch_add(1);
    }

    void done() {
        doneInternal(false);
    }

    void doneForSnapshot() {
        doneInternal(true);
    }

    void doneInternal(bool snapshot = false) {
        if (snapshot) {
            assert(snapCount);
        } else {
            assert(refCount);
        }
        // WARNING: MONSTOR-11561
        //   We should check `removed` or `evicted` flag first
        //   and then decrease `refCount`.
        //
        //   e.g.)
        //     T1: done(), decrease refCount to 0, count = 1.
        //       --- context switch ---
        //     T2: Increase ref count to 1.
        //     T2: Do flush, remove file, removed = true.
        //     T2: done(), decrease refCount to 0, count = 1.
        //     T2: removed == true && count == 1, destroy the file.
        //       --- context switch ---
        //     T1: removed == true && count == 1, destroy the file.
        if (removed) {
            uint64_t snap_count = 0;
            uint64_t ref_count = 0;
            bool destroy_file = false;
            if (snapshot) {
                snap_count = snapCount.fetch_sub(1);
                ref_count = refCount;
                destroy_file = (snap_count == 1 && ref_count == 0);
            } else {
                snap_count = snapCount;
                ref_count = refCount.fetch_sub(1);
                destroy_file = (snap_count == 0 && ref_count == 1);
            }

            if (destroy_file) {
                // WARNING:
                //   While reclaimer is evicting memtable below (in that case
                //   refCount == 0 already), LogMgr::flush() may grab a file,
                //   remove it, and then release, which will get into here.
                //   In this case this file will not be protected and causes crash.
                //   We should do busy-waiting for eviction here.
                //
                // * Note that opposite CANNOT happen, because
                //   1) This node is already detached from skiplist, and
                //   2) once `removed` flag is set, reclaimer cannot handle this file.
                //
                if (evicted) {
                    while (!file->isMemTablePurged()) std::this_thread::yield();
                    // Wait for other job done.
                }
                file->destroySelf();
                delete file;
                delete this;
            }
            return;
        }

        if (snapshot == false && evicted) {
            uint64_t count = refCount.fetch_sub(1);
            if (count == 1) {
                std::lock_guard<std::mutex> l(evictionLock);
                // WARNING:
                //   There shouldn't be another thread that called grab() before this.
                uint64_t count_protected = refCount.load();
                if (count_protected == 0 && !file->isMemTablePurged()) {
                    file->purgeMemTable();
                }
            }
            return;
        }

        // Normal case.
        if (snapshot) {
            snapCount.fetch_sub(1);
        } else {
            refCount.fetch_sub(1);
        }
    }

    uint64_t getRefCount() const { return refCount.load(); }
    void setRemoved() { removed.store(true); }
    bool isRemoved() { return removed.load(); }
    void setEvicted() {
        std::lock_guard<std::mutex> l(evictionLock);
        evicted.store(true);
    }
    bool isEvicted() { return evicted.load(); }

    skiplist_node snode;
    skiplist_node snodeBySeq;

    uint64_t logFileNum;

    uint64_t startSeq;

    LogFile* file;

    // Reference counter (accessing MemTable).
    std::atomic<uint64_t> refCount;

    // Snapshot counter (blocking file removal).
    std::atomic<uint64_t> snapCount;

    // Flag indicating whether or not this file is removed.
    std::atomic<bool> removed;

    // Flag indicating whether or not this file is evicted from LRU.
    std::atomic<bool> evicted;

    // Lock for loading & evicting mem-table.
    std::mutex evictionLock;
};

struct LogFileInfoGuard {
    LogFileInfoGuard(LogFileInfo* _ptr) : ptr(_ptr) {}
    ~LogFileInfoGuard() { if (ptr) ptr->done(); }
    void operator=(const LogFileInfoGuard& src) {
        LogFileInfo* tmp = ptr;
        ptr = src.ptr;
        ptr->grab(true);
        if (tmp) tmp->done();
    }
    bool empty() const { return (ptr == nullptr); }
    LogFileInfo* operator->() const { return ptr; }
    LogFile* file() { return ptr->file; }
    LogFileInfo* ptr;
};

class LogMgr;
class LogManifest {
public:
    LogManifest(LogMgr* log_mgr, FileOps* _f_ops, FileOps* _f_l_ops);
    ~LogManifest();

    bool isLogReclaimerActive();

    void spawnReclaimer();

    Status create(const std::string& path,
                  const std::string& filename,
                  const uint64_t prefix_num);
    Status load(const std::string& path,
                const std::string& filename,
                const uint64_t prefix_num);

    Status store(bool call_fsync);

    Status clone(const std::string& dst_path);

    static Status copyFile(FileOps* f_ops,
                           const std::string& src_file,
                           const std::string& dst_file);
    static Status backup(FileOps* f_ops,
                         const std::string& filename);
    static Status restore(FileOps* f_ops,
                          const std::string& filename);

    Status issueLogFileNumber(uint64_t& new_log_file_number);

    Status rollbackLogFileNumber(uint64_t to);

    Status addNewLogFile(uint64_t log_num,
                         LogFile* log_file,
                         uint64_t start_seqnum);
    Status removeLogFile(uint64_t log_num);

    bool logFileExist(const uint64_t log_num);

    Status getLogFileInfo(uint64_t log_num,
                          LogFileInfo*& info_out,
                          bool force_not_load_memtable = false);

    Status getLogFileInfoSnapshot(const uint64_t log_num,
                              LogFileInfo*& info_out);

    Status getLogFileInfoRange(const uint64_t s_log_inc,
                               const uint64_t e_log_inc,
                               std::vector<LogFileInfo*>& info_out,
                               bool force_not_load_memtable = false);

    Status getLogFileInfoBySeq(const uint64_t seq_num,
                               LogFileInfo*& info_out,
                               bool force_not_load_memtable = false,
                               bool allow_non_exact_match = false);

    LogFileInfo* getLogFileInfoP(uint64_t log_num,
                                 bool force_not_load_memtable = false);

    Status getLogFileNumBySeq(const uint64_t seq_num,
                              uint64_t& log_file_num_out,
                              bool force_not_load_memtable = false,
                              bool ignore_max_seq_num = false);

    Status getMaxLogFileNum(uint64_t& log_file_num_out);
    Status setMaxLogFileNum(uint64_t cur_num, uint64_t new_num);

    Status getMinLogFileNum(uint64_t& log_file_num_out);
    Status getLastFlushedLog(uint64_t& last_purged_log);
    Status getLastSyncedLog(uint64_t& last_synced_log);

    void setLastSyncedLog(const uint64_t log_num) {
        lastSyncedLog = log_num;
    }
    void setLastFlushedLog(const uint64_t log_num) {
        lastFlushedLog = log_num;
    }

    size_t getNumLogFiles();

    void reclaimExpiredLogFiles();

    void setLogger(SimpleLogger* logger) { myLog = logger; }

private:
    Status storeInternal(bool call_fsync);

    FileOps* fOps;
    FileOps* fLogOps;
    FileHandle* mFile;
    std::string dirPath;
    std::string mFileName;
    uint64_t prefixNum;
    std::atomic<uint64_t> lastFlushedLog;
    std::atomic<uint64_t> lastSyncedLog;
    std::atomic<uint64_t> maxLogFileNum;

    /**
     * Log files by its file number.
     */
    skiplist_raw logFiles;

    /**
     * Log files by its starting (minimum) seq number.
     * Entries are shared with `logFiles`.
     */
    skiplist_raw logFilesBySeq;

    /**
     * Buffer for caching the last written manifest file data.
     */
    SizedBuf cachedManifest;

    /**
     * Actual length of data in `cachedManifest`, as `cachedManifest`
     * reserves more memory.
     */
    size_t lenCachedManifest;

    /**
     * Only one thread is allowed to write the manifest file.
     */
    std::mutex mFileWriteLock;

    /**
     * If `true`, there is a mismatch between `cachedManifest`
     * and backup file, so that we cannot do partial write.
     */
    bool fullBackupRequired;

    LogMgr* logMgr;
    SimpleLogger* myLog;
};

} // namespace jungle

