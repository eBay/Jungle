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

#include "record.h"
#include "status.h"

#include <functional>
#include <vector>

#include <stddef.h>
#include <stdint.h>

namespace jungle {

/**
 * Used `typedef` to make it compatible with ForestDB's function type.
 */
typedef int (*CustomCmpFunc)
            ( void* a, size_t len_a,
              void* b, size_t len_b,
              void* user_param );

enum CompactionCbDecision : int {
    /**
     * Keep this record. This record will survive after compaction.
     */
    KEEP = 0,

    /**
     * Drop this record. This record will not exist after compaction.
     */
    DROP = 1,
};

struct CompactionCbParams {
    CompactionCbParams() {}
    Record rec;
};

using CompactionCbFunc =
    std::function< CompactionCbDecision(const CompactionCbParams&) >;


enum SearchCbDecision : int {
    /**
     * Continue searching.
     */
    NEXT = 0,

    /**
     * Stop searching.
     */
    STOP = 1,
};

struct SearchCbParams {
    Record rec;
};

using SearchCbFunc =
    std::function< SearchCbDecision(const SearchCbParams&) >;


/**
 * Parameters for `HashKeyLenCbFunc`.
 */
struct HashKeyLenParams {
    HashKeyLenParams(const SizedBuf& k = SizedBuf(), bool is_partial_key = false) {}

    /**
     * Key to calculate the hash value.
     */
    SizedBuf key;

    /**
     * `true` if the given `key` is the prefix of the original key.
     */
    bool isPartialKey;
};

/**
 * Callback function to customize then length of prefix of a key for hash calculation.
 */
using HashKeyLenCbFunc = std::function< size_t(const HashKeyLenParams&) >;

class DB;
class DBConfig {
public:
    DBConfig()
        : allowOverwriteSeqNum(false)
        , logSectionOnly(false)
        , truncateInconsecutiveLogs(true)
        , logFileTtl_sec(0)
        , maxKeepingMemtables(0)
        , maxKeepingCheckpoints(10)
        , maxEntriesInLogFile(16384)        // 16K
        , maxLogFileSize(4194304)           // 4 MiB
        , cmpFunc(nullptr)
        , cmpFuncParam(nullptr)
        , compactionCbFunc(nullptr)
        , allowLogging(true)
        , throttlingThreshold(10000)
        , throttlingNumLogFilesSoft(16)
        , throttlingNumLogFilesHard(128)
        , bulkLoading(false)
        , numL0Partitions(4)
        , minNumTablesPerLevel(8)
        , minFileSizeToCompact(16777216)    // 16 MiB
        , minWssToMerge(64 * 1024)          // 64 KiB
        , minBlockReuseCycleToCompact(0)
        , maxBlockReuseCycle(1)
        , compactionFactor(300)             // 300%
        , blockReuseFactor(300)             // 300%
        , numWritesToCompact(0)
        , useBloomFilterForGet(true)
        , bloomFilterBitsPerUnit(0.0)
        , customLenForHash(nullptr)
        , nextLevelExtension(true)
        , maxL0TableSize(1073741824)                // 1 GiB
        , maxL1TableSize(2684354560)                // 2.5 GiB
        , maxL1Size((uint64_t)1024 * 1073741824)    // 1 TiB
        , maxParallelWritesPerJob(0)
        , readOnly(false)
        , preFlushDirtyInterval_sec(5)
        , preFlushDirtySize(0)
        , numExpectedUserThreads(8)
        , purgeDeletedDocImmediately(true)
        , fastIndexScan(false)
        , seqLoadingDelayFactor(0)
        , safeMode(false)
        , serializeMultiThreadedLogFlush(false)
    {
        tableSizeRatio.push_back(2.5);
        levelSizeRatio.push_back(10.0);

        lookupBoosterLimit_mb.push_back(100);
        lookupBoosterLimit_mb.push_back(200);
    }

    /**
     * Check if this config is valid.
     *
     * @return `true` if valid.
     */
    bool isValid() const;

    /**
     * Calculate the maximum table size of the given level.
     *
     * @param level Level.
     * @return Maximum table size in bytes.
     */
    uint64_t getMaxTableSize(size_t level) const;

    /**
     * Calculate the maximum parallel disk write threads per compaction.
     *
     * @return The number of threads.
     */
    size_t getMaxParallelWriters() const;

    /**
     * Allow overwriting logs that already exist.
     */
    bool allowOverwriteSeqNum;

    /*
     * Disable table section and use logging part only.
     */
    bool logSectionOnly;

    /*
     * (Only when `logSectionOnly == true`)
     * Truncate tail logs if they are inconsecutive,
     * to avoid empty log (a hole) in the middle.
     */
    bool truncateInconsecutiveLogs;

    /**
     * (Only when `logSectionOnly == true`)
     * TTL for log file in second.
     * If it is non-zero, the mem-table of the log file will
     * be purged once that file is not accessed for the given time.
     */
    uint32_t logFileTtl_sec;

    /**
     * (Only when `logSectionOnly == true`)
     * Number of memtables kept in memory at the same time.
     * If it is non-zero, and if the number of memtables exceeds
     * this number, the oldest memtable will be purged from memory
     * even before the TTL of corresponding log file.
     */
    uint32_t maxKeepingMemtables;

    /**
     * Number of checkpoints (i.e., persistent snapshots) kept
     * in database. Once the number exceeds this limit, older one
     * will be purged sequentially.
     */
    uint32_t maxKeepingCheckpoints;

    /**
     * Max number of logs in a file.
     */
    uint32_t maxEntriesInLogFile;

    /**
     * Max size of a log file.
     */
    uint32_t maxLogFileSize;

    /**
     * Custom comparison function.
     */
    CustomCmpFunc cmpFunc;

    /**
     * Parameter for custom comparison function.
     */
    void* cmpFuncParam;

    /**
     * Compaction callback function.
     */
    CompactionCbFunc compactionCbFunc;

    /**
     * Allow logging system info.
     */
    bool allowLogging;

    /**
     * Minimum number of flushed records for triggering write throttling.
     */
    uint32_t throttlingThreshold;

    /**
     * Write throttling will be enabled only if the current number of
     * log files is greater than this number.
     */
    uint32_t throttlingNumLogFilesSoft;

    /**
     * If the current number of log files is greater than this number,
     * Jungle will do an extreme throttling which may cause write stalls.
     */
    uint32_t throttlingNumLogFilesHard;

    /**
     * Bulk loading mode.
     */
    bool bulkLoading;

    /**
     * Number of partitions in level-0.
     */
    uint32_t numL0Partitions;

    /**
     * Minimum number of tables in each level, except for level-0.
     */
    uint32_t minNumTablesPerLevel;

    /**
     * Minimum file size that can be compacted.
     */
    uint64_t minFileSizeToCompact;

    /**
     * Working set size threshold for merge.
     * If this value is non-zero, and if the size of active data of a table file
     * is below this threshold, that file will be merged to the adjacent table file.
     */
    uint64_t minWssToMerge;

    /**
     * Minimum block re-use cycle to trigger compaction.
     */
    uint32_t minBlockReuseCycleToCompact;

    /**
     * If non-zero, ForestDB's block reuse cycle will be
     * limited to given number. After that the file will
     * be growing without reusing.
     */
    uint32_t maxBlockReuseCycle;

    /**
     * File size ratio threshold to trigger compaction, in percentage.
     * e.g.) 150 == 150%, which means that compactio will
     * be triggered if file size becomes 150% of the active
     * data size.
     */
    uint32_t compactionFactor;

    /**
     * File size ratio threshold to trigger block reuse, in percentage.
     */
    uint32_t blockReuseFactor;

    /**
     * If non-zero, in-place compaction will be triggered after the
     * given number of writes, even though the stale data ratio
     * doesn't reach `compactionFactor`.
     */
    uint32_t numWritesToCompact;

    /**
     * If `false`, point get will not use bloom filter even though it exists.
     */
    bool useBloomFilterForGet;

    /**
     * LSM-mode: Bloom filter's bits per key.
     * Jungle mode: Bloom filter's bits per 1KB portion of table.
     */
    double bloomFilterBitsPerUnit;

    /**
     * If given, the return value of this callback function will be used for
     * the hash number calculation for each key. If the return value is
     *   - zero: the entire key is used for hash calculation.
     *   - non-zero: the first given number of bytes are used for hash
     *               calculation if the key is longer than that.
     *               It will provide better performance for prefix search,
     *               but may cause skew if the length is too small.
     */
    HashKeyLenCbFunc customLenForHash;

    /**
     * Use range-partitioned L1+ for non-LSM mode.
     */
    bool nextLevelExtension;

    /**
     * L0 table size limit.
     */
    uint64_t maxL0TableSize;

    /**
     * L1+ table size limit.
     */
    uint64_t maxL1TableSize;

    /**
     * L1 level size limit.
     * The other levels (L2, L3, ...) will be determined by
     * both `maxL1Size` and `multiplicationFactor`.
     */
    uint64_t maxL1Size;

    /**
     * Starting from L2, the size ratio of table compared to
     * the previous level: { L2/L1, L3/L2, ... }.
     * For the levels not given in this vector, the last
     * ratio will be used.
     * If not given, it will be fixed to 10.
     */
    std::vector<double> tableSizeRatio;

    /**
     * Starting from L2, the size ratio of level compared to
     * the previous level: { L2/L1, L3/L2, ... }.
     * For the levels not given in this vector, the last
     * ratio will be used.
     * If not given, `multiplicationFactor` will be used.
     */
    std::vector<double> levelSizeRatio;

    /**
     * Size limit of in-memory lookup booster for each level.
     */
    std::vector<uint32_t> lookupBoosterLimit_mb;

    /**
     * Maximum number of writers for each job (compaction, split).
     * If 0, this number will be automatically adjusted considering
     * the number of flushers and compactors.
     */
    uint32_t maxParallelWritesPerJob;

    /**
     * If `true`, read-only mode. No modify, recovery, and compaction.
     */
    bool readOnly;

    struct DirectIoOptions{
        DirectIoOptions()
            : enabled(false)
            , bufferSize(4096)
            , alignSize(4096)
            , readaheadSize(65536)
            {}

        /**
         * If `true`, use direct-IO bypassing OS page cache.
         * Default: `false`
         */
        bool enabled;

        /**
         * The size of memory buffer for direct-IO.
         */
        size_t bufferSize;

        /**
         * The alignment size of memory buffer for direct-IO.
         */
        size_t alignSize;

        /**
         * The amount of read-ahead in bytes.
         * Disabled if zero.
         */
        size_t readaheadSize;
    };

    /**
     * [EXPERIMENTAL] Direct-IO related options.
     */
    DirectIoOptions directIoOpt;

    /**
     * Below two options are for flushing dirty data during
     * long-running compaction or split.
     *   * Without these options: dirty data will be flushed only once
     *     at the end of the task.
     *     - Pros: can minimize the number of disk writes.
     *     - Cons: may cause high latency of other operations during
     *             the burst write.
     *   * With these options: dirty data can be flushed in the
     *     middle of the task.
     *     - Pros: can mitigate the high latency issue.
     *     - Cons: may increase the number of disk writes, and also
     *             make tasks slower.
     */

    /**
     * Periodic flush by time (seconds).
     * Disabled if zero.
     */
    uint32_t preFlushDirtyInterval_sec;

    /**
     * Periodic flush by size (bytes).
     * Disabled if zero.
     */
    uint64_t preFlushDirtySize;

    /**
     * Type definition for pluggable compression options.
     */
    struct CompressionOptions {
        CompressionOptions()
            : cbGetMaxSize(nullptr)
            , cbCompress(nullptr)
            , cbDecompress(nullptr)
            {}

        /**
         * Callback function to get the maximum (worst case) size of
         * the expected output data after the compression.
         *
         * @param DB* Jungle instance.
         * @param Record& Record to compress.
         * @return The maximum size of the expected output data.
         *         If this function returns 0 or negative value,
         *         Jungle will not compress this record.
         */
        std::function< ssize_t(DB*, const Record&) > cbGetMaxSize;

        /**
         * Callback function to compress the given record value.
         *
         * @param DB* Jungle instance.
         * @param Record& Record to compress.
         * @param SizedBuf&
         *     Buffer where the compression output data will
         *     be stored. The size of this buffer will be the same as
         *     the return value of `cbGetMaxSize`. Jungle will manage
         *     the allocation and deallocation of this buffer.
         * @return The actual size of compressed data.
         *         If the return value is negative, Jungle will treat it
         *         as an error code.
         *         If it returns 0, Jungle will treat it as the cancellation
         *         of the record compression, not an error.
         */
        std::function< ssize_t(DB*, const Record&, SizedBuf&) > cbCompress;

        /**
         * Callback function to decompress the given compressed data.
         *
         * @param DB* Jungle instance.
         * @param SizedBuf& Compressed data to decompress.
         * @param SizedBuf&
         *     Buffer where the decompression output data will be
         *     stored. The size of this buffer will be the same as
         *     the original data size before the compression. Jungle
         *     will manage the allocation and deallocation of this buffer.
         * @return The size of decompressed data.
         *         If the return value does not match the size of
         *         output buffer, Jungle will treat it as an error code.
         */
        std::function< ssize_t(DB*, const SizedBuf&, SizedBuf&) > cbDecompress;
    };

    /**
     * Compression options.
     */
    CompressionOptions compOpt;

    /**
     * The expected number of user threads that will execute DB operations
     * concurrently. It will be used for internal settings to optimize
     * performance and resource usage.
     */
    uint32_t numExpectedUserThreads;

    /**
     * If `true`, deleted records will purged as soon as it
     * is merged into the bottom-most level. After that,
     * its meta data will not be retrieved even with `meta_only` flag.
     */
    bool purgeDeletedDocImmediately;

    /**
     * [EXPERIMENTAL]
     * If `true`, do fast/compact index scan during compaction,
     * instead of iteration.
     */
    bool fastIndexScan;

    /**
     * Compaction tasks will be delayed while keys are being
     * loaded in sequential order. If this value is non-zero,
     * compactions will be triggered if the file size of an L0 table
     * is greater than the max table size multiplied by this value.
     */
    uint32_t seqLoadingDelayFactor;

    /**
     * [EXPERIMENTAL]
     * Only with read-only flag and log store mode.
     * If given, Jungle will open the DB instance, but use the manifest
     * files in the given path, instead of those in the original path.
     *
     * The main purpose of this is for opening a snapshot by separate
     * processes. While a process is working on the DB, another process
     * can open the same DB path in read-only mode, and sees the snapshot
     * of it provided by the manifest files in the path.
     *
     * The custom manifest files should be created by `cloneManifest()` API.
     */
    std::string customManifestPath;

    /**
     * If DB files are corrupted, Jungle will try to avoid the process crash
     * as much as possible.
     * This mode will slow down operations, thus should not be used in
     * real production environment.
     */
    bool safeMode;

    /**
     * If `true`, `sync` and `flushLogs` calls by multiple threads will be serialized,
     * and executed one by one.
     * If `false`, only one thread will execute `sync` and `flushLogs` calls, while
     * the other concurrent threads will get `OPERATION_IN_PROGRESS` status.
     */
    bool serializeMultiThreadedLogFlush;
};

class GlobalConfig {
public:
    GlobalConfig()
        : globalLogPath("./")
        , numFlusherThreads(1)
        , numDedicatedFlusherForAsyncReqs(0)
        , flusherSleepDuration_ms(500)
        , flusherMinRecordsToTrigger(65536)
        , flusherMinLogFilesToTrigger(16)
        , flusherAutoSync(false)
        , numCompactorThreads(2)
        , compactorSleepDuration_ms(5000)
        , logFileReclaimerSleep_sec(5)
        , fdbCacheSize(0)
        , numTableWriters(8)
        , memTableFlushBufferSize(32768)
        , shutdownLogger(true) {}

    /**
     * Path where Jungle's global log will be located.
     */
    std::string globalLogPath;

    /**
     * Max number of flusher threads.
     */
    size_t numFlusherThreads;

    /**
     * Create dedicated flushers for async request. Only effective when it is
     * not 0 and `numFlusherThreads` > `numDedicatedFlusherForAsyncReqs`.
     */
    size_t numDedicatedFlusherForAsyncReqs;

    /**
     * Fluhser thread sleep time in ms.
     */
    size_t flusherSleepDuration_ms;

    /**
     * Minimum number of records that triggers flushing.
     */
    size_t flusherMinRecordsToTrigger;

    /**
     * Minimum number of log files that triggers flushing.
     */
    size_t flusherMinLogFilesToTrigger;

    /**
     * Automatic sync before flushing.
     */
    bool flusherAutoSync;

    /**
     * Max number of compactor threads.
     */
    size_t numCompactorThreads;

    /**
     * Compactor thread sleep time in ms.
     */
    size_t compactorSleepDuration_ms;

    /**
     * Sleep duration of background log reclaimer.
     */
    size_t logFileReclaimerSleep_sec;

    /**
     * Underlying ForestDB's buffer cache size.
     */
    uint64_t fdbCacheSize;

    /**
     * Size of thread pool for table mutate tasks
     * (flush, compaction, and split).
     * NOTE: Both `numTableWriterGroups` and `numTableWritersPerGroup`
     *       have been deprecated.
     */
    size_t numTableWriters;

    /**
     * Size of buffer when flushing MemTable to log file.
     */
    size_t memTableFlushBufferSize;

    /**
     * Settings for idle time compaction.
     *
     * Compaction factor will be adjusted to the given value
     * if traffic to this process is lower than the given threshold
     * for the given time window.
     */
    struct IdleTimeCompactionOptions {
        IdleTimeCompactionOptions()
            : timeWindow_sec(0)
            , startHour(0)
            , endHour(0)
            , iopsThreshold(1000)
            , factor(125)
            {}

        /**
         * Time window to check whether the process is idle.
         * If zero, idle time compaction will not be activated.
         */
        uint32_t timeWindow_sec;

        /**
         * Start hour (24h format) to trigger compaction.
         * If start and end hours are the same, this condition will be ignored.
         */
        uint32_t startHour;

        /**
         * End hour (24h format) to trigger compaction.
         * If start and end hours are the same, this condition will be ignored.
         */
        uint32_t endHour;

        /**
         * IOPS threshold to determine whether the process is idle.
         */
        uint32_t iopsThreshold;

        /**
         * Temporary compaction factor if the process is idle.
         */
        uint32_t factor;
    };

    /**
     * Idle time compaction options.
     */
    IdleTimeCompactionOptions itcOpt;

    /**
     * Settings for compaction (inclugin split/merge) throttling.
     *
     * Throttling is disabled by default, so that compactor thread
     * will do compaction at its best if below values are unchanged.
     */
    struct CompactionThrottlingOptions {
        CompactionThrottlingOptions()
            : resolution_ms(200)
            , throttlingFactor(0)
            {}

        /**
         * Time interval to execute throttling.
         */
        uint32_t resolution_ms;

        /**
         * A number deciding how much it will throttle the compaction,
         * ranged between 0 to 99.
         * If
         *   1) zero, throttling is completely disabled.
         *   2) F, compactor thread's utilization will be at most (100-X) %.
         *     e.g.)
         *       F = 10, compactor will run at 90% of its max speed.
         *       F = 50, compactor will run at 50% of its max speed.
         *       F = 80, compactor will run at 20% of its max speed.
         */
        uint32_t throttlingFactor;
    };

    /**
     * Compaction throttling options.
     *
     * It can be used to reduce the influence on user-facing latency
     * by heavy IO of compaction tasks.
     *
     * Interlevel/in-place compactions, split, and merge will get affected.
     * Log flushing (to L0 tables) will have no effect.
     */
    CompactionThrottlingOptions ctOpt;

    /**
     * Shutdown system logger on shutdown of Jungle.
     */
    bool shutdownLogger;
};

} // namespace jungle

