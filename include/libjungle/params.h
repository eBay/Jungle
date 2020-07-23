/************************************************************************
Copyright 2017-2020 eBay Inc.

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

#include <functional>

namespace jungle {

class FlushOptions {
public:
    FlushOptions()
        : purgeOnly(false)
        , syncOnly(false)
        , callFsync(false)
        , beyondLastSync(false)
        , numFilesLimit(0)
        , execDelayUs(0)
        {}

    /**
     * If `true`, records will not be stored in back-end table,
     * but just will be purged from log.
     */
    bool purgeOnly;

    /**
     * (Only in async flush)
     * If `true`, records will be written to log file only,
     * will not be flushed to table section.
     */
    bool syncOnly;

    /**
     * (Only in async flush)
     * If `true`, call `fsync()` on log files before flushing
     * to table section.
     */
    bool callFsync;

    /**
     * If `true`, flush all logs currently exist,
     * including logs not explicitly synced yet.
     * If `false`, flushing only happens upto the last synced log.
     */
    bool beyondLastSync;

    /**
     * Limit the number of log files to be flushed at once.
     * Disabled if 0.
     */
    uint32_t numFilesLimit;

    /**
     * (Only in async flush)
     * If non-zero, given request will not be executed immediately,
     * and Jungle will wait and merge incoming requests for the given
     * time delay, and then execute them at once.
     */
    uint32_t execDelayUs;
};

class CompactOptions {
public:
    CompactOptions()
        : preserveTombstone(false)
        {}

    /**
     * If true, deletion marker (i.e., tombstone) will be
     * alive even after compaction.
     */
    bool preserveTombstone;
};

struct DebugParams {
    DebugParams()
        : compactionDelayUs(0)
        , compactionItrScanDelayUs(0)
        , urgentCompactionFilesize(0)
        , urgentCompactionRatio(0)
        , urgentCompactionNumWrites(0)
        , urgentCompactionMaxTableIdx(0)
        , rollbackDelayUs(0)
        , logDetailsOfKeyNotFound(false)
        , disruptSplit(false)
        , tableSetBatchCb(nullptr)
        , addNewLogFileCb(nullptr)
        , newLogBatchCb(nullptr)
        , forceMerge(false)
        {}

    /**
     * If non-zero, every record copy during compaction will
     * sleep this amount of time.
     */
    uint32_t compactionDelayUs;

    /**
     * If non-zero, every record scan at the 2nd phase of compaction
     * will sleep this amount of time.
     */
    uint32_t compactionItrScanDelayUs;

    /**
     * If non-zero, background compaction will be invoked
     * once file size becomes bigger than this value,
     * regardless of other factors such as block reuse cycle
     * or stale data ratio.
     */
    uint64_t urgentCompactionFilesize;

    /**
     * If bigger than 100, compaction factors (ratio) of all opened DBs
     * are temporarily overwritten by this value.
     * The same as compaction factor, the unit is percentage:
     * e.g.) 200 -> trigger compaction at 200%.
     */
    uint64_t urgentCompactionRatio;

    /**
     * If non-zero, compaction will be triggered when the accumulated
     * number of writes to a DB is bigger than this number.
     * e.g.) 10000 -> trigger compaction for every 10K writes.
     *
     * If `DbConfig::numWritesToCompact` is also set, smaller number
     * will take effect.
     */
    uint64_t urgentCompactionNumWrites;

    /**
     * If non-zero, compaction will be triggered sequentially,
     * for tables whose index number is equal to or smaller than
     * this number.
     */
    uint64_t urgentCompactionMaxTableIdx;

    /**
     * If non-zero, every file removal or truncation during rollback
     * will sleep this amount of time.
     */
    uint32_t rollbackDelayUs;

    /**
     * If `true`, leave detailed logs if given key is not found.
     */
    bool logDetailsOfKeyNotFound;

    /**
     * If `true`, split pre-scanning will result only one output table.
     */
    bool disruptSplit;

    struct GenericCbParams {
        GenericCbParams() {}
    };

    /**
     * Callback function that will be invoked at the end of each
     * table write batch.
     */
    std::function< void(const GenericCbParams&) > tableSetBatchCb;

    /**
     * Callback function that will be invoked at the moment
     * new log file is added, but right before appending the first log.
     */
    std::function< void(const GenericCbParams&) > addNewLogFileCb;

    /**
     * Callback function that will be invoked at the moment
     * new batch (set of records) is appended, but before they become
     * visible.
     */
    std::function< void(const GenericCbParams&) > newLogBatchCb;

    /**
     * If true, merge will proceed the task even with the small number
     * of tables in the level.
     */
    bool forceMerge;
};

}

