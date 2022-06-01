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

#include "sized_buf.h"

#include <list>
#include <map>

#include <stddef.h>
#include <stdint.h>

namespace jungle {

class DBStatsOptions {
public:
    DBStatsOptions() : getTableHierarchy(false) {}

    /**
     * If `true`, `DBStats` will include the info of all tables throughout all levels.
     */
    bool getTableHierarchy;
};

class TableHierarchyInfo {
public:
    TableHierarchyInfo()
        : tableIdx(0)
        , level(0)
        , partitionIdx(0)
        {}

    TableHierarchyInfo(TableHierarchyInfo&& src)
        : tableIdx(src.tableIdx)
        , level(src.level)
        , partitionIdx(src.partitionIdx)
    {
        src.minKey.moveTo(minKey);
    }

    TableHierarchyInfo& operator=(TableHierarchyInfo&& src) {
        tableIdx = src.tableIdx;
        level = src.level;
        partitionIdx = src.partitionIdx;
        src.minKey.moveTo(minKey);
        return *this;
    }

    // No copy.
    TableHierarchyInfo(const TableHierarchyInfo& src) = delete;
    TableHierarchyInfo& operator=(const TableHierarchyInfo& src) = delete;

    ~TableHierarchyInfo() {
        minKey.free();
    }

    /**
     * Table index number.
     */
    uint64_t tableIdx;

    /**
     * Level that this table belongs to.
     */
    uint32_t level;

    /**
     * Minimum key that this table has (only for L1+).
     * This buffer will be freed when `TableHierarchyInfo` instnace is destroyed.
     */
    SizedBuf minKey;

    /**
     * Hash partition index (only for L0).
     */
    uint32_t partitionIdx;
};

/**
 * Set of `TableHierarchyInfo` for each level.
 * Key: level, value: list of `TableHierarchyInfo`.
 */
using TableHierarchy = std::map<uint32_t, std::list<TableHierarchyInfo>>;

class DBStats {
public:
    DBStats()
        : numKvs(0)
        , workingSetSizeByte(0)
        , numIndexNodes(0)
        , cacheSizeByte(0)
        , cacheUsedByte(0)
        , numOpenMemtables(0)
        , numBgTasks(0)
        , minTableIndex(0)
        , maxTableIndex(0)
        , minLogIndex(0)
        , maxLogIndex(0)
        {}

    /**
     * [Global]: process-wide global stat.
     * [Local]: DB-specific stat.
     */

    /**
     * [Local]
     * Approximate the number of key-value pairs in DB.
     */
    uint64_t numKvs;

    /**
     * [Local]
     * Total working set (i.e., valid KV pairs) size.
     */
    uint64_t workingSetSizeByte;

    /**
     * [Local]
     * Total number of index nodes, where each index node is 4KB.
     */
    uint64_t numIndexNodes;

    /**
     * [Global]
     * Total block cache capacity (byte).
     */
    uint64_t cacheSizeByte;

    /**
     * [Global]
     * Amount of cache used (byte).
     */
    uint64_t cacheUsedByte;

    /**
     * [Local]
     * Number of Memtables currently open.
     */
    uint32_t numOpenMemtables;

    /**
     * [Local]
     * Number of background tasks currently running.
     */
    uint32_t numBgTasks;

    /**
     * [Local]
     * Smallest (i.e., oldest) table file index number.
     */
    uint64_t minTableIndex;

    /**
     * [Local]
     * Greatest (i.e., youngest) table file index number.
     */
    uint64_t maxTableIndex;

    /**
     * [Local]
     * Smallest (i.e., oldest) log file index number.
     * This value can be `-1` if any error happens.
     */
    uint64_t minLogIndex;

    /**
     * [Local]
     * Greatest (i.e., youngest) log file index number.
     * This value can be `-1` if any error happens.
     */
    uint64_t maxLogIndex;

    /**
     * [Local]
     * The details of all tables throughout all levels.
     * The value will be returned only when the corresponding option is set.
     */
    TableHierarchy tableHierarchy;
};

} // namespace jungle

