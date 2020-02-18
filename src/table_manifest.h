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
#include "skiplist.h"
#include "table_file.h"

#include <libjungle/jungle.h>

#include _MACRO_TO_STR(LOGGER_H)

#include <cassert>
#include <list>
#include <set>
#include <vector>

namespace jungle {

struct TableInfo;
struct TableStack {
    TableStack() {}
    ~TableStack() {}
    std::list<TableInfo*> tables;
    std::mutex lock;
};

struct TableInfo {
    enum Status {
        NORMAL = 0,
        COMPACT_SRC = 1,
        COMPACT_DST = 2,
        QUERY_PURPOSE = 10000,
    };

    TableInfo(const uint32_t _level,
              const uint64_t _num,
              const uint64_t _hash_num,
              bool query_purpose = false)
        : level(_level)
        , number(_num)
        , hashNum(_hash_num)
        , file(nullptr)
        , refCount(0)
        , removed(false)
        , migration(false)
        , status( query_purpose ? QUERY_PURPOSE : NORMAL )
        , stack(nullptr)
        , baseTable(true)
    {
        if (query_purpose) return;
        skiplist_init_node(&snode);
    }
    ~TableInfo() {
        if (status == QUERY_PURPOSE) return;
        skiplist_free_node(&snode);
        minKey.free();
        delete stack.load();
        stack = nullptr;
        assert(refCount.load() == 0);
    }

    static int cmp(skiplist_node *a, skiplist_node *b, void *aux) {
        TableInfo *aa, *bb;
        aa = _get_entry(a, TableInfo, snode);
        bb = _get_entry(b, TableInfo, snode);
        return SizedBuf::cmp(aa->minKey, bb->minKey);
    }

    void grab() { refCount.fetch_add(1); }
    void done() {
        assert(refCount);
        // WARNING: Refer to the comment in `LogFileInfo::done()`.
        if (removed) {
            uint64_t count = refCount.fetch_sub(1);
            SimpleLogger* temp = file->getLogger();
            _log_info(temp, "removed level %zu file %zu "
                      "ref count %zu -> %zu status %d mflag %s",
                      level, number, count, count-1, status.load(),
                      (migration ? "ON" : "OFF") );
            // NOTE: We should not remove file object if this
            //       table is being migrated to next level, as
            //       next level's table info will reuse it.
            if (count == 1 && !migration) {
                file->destroySelf();
                delete file;
                delete this;
            }
            return;
        }

        // Normal case.
        refCount.fetch_sub(1);
    }
    uint64_t getRefCount() const { return refCount.load(MOR); }
    void setRemoved()   { removed.store(true, MOR); }
    bool isRemoved()    { return removed.load(MOR); }

    void setMigration()   { migration.store(true, MOR); }
    bool isMigrating()    { return migration.load(MOR); }

    void setCompactSrc()    { return status.store(COMPACT_SRC, MOR); }
    void setNormal()        { return status.store(NORMAL, MOR); }
    bool isNormal() const   { return status.load(MOR) == NORMAL; }
    bool isSrc() const      { return status.load(MOR) == COMPACT_SRC; }
    bool isValid() const    { return status.load(MOR) != COMPACT_DST; }

    // Skiplist metadata.
    skiplist_node snode;

    // Level number.
    uint32_t level;

    // Table number.
    uint64_t number;

    // For tables in level-0 (hash-partition).
    uint32_t hashNum;

    // For tables in level-1+ (range-partition).
    SizedBuf minKey;

    // Table file instance.
    TableFile* file;

    // Reference counter.
    std::atomic<uint64_t> refCount;

    // Flag indicating whether or not this file is removed.
    std::atomic<bool> removed;

    // Flag indicating that this table is being migrated.
    std::atomic<bool> migration;

    // Current table status.
    std::atomic<Status> status;

    // Tiering: list of stacks: oldest -> ... -> newest.
    std::atomic<TableStack*> stack;

    // `true` if this table is the owner of stack.
    // `false` if this table belongs to the stack of other table.
    bool baseTable;
};

class TableManifest {
public:
    TableManifest(const TableMgr* table_mgr, FileOps* _f_ops);
    ~TableManifest();

    Status create(const std::string& path,
                  const std::string& filename);
    Status load(const std::string& path,
                const uint64_t prefix_num,
                const std::string& filename);
    Status store();
    Status storeTableStack(RwSerializer& ss, TableInfo* base_table);

    Status sync();
    Status extendLevel();
    Status issueTableNumber(uint64_t& new_table_number);
    Status addTableFile(size_t level_num,
                        uint32_t hash_num,
                        SizedBuf min_key,
                        TableFile* t_file,
                        bool allow_stacking = true);
    Status removeTableFile(size_t level, TableInfo* table_to_remove);

    bool doesHashPartitionExist(uint32_t hash_num);

    Status getL0Tables(uint32_t target_hash_num,
                       std::list<TableInfo*>& tables_out);

    // Note: done() should be called after use.
    Status getTablesPoint(const size_t level,
                          const SizedBuf& key,
                          std::list<TableInfo*>& tables_out);

    // Note: done() should be called after use.
    Status getTablesRange(const size_t level,
                          const SizedBuf& min_key,
                          const SizedBuf& max_key,
                          std::list<TableInfo*>& tables_out);

    void getTableNumbers(std::set<uint64_t>& numbers_out);

    size_t getNumLevels();

    Status getNumTables(size_t level, size_t& num_tables_out) const;

    std::mutex& getLock() { return tableUpdateLock; }

    void setLogger(SimpleLogger* logger) { myLog = logger; }

private:
    struct LevelInfo;

    // Always open Table files belonging to level up to 1.
    static const size_t MAX_OPENED_FILE_LEVEL = 1;
    static const size_t NUM_RESERVED_LEVELS = 16;

    Status getTablesByHash(LevelInfo* l_info,
                           uint32_t target_hash_num,
                           std::list<TableInfo*>& tables_out);

    void pushTablesInStack(TableInfo* t_info,
                           std::list<TableInfo*>& tables_out);

    // Backward pointer to table manager.
    const TableMgr* tableMgr;

    // File operations.
    FileOps* fOps;

    // Manifest file handle.
    FileHandle* mFile;

    // Path.
    std::string dirPath;

    // Manifest file name.
    std::string mFileName;

    // Total LSM levels.
    std::mutex levelsLock;
    std::vector<LevelInfo*> levels;

    // Current greatest table number.
    std::atomic<uint64_t> maxTableNum;

    // To guarantee atomic update of manifest.
    std::mutex tableUpdateLock;

    // Logger.
    SimpleLogger* myLog;
};

} // namespace jungle
