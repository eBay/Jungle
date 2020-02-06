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
#include "fileops_base.h"
#include "internal_helper.h"
#include "table_file.h"
#include "table_helper.h"
#include "table_manifest.h"
#include "table_writer.h"

#include <libjungle/jungle.h>

#include <list>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace jungle {

class TableMgrOptions {
public:
    TableMgrOptions()
        : fOps(nullptr)
        , prefixNum(0)
        , dbConfig(nullptr)
        {}

    std::string path;

    FileOps* fOps;

    // KVS ID.
    uint64_t prefixNum;

    // Pointer to the parent DB handle's config.
    const DBConfig* dbConfig;
};

class TableStats {
public:
    TableStats()
        : numKvs(0)
        , workingSetSizeByte(0)
        , totalSizeByte(0)
        , cacheUsedByte(0)
        , blockReuseCycle(0)
        , lastSeqnum(0)
        , approxDocCount(0)
        , approxDelCount(0)
        {}
    uint64_t numKvs;
    uint64_t workingSetSizeByte;
    uint64_t totalSizeByte;
    uint64_t cacheUsedByte;
    uint64_t blockReuseCycle;
    uint64_t lastSeqnum;
    uint64_t approxDocCount;
    uint64_t approxDelCount;
};

namespace checker { class Checker; }

class TableMgr {
    friend class checker::Checker;
    friend class Compactor;
    friend class TableLockHolder;
    friend class LevelLockHolder;

private:
    struct LsmFlushResult {
        LsmFlushResult() : tFile(nullptr) {}
        LsmFlushResult(TableFile* _file) : tFile(_file) {}
        LsmFlushResult(TableFile* _file, const SizedBuf& _key)
            : tFile(_file), minKey(_key) {}

        // Less function, descending `minKey` order:
        // `true` if `ll.minKey > rr.minKey`.
        static bool cmp(const LsmFlushResult& ll, const LsmFlushResult& rr) {
            return (ll.minKey > rr.minKey);
        }

        TableFile* tFile;
        SizedBuf minKey;
    };

    enum MergeType {
        LEVELING = 0x0,
        TIERING = 0x1,
        APPEND = 0x2,
    };

public:
    TableMgr(DB* parent_db);

    ~TableMgr();

    enum VictimPolicy {
        WORKING_SET_SIZE = 0x0,
        STALE_RATIO = 0x1,
        STACK_SIZE = 0x2,
        WORKING_SET_SIZE_SPLIT = 0x3,
        SMALL_WORKING_SET = 0x4,
    };

    enum MergeStrategy {
        NONE = 0x0,
        INTERLEVEL = 0x1,
        SPLIT = 0x2,
        INPLACE = 0x3,
        MERGE = 0x4,
        INPLACE_OLD = 0x5,
    };

    Status init(const TableMgrOptions& _options);

    Status removeStaleFiles();

    Status shutdown();

    Status openSnapshot(DB* snap_handle,
                        const uint64_t checkpoint,
                        std::list<TableInfo*>*& table_list_out);
    Status closeSnapshot(DB* snap_handle);

    Status setBatch(std::list<Record*>& batch,
                    std::list<uint64_t>& checkpoints,
                    bool bulk_load_mode = false);

    Status setBatchHash(std::list<Record*>& batch,
                        std::list<uint64_t>& checkpoints,
                        bool bulk_load_mode);

    Status splitTableItr(TableInfo* victim_table);

    Status storeManifest();

    Status get(DB* snap_handle,
               Record& rec_inout,
               bool meta_only = false);

    size_t getNumLevels() const;

    Status getLevelSize(size_t level,
                        uint64_t& wss_out,
                        uint64_t& total_out,
                        uint64_t& max_stack_size_out);

    uint64_t getLevelSizeLimit(size_t level) const;

    Status pickVictimTable(size_t level,
                           VictimPolicy policy,
                           bool check_limit,
                           TableInfo*& victim_table_out,
                           uint64_t& wss_out,
                           uint64_t& total_out);

    TableInfo* findLocalVictim(size_t level,
                               TableInfo* given_victim,
                               VictimPolicy policy,
                               bool honor_limit);

    Status splitLevel(const CompactOptions& options,
                      TableInfo* victim_table,
                      size_t level);

    Status mergeLevel(const CompactOptions& options,
                      TableInfo* victim_table,
                      size_t level);

    Status compactLevelItr(const CompactOptions& options,
                           TableInfo* victim_table,
                           size_t level);

    Status compactInPlace(const CompactOptions& options,
                          TableInfo* victim_table,
                          size_t level,
                          bool oldest_one_first);

    Status compactL0(const CompactOptions& options,
                     uint32_t hash_num);

    bool isL0CompactionInProg();

    bool chkL0CompactCond(uint32_t hash_num);

    Status chkLPCompactCond(size_t level,
                            TableMgr::MergeStrategy& s_out,
                            TableInfo*& victim_table_out);

    bool disallowCompaction();

    Status close();

    Status getAvailCheckpoints(std::list<uint64_t>& chk_out);

    const DBConfig* getDbConfig() const { return opt.dbConfig; }

    const TableMgrOptions* getTableMgrOptions() const { return &opt; }

    uint32_t getNumL0Partitions() const { return numL0Partitions; }

    void setLogger(SimpleLogger* logger) {
        myLog = logger;
        if (mani) mani->setLogger(myLog);
    }

    Status getStats(TableStats& aggr_stats_out);

    Status getLastSeqnum(uint64_t& seqnum_out);

    uint64_t getBoosterLimit(size_t level) const;

    bool isCompactionAllowed() const { return allowCompaction; }

    void setTableFile(std::list<Record*>& batch,
                      std::list<uint64_t>& checkpoints,
                      bool bulk_load_mode,
                      TableFile* table_file,
                      uint32_t target_hash,
                      const SizedBuf& min_key,
                      const SizedBuf& next_min_key);

    void setTableFileOffset(std::list<uint64_t>& checkpoints,
                            TableFile* src_file,
                            TableFile* dst_file,
                            std::vector<uint64_t>& offsets,
                            uint64_t start_index,
                            uint64_t count);

    void setTableFileItrFlush(TableFile* dst_file,
                              std::list<Record*>& recs_batch,
                              bool without_commit);

    uint64_t getNumWrittenRecords() const { return numWrittenRecords; }

    void resetNumWrittenRecords() { numWrittenRecords = 0; }

    struct Iterator {
    public:
        Iterator();
        ~Iterator();

        enum SeekOption {
            GREATER = 0,
            SMALLER = 1,
        };

        Status init(DB* snap_handle,
                    TableMgr* table_mgr,
                    const SizedBuf& start_key,
                    const SizedBuf& end_key);
        Status initSN(DB* snap_handle,
                      TableMgr* table_mgr,
                      uint64_t min_seq,
                      uint64_t max_seq);
        Status get(Record& rec_out);
        Status prev();
        Status next();
        Status seek(const SizedBuf& key, SeekOption opt = GREATER);
        Status seekSN(const uint64_t seqnum, SeekOption opt = GREATER);
        Status gotoBegin();
        Status gotoEnd();
        Status close();
    private:
        enum Type {
            BY_KEY = 0,
            BY_SEQ = 1
        };
        struct ItrItem {
            ItrItem() : flags(0x0), tInfo(nullptr), tItr(nullptr) {}
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

                // In case of the same seq number,
                // we should compare pointer to make AVL-tree distinguish
                // different objects who have the same seq number.
                // NOTE: iterator will not care of equality (==) condition.
                if (aa < bb) return -1;
                else if (aa > bb) return 1;

                // Return 0 only if a and b are the same memory object.
                return 0;
            }
            inline static int cmpKey(avl_node *a, avl_node *b, void *aux) {
                ItrItem* aa = _get_entry(a, ItrItem, an);
                ItrItem* bb = _get_entry(b, ItrItem, an);

                CMP_NULL_CHK(aa->lastRec.kv.key.data, bb->lastRec.kv.key.data);

                int cmp = 0;
                if (aux) {
                    // Custom cmp mode.
                    TableMgr* tm = reinterpret_cast<TableMgr*>(aux);
                    CustomCmpFunc func = tm->getDbConfig()->cmpFunc;
                    void* param = tm->getDbConfig()->cmpFuncParam;
                    cmp = func(aa->lastRec.kv.key.data, aa->lastRec.kv.key.size,
                               bb->lastRec.kv.key.data, bb->lastRec.kv.key.size,
                               param);
                } else {
                    cmp = SizedBuf::cmp(aa->lastRec.kv.key, bb->lastRec.kv.key);
                }

                // Note: key: ascending, seq: descending order.
                if (cmp == 0) return cmpSeq(b, a, aux);
                return cmp;
            }
            avl_node an;
            uint8_t flags;
            TableInfo* tInfo;
            TableFile::Iterator* tItr;
            Record lastRec;
        };

        void addTableItr(DB* snap_handle, TableInfo* t_info);
        Status initInternal(DB* snap_handle,
                            TableMgr* table_mgr,
                            uint64_t min_seq,
                            uint64_t max_seq,
                            const SizedBuf& start_key,
                            const SizedBuf& end_key,
                            Type _type);
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
        TableMgr* tMgr;
        std::vector< std::vector<ItrItem*> > tables;
        std::list<TableInfo*>* snapTableList;
        uint64_t minSeqSnap;
        uint64_t maxSeqSnap;
        SizedBuf startKey;
        SizedBuf endKey;
        avl_tree curWindow;
        avl_node* windowCursor;
    };

protected:
// === TYPES
    struct RecGroup {
        RecGroup(std::list<Record*>* _recs,
                 TableInfo* _table)
            : recs(_recs), table(_table) {}
        // List of records to put into table.
        std::list<Record*>* recs;
        // If null, no corresponding table. Should create a new table.
        TableInfo* table;
    };

    struct RecGroupItr {
        RecGroupItr(const SizedBuf& _min_key,
                    uint64_t _index,
                    TableInfo* _table)
            : index(_index)
            , table(_table)
        {
            _min_key.copyTo(minKey);
        }

        ~RecGroupItr() {
            minKey.free();
        }

        // Min key for this table.
        SizedBuf minKey;

        // Starting index of this table, in offset array.
        uint64_t index;

        // If null, no corresponding table. Should create a new table.
        TableInfo* table;
    };

    using TableList = std::list<TableInfo*>;
    using SnapMap = std::unordered_map<DB*, TableList*>;

// === FUNCTIONS
    void logTableSettings(const DBConfig* db_config);

    Status createNewTableFile(size_t level,
                              TableFile*& table_file_out,
                              const TableFileOptions& t_opt);

    TableInfo* getSmallestSrcTable(const std::list<TableInfo*>& tables,
                                   uint32_t target_hash_num = _SCU32(-1));

    void getTwoSmallSrcTables(const std::list<TableInfo*>& tables,
                              uint32_t target_hash_num,
                              TableInfo*& table1_out,
                              TableInfo*& table2_out);

    TableInfo* getSmallestNormalTable(const std::list<TableInfo*>& tables,
                                      uint32_t target_hash_num = _SCU32(-1));

    void getTwoSmallNormalTables(const std::list<TableInfo*>& tables,
                                 uint32_t target_hash_num,
                                 TableInfo*& table1_out,
                                 TableInfo*& table2_out);

    void putLsmFlushResult(TableFile* cur_file,
                           const std::list<Record*>& local_records,
                           std::list<TableMgr::LsmFlushResult>& res_out);

    void putLsmFlushResultWithKey(TableFile* cur_file,
                                  const SizedBuf& key,
                                  std::list<TableMgr::LsmFlushResult>& res_out);

    bool isTableLocked(uint64_t t_number);

    bool isLevelLocked(uint64_t l_number);

// === VARIABLES
    const size_t APPROX_META_SIZE;

    // Backward pointer to parent DB instance.
    DB* parentDb;

    std::atomic<bool> allowCompaction;

    TableMgrOptions opt;
    TableManifest* mani;

    std::mutex sMapLock;
    SnapMap sMap;

    std::mutex L0Lock;

    uint32_t numL0Partitions;

    // Compaction status of L0 hash partitions.
    std::vector< std::atomic<bool>* > compactStatus;

    // Number of on-going compactions in L1,
    // only used for level extension mode.
    std::atomic<size_t> numL1Compactions;

    // Set of tables being compacted/merged/split.
    std::set<uint64_t> lockedTables;
    std::mutex lockedTablesLock;

    // Set of (source) levels that interlevel compaction is in progress.
    std::unordered_set<size_t> lockedLevels;
    std::mutex lockedLevelsLock;

    // Total accumulated number of records written to table.
    std::atomic<uint64_t> numWrittenRecords;

    SimpleLogger* myLog;
};

} // namespace jungle

