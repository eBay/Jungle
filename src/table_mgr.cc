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

#include "table_mgr.h"

#include "db_internal.h"
#include "internal_helper.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <algorithm>
#include <atomic>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_set>

namespace jungle {

TableMgr::TableMgr(DB* parent_db)
    : APPROX_META_SIZE(96)
    , parentDb(parent_db)
    , allowCompaction(false)
    , mani(nullptr)
    , numL0Partitions(1)
    , numL1Compactions(0)
    , numWrittenRecords(0)
    , urgentCompactionMaxTableIdx(0)
    , myLog(nullptr)
    {}

TableMgr::~TableMgr() {
    assert(sMap.size() == 0);

    for (size_t ii=0; ii<compactStatus.size(); ++ii) {
        delete compactStatus[ii];
    }

    delete mani;
    shutdown();
}

Status TableMgr::createNewTableFile( size_t level,
                                     TableFile*& table_file_out,
                                     const TableFileOptions& t_opt ) {
    Status s;
    uint64_t t_num = 0;
    EP( mani->issueTableNumber(t_num) );

    TableFile* t_file = new TableFile(this);
    t_file->setLogger(myLog);
    std::string t_filename =
        TableFile::getTableFileName(opt.path, opt.prefixNum, t_num);

    EP( t_file->create(level, t_num, t_filename, opt.fOps, t_opt) );
    uint64_t bf_size = t_file->getBfSize();
    _log_info(myLog, "level %zu: created new table %zu_%zu, "
              "BF size %zu bytes (%zu bits)",
              level, opt.prefixNum, t_num,
              bf_size / 8, bf_size);

    table_file_out = t_file;
    return Status();
}

void TableMgr::logTableSettings(const DBConfig* db_config) {
    if (db_config->compactionFactor) {
        _log_info( myLog, "compaction factor %u, reuse factor %zu, "
                   "num writes to compact %zu, "
                   "min file size %zu, cycle at least %u at most %u, "
                   "tombstones: %s",
                   db_config->compactionFactor,
                   db_config->blockReuseFactor,
                   db_config->numWritesToCompact,
                   db_config->minFileSizeToCompact,
                   db_config->minBlockReuseCycleToCompact,
                   db_config->maxBlockReuseCycle,
                   db_config->purgeDeletedDocImmediately
                   ? "purge immediately" : "keep until compaction");
    } else {
        _log_info(myLog, "auto compaction is disabled");
    }
    if (db_config->compactionCbFunc) {
        _log_info(myLog, "compaction callback function is given by user");
    }
    _log_info( myLog, "table lookup booster limit %zu %zu",
               getBoosterLimit(0), getBoosterLimit(1) );
    _log_info( myLog, "next level extension %s",
               getOnOffStr(db_config->nextLevelExtension) );
    _log_info( myLog, "bloom filter bits per unit: %.1f",
               db_config->bloomFilterBitsPerUnit );
    if (db_config->nextLevelExtension) {
        _log_info( myLog, "L0 table limit %zu, L1 table limit %zu, "
                   "L1 size limit %zu",
                   db_config->maxL0TableSize,
                   db_config->maxL1TableSize,
                   db_config->maxL1Size );
        std::string ratio_str;
        for (double dd: db_config->tableSizeRatio) {
            ratio_str += std::to_string(dd) + " ";
        }
        ratio_str += ", ";
        for (double dd: db_config->levelSizeRatio) {
            ratio_str += std::to_string(dd) + " ";
        }
        _log_info( myLog, "ratio: %s", ratio_str.c_str() );
    }

    DBMgr* mgr = DBMgr::getWithoutInit();
    if (mgr) {
        GlobalConfig* g_conf = mgr->getGlobalConfig();
        _log_info( myLog, "flusher: %zu, compactor: %zu, writer: %zu, "
                   "max parallel writes: %zu",
                   g_conf->numFlusherThreads,
                   g_conf->numCompactorThreads,
                   g_conf->numTableWriters,
                   db_config->getMaxParallelWriters() );
    }

    _log_info(myLog, "given compression callbacks: "
              "cbGetMaxSize %s, cbCompress %s, cbDecompress %s",
              db_config->compOpt.cbGetMaxSize ? "O" : "X",
              db_config->compOpt.cbCompress ? "O" : "X",
              db_config->compOpt.cbDecompress ? "O" : "X");

    _log_info(myLog, "expected user threads: %zu, upper prime %zu",
              db_config->numExpectedUserThreads,
              PrimeNumber::getUpper(db_config->numExpectedUserThreads));
}

Status TableMgr::init(const TableMgrOptions& _options) {
    if (mani) return Status::ALREADY_INITIALIZED;

    opt = _options;
    const DBConfig* db_config = getDbConfig();

    if ( db_config->compOpt.cbGetMaxSize &&
         db_config->compOpt.cbCompress &&
         db_config->compOpt.cbDecompress ) {
        // Cache the compression setting.
        opt.compressionEnabled = true;
    }

    compactStatus.resize(db_config->numL0Partitions);
    for (size_t ii=0; ii<compactStatus.size(); ++ii) {
        std::atomic<bool>*& entry = compactStatus[ii];
        entry = new std::atomic<bool>(false);
    }

    Status s;
    mani = new TableManifest(this, opt.fOps);
    mani->setLogger(myLog);

    char p_num[16];
    sprintf(p_num, "%04" PRIu64, opt.prefixNum);
    std::string m_filename = opt.path + "/table" + p_num + "_manifest";

   try {
    if (opt.fOps->exist(m_filename.c_str())) {
        // Manifest file already exists, load it.
        s = mani->load(opt.path, opt.prefixNum, m_filename);
        if (!s) {
            // Error happened, try again using backup file.
            _log_err(myLog, "loading manifest error: %d, try again", s);
            TC(BackupRestore::restore(opt.fOps, m_filename));
            s = mani->load(opt.path, opt.prefixNum, m_filename);
        }
        if (!s) throw s;

        // Check & adjust num level-0 partitions.
        size_t ii = 0;
        size_t num_tables = 0;
        s = mani->getNumTables(0, num_tables);
        while (ii < num_tables) {
            if (!mani->doesHashPartitionExist(ii)) break;
            ++ii;
        }
        numL0Partitions = ii;

        // Adjust table file option for block reuse min size.
        if ( !db_config->nextLevelExtension ) {
            for (size_t ii=0; ii<numL0Partitions; ++ii) {
                std::list<TableInfo*> tables;
                s = mani->getL0Tables(ii, tables);
                if (!s) continue;

                // Find max working set size.
                uint64_t max_working_set_size = 0;
                for (TableInfo* t_info: tables) {
                    TableStats t_stats;
                    t_info->file->getStats(t_stats);
                    max_working_set_size = std::max( max_working_set_size,
                                                     t_stats.workingSetSizeByte );
                }

                for (TableInfo* t_info: tables) {
                    TableFileOptions t_opt;
                    t_opt.minBlockReuseFileSize =
                        std::max( t_opt.minBlockReuseFileSize,
                                  max_working_set_size *
                                      getDbConfig()->blockReuseFactor / 100 );
                    t_info->file->changeOptions(t_opt);
                    t_info->done();
                }
            }
        }

    } else {
        // Not exist, initial setup phase.

        // Create manifest file.
        s = mani->create(opt.path, m_filename);
        if (!s) return s;

        numL0Partitions = db_config->numL0Partitions;
        if ( !db_config->logSectionOnly &&
             !db_config->readOnly ) {
            // Hash partition.
            for (size_t ii=0; ii<numL0Partitions; ++ii) {
                TableFile* t_file = nullptr;
                TableFileOptions t_opt;
                // Set 1M bits as an initial size.
                // It will be converging to some value as compaction happens.
                t_opt.bloomFilterSize = 1024 * 1024;
                EP( createNewTableFile(0, t_file, t_opt) );
                EP( mani->addTableFile(0, ii, SizedBuf(), t_file) );
            }

        } else {
            // Otherwise: table section is disabled.
            numL0Partitions = 0;
        }

        // Store manifest file.
        mani->store(true);
    }
    logTableSettings(db_config);

    removeStaleFiles();

    const DBConfig* db_config = getDbConfig();
    if (!db_config->readOnly) {
        allowCompaction = true;
    }
    return Status();

   } catch (Status s) {
    _log_err(myLog, "init manifest error: %d", s);
    DELETE(mani);
    return s;
   }
}

Status TableMgr::removeStaleFiles() {
    // Do nothing in read only mode.
    if (getDbConfig()->readOnly) return Status();

    std::vector<std::string> files;
    FileMgr::scan(opt.path, files);

    char p_num[16];
    sprintf(p_num, "%04" PRIu64, opt.prefixNum);
    std::string prefix = "table";
    prefix += p_num;
    prefix += "_";
    size_t prefix_len = prefix.size();

    std::string m_filename = "table";
    m_filename += p_num;
    m_filename += "_manifest";

    std::set<uint64_t> table_numbers;
    mani->getTableNumbers(table_numbers);

    for (auto& entry: files) {
        std::string& ff = entry;
        size_t pos = ff.find(prefix);
        if ( pos != std::string::npos &&
             ff.find(m_filename) == std::string::npos ) {
            // Check if it is in manifest.
            uint64_t t_num = atoi( ff.substr( prefix_len,
                                              ff.size() - prefix_len ).c_str() );
            if (table_numbers.find(t_num) == table_numbers.end()) {
                Timer tt;
                opt.fOps->remove(opt.path + "/" + ff);
                _log_warn(myLog, "%s does not exist in manifest, removed. %zu us",
                          ff.c_str(), tt.getUs());
            }
        }
    }
    return Status();
}

Status TableMgr::shutdown() {
    fdb_status fs = fdb_shutdown();
    if (fs != FDB_RESULT_SUCCESS) {
        return Status::ERROR;
    }
    return Status();
}

Status TableMgr::openSnapshot(DB* snap_handle,
                              const uint64_t checkpoint,
                              std::list<TableInfo*>*& table_list_out)
{
    Status s;
    TableList* t_list = new TableList();
    {   mGuard l(sMapLock);
        sMap.insert( std::make_pair(snap_handle, t_list) );
    }

    SizedBuf empty_key;
    size_t num_levels = mani->getNumLevels();

    for (size_t ii=0; ii<num_levels; ++ii) {
        s = mani->getTablesRange(ii, empty_key, empty_key, *t_list);
        if (!s) continue;
    }

    // If `checkpoint == 0`, take the latest one.
    for (auto& entry: *t_list) {
        TableInfo* info = entry;
        info->file->openSnapshot(snap_handle, checkpoint);
    }

    table_list_out = t_list;
    return Status();
}

Status TableMgr::closeSnapshot(DB* snap_handle) {
    Status s;
    TableList* t_list = nullptr;
    {   mGuard l(sMapLock);
        auto entry = sMap.find(snap_handle);
        assert(entry != sMap.end());
        t_list = entry->second;
        sMap.erase(entry);
    }

    for (auto& entry: *t_list) {
        TableInfo* info = entry;
        info->file->closeSnapshot(snap_handle);
        info->done();
    }
    delete t_list;

    return Status();
}

Status TableMgr::storeManifest() {
    Status s;
    EP( mani->store(true) );
    return Status();
}

Status TableMgr::get(DB* snap_handle,
                     Record& rec_inout,
                     bool meta_only)
{
    // NOTE:
    //  `rec_inout.kv.key` is given by user, shouldn't free in here.
    const DBConfig* db_config = getDbConfig();
    if (db_config->logSectionOnly) return Status::KEY_NOT_FOUND;

    Status s;
    Record latest_rec;
    size_t num_levels = mani->getNumLevels();

    for (size_t ii=0; ii<num_levels; ++ii) {
        std::list<TableInfo*> tables;
        s = mani->getTablesPoint(ii, rec_inout.kv.key, tables);
        if (!s) continue;

        TableInfo* sm_table = nullptr;
        // Search smallest normal table first and then the others,
        // as it always has the newest data for the same key.
        sm_table = getSmallestNormalTable(tables);
        if (sm_table) {
            Record new_rec;
            new_rec.kv.key = rec_inout.kv.key;
            s = sm_table->file->get(snap_handle, new_rec, meta_only);
            if (s) {
                // `latest_rec` should be empty.
                assert(latest_rec.empty());
                new_rec.moveTo(latest_rec);
                for (TableInfo* tt: tables) tt->done();
                break;
            }
        }
        // Smallest table doesn't exist or exists but doesn't have the key.

        for (TableInfo* table: tables) {
            if (sm_table && table == sm_table) {
                // We already searched smallest normal table, skip.
                sm_table->done();
                continue;
            }

            Record new_rec;
            // WARNING: SHOULD NOT free `key` here
            //          as it is given by caller.
            new_rec.kv.key = rec_inout.kv.key;
            s = table->file->get(snap_handle, new_rec, meta_only);
            if ( s.ok() &&
                 ( latest_rec.empty() ||
                   latest_rec.seqNum < new_rec.seqNum ) ) {
                // `new_rec` is newer.
                latest_rec.kv.key.clear();
                latest_rec.free();
                new_rec.moveTo(latest_rec);
            } else {
                // `latest_rec` is newer.
                new_rec.kv.key.clear();
                new_rec.free();
            }
            table->done();
        }
        if (!latest_rec.empty()) break;
    }
    if (latest_rec.empty()) return Status::KEY_NOT_FOUND;

    // Since `key` is given by caller, change the other fields only.
    latest_rec.kv.value.moveTo(rec_inout.kv.value);
    latest_rec.meta.moveTo(rec_inout.meta);
    rec_inout.seqNum = latest_rec.seqNum;
    rec_inout.type  = latest_rec.type;
    return Status();
}

Status TableMgr::getNearest(DB* snap_handle,
                            const SizedBuf& key,
                            Record& rec_inout,
                            SearchOptions s_opt,
                            bool meta_only)
{
    // NOTE:
    //  `rec_inout.kv.key` is given by user, shouldn't free in here.
    const DBConfig* db_config = getDbConfig();
    if (db_config->logSectionOnly) return Status::KEY_NOT_FOUND;

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

    auto update_cur_nearest = [&](Record& rec_out) {
        bool update_cur_nearest = false;
        if (cur_nearest.kv.key.empty()) {
            update_cur_nearest = true;
        } else {
            int cmp = 0;
            if (s_opt.isGreater()) {
                cmp = cmp_func(rec_out.kv.key, cur_nearest.kv.key);
            } else if (s_opt.isSmaller()) {
                cmp = cmp_func(cur_nearest.kv.key, rec_out.kv.key);
            }
            update_cur_nearest = (cmp < 0);
            if (cmp == 0) {
                // In case of exact match, compare the sequence number,
                // and choose the higher one.
                update_cur_nearest = (rec_out.seqNum > cur_nearest.seqNum);
            }
        }
        if (update_cur_nearest) {
            cur_nearest.free();
            rec_out.moveTo(cur_nearest);
        } else {
            rec_out.free();
        }
    };

    Status s;
    size_t num_levels = mani->getNumLevels();

    // NOTE:
    //   Unlike point query, nearest search should check the all level,
    //   as closer key may exist in lower levels.
    for (size_t ii=0; ii<num_levels; ++ii) {
        std::list<TableInfo*> tables;
        s = mani->getTablesNearest(ii, key, tables, s_opt);
        if (!s) continue;

        for (TableInfo* table: tables) {
            Record returned_rec;
            // WARNING: SHOULD NOT free `key` here
            //          as it is given by caller.
            s = table->file->getNearest( snap_handle,
                                         key,
                                         returned_rec,
                                         s_opt,
                                         meta_only );
            if (s) {
                update_cur_nearest(returned_rec);
            }
            table->done();
        }
    }
    if (cur_nearest.empty()) return Status::KEY_NOT_FOUND;

    cur_nearest.moveTo(rec_inout);
    return Status();
}

Status TableMgr::getPrefix(DB* snap_handle,
                           const SizedBuf& prefix,
                           SearchCbFunc cb_func)
{
    const DBConfig* db_config = getDbConfig();
    if (db_config->logSectionOnly) return Status::KEY_NOT_FOUND;

    Status s;
    size_t num_levels = mani->getNumLevels();

    for (size_t ii=0; ii<num_levels; ++ii) {
        std::list<TableInfo*> tables;
        s = mani->getTablesPrefix(ii, prefix, tables);
        if (!s) continue;

        bool stopped = false;
        for (TableInfo* table: tables) {
            if (!stopped) {
                s = table->file->getPrefix(snap_handle, prefix, cb_func);
            }
            table->done();
            if (s == Status::OPERATION_STOPPED) stopped = true;
        }
        if (stopped) break;
    }
    return s;
}

TableInfo* TableMgr::getSmallestSrcTable(const std::list<TableInfo*>& tables,
                                         uint32_t target_hash_num)
{
    TableInfo* target_table = nullptr;
    uint64_t min_tnum = std::numeric_limits<uint64_t>::max();
    for (TableInfo* table: tables) {
        if ( table->isSrc() &&
             table->number < min_tnum &&
             ( target_hash_num == _SCU32(-1) ||
               target_hash_num == table->hashNum ) ) {
            min_tnum = table->number;
            target_table = table;
        }
    }
    return target_table;
}

void TableMgr::getTwoSmallSrcTables(const std::list<TableInfo*>& tables,
                                    uint32_t target_hash_num,
                                    TableInfo*& table1_out,
                                    TableInfo*& table2_out)
{
    std::map<uint64_t, TableInfo*> sorted_tables;
    for (TableInfo* table: tables) {
        if ( table->isSrc() &&
             ( target_hash_num == _SCU32(-1) ||
               target_hash_num == table->hashNum ) ) {
            sorted_tables.emplace( table->number, table );
        }
    }

    size_t cnt = 0;
    table1_out = table2_out = nullptr;
    for (auto& entry: sorted_tables) {
        if (cnt == 0) table1_out = entry.second;
        else if (cnt == 1) table2_out = entry.second;
        cnt++;
    }
}

TableInfo* TableMgr::getSmallestNormalTable(const std::list<TableInfo*>& tables,
                                            uint32_t target_hash_num)
{
    TableInfo* target_table = nullptr;
    uint64_t min_tnum = std::numeric_limits<uint64_t>::max();
    for (TableInfo* table: tables) {
        if ( table->isNormal() &&
             table->number < min_tnum &&
             ( target_hash_num == _SCU32(-1) ||
               target_hash_num == table->hashNum ) ) {
            min_tnum = table->number;
            target_table = table;
        }
    }
    return target_table;
}

void TableMgr::getTwoSmallNormalTables(const std::list<TableInfo*>& tables,
                                       uint32_t target_hash_num,
                                       TableInfo*& table1_out,
                                       TableInfo*& table2_out)
{
    std::map<uint64_t, TableInfo*> sorted_tables;
    for (TableInfo* table: tables) {
        if ( table->isNormal() &&
             ( target_hash_num == _SCU32(-1) ||
               target_hash_num == table->hashNum ) ) {
            sorted_tables.emplace( table->number, table );
        }
    }

    size_t cnt = 0;
    table1_out = table2_out = nullptr;
    for (auto& entry: sorted_tables) {
        if (cnt == 0) table1_out = entry.second;
        else if (cnt == 1) table2_out = entry.second;
        cnt++;
    }
}

size_t TableMgr::getNumLevels() const {
    return mani->getNumLevels();
}

Status TableMgr::getLevelSize(size_t level,
                              uint64_t& wss_out,
                              uint64_t& total_out,
                              uint64_t& max_stack_size_out)
{
    if (level >= mani->getNumLevels()) return Status::INVALID_LEVEL;

    Status s;
    std::list<TableInfo*> tables;
    SizedBuf empty_key;
    mani->getTablesRange(level, empty_key, empty_key, tables);

    uint64_t wss_local = 0;
    uint64_t total_local = 0;
    uint64_t max_stack_local = 0;
    for (TableInfo*& t_info: tables) {
        TableStats t_stats;
        EP( t_info->file->getStats(t_stats) );
        wss_local += t_stats.workingSetSizeByte;
        total_local += t_stats.totalSizeByte;

        uint64_t cur_stack_size = 1;
        TableStack* stack = t_info->stack;
        if (stack) {
            std::lock_guard<std::mutex> l(stack->lock);
            cur_stack_size += stack->tables.size();
        }
        max_stack_local = std::max( cur_stack_size, max_stack_local );

        t_info->done();
    }

    wss_out = wss_local;
    total_out = total_local;
    max_stack_size_out = max_stack_local;

    return Status::OK;
}

uint64_t TableMgr::getLevelSizeLimit(size_t level) const {
    if (level >= mani->getNumLevels()) return 0;
    const DBConfig* db_config = getDbConfig();

    if (level == 0) {
        return (uint64_t)
               db_config->numL0Partitions *
               db_config->maxL0TableSize;
    }
    uint64_t ret = db_config->maxL1Size;
    size_t num_ratio_elems = db_config->levelSizeRatio.size();
    double last_ratio = num_ratio_elems
                        ? *db_config->levelSizeRatio.rbegin()
                        : 10;
    for (size_t ii = 1; ii < level; ++ii) {
        size_t vector_idx = ii - 1;
        if (num_ratio_elems > vector_idx) {
            ret *= db_config->levelSizeRatio[vector_idx];
        } else {
            ret *= last_ratio;
        }
    }
    return ret;
}

bool TableMgr::disallowCompaction() {
    bool old_value = allowCompaction;
    allowCompaction = false;
    return old_value;
}

Status TableMgr::close() {
    disallowCompaction();

    _log_info(myLog, "Wait for on-going compaction.");
    bool wait_more = false;
    uint64_t ticks = 0;
    do {
        wait_more = false;
        // non-LSM mode compaction.
        for (size_t ii=0; ii<numL0Partitions; ++ii) {
            if (compactStatus[ii]->load()) {
                wait_more = true;
                break;
            }
        }
        // LSM mode compaction.
        {   std::lock_guard<std::mutex> l(lockedTablesLock);
            if (lockedTables.size()) wait_more = true;
        }
        if (wait_more) {
            ticks++;
            Timer::sleepMs(100);
        }
    } while (wait_more);
    _log_info(myLog, "Closing TableMgr, %zu ticks", ticks);

    return Status();
}

Status TableMgr::getAvailCheckpoints(std::list<uint64_t>& chk_out) {
    if (!mani) return Status::NOT_INITIALIZED;

    // Get tables in level-0.
    std::list<TableInfo*> t_info_ret;
    SizedBuf empty_key;

    // NOTE: checkpoint is a special case, we only search for level-0.
    mani->getTablesRange(0, empty_key, empty_key, t_info_ret);

    // For each table, get checkpoints.
    std::vector< std::unordered_set<uint64_t> > chks_by_hash(numL0Partitions);
    for (auto& entry: t_info_ret) {
        TableInfo* info = entry;
        std::list<uint64_t> chks_by_file;
        info->file->getAvailCheckpoints(chks_by_file);

        // Insert into hash set.
        for (auto& le: chks_by_file) {
            // 0: special case, skip.
            if (le == 0) continue;
            chks_by_hash[info->hashNum].insert(le);
        }

        info->done();
    }

    // Checkpoint should exist in all hash partitions.
    for (auto& e_outer: chks_by_hash[0]) {
        bool found_outer = true;
        for (size_t ii=1; ii<numL0Partitions; ++ii) {
            auto e_inner = chks_by_hash[ii].find(e_outer);
            if (e_inner == chks_by_hash[ii].end()) {
                found_outer = false;
                break;
            }
        }
        if (found_outer) {
            chk_out.push_back(e_outer);
        }
    }

    return Status();
}

Status TableMgr::getStats(TableStats& aggr_stats_out) {
    Status s;
    aggr_stats_out.numKvs = 0;

    // Get list of tables across all levels.
    std::list<TableInfo*> tables;
    size_t num_levels = mani->getNumLevels();
    for (size_t ii=0; ii<num_levels; ++ii) {
        SizedBuf empty_key;
        s = mani->getTablesRange(ii, empty_key, empty_key, tables);
        if (!s) continue;
    }

    uint64_t min_table_idx = std::numeric_limits<uint64_t>::max();
    uint64_t max_table_idx = 0;
    for (TableInfo*& cur_table: tables) {
        TableStats local_stat;
        cur_table->file->getStats(local_stat);
        aggr_stats_out.numKvs += local_stat.numKvs;
        aggr_stats_out.workingSetSizeByte += local_stat.workingSetSizeByte;
        aggr_stats_out.numIndexNodes += local_stat.numIndexNodes;

        min_table_idx = std::min(cur_table->number, min_table_idx);
        max_table_idx = std::max(cur_table->number, max_table_idx);

        cur_table->done();
    }

    aggr_stats_out.minTableIdx = min_table_idx;
    aggr_stats_out.maxTableIdx = max_table_idx;
    aggr_stats_out.cacheUsedByte = fdb_get_buffer_cache_used();
    return Status();
}

Status TableMgr::getLastSeqnum(uint64_t& seqnum_out) {
    Status s;
    std::list<TableInfo*> tables;
   try {
    for (size_t ii=0; ii<numL0Partitions; ++ii) {
        TC( mani->getL0Tables(ii, tables) );
    }

    uint64_t seqnum = 0;
    for (TableInfo*& cur_table: tables) {
        TableStats stats;
        cur_table->file->getStats(stats);
        _log_info(myLog, "table %zu (hash %zu) last seq %zu",
                  cur_table->number, cur_table->hashNum, stats.lastSeqnum);
        if (valid_number(stats.lastSeqnum) && stats.lastSeqnum > seqnum) {
            seqnum = stats.lastSeqnum;
        }
        cur_table->done();
    }
    if (seqnum) seqnum_out = seqnum;
    return Status();

   } catch (Status s) {
    for (TableInfo*& cur_table: tables) cur_table->done();
    return s;
   }
}

uint64_t TableMgr::getBoosterLimit(size_t level) const {
    const DBConfig* db_config = getDbConfig();

    // Invalid mode.
    if (!db_config->nextLevelExtension) return 0;

    // Only for level 0 and 1.
    if ( level >= 2 ||
         level + 1 > db_config->lookupBoosterLimit_mb.size() ) {
        return 0;
    }

    if (level == 0) {
        uint64_t size_limit = (uint64_t)db_config->lookupBoosterLimit_mb[0]
                              * 1024 * 1024;
        uint64_t ret = size_limit
                       / sizeof(TableLookupBooster::Elem)
                       / db_config->numL0Partitions;
        return ret;
    }
    if (level == 1) {
        uint64_t size_limit = (uint64_t)db_config->lookupBoosterLimit_mb[1]
                              * 1024 * 1024;
        size_t l1_tables = 1;
        mani->getNumTables(level, l1_tables);
        l1_tables = std::max(l1_tables, (size_t)db_config->numL0Partitions);
        uint64_t ret = size_limit
                       / sizeof(TableLookupBooster::Elem)
                       / l1_tables;
        return ret;
    }
    return 0;
}

void TableMgr::doCompactionThrottling
               ( const GlobalConfig::CompactionThrottlingOptions& t_opt,
                 Timer& throttling_timer )
{
    // Do throttling, if enabled.
    if ( t_opt.resolution_ms &&
         t_opt.throttlingFactor &&
         throttling_timer.timeout() ) {
        uint32_t factor = std::min(t_opt.throttlingFactor, (uint32_t)99);
        uint64_t elapsed_ms = throttling_timer.getMs();
        uint64_t to_sleep_ms =
            elapsed_ms * factor / (100 - factor);
        if (to_sleep_ms) {
            Timer::sleepMs(to_sleep_ms);
        }
        throttling_timer.reset();
    }
}

void TableMgr::setUrgentCompactionTableIdx(uint64_t to, size_t expiry_sec) {
    uint64_t prev = urgentCompactionMaxTableIdx;
    urgentCompactionMaxTableIdx = to;

    urgentCompactionTimer.setDurationMs(expiry_sec * 1000);
    urgentCompactionTimer.reset();

    _log_info(myLog, "set urgent compaction table index number "
              "to %zu (prev %zu), expiry %zu seconds",
              to, prev, expiry_sec);
}

void TableMgr::autoClearUrgentCompactionTableIdx() {
    if (!urgentCompactionMaxTableIdx) return;

    uint64_t min_table_idx = 0;
    Status s = mani->getSmallestTableIdx(min_table_idx);
    if (!s || !min_table_idx) return;

    if (min_table_idx) {
        if (min_table_idx > urgentCompactionMaxTableIdx) {
            _log_info(myLog, "current smallest table index %zu is "
                      "smaller than urgent compaction number %zu",
                      min_table_idx,
                      urgentCompactionMaxTableIdx.load());
            setUrgentCompactionTableIdx(0, 0);
        }

        uint64_t duration_sec = urgentCompactionTimer.durationUs / 1000 / 1000;
        if (duration_sec && urgentCompactionTimer.timeout()) {
            _log_info(myLog, "urgent compaction task (up to %zu) is "
                      "expired (TTL %zu seconds), "
                      "current smallest table index %zu",
                      urgentCompactionMaxTableIdx.load(),
                      duration_sec,
                      min_table_idx);
            setUrgentCompactionTableIdx(0, 0);
        }
    }
}

} // namespace jungle

