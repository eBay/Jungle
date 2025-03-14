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

namespace jungle {

// Logically do the same thing as `compactLevel()`,
// but does not keep records in memory, and instead
// use iterator directly.
//
// Trade-off: less memory, but needs two-phase scan.
//
Status TableMgr::compactLevelItr(const CompactOptions& options,
                                 TableInfo* victim_table,
                                 size_t level) {
    if (level >= mani->getNumLevels()) return Status::INVALID_LEVEL;

    Status s;
    Timer tt;

    const DBConfig* db_config = getDbConfig();
    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    const GlobalConfig* global_config = mgr->getGlobalConfig();
    const GlobalConfig::CompactionThrottlingOptions& t_opt =
        global_config->ctOpt;

    bool is_last_level = (level + 1 == mani->getNumLevels());
    if (is_last_level && level >= 1) {
        return migrateLevel(options, level);
    }

    TableInfo* local_victim = findLocalVictim( level, victim_table,
                                               WORKING_SET_SIZE, false );
    if (!local_victim) return Status::TABLE_NOT_FOUND;

    _log_info( myLog, "interlevel compaction %zu -> %zu, victim %zu_%zu, "
               "min key %s",
               level, level+1, opt.prefixNum, local_victim->number,
               local_victim->minKey.toReadableString().c_str() );

    // List of overlapping tables (before compaction) in the next level.
    std::list<TableInfo*> tables;

    // List of tables to write in the next level.
    // If we need to create a new table, `TableInfo` of that
    // element will be `nullptr`.
    // `minKey` of every element occupies newly allocated memory region,
    // so that should be freed at the end of this function.
    std::vector<RecGroupItr*> new_tables;
    TableFile::Iterator* itr = nullptr;

   try {
    // Lock level (only one interlevel compaction is allowed at a time).
    LevelLockHolder lv_holder(this, level);
    if (!lv_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    // Lock the victim table.
    // The other tables will be locked below (in `setBatchLsm()`).
    TableLockHolder tl_src_holder(this, {local_victim->number});
    if (!tl_src_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    // Get overlapping tables.
    SizedBuf min_key_victim = local_victim->minKey;
    SizedBuf max_key_victim;
    SizedBuf::Holder h_max_key_victim(max_key_victim);
    local_victim->file->getMaxKey(max_key_victim);

    {   // WARNING: See the comment at `mani->removeTableFile` in `compactLevelItr`.
        std::lock_guard<std::mutex> l(mani->getLock());
        mani->getTablesRange(level+1, min_key_victim, max_key_victim, tables);
    }
    _log_info( myLog, "victim table's min key %s max key %s, "
               "%zu overlapping table in level %zu",
               min_key_victim.toReadableString().c_str(),
               max_key_victim.toReadableString().c_str(),
               tables.size(), level + 1 );
    for (auto& entry: tables) {
        TableInfo* tt = entry;
        _log_info( myLog, "table %zu %s at level %zu",
                   tt->number,
                   tt->minKey.toReadableString().c_str(),
                   level + 1 );
    }

    // Lock destination tables.
    TableLockHolder tl_dst_holder( this, table_list_to_number(tables) );
    if (!tl_dst_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    TableStats victim_stats;
    local_victim->file->getStats(victim_stats);

    auto t_itr = tables.begin();
    auto t_itr_end = tables.rbegin();

    TableInfo* min_table = (t_itr != tables.end()) ? *t_itr : nullptr;
    TableInfo* max_table = (t_itr_end != tables.rend()) ? *t_itr_end : nullptr;
    TableInfo* next_table = nullptr;
    RecGroupItr* cur_group = nullptr;
    SizedBuf max_key_table;
    SizedBuf::Holder h_max_key_table(max_key_table);
    if (max_table) {
        max_table->file->getMaxKey(max_key_table);
        if (max_key_table.empty()) {
            _log_info(myLog, "Table is empty, setting table's min key as the max key");
            max_table->minKey.copyTo(max_key_table);
        }
        _log_info( myLog, "max key among tables %s",
                   max_key_table.toReadableString().c_str() );
    }

    uint64_t acc_size = 0;
    uint64_t acc_records = 0;
    if ( !min_table || min_key_victim < min_table->minKey ) {
        // Should create a new table for min key.
        cur_group = new RecGroupItr(SizedBuf(), 0, nullptr);
        new_tables.push_back(cur_group);
        if (min_table) next_table = min_table;

    } else {
        // `min_table` should be the first table.
        cur_group = new RecGroupItr(min_table->minKey, 0, min_table);
        new_tables.push_back(cur_group);
        while (t_itr != tables.end()) {
            TableInfo* tt = *t_itr;
            if (tt->minKey <= min_key_victim) {
                cur_group->table = tt;
            } else {
                break;
            }
            t_itr++;
        }
        next_table = (t_itr == tables.end()) ? nullptr : *t_itr;

        if (cur_group->table) {
            TableStats stats_out;
            cur_group->table->file->getStats(stats_out);
            acc_size = stats_out.workingSetSizeByte;
            acc_records = stats_out.numKvs;
            _log_info( myLog, "acc size of table %zu: %zu, %zu records",
                       cur_group->table->number,
                       acc_size,
                       acc_records );
        }
    }

    // Read all records from the victim table.
    uint64_t num_records_read = 0;
    uint64_t TABLE_LIMIT = db_config->getMaxTableSize(level + 1);
    uint64_t TABLE_LIMIT_NUM = 0;
    size_t min_num_tables_for_new_level =
        std::max( (size_t)db_config->minNumTablesPerLevel,
                  db_config->getMaxParallelWriters() );
    if ( max_key_table.empty() &&
         db_config->nextLevelExtension &&
         min_num_tables_for_new_level ) {
        // It means that this is the first flush to L1.
        // Ignore given table size, and divide evenly
        // (basic assumption is that L0 is hash-partitioned so that it
        //  won't harm the key distribution of L1).
        uint64_t tmp_backup = TABLE_LIMIT;

        TABLE_LIMIT = victim_stats.workingSetSizeByte / min_num_tables_for_new_level;
        if (!TABLE_LIMIT) TABLE_LIMIT = tmp_backup;

        TABLE_LIMIT_NUM = (victim_stats.numKvs / min_num_tables_for_new_level) + 1;
        if (!TABLE_LIMIT_NUM) TABLE_LIMIT_NUM = tmp_backup;

        _log_info(myLog, "[FIRST L1 WRITE] table limit is adjusted to %zu and %zu, "
                  "num tables %zu, victim WSS %zu, %zu records",
                  TABLE_LIMIT,
                  TABLE_LIMIT_NUM,
                  min_num_tables_for_new_level,
                  victim_stats.workingSetSizeByte,
                  victim_stats.numKvs);
    }

    if (!TABLE_LIMIT_NUM && tables.size() && db_config->fastIndexScan) {
        uint64_t dst_total_records = 0;
        uint64_t dst_total_size = 0;
        for (TableInfo* ti: tables) {
            TableStats t_st;
            ti->file->getStats(t_st);
            dst_total_records += t_st.numKvs;
            dst_total_size += t_st.workingSetSizeByte;
        }

        if (dst_total_records && dst_total_size) {
            // Adjust limit using the ratio max L1 size : cur average size.
            TABLE_LIMIT_NUM = dst_total_records / tables.size();
            TABLE_LIMIT_NUM *= db_config->maxL1TableSize * tables.size();
            TABLE_LIMIT_NUM /= dst_total_size;
            _log_info(myLog, "[FAST SCAN] table limit is adjusted to %zu and %zu, "
                      "num tables %zu, total records %zu, total size %zu",
                      TABLE_LIMIT,
                      TABLE_LIMIT_NUM,
                      tables.size(),
                      dst_total_records,
                      dst_total_size);
        }
    }

    std::vector<uint64_t> offsets;
    // Reserve 10% more headroom, just in case.
    offsets.reserve(victim_stats.approxDocCount * 11 / 10);

    Timer throttling_timer(t_opt.resolution_ms);

    auto update_next_table_info = [&](const Record& cur_rec,
                                      uint64_t cur_offset,
                                      uint64_t cur_valuesize) {
        offsets.push_back(cur_offset);
        uint64_t cur_index = offsets.size() - 1;

        if ( next_table &&
             cur_rec.kv.key >= next_table->minKey ) {
            // New table.
            cur_group = new RecGroupItr( next_table->minKey,
                                         cur_index,
                                         next_table );
            new_tables.push_back(cur_group);
            acc_size = 0;
            acc_records = 0;

            // Skip tables whose range is not overlapping.
            while (t_itr != tables.end()) {
                TableInfo* tt = *t_itr;
                if (tt->minKey <= cur_rec.kv.key) {
                    cur_group->table = tt;
                } else {
                    break;
                }
                t_itr++;
            }
            next_table = (t_itr == tables.end()) ? nullptr : *t_itr;

            if (cur_group->table) {
                TableStats stats_out;
                cur_group->table->file->getStats(stats_out);
                acc_size = stats_out.workingSetSizeByte;
                acc_records = stats_out.numKvs;
                _log_info( myLog, "acc size of table %zu: %zu, %zu records",
                           cur_group->table->number,
                           acc_size,
                           acc_records );
            }

            if (next_table) {
                _log_info( myLog, "next table changed to %zu, %s, cur rec %s",
                           next_table->number,
                           next_table->minKey.toReadableString().c_str(),
                           cur_rec.kv.key.toReadableString().c_str() );
            } else {
                _log_info( myLog, "next table changed to NULL, cur rec %s",
                           cur_rec.kv.key.toReadableString().c_str() );
            }

        } else if ( acc_size > TABLE_LIMIT ||
                    ( TABLE_LIMIT_NUM &&
                      acc_records > TABLE_LIMIT_NUM ) ) {
            // If not, but accumulated size exceeds the limit,
            // move to a new table that is not in `tables`.
            //
            // BUT, ONLY WHEN THE KEY IS BIGGER THAN THE LAST TABLE'S MAX KEY.
            if (cur_rec.kv.key > max_key_table) {
                _log_info( myLog, "rec key %s is greater than max table key %s, "
                           "urgent split at level %zu, acc size %zu, "
                           "acc records %zu, limit %zu",
                           cur_rec.kv.key.toReadableString().c_str(),
                           max_key_table.toReadableString().c_str(),
                           level + 1,
                           acc_size,
                           acc_records,
                           TABLE_LIMIT );
                if (db_config->fastIndexScan) {
                    // If fast index scan mode, `cur_rec` may not have the
                    // full version of key. Read it from the offset.
                    Record rec_from_disk;
                    Record::Holder h(rec_from_disk);
                    local_victim->file->getByOffset(nullptr, cur_offset, rec_from_disk);
                    _log_info( myLog, "fast index scan mode, full key %s",
                               rec_from_disk.kv.key.toReadableString().c_str() );
                    cur_group = new RecGroupItr( rec_from_disk.kv.key,
                                                 cur_index,
                                                 nullptr);
                } else {
                    cur_group = new RecGroupItr( cur_rec.kv.key,
                                                 cur_index,
                                                 nullptr);
                }
                new_tables.push_back(cur_group);
                acc_size = 0;
                acc_records = 0;
            }
            // We SHOULD NOT move table cursor (`t_itr`) here.
        }

        // WARNING:
        //   If `fastIndexScan` option is on, `acc_size` is meaningless.
        acc_size += (cur_rec.size() + cur_valuesize + APPROX_META_SIZE);
        acc_records++;
        num_records_read++;

        if (d_params.compactionDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.compactionDelayUs);
        }

        // Do throttling, if enabled.
        TableMgr::doCompactionThrottling(t_opt, throttling_timer);
    };

    SizedBuf empty_key;
    auto it_cb = [&](const TableFile::IndexTraversalParams& params) ->
                 TableFile::IndexTraversalDecision {
        Record cur_rec;
        size_t value_size = 0;
        bool need_to_free = false;
        GcFunc gc{[&](){ if (need_to_free) cur_rec.free(); }};
        cur_rec.kv.key = params.key;

        // Remove trailing NULL chars,
        // as it may contain padding bytes at the end.
        // `cur_rec.kv.key.size` should be non-zero.
        while (cur_rec.kv.key.size > 1 &&
               cur_rec.kv.key.data[cur_rec.kv.key.size - 1] == 0x0) {
            cur_rec.kv.key.size--;
        }

        // Check if `params.key` is prefix of `next_table->minKey`.
        if ( next_table &&
             cur_rec.kv.key.size < next_table->minKey.size ) {
            SizedBuf tmp(cur_rec.kv.key.size, next_table->minKey.data);
            if (SizedBuf::cmp(cur_rec.kv.key, tmp) == 0) {
                // It is prefix, should read the whole record.
                local_victim->file->getByOffset(nullptr, params.offset, cur_rec);
                value_size = cur_rec.kv.value.size;
                need_to_free = true;
            }
        }
        update_next_table_info(cur_rec, params.offset, value_size);

        return TableFile::IndexTraversalDecision::NEXT;
    };

    if (db_config->fastIndexScan) {
        // Fast scanning by index traversal.
        _log_info(myLog, "do fast scan by index traversal");
        local_victim->file->traverseIndex(nullptr, empty_key, it_cb);

    } else {
        // Normal scanning by iterator.
        _log_info(myLog, "do normal scan by iteration");
        itr = new TableFile::Iterator();
        TC( itr->init(nullptr, local_victim->file, empty_key, empty_key) );

        // Initial scan to get
        //   1) number of files after split, and
        //   2) min keys for each new file.
        do {
            if (!isCompactionAllowed()) {
                throw Status(Status::COMPACTION_CANCELLED);
            }

            Record rec_out;
            Record::Holder h_rec_out(rec_out);
            size_t value_size_out = 0;
            uint64_t offset_out = 0;
            s = itr->getMeta(rec_out, value_size_out, offset_out);
            if (!s) break;

            update_next_table_info(rec_out, offset_out, value_size_out);

        } while (itr->next().ok());
        itr->close();
        DELETE(itr);
    }

    uint64_t elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    double scan_rate = (double)num_records_read * 1000000 / elapsed_us;
    _log_info(myLog, "reading table %zu_%zu done %zu us, "
              "tables before %zu after %zu, "
              "%zu records, %.1f iops",
              opt.prefixNum, local_victim->number, elapsed_us,
              tables.size(), new_tables.size(),
              num_records_read, scan_rate);

    size_t num_new_tables = new_tables.size();
    size_t max_writers = getDbConfig()->getMaxParallelWriters();
    std::list<uint64_t> dummy_chk;
    std::list<LsmFlushResult> results;

    if (num_new_tables) {
        // Extend new level if needed.
        if (is_last_level) mani->extendLevel();
    }

    for (size_t ii = 0; ii < num_new_tables; ) {
        size_t upto_orig = std::min(ii + max_writers, num_new_tables);

        // NOTE: request `req_writers - 1`, as the other one is this thread.
        size_t req_writers = upto_orig - ii;
        TableWriterHolder twh(mgr->tableWriterMgr(), req_writers - 1);

        // Lease may not succeed, adjust `upto`.
        size_t leased_writers = twh.leasedWriters.size();
        size_t upto = ii + leased_writers + 1;

        for (size_t jj = ii; jj < upto; jj++) {
            size_t worker_idx = jj - ii;
            bool leased_thread = (jj + 1 < upto);

            // Create a new file if necessary.
            TableFile* cur_file = nullptr;
            if (!new_tables[jj]->table) {
                TableFileOptions t_opt;
                s = createNewTableFile(level + 1, cur_file, t_opt);
                if (!s) continue;
            } else {
                cur_file = new_tables[jj]->table->file;
            }

            TableWriterArgs local_args;
            local_args.myLog = myLog;

            TableWriterArgs* w_args = (leased_thread)
                                      ? &twh.leasedWriters[worker_idx]->writerArgs
                                      : &local_args;
            w_args->callerAwaiter.reset();

            uint64_t count = (jj + 1 == num_new_tables)
                             ? offsets.size() - new_tables[jj]->index
                             : new_tables[jj+1]->index - new_tables[jj]->index;
            w_args->payload = TableWritePayload( this,
                                                 &offsets,
                                                 new_tables[jj]->index,
                                                 count,
                                                 &dummy_chk,
                                                 local_victim->file,
                                                 cur_file );
            if (!new_tables[jj]->table) {
                // Newly created table, put into the list.
                putLsmFlushResultWithKey( cur_file,
                                          new_tables[jj]->minKey,
                                          results );
            }

            if (leased_thread) {
                // Leased threads.
                w_args->invoke();
            } else {
                // This thread.
                TableWriterMgr::doTableWrite(w_args);
            }
        }

        // Wait for workers.
        for (size_t jj = ii; jj < upto - 1; jj++) {
            size_t worker_idx = jj - ii;
            TableWriterArgs* w_args = &twh.leasedWriters[worker_idx]->writerArgs;
            while ( !w_args->payload.isEmpty() ) {
                w_args->callerAwaiter.wait_ms(1000);
                w_args->callerAwaiter.reset();
            }
        }

        if (!isCompactionAllowed()) {
            // NOTE: keys will be freed below.
            for (LsmFlushResult& rr: results) delete rr.tFile;
            throw Status(Status::COMPACTION_CANCELLED);
        }

        ii += leased_writers + 1;
    }

    {   // Grab lock, add first, and then remove next.
        std::lock_guard<std::mutex> l(mani->getLock());
        // WARNING:
        //   We should add in descending order of min key.
        //   Otherwise, if there is a point query in the middle,
        //   it may go to wrong (newly created) table which
        //   causes false "key not found".
        results.sort( LsmFlushResult::cmp );
        for (LsmFlushResult& rr: results) {
            TC( mani->addTableFile( level + 1, 0, rr.minKey, rr.tFile ) );
            rr.tFile = nullptr;
        }

        // WARNING:
        //   Other compaction threads may call `getTablesRange` right after
        //   this API call. In such a case, if the min key of the current victim
        //   is empty (the smallest table), the table list that other threads
        //   get is incomplete and results in data corruption.
        //
        //   Hence, all threads who are going to write data to the same level
        //   should hold `mani->getLock()`.
        if (!options.doNotRemoveOldFile) {
            mani->removeTableFile(level, local_victim);
        }

        // NOTE:
        //   As an optimization, if this level is neither zero nor last one,
        //   create an empty table for the same range, to avoid unnecessary
        //   split in the future.
        //
        //   This operation is protected by `mani->getLock()`. Concurrent read
        //   threads should be fine without the lock.
        if (level > 0 && !is_last_level) {
            size_t num_tables_lv = 0;
            // Only if the number of tables is not enough, if there are
            // many (more than the threshold), we don't need to do this.
            mani->getNumTables(level, num_tables_lv);
            if (num_tables_lv < db_config->numL0Partitions * 2) {
                TableFileOptions t_opt;
                TableFile* cur_file = nullptr;
                s = createNewTableFile(level, cur_file, t_opt);
                if (s) {
                    mani->addTableFile(level, 0, local_victim->minKey, cur_file);
                }
            }
        }
    }

    // WARNING:
    //   Release it ONLY WHEN this table is not given by caller.
    //   If not, caller is responsible to release the table.
    if (!victim_table) local_victim->done();

    mani->store(true);

    // Update throttling rate.
    elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    double write_rate = (double)num_records_read * 1000000 / elapsed_us;
    if (parentDb && level == 0) {
        parentDb->p->tStats.lastTableFlushRate = write_rate;
        parentDb->p->tStats.lastTableFlushRateExpiry.reset();
    }
    _log_info( myLog, "L%zu write done: compaction %zu -> %zu, "
               "%zu records, %zu target tables, %zu us, %.1f iops",
               level+1, level, level+1, num_records_read,
               num_new_tables, elapsed_us, write_rate );

    for (TableInfo*& entry: tables) entry->done();
    for (RecGroupItr*& entry: new_tables) delete entry;
    return Status();

   } catch (Status s) { // ------------------------------------------------
    _log_err(myLog, "compaction failed: %d", (int)s);

    if (itr) {
        itr->close();
        DELETE(itr);
    }

    // WARNING: Ditto.
    if (!victim_table) local_victim->done();

    for (TableInfo*& entry: tables) entry->done();
    for (RecGroupItr*& entry: new_tables) delete entry;
    return s;
   }
}

Status TableMgr::migrateLevel(const CompactOptions& options,
                              size_t level)
{
    if (level >= mani->getNumLevels()) return Status::INVALID_LEVEL;

    Status s;
    Timer tt;
    SizedBuf empty_key;

    // Only the current last level can do this.
    bool is_last_level = (level + 1 == mani->getNumLevels());
    if (!is_last_level) return Status::INVALID_LEVEL;

    _log_info( myLog, "interlevel compaction %zu -> %zu, do migration",
               level, level+1 );

    // List of overlapping tables (before compaction) in the next level.
    std::list<TableInfo*> tables;
    {   // WARNING: See the comment at `mani->removeTableFile` in `compactLevelItr`.
        std::lock_guard<std::mutex> l(mani->getLock());
        mani->getTablesRange(level, empty_key, empty_key, tables);
    }
    if (tables.empty()) return Status::INVALID_LEVEL;

   try {
    // Lock level (only one interlevel compaction is allowed at a time).
    LevelLockHolder lv_holder(this, level);
    if (!lv_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    // Lock all tables in the current level.
    // The other tables will be locked below (in `setBatchLsm()`).
    TableLockHolder tl_src_holder(this, table_list_to_number(tables));
    if (!tl_src_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    mani->extendLevel();

    {   // Grab lock, add first, and then remove next.
        std::lock_guard<std::mutex> l(mani->getLock());

        // WARNING:
        //   We should add in descending order of min key.
        //   Otherwise, if there is a point query in the middle,
        //   it may go to wrong (newly created) table which
        //   causes false "key not found".
        auto eer = tables.rbegin();
        while (eer != tables.rend()) {
            TableInfo* ti = *eer;
            ti->setMigration();
            TC( mani->addTableFile( level + 1, 0, ti->minKey, ti->file ) );
            eer++;
        }

        auto eef = tables.begin();
        while (eef != tables.end()) {
            TableInfo* ti = *eef;
            mani->removeTableFile(level, ti);
            eef++;
        }
    }

    mani->store(true);

    // Update throttling rate.
    uint64_t elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    _log_info( myLog, "migration done: compaction %zu -> %zu, "
               "%zu moved tables, %zu us",
               level, level+1, tables.size(), elapsed_us );

    for (TableInfo*& entry: tables) entry->done();
    return Status();

   } catch (Status s) { // ------------------------------------------------
    _log_err(myLog, "compaction failed: %d", (int)s);

    for (TableInfo*& entry: tables) entry->done();
    return s;
   }
}

Status TableMgr::compactInPlace(const CompactOptions& options,
                                TableInfo* victim_table,
                                size_t level,
                                bool oldest_one_first)
{
    if (level >= mani->getNumLevels()) return Status::INVALID_LEVEL;

    Status s;
    Timer tt;
    VictimPolicy v_policy = oldest_one_first ? WORKING_SET_SIZE : STALE_RATIO;
    TableInfo* local_victim = findLocalVictim( level, victim_table,
                                               v_policy, false );
    if (!local_victim) return Status::TABLE_NOT_FOUND;

    _log_info( myLog, "in-place compaction at level %zu victim %zu_%zu, "
               "min key %s",
               level, opt.prefixNum, local_victim->number,
               local_victim->minKey.toReadableString().c_str() );

    void* dst_handle = nullptr;

   try {
    if (level == 1) numL1Compactions.fetch_add(1);

    // Lock victim table.
    TableLockHolder tl_holder(this, {local_victim->number});
    if (!tl_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    uint64_t dst_table_num = 0;
    std::string dst_filename;
    EP( mani->issueTableNumber(dst_table_num) );
    dst_filename = TableFile::getTableFileName(opt.path, opt.prefixNum, dst_table_num);

    if (opt.fOps->exist(dst_filename)) {
        // Previous file exists, which means that there is a legacy log file.
        // We should overwrite it.
        _log_warn( myLog, "table %s already exists, remove it",
                   dst_filename.c_str() );
        opt.fOps->remove(dst_filename);
    }

    // WARNING:
    //   The handle of the destination file should be cloased
    //   AFTER the file is officially opened by the manifest.
    //   Otherwise, the cached block will be purged and re-loaded.
    TC( local_victim->file->compactTo(dst_filename, options, dst_handle) );

    // Open newly compacted file, and add it to manifest.
    TableFile* newly_compacted_file = new TableFile(this);
    newly_compacted_file->setLogger(myLog);
    newly_compacted_file->load( level, dst_table_num, dst_filename,
                                opt.fOps, TableFileOptions() );
    mani->addTableFile(level, 0, local_victim->minKey, newly_compacted_file);

    // Add special checkpoint 0, for base snapshot.
    uint64_t new_file_last_seqnum = 0;
    newly_compacted_file->getLatestSnapMarker(new_file_last_seqnum);
    newly_compacted_file->addCheckpoint(0, new_file_last_seqnum);

    // Close the destination handle.
    local_victim->file->releaseDstHandle(dst_handle);

    {   // Remove source table file.
        std::lock_guard<std::mutex> l(mani->getLock());
        mani->removeTableFile(level, local_victim);
    }

    // WARNING:
    //   Release it ONLY WHEN this table is not given by caller.
    //   If not, caller is responsible to release the table.
    if (!victim_table) local_victim->done();

    mani->store(true);
    _log_info(myLog, "in-place compaction at level %zu done, %zu us",
              level, tt.getUs());

    if (level == 1) numL1Compactions.fetch_sub(1);
    return Status::OK;

   } catch (Status s) { // ------------------------------------
    // Close the destination handle.
    local_victim->file->releaseDstHandle(dst_handle);

    // WARNING: Ditto.
    if (!victim_table) local_victim->done();

    if (level == 1) numL1Compactions.fetch_sub(1);
    return s;
   }
}

Status TableMgr::compactL0(const CompactOptions& options,
                           uint32_t hash_num)
{
    if (!allowCompaction) {
        _log_warn(myLog, "compaction is not allowed");
        return Status::COMPACTION_IS_NOT_ALLOWED;
    }

    Status s;
    const DBConfig* db_config = getDbConfig();

    std::list<TableInfo*> tables;
    s = mani->getL0Tables(hash_num, tables);
    if (!s) {
        _log_warn(myLog, "tables of hash %zu not found", hash_num);
        return s;
    }

    bool exp = false;
    bool val = true;
    if (!compactStatus[hash_num]->compare_exchange_strong(exp, val)) {
        // Compaction is already in progress.
        _log_warn(myLog, "compaction hash %zu is in progress", hash_num);
        return Status::OPERATION_IN_PROGRESS;
    }

    std::stringstream s_log_msg_prefix;
    DBMgr* db_mgr = DBMgr::getWithoutInit();
    SimpleLogger* global_log = db_mgr->getLogger();

   try {
    // NOTE:
    //  Case 1) only one file exists: e.g.) table_000.
    //   - Create a new table: table_001, it will serve new writes.
    //   - Compact from table_000 to table_002.
    //  Case 2) two files exist: e.g.) table_001 and table_002.
    //   - Compact from table_001 to table_003.
    //   - table_002 will serve new writes.

    // WARNING: Should compact src file first if exists.
    TableInfo* origin_table = nullptr;
    TableInfo* merge_table = nullptr;
    getTwoSmallSrcTables(tables, hash_num, origin_table, merge_table);
    if (!origin_table) {
        getTwoSmallNormalTables(tables, hash_num, origin_table, merge_table);
        if (!origin_table) throw Status(Status::TABLE_NOT_FOUND);
    }

    TableStats origin_stat;
    origin_table->file->getStats(origin_stat);
    uint64_t target_filesize = origin_stat.workingSetSizeByte;
    target_filesize *= db_config->blockReuseFactor;
    target_filesize /= 100;

    uint64_t min_block_reuse_size = (uint64_t)64 * 1024 * 1024;
    if (!db_config->nextLevelExtension) {
        min_block_reuse_size = std::max(min_block_reuse_size, target_filesize);
    }
    _log_info(myLog, "target block reuse size %zu", min_block_reuse_size);

    // Create a new table file to serve writes from now on.
    //
    // WARNING: Only when the origin table is in NORMAL status.
    uint64_t new_normal_table_num = 0;
    TableFileOptions t_opt;
    if (origin_table->isNormal()) {
        if (db_config->nextLevelExtension && merge_table) {
            // Mode change happened. `merge_table` is older one, so that
            // we should compact it, and keep `origin_table`.
            new_normal_table_num = origin_table->number;
            origin_table = merge_table;
            merge_table = nullptr;
            _log_info( myLog, "detected legacy file format, "
                       "keep new normal table %zu and "
                       "compact the old table %zu to L1",
                       new_normal_table_num,
                       origin_table->number );

        } else {
            TableFile* new_normal_file = nullptr;
            t_opt.minBlockReuseFileSize = min_block_reuse_size;
            if ( db_config->bloomFilterBitsPerUnit > 0.0 ) {
                // New table's bloom filter size is based on the current WSS.
                t_opt.bloomFilterSize =
                    TableFile::getBfSizeByWss(db_config,
                                              origin_stat.workingSetSizeByte);
                if (!t_opt.bloomFilterSize) {
                    t_opt.bloomFilterSize =
                        TableFile::getBfSizeByLevel(db_config, 0);
                }
            }
            createNewTableFile(0, new_normal_file, t_opt);
            EP( mani->addTableFile(0, hash_num, SizedBuf(), new_normal_file) );
            new_normal_table_num = new_normal_file->getNumber();
        }

    } else {
        TableInfo* existing_normal_info =
            getSmallestNormalTable(tables, hash_num);
        new_normal_table_num = existing_normal_info->number;
    }

    uint64_t dst_table_num = 0;
    std::string dst_filename;
    {   // NOTE:
        //   To avoid the situation that new updates are written to
        //   the old file (i.e., compaction source), both setBatch()
        //   and compactL0() should be mutual exclusive.
        std::unique_lock<std::mutex> l(L0Lock);

        if (db_config->nextLevelExtension && !merge_table) {
            // Level-extension mode.
            s_log_msg_prefix
                << "hash " << hash_num << ", "
                << "table " << opt.prefixNum << "_" << origin_table->number
                << " -> next-level LSM, "
                << "new normal table " << opt.prefixNum << "_"
                << new_normal_table_num;

        } else {
            // L0-only mode
            // (or level-extension mode but cancelled legacy compaction).

            // Get DST file name.
            TC( mani->issueTableNumber(dst_table_num) );
            dst_filename = TableFile::getTableFileName
                           ( opt.path, opt.prefixNum, dst_table_num );

            if (merge_table) {
                s_log_msg_prefix
                    << "hash " << hash_num << ", "
                    << "table " << opt.prefixNum << "_" << origin_table->number
                    << " + " << opt.prefixNum << "_" << merge_table->number
                    << " -> " << opt.prefixNum << "_" << dst_table_num << ", "
                    << "new normal table " << opt.prefixNum << "_"
                    << new_normal_table_num;
            } else {
                s_log_msg_prefix
                    << "hash " << hash_num << ", "
                    << "table " << opt.prefixNum << "_" << origin_table->number
                    << " -> " << opt.prefixNum << "_" << dst_table_num << ", "
                    << "new normal table " << opt.prefixNum << "_"
                    << new_normal_table_num;
            }
        }
        _log_info( myLog, "[COMPACTION BEGIN] %s",
                   s_log_msg_prefix.str().c_str() );
        _log_info( global_log, "[COMPACTION BEGIN] %s",
                   s_log_msg_prefix.str().c_str() );

        // Before executing compaction, change the status.
        // Now all incoming writes will go to the next file.
        origin_table->setCompactSrc();
        if (merge_table) merge_table->setCompactSrc();
    }

    // Store manifest file.
    mani->store(true);

    if ( !dst_filename.empty() &&
         opt.fOps->exist(dst_filename) ) {
        // Previous file exists, which means that there is a legacy log file.
        // We should overwrite it.
        _log_warn( myLog, "table %s already exists, remove it",
                   dst_filename.c_str() );
        opt.fOps->remove(dst_filename);
    }

    Timer tt;

    // Do compaction.
    if (merge_table) {
        // Merge table exists: do merge compaction.
        s = origin_table->file->mergeCompactTo(merge_table->file->getName(),
                                               dst_filename,
                                               options);
        if (!s) throw s;

    } else {
        // Otherwise: single compaction
        if (db_config->nextLevelExtension) {
            // 1) Level-extension mode: merge (append) to the next level.
            s = compactLevelItr(options, origin_table, 0);

        } else {
            // 2) L0-only mode: only happens at the beginning.
            void* dst_handle = nullptr;
            s = origin_table->file->compactTo(dst_filename, options, dst_handle);
            origin_table->file->releaseDstHandle(dst_handle);
        }
        if (!s) throw s;
    }

    _log_info( myLog, "[COMPACTION END] %s, %zu us elapsed",
               s_log_msg_prefix.str().c_str(), tt.getUs() );
    _log_info( global_log, "[COMPACTION END] %s, %zu us elapsed",
               s_log_msg_prefix.str().c_str(), tt.getUs() );

    if (!db_config->nextLevelExtension || merge_table) {
        // L0-only mode
        // (or extension mode but merge table exists due to legacy file format).
        // Open newly compacted file, and add it to manifest.
        TableFile* newly_compacted_file = new TableFile(this);
        newly_compacted_file->setLogger(myLog);
        newly_compacted_file->load(0, dst_table_num, dst_filename, opt.fOps, t_opt);
        mani->addTableFile(0, hash_num, SizedBuf(), newly_compacted_file);

        // Add special checkpoint 0, for base snapshot.
        uint64_t new_file_last_seqnum = 0;
        newly_compacted_file->getLatestSnapMarker(new_file_last_seqnum);
        newly_compacted_file->addCheckpoint(0, new_file_last_seqnum);

        // Remove old file from manifest, and delete it.
        mani->removeTableFile(0, origin_table);
        if (merge_table) mani->removeTableFile(0, merge_table);

        for (TableInfo*& table: tables) table->done();

        // Store manifest file.
        mani->store(true);

    } else {
        // Level-extension mode:
        //   `compactLevel` already removed & synced files.
        //   Just release.
        for (TableInfo*& table: tables) table->done();
    }

    compactStatus[hash_num]->store(false);
    return Status();

   } catch (Status s) { // ========

    if (s == Status::COMPACTION_CANCELLED) {
        _log_warn( myLog, "[COMPACTION CANCELLED] %s",
                   s_log_msg_prefix.str().c_str() );
        _log_warn( global_log, "[COMPACTION CANCELLED] %s",
                   s_log_msg_prefix.str().c_str() );
    } else {
        _log_warn( myLog, "[COMPACTION ERROR]: %s, %d",
                   s_log_msg_prefix.str().c_str(), s );
        _log_warn( global_log, "[COMPACTION ERROR]: %s, %d",
                   s_log_msg_prefix.str().c_str(), s );
    }
    mani->store(true);

    for (TableInfo*& table: tables) table->done();
    compactStatus[hash_num]->store(false);
    return s;
   }
}

}; // namespace jungle

