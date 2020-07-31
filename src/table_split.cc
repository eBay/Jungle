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

namespace jungle {

Status TableMgr::splitLevel(const CompactOptions& options,
                            TableInfo* victim_table,
                            size_t level)
{
    if (level >= mani->getNumLevels()) return Status::INVALID_LEVEL;
    if ( victim_table &&
         victim_table->level != level ) return Status::INVALID_PARAMETERS;

    Status s;
    const DBConfig* db_config = getDbConfig();

    bool honor_limit = false;
    VictimPolicy vp = WORKING_SET_SIZE_SPLIT;
    TableInfo* local_victim = findLocalVictim( level, victim_table,
                                               vp, honor_limit );
    if (!local_victim) return Status::TABLE_NOT_FOUND;

    if (db_config->nextLevelExtension) {
        s = splitTableItr(local_victim);
    }

    // WARNING:
    //   Release it ONLY WHEN this table is not given by caller.
    //   If not, caller is responsible to release the table.
    if (!victim_table) local_victim->done();

    return s;
}

// Logically do the same thing as `splitTable()`,
// but does not keep records in memory, and instead
// use iterator directly.
//
// Trade-off: less memory, but needs two-phase scan.
//
Status TableMgr::splitTableItr(TableInfo* victim_table) {
    size_t level = victim_table->level;
    if (level >= mani->getNumLevels()) return Status::INVALID_LEVEL;

    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    const GlobalConfig* global_config = mgr->getGlobalConfig();
    const GlobalConfig::CompactionThrottlingOptions& t_opt =
        global_config->ctOpt;

    const DBConfig* db_config = getDbConfig();
    Status s;
    SizedBuf empty_key;
    Timer tt;

    // Lock victim table only, all the others will be newly created.
    TableLockHolder tl_holder(this, {victim_table->number});
    if (!tl_holder.ownsLock()) return Status::OPERATION_IN_PROGRESS;

    _log_info( myLog, "split table %zu_%zu, level %zu, min key %s begins",
               opt.prefixNum, victim_table->number, level,
               victim_table->minKey.toReadableString().c_str() );

    std::list<LsmFlushResult> results;
    // Clone of min keys of newly created tables.
    std::vector<SizedBuf> min_keys;

    TableStats victim_stats;
    victim_table->file->getStats(victim_stats);

    uint64_t num_records_read = 0;
    uint64_t TABLE_LIMIT = db_config->getMaxTableSize(level);
    uint64_t NUM_OUTPUT_TABLES =
        (victim_stats.workingSetSizeByte / TABLE_LIMIT) + 1;
    uint64_t EXP_DOCS = (victim_stats.numKvs / NUM_OUTPUT_TABLES) + 1;
    uint64_t EXP_SIZE =
        (victim_stats.workingSetSizeByte / NUM_OUTPUT_TABLES) * 1.1; // 10% headroom.
    bool moved_to_new_table = true;
    _log_info(myLog, "split table WSS %zu limit %zu num docs %zu "
              "output tables %zu expected docs per table %zu "
              "expected size per table %zu",
              victim_stats.workingSetSizeByte,
              TABLE_LIMIT,
              victim_stats.numKvs,
              NUM_OUTPUT_TABLES,
              EXP_DOCS,
              EXP_SIZE);

    TableFile::Iterator* itr = new TableFile::Iterator();
    EP( itr->init(nullptr, victim_table->file, empty_key, empty_key) );

    std::vector<uint64_t> offsets;
    // Reserve 10% more headroom, just in case.
    offsets.reserve(victim_stats.approxDocCount * 11 / 10);
    std::vector<uint64_t> start_indexes;

   try {
    if (level == 1) numL1Compactions.fetch_add(1);

    uint64_t cur_docs_acc = 0;
    uint64_t cur_size_acc = 0, total_size = 0;

    Timer throttling_timer(t_opt.resolution_ms);
    bool scan_retry = false;

    do {
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

            offsets.push_back(offset_out);
            uint64_t cur_index = offsets.size() - 1;

            if (moved_to_new_table) {
                moved_to_new_table = false;

                SizedBuf key_clone;
                if (min_keys.size() == 0) {
                    // First split table: should inherit
                    // victim table's min key.
                    victim_table->minKey.copyTo(key_clone);
                } else {
                    rec_out.kv.key.copyTo(key_clone);
                }
                min_keys.push_back( key_clone );
                start_indexes.push_back( cur_index );
            }

            cur_docs_acc++;
            num_records_read++;
            cur_size_acc += (rec_out.size() + value_size_out);
            total_size += (rec_out.size() + value_size_out);

            if (d_params.compactionDelayUs) {
                // If debug parameter is given, sleep here.
                Timer::sleepUs(d_params.compactionDelayUs);
            }

            // Do throttling, if enabled.
            TableMgr::doCompactionThrottling(t_opt, throttling_timer);

            // WARNING:
            //   In case of value size skew, we should make sure that
            //   accumulated size should be at least bigger than 70% of
            //   expected split table size.
            //
            //   If we don't do this, there we be split/merge thrashing.
            //   Split result will be unbalanced -> merge them again ->
            //   split again -> ...
            //
            //   Also, even though the number of records doesn't meet the
            //   condition, we should go to next table if table size goes
            //   beyond the limit.
            if ( ( cur_docs_acc > EXP_DOCS &&
                   cur_size_acc > EXP_SIZE * 0.7 ) ||
                 cur_size_acc > EXP_SIZE ) {
                // Go to next table.
                cur_docs_acc = 0;
                cur_size_acc = 0;
                moved_to_new_table = true;

                if (!scan_retry && d_params.disruptSplit) {
                    // If flag is set and the first try, prevent having new table.
                    moved_to_new_table = false;
                }
            }

        } while (itr->next().ok());

        itr->close();
        DELETE(itr);

        if (min_keys.size() <= 1) {
            if (NUM_OUTPUT_TABLES <= 1) ++NUM_OUTPUT_TABLES;
            EXP_DOCS = (num_records_read / NUM_OUTPUT_TABLES) + 1;
            EXP_SIZE = (total_size / NUM_OUTPUT_TABLES) * 1.1;
            _log_warn(myLog, "total %zu records, %zu bytes, but number of "
                      "new tables is %zu, try re-scan with adjusted parameters: "
                      "%zu records %zu byte",
                      num_records_read, total_size,
                      min_keys.size(), EXP_DOCS, EXP_SIZE);

            if (scan_retry) {
                _log_err(myLog, "second scanning failed again, cancel this task");
                throw Status(Status::COMPACTION_CANCELLED);
            }

            itr = new TableFile::Iterator();
            TC( itr->init(nullptr, victim_table->file, empty_key, empty_key) );
            scan_retry = true;
            cur_docs_acc = cur_size_acc = total_size = num_records_read = 0;
            offsets.clear();
            moved_to_new_table = false;

        } else {
            scan_retry = false;
        }

    } while (scan_retry);

    uint64_t elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    double scan_rate = (double)num_records_read * 1000000 / elapsed_us;
    size_t num_new_tables = min_keys.size();

    _log_info(myLog, "reading table %zu_%zu for split, level %zu, "
              "%zu records, %zu bytes, %zu files, "
              "initial scan %zu us, %.1f iops",
              opt.prefixNum, victim_table->number,
              level, num_records_read, total_size, num_new_tables,
              elapsed_us, scan_rate);

    size_t max_writers = db_config->getMaxParallelWriters();
    std::list<uint64_t> dummy_chk;

    for (size_t ii=0; ii<num_new_tables; ) {
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

            // Create a new file and then flush.
            TableFile* cur_file = nullptr;
            TableFileOptions t_opt;
            s = createNewTableFile(level, cur_file, t_opt);
            if (!s) continue;

            TableWriterArgs local_args;
            local_args.myLog = myLog;

            TableWriterArgs* w_args = (leased_thread)
                                      ? &twh.leasedWriters[worker_idx]->writerArgs
                                      : &local_args;
            w_args->callerAwaiter.reset();

            uint64_t count = (jj + 1 == num_new_tables)
                             ? offsets.size() - start_indexes[jj]
                             : start_indexes[jj+1] - start_indexes[jj];
            w_args->payload = TableWritePayload( this,
                                                 &offsets,
                                                 start_indexes[jj],
                                                 count,
                                                 &dummy_chk,
                                                 victim_table->file,
                                                 cur_file );
            putLsmFlushResultWithKey(cur_file, min_keys[jj], results);

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

        if (!isCompactionAllowed()) throw Status(Status::COMPACTION_CANCELLED);

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
            TC( mani->addTableFile( level, 0, rr.minKey, rr.tFile ) );
            rr.tFile = nullptr;
        }
        mani->removeTableFile(level, victim_table);
    }
    mani->store(true);

    elapsed_us = std::max( tt.getUs(), (uint64_t)1 );
    double split_rate = (double)num_records_read * 1000000 / elapsed_us;
    if (parentDb) {
        parentDb->p->tStats.lastSplitRate = split_rate;
        parentDb->p->tStats.lastSplitRateExpiry.reset();
    }
    _log_info( myLog, "split table done: %zu us, %zu records, %.1f iops",
               elapsed_us, num_records_read, split_rate);

    for (SizedBuf& sb: min_keys) sb.free();

    if (level == 1) numL1Compactions.fetch_sub(1);
    return Status();

   } catch (Status s) { // ---------------------------------------------------
    _log_err(myLog, "split failed: %d", (int)s);

    if (itr) {
        itr->close();
        DELETE(itr);
    }

    for (SizedBuf& sb: min_keys) sb.free();

    for (auto entry: results) {
        LsmFlushResult& rr = entry;
        delete rr.tFile;
        // WARNING: `rr.minKey` has reference to `min_keys`
        //          so we should not free it here again.
    }
    if (level == 1) numL1Compactions.fetch_sub(1);
    return s;
   }
}

Status TableMgr::mergeLevel(const CompactOptions& options,
                            TableInfo* victim_table,
                            size_t level)
{
    if (level == 0 || level >= mani->getNumLevels()) return Status::INVALID_LEVEL;
    if ( victim_table &&
         victim_table->level != level ) return Status::INVALID_PARAMETERS;

    Status s;
    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    bool honor_limit = false;
    VictimPolicy vp = SMALL_WORKING_SET;
    TableInfo* local_victim = findLocalVictim( level, victim_table,
                                               vp, honor_limit );
    if (!local_victim) return Status::TABLE_NOT_FOUND;

    // Find the target table (i.e., right before the victim).
    std::list<TableInfo*> tables;
    SizedBuf empty_key;
    mani->getTablesRange(level, empty_key, empty_key, tables);

    TableInfo* target_table = nullptr;
    for (TableInfo* tt: tables) {
        // NOTE: `tables` is sorted by `minKey`, so if we break here,
        //       `target_table` is the one right before `local_victim`.
        if (tt == local_victim) break;
        target_table = tt;
    }
    for (TableInfo* tt: tables) {
        if (tt != target_table) tt->done();
    }

    TableFile::Iterator* itr = nullptr;
   try {
    assert(target_table);
    if (!target_table) {
        // It should not happen anyway.
        throw Status( Status::TABLE_NOT_FOUND );
    }

    _log_info( myLog,
               "merge table level %zu, %zu_%zu min key %s -> "
               "%zu_%zu min key %s begins",
               level,
               opt.prefixNum, local_victim->number,
               local_victim->minKey.toReadableString().c_str(),
               opt.prefixNum, target_table->number,
               target_table->minKey.toReadableString().c_str() );

    Timer timer;
    TableStats victim_stats;
    local_victim->file->getStats(victim_stats);
    _log_info(myLog, "merge victim table WSS %zu / total %zu, %zu records",
              victim_stats.workingSetSizeByte,
              victim_stats.totalSizeByte,
              victim_stats.numKvs);

    // Lock order (in the same level):
    //   smaller number table to bigger number table.
    uint64_t num_sm = std::min(target_table->number, local_victim->number);
    uint64_t num_gt = std::max(target_table->number, local_victim->number);

    TableLockHolder tl_sm_holder( this, {num_sm} );
    if (!tl_sm_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    TableLockHolder tl_gt_holder(this, {num_gt});
    if (!tl_gt_holder.ownsLock()) throw Status(Status::OPERATION_IN_PROGRESS);

    // Read all records from the victim table.
    itr = new TableFile::Iterator();
    TC( itr->init(nullptr, local_victim->file, empty_key, empty_key) );

    uint64_t total_count = 0;
    do {
        if (d_params.compactionItrScanDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.compactionItrScanDelayUs);
        }

        if (!isCompactionAllowed()) {
            throw Status(Status::COMPACTION_CANCELLED);
        }

        Record rec_out;
        Record::Holder h(rec_out);
        s = itr->get(rec_out);
        if (!s) break;

        uint32_t key_hash_val = getMurmurHash32(rec_out.kv.key);;
        uint64_t offset_out = 0; // not used.
        target_table->file->setSingle(key_hash_val, rec_out, offset_out);
        total_count++;
    } while (itr->next().ok());
    itr->close();
    delete itr;

    // Set a dummy batch to trigger commit.
    std::list<Record*> dummy_batch;
    std::list<uint64_t> dummy_chk;
    SizedBuf empty_key;
    target_table->file->setBatch(dummy_batch, dummy_chk, empty_key, empty_key,
                                 _SCU32(-1), false);

    {   // Grab lock, add first, and then remove next.
        std::lock_guard<std::mutex> l(mani->getLock());
        mani->removeTableFile(level, local_victim);
    }

    uint64_t elapsed_us = std::max( timer.getUs(), (uint64_t)1 );
    double write_rate = (double)total_count * 1000000 / elapsed_us;
    _log_info( myLog,
               "merge table level %zu, %zu_%zu min key %s -> "
               "%zu_%zu min key %s done, %zu records, %zu us, %.1f iops",
               level,
               opt.prefixNum, local_victim->number,
               local_victim->minKey.toReadableString().c_str(),
               opt.prefixNum, target_table->number,
               target_table->minKey.toReadableString().c_str(),
               total_count, elapsed_us, write_rate );

    // WARNING:
    //   Release it ONLY WHEN this table is not given by caller.
    //   If not, caller is responsible to release the table.
    if (!victim_table) local_victim->done();
    if (target_table) target_table->done();

    mani->store(true);

    return Status();

   } catch (Status ss) {
    _log_err(myLog, "merge failed: %d", (int)ss);

    if (itr) {
        itr->close();
        delete itr;
    }
    if (!victim_table) local_victim->done();
    if (target_table) target_table->done();
    return ss;
   }
}

}

