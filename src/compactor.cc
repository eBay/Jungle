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

#include "compactor.h"

#include "db_mgr.h"
#include "db_internal.h"
#include "log_mgr.h"
#include "skiplist.h"
#include "table_mgr.h"

#include <cassert>
#include <cstdlib>

#include <pthread.h>

namespace jungle {

Compactor::Compactor( const std::string& _w_name,
                      const GlobalConfig& _config )
    : lastCheckedFileIndex(0xffff) // Any big number to start from 0.
    , lastCompactedHashNum(0)
{
    workerName = _w_name;
    gConfig = _config;
    CompactorOptions options;
    options.sleepDuration_ms = gConfig.compactorSleepDuration_ms;
    options.worker = this;
    curOptions = options;
    handle = std::thread(WorkerBase::loop, &curOptions);
}

Compactor::~Compactor() {
}

bool Compactor::chkLevel(size_t level,
                         DBWrap* dbwrap,
                         DB*& target_db_out,
                         size_t& target_level_out,
                         size_t& target_hash_num_out,
                         TableInfo*& target_table_out,
                         TableMgr::MergeStrategy& target_strategy_out)
{
    Status s;
    bool found = false;
    const DBConfig& db_config = dbwrap->db->p->dbConfig;
    size_t num_l0 = dbwrap->db->p->tableMgr->getNumL0Partitions();

    if (level == 0) {
        // L0: Special case (hash).
        for (size_t ii=0; ii<num_l0; ++ii) {
            size_t idx = (lastCompactedHashNum + 1 + ii) % num_l0;
            if (dbwrap->db->p->tableMgr->chkL0CompactCond(idx)) {
                target_db_out = dbwrap->db;
                target_level_out = 0;
                target_hash_num_out = idx;
                lastCompactedHashNum = idx;
                found = true;
                break;
            }
        }
        if (found) return true;

    } else {
        // L1+: Key range.
        if (db_config.nextLevelExtension) {
            // Check L1 conditions.
            TableMgr::MergeStrategy ms;
            TableInfo* victim = nullptr;
            s = dbwrap->db->p->tableMgr->chkLPCompactCond(level, ms, victim);
            if (s) {
                target_db_out = dbwrap->db;
                target_level_out = level;
                target_table_out = victim;
                target_strategy_out = ms;
                found = true;
            }
        }
        if (found) return true;
    }

    return false;
}

void Compactor::work(WorkerOptions* opt_base) {
    Status s;

    DBMgr* dbm = DBMgr::getWithoutInit();
    if (!dbm) return;

    DB* target_db = nullptr;
    size_t target_level = 0;

    // only for L0.
    size_t target_hash_num = 0;

    // only for L1.
    TableInfo* target_table = nullptr;
    TableMgr::MergeStrategy target_strategy = TableMgr::INPLACE;

    {   std::lock_guard<std::mutex> l(dbm->dbMapLock);

        // NOTE:
        //   Start from right next DB of the last checked one.
        //   Checking outside skiplist's loop will be safe
        //   as long as we are holding `dbMapLock`.
        std::vector<DBWrap*> dbs_to_check;

        skiplist_node* cursor = skiplist_begin(&dbm->dbMap);
        while (cursor) {
            DBWrap* dbwrap = _get_entry(cursor, DBWrap, snode);
            dbs_to_check.push_back(dbwrap);
            cursor = skiplist_next(&dbm->dbMap, cursor);
            skiplist_release_node(&dbwrap->snode);
        }
        if (cursor) skiplist_release_node(cursor);

        size_t num_dbs = dbs_to_check.size();
        if (++lastCheckedFileIndex >= num_dbs) lastCheckedFileIndex = 0;

        size_t s_idx = lastCheckedFileIndex;
        size_t e_idx = lastCheckedFileIndex + num_dbs;
        for (size_t ii = s_idx; ii < e_idx; ++ii) {
            lastCheckedFileIndex = ii % num_dbs;
            DBWrap* dbwrap = dbs_to_check[lastCheckedFileIndex];
            const DBConfig& db_config = dbwrap->db->p->dbConfig;

            if ( db_config.nextLevelExtension &&
                 dbwrap->db->p->tableMgr->isL0CompactionInProg() ) {
                // In level extension mode, there is nothing we can do
                // if L0 -> L1 compaction is in progress,
                // as it will touch (possibly) all tables in L1.

                // TODO:
                //   We can support parallel L0 -> L1 compaction
                //   as we can write to the same table simultaneously.
                continue;
            }

            // Clear urgent copmaction by index if necessary.
            dbwrap->db->p->tableMgr->autoClearUrgentCompactionTableIdx();

            // Currently all levels have even probability.
            // TODO: Give higher priority to upper levels?
            size_t num_levels = dbwrap->db->p->tableMgr->getNumLevels();
            std::vector<size_t> prob_dist(num_levels, 1);
            size_t start_level = RndGen::fromProbDist(prob_dist);

            bool found = false;
            for (size_t ii = start_level; ii < start_level + num_levels; ++ii) {
                size_t lv = ii % num_levels;
                found = chkLevel( lv, dbwrap, target_db, target_level,
                                  target_hash_num, target_table, target_strategy );
                if (found) break;
            }

            uint64_t num_writes_to_compact = db_config.numWritesToCompact;
            if ( dbm->isDebugParamEffective() &&
                 dbm->getDebugParams().urgentCompactionNumWrites ) {
                if (!num_writes_to_compact) {
                    num_writes_to_compact =
                        dbm->getDebugParams().urgentCompactionNumWrites;
                } else {
                    num_writes_to_compact =
                        std::min( num_writes_to_compact,
                                  dbm->getDebugParams().urgentCompactionNumWrites );
                }
            }
            // WARNING:
            //   Under heavy load, L0 compaction will always hit and
            //   that prevents the urgent compaction of L1+, which
            //   doesn't help reducing space.
            //   In such a case, we will trigger compaction of each
            //   level alternately.
            if ( start_level != 0 &&
                 num_levels >= 2 &&
                 num_writes_to_compact &&
                 dbwrap->db->p->tableMgr->getNumWrittenRecords() >
                     num_writes_to_compact ) {
                target_db = dbwrap->db;
                if (target_table) {
                    // WARNING: If there is a table already chosen,
                    //          we should release it here.
                    _log_info(dbwrap->db->p->myLog,
                              "urgent compaction, discard table %zu and "
                              "find a new victim", target_table->number);
                    target_table->done();
                }
                target_table = nullptr;
                target_level = start_level;
                target_strategy = TableMgr::INPLACE_OLD;
                found = true;
            }

            if (found) {
                target_db->p->incBgTask();
                break;
            }
        }
    }

    if (target_db) {
        // Found a DB to compact.
        CompactOptions c_opt;

        if (target_level == 0) {
            s = target_db->p->tableMgr->compactL0(c_opt, target_hash_num);

        } else {
            target_db->p->tableMgr->resetNumWrittenRecords();

            if (target_strategy == TableMgr::INTERLEVEL) {
                s = target_db->p->tableMgr->compactLevelItr
                    ( c_opt, target_table, target_level );

            } else if ( target_strategy == TableMgr::SPLIT &&
                        target_table ) {
                s = target_db->p->tableMgr->splitLevel
                    ( c_opt, target_table, target_level );

            } else if ( target_strategy == TableMgr::INPLACE &&
                        target_table ) {
                s = target_db->p->tableMgr->compactInPlace
                    ( c_opt, target_table, target_level, false );

            } else if ( target_strategy == TableMgr::MERGE &&
                        target_table ) {
                s = target_db->p->tableMgr->mergeLevel
                    ( c_opt, target_table, target_level );

            } else if ( target_strategy == TableMgr::INPLACE_OLD ) {
                s = target_db->p->tableMgr->compactInPlace
                    ( c_opt, target_table, target_level, true );

            } else if ( target_strategy == TableMgr::FIX ) {
                s = target_db->p->tableMgr->fixTable
                    ( c_opt, target_table, target_level );

            } else {
                assert(0);
            }
            if (target_table) target_table->done();
        }

        // Do not sleep next time to continue
        // to quickly compact other DB.
        if (s) {
            doNotSleepNextTime = true;
        }

        target_db->p->decBgTask();
    }
}

} // namespace jungle

