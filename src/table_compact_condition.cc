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

struct VictimCandidate {
    VictimCandidate(TableInfo* _table = nullptr,
                    uint64_t _wss = 0,
                    uint64_t _total = 0)
        : table(_table), wss(_wss), total(_total) {}
    TableInfo* table;
    uint64_t wss;
    uint64_t total;
};

Status TableMgr::pickVictimTable(size_t level,
                                 VictimPolicy policy,
                                 bool honor_limit,
                                 TableInfo*& victim_table_out,
                                 uint64_t& wss_out,
                                 uint64_t& total_out)
{
    TableInfo* victim_table = nullptr;
    Status s;

    DBMgr* db_mgr = DBMgr::getWithoutInit();
    GlobalConfig* g_config = db_mgr->getGlobalConfig();
    DebugParams d_params = db_mgr->getDebugParams();
    bool d_params_effective = db_mgr->isDebugParamsEffective();

    const DBConfig* db_config = getDbConfig();
    uint64_t MAX_TABLE_SIZE = db_config->getMaxTableSize(level);

    std::list<TableInfo*> tables;
    SizedBuf empty_key;
    mani->getTablesRange(level, empty_key, empty_key, tables);

    // No table to compact, return.
    if (!tables.size()) return Status::TABLE_NOT_FOUND;
    if ( policy == SMALL_WORKING_SET &&
         tables.size() <= db_config->numL0Partitions * 2 ) {
        // Too small number of tables in the level,
        // don't need to merge.
        if ( d_params_effective && d_params.forceMerge ) {
            // Debug mode is on, proceed it.
        } else {
            for (TableInfo*& entry: tables) entry->done();
            return Status::TABLE_NOT_FOUND;
        }
    }

    std::vector<VictimCandidate> candidates;

    uint64_t total_local = 0;
    uint64_t wss_local = 0;
    size_t max_ratio = 0;
    uint64_t min_wss = std::numeric_limits<uint64_t>::max();
    uint64_t wss_avg = 0;
    size_t cur_idx = 0;
    for (auto& entry: tables) {
        cur_idx++;
        TableInfo* t_info = entry;

        // Stack of other base table, skip.
        if (!t_info->baseTable) continue;

        // This table shouldn't be involved in other compaction.
        if ( isTableLocked(t_info->number) ) continue;

        TableStats t_stats;
        s = t_info->file->getStats(t_stats);
        if (!s) continue;

        uint64_t w_size = t_stats.workingSetSizeByte;
        uint64_t t_size = t_stats.totalSizeByte;
        size_t stack_size = 1;

        // If stack exists, sum up all.
        TableStack* stack = t_info->stack;
        if (stack) {
            std::lock_guard<std::mutex> l(stack->lock);
            for (TableInfo* tt: stack->tables) {
                TableStats stack_stats;
                s = tt->file->getStats(stack_stats);
                if (!s) continue;

                w_size += t_stats.workingSetSizeByte;
                t_size += t_stats.totalSizeByte;
                stack_size++;
            }
        }

        _log_trace( myLog, "table %zu wss %zu total %zu policy %d",
                    t_info->number, w_size, t_size, policy );

        if ( policy == WORKING_SET_SIZE ||
             policy == WORKING_SET_SIZE_SPLIT ) {

            double factor = (policy == WORKING_SET_SIZE_SPLIT)
                            ? 1.5 : 1.0;
            // Find any table whose WSS exceeds the limit.
            if ( honor_limit &&
                 w_size <= MAX_TABLE_SIZE * factor ) {
                // If we honor limit, skip if
                //   1) Table size is smaller than the limit.
                continue;
            }
            // Push to list for random picking.
            candidates.push_back( VictimCandidate(t_info, w_size, t_size) );

        } else if (policy == STACK_SIZE) {
            // TODO: Deprecated, should remove relative code.
            candidates.push_back( VictimCandidate(t_info, w_size, t_size) );

        } else if (policy == STALE_RATIO) {
            // Find biggest stale ratio table.
            if (!w_size) continue;
            if (t_size < db_config->minFileSizeToCompact) continue;

            size_t ratio = t_size * 100 / w_size;
            if (honor_limit) {
                if ( d_params_effective &&
                     d_params.urgentCompactionRatio ) {
                    // Debugging ratio is set. Ignore the original ratio.
                    if ( ratio < d_params.urgentCompactionRatio ) continue;
                } else if ( db_mgr->isIdleTraffic() ) {
                    // Idle traffic compaction.
                    if ( ratio < g_config->itcOpt.factor ) continue;
                } else {
                    // Otherwise.
                    if ( ratio < db_config->compactionFactor ) continue;
                }
            }

            if (ratio > max_ratio) {
                max_ratio = ratio;
                wss_local = w_size;
                total_local = t_size;
                victim_table = t_info;
            }

        } else if (policy == SMALL_WORKING_SET) {
            // Find a table with the smallest working set size.

            // The first (smallest key) and the last (greatest key)
            // table cannot be merged.
            if ( !t_info->minKey.empty() &&
                 cur_idx < tables.size() &&
                 w_size < min_wss ) {
                min_wss = w_size;
                wss_local = w_size;
                total_local = t_size;
                victim_table = t_info;
            }

        } else {
            assert(0);
        }
        wss_avg += w_size;
    }
    if (tables.size()) {
        wss_avg /= tables.size();
    }

    if (policy == SMALL_WORKING_SET) {
        bool do_merge = false;
        if (min_wss < std::numeric_limits<uint64_t>::max()) {
            if (honor_limit) {
                if(min_wss < wss_avg * 0.2) {
                    // If we honor the limit, merge the table if the smallest
                    // table's WSS is smaller than 20% of average.
                    do_merge = true;
                }
            } else {
                // If we don't honor the limit, just merge the smallest table.
                do_merge = true;
            }
        }
        if (!do_merge) {
            victim_table = nullptr;
        }
    }

    // Leveling: randomly pick among candidates.
    // Others:   choose table whose number is minimum (i.e., oldest).
    if (!candidates.empty()) {
        uint64_t min_num = std::numeric_limits<uint64_t>::max();
        for (VictimCandidate& vc: candidates) {
            if (vc.table->number < min_num) {
                min_num = vc.table->number;
                victim_table = vc.table;
                wss_local = vc.wss;
                total_local = vc.total;
            }
        }
    }

    // Remove other tables except for victim family.
    auto entry = tables.begin();
    while (entry != tables.end()) {
        TableInfo* tt = *entry;
        if ( tt == victim_table ||
             ( victim_table &&
               tt->minKey == victim_table->minKey ) ) {
            // Keep it.
        } else {
            tt->done();
        }
        entry++;
    }

    if (victim_table) {
        TableStats v_stats;
        victim_table->file->getStats(v_stats);

        _log_( SimpleLogger::INFO, myLog,
               "table lv %zu num %zu min key %s policy %d: "
               "file size %zu (%s), active data %zu (%s), table max %zu (%s), "
               "(ratio %zu vs. factor %zu), block reuse cycle %zu, "
               "num records %zu, level active data average %zu (%s) out of %zu",
               victim_table->level, victim_table->number,
               victim_table->minKey.toReadableString().c_str(),
               (int)policy,
               total_local, Formatter::sizeToString(total_local, 2).c_str(),
               wss_local, Formatter::sizeToString(wss_local, 2).c_str(),
               MAX_TABLE_SIZE, Formatter::sizeToString(MAX_TABLE_SIZE, 2).c_str(),
               ( wss_local
                 ? total_local * 100 / wss_local
                 : 0 ),
               db_config->compactionFactor,
               v_stats.blockReuseCycle,
               v_stats.approxDocCount,
               wss_avg, Formatter::sizeToString(wss_avg, 2).c_str(),
               tables.size() );
    }

    victim_table_out = victim_table;
    wss_out = wss_local;
    total_out = total_local;

    return Status::OK;
}

TableInfo* TableMgr::findLocalVictim(size_t level,
                                     TableInfo* given_victim,
                                     VictimPolicy policy,
                                     bool honor_limit)
{
    if (given_victim) {
        _log_info(myLog, "victim table is given: %zu_%zu at %zu",
                  opt.prefixNum, given_victim->number, level);
        return given_victim;
    }

    // If victim table is not given,
    // pick a table whose active data size is the biggest in the level.
    TableInfo* local_victim = nullptr;
    uint64_t wss = 0;
    uint64_t total = 0;
    Status s = pickVictimTable( level, policy, honor_limit,
                                local_victim, wss, total );
    if (s && local_victim) {
        _log_info(myLog, "victim table is not given, "
                  "table manager locally picked up %zu_%zu level %zu, "
                  "WSS %zu total %zu, policy %d, honor limit %d",
                  opt.prefixNum, local_victim->number, level,
                  wss, total, policy, honor_limit);
    } else {
        _log_info(myLog, "victim table is not given, and cannot find one, "
                  "level %zu, policy %d, honor limit %d",
                  level, policy, honor_limit);
    }
    return local_victim;
}

bool TableMgr::isL0CompactionInProg() {
    size_t num_p = getNumL0Partitions();
    for (size_t ii=0; ii<num_p; ++ii) {
        if (compactStatus[ii]->load()) return true;
    }
    return false;
}

bool TableMgr::chkL0CompactCond(uint32_t hash_num) {
    const DBConfig* db_config = getDbConfig();

    // Read-only mode.
    if ( db_config->readOnly ) return false;

    // Compaction is disabled.
    if ( !allowCompaction ) return false;

    // Log section mode.
    if ( db_config->logSectionOnly ) return false;

    // Validity of `hash_num`.
    if ( hash_num >= numL0Partitions ) return false;

    // Check if compaction is already in progress.
    if ( compactStatus[hash_num]->load() == true ) return false;

    // If compaction factor is zero, do not compact DB.
    if ( !db_config->compactionFactor ) return false;

    // If level extension mode, L1 compaction shouldn't be happening.
    if ( db_config->nextLevelExtension &&
         numL1Compactions > 0 ) return false;

    // If sequential loading, delay L0 -> L1 compaction.
    // TODO: We cannot delay forever, should check disk free space.
    if ( db_config->nextLevelExtension &&
         parentDb->p->flags.seqLoading ) return false;

    Status s;
    std::list<TableInfo*> tables;
    s = mani->getL0Tables(hash_num, tables);
    if (!s) return s;

   try {
    // If src table exists, that means that the engine was closed
    // while compaction is running. We need to compact this table first.
    TableInfo* target_table = nullptr;
    TableInfo* sup_table = nullptr;
    bool force_compaction = false;
    getTwoSmallSrcTables(tables, hash_num, target_table, sup_table);
    if (!target_table) {
        getTwoSmallNormalTables(tables, hash_num, target_table, sup_table);
        if (!target_table) throw Status(Status::TABLE_NOT_FOUND);
    } else {
        _log_warn(myLog, "previously engine terminated while table %zu is "
                  "being compacted", target_table->number);
        force_compaction = true;
    }

    TableStats t_stats, s_stats;
    TC( target_table->file->getStats(t_stats) );
    if (sup_table) {
        TC( sup_table->file->getStats(s_stats) );
    }

    size_t ratio = 0;
    if (t_stats.workingSetSizeByte) {
        ratio = t_stats.totalSizeByte * 100 / t_stats.workingSetSizeByte;
    }

    size_t s_ratio = (uint64_t)0xffffffff;
    if (sup_table && s_stats.workingSetSizeByte) {
        // If one more normal file exists,
        // we should check the ratio between this file and that file as well.
        s_ratio = (t_stats.totalSizeByte + s_stats.totalSizeByte) * 100 /
                  (s_stats.workingSetSizeByte);
    }

    bool decision = false;
    DBMgr* db_mgr = DBMgr::getWithoutInit();
    GlobalConfig* g_config = db_mgr->getGlobalConfig();
    DebugParams d_params = db_mgr->getDebugParams();
    bool d_params_effective = db_mgr->isDebugParamsEffective();

    // Urgent compaction:
    //   => If block reuse cycle goes beyond 2x of threshold,
    if ( db_config->minBlockReuseCycleToCompact &&
         t_stats.blockReuseCycle >= db_config->minBlockReuseCycleToCompact * 2 )
    {
        decision = true;
    }

    // Urgent compaction (not in next-level mode):
    //   => If stale block ratio goes beyond 2x of threshold,
    //      when file size is bigger than 64MB.
    if ( !db_config->nextLevelExtension &&
         t_stats.totalSizeByte > (uint64_t)64*1024*1024 &&
         ratio > db_config->compactionFactor * 2 &&
         s_ratio > db_config->compactionFactor * 2 )
    {
        decision = true;
    }

    // Urgent compaction:
    //   => If file size is bigger than debugging parameter.
    if ( d_params_effective &&
         d_params.urgentCompactionFilesize &&
         t_stats.totalSizeByte > d_params.urgentCompactionFilesize )
    {
        _log_info(myLog, "[URGENT COMPACTION] by size: %zu > %zu",
                  t_stats.totalSizeByte, d_params.urgentCompactionFilesize);
        decision = true;
    }

    // Urgent compaction:
    //   => If stale ratio is bigger than debugging parameter.
    //   => Effective only when L0 is the last level.
    int urgent_or_itc = 0;
    size_t num_levels = getNumLevels();
    if ( d_params_effective &&
         d_params.urgentCompactionRatio &&
         num_levels == 1 &&
         t_stats.totalSizeByte > db_config->minFileSizeToCompact &&
         ratio > d_params.urgentCompactionRatio ) {
        urgent_or_itc = 1;
    }

    // Idle traffic compaction:
    //   => If stale ratio is bigger than idle traffic factor.
    if ( db_mgr->isIdleTraffic() &&
         g_config->itcOpt.factor &&
         t_stats.totalSizeByte > db_config->minFileSizeToCompact &&
         ratio > g_config->itcOpt.factor ) {
        urgent_or_itc = 2;
    }

    if (urgent_or_itc) {
        std::string type_str = (urgent_or_itc == 1)
                               ? "URGENT COMPACTION"
                               : "IDLE COMPACTION";
        if (sup_table) {
            // Sup table exists.
            // To avoid small files being compacted (the benefit is very
            // marginal), check the actual file size. It should be bigger
            // than some proportion of the sup-table size.
            if ( t_stats.totalSizeByte * 100 >
                     s_stats.totalSizeByte *
                     (d_params.urgentCompactionRatio - 100) ) {
                _log_info(myLog, "[%s] by ratio: %zu > %zu, "
                          "size: %zu %zu",
                          type_str.c_str(),
                          ratio, d_params.urgentCompactionRatio,
                          t_stats.totalSizeByte,
                          s_stats.totalSizeByte);
                decision = true;
            }

        } else {
            // Just single file.
            _log_info(myLog, "[%s] by ratio: %zu > %zu",
                      type_str.c_str(),
                      ratio, d_params.urgentCompactionRatio);
            decision = true;
        }
    }

    // Urgent compaction (in level extension mode):
    //   => If file size is bigger than given L0 limit.
    if ( db_config->nextLevelExtension &&
         ( t_stats.workingSetSizeByte +
           s_stats.workingSetSizeByte ) > db_config->maxL0TableSize )
    {
        decision = true;
    }

    // Normal condition:
    if ( t_stats.totalSizeByte > db_config->minFileSizeToCompact &&
         t_stats.blockReuseCycle >= db_config->minBlockReuseCycleToCompact &&
         ratio > db_config->compactionFactor &&
         s_ratio > db_config->compactionFactor )
    {
        if (!db_config->nextLevelExtension) {
            decision = true;
        } else {
            // Next-level extension mode:
            //   File size should exceed either
            //   1) the given L0 threshold, or
            //   2) current L1 size / num L0 tables (if L1 exists).
            uint64_t wss = 0, total = 0, max_stack = 0;
            s = getLevelSize(1, wss, total, max_stack);
            if (!s) decision = true; // Level-1 doesn't exist.

            if ( t_stats.totalSizeByte > db_config->maxL0TableSize ||
                 t_stats.totalSizeByte * db_config->numL0Partitions > total ) {
                decision = true;
            }
        }
    }

    // If `force_compaction` is set, always set `decision`.
    if (force_compaction) decision = true;

    SimpleLogger::Levels log_lv = (decision)
                                  ? SimpleLogger::INFO
                                  : SimpleLogger::DEBUG;
    _log_(log_lv, myLog,
        "table lv %zu num %zu hash %zu: "
        "file size %zu (%s), block reuse cycle %zu, active data %zu (%s), "
        "sup file size %zu (%s), sup active data %zu (%s), "
        "(ratio %zu, s_ratio %zu vs. factor %zu), "
        "decision: %s",
        target_table->level, target_table->number, target_table->hashNum,
        t_stats.totalSizeByte,
        Formatter::sizeToString(t_stats.totalSizeByte, 2).c_str(),
        t_stats.blockReuseCycle,
        t_stats.workingSetSizeByte,
        Formatter::sizeToString(t_stats.workingSetSizeByte, 2).c_str(),
        s_stats.totalSizeByte,
        Formatter::sizeToString(s_stats.totalSizeByte, 2).c_str(),
        s_stats.workingSetSizeByte,
        Formatter::sizeToString(s_stats.workingSetSizeByte, 2).c_str(),
        ratio,
        (s_ratio == (uint64_t)0xffffffff) ? 0 : s_ratio,
        db_config->compactionFactor,
        (decision ? "COMPACT" : "x") );

    for (TableInfo*& table: tables) table->done();
    return decision;

   } catch (Status s) {
    for (TableInfo*& table: tables) table->done();
    return false;
   }
}

Status TableMgr::chkLPCompactCond(size_t level,
                                  TableMgr::MergeStrategy& s_out,
                                  TableInfo*& victim_table_out)
{
    const DBConfig* db_config = getDbConfig();
    Status s;

    // Read-only mode.
    if ( db_config->readOnly ) return Status::INVALID_MODE;

    // Compaction is disabled.
    if ( !allowCompaction ) return Status::COMPACTION_IS_NOT_ALLOWED;

    // Log section mode.
    if ( db_config->logSectionOnly ) return Status::INVALID_MODE;

    // We can't do L1 compaction when L0 compaction is in progress.
    if ( level == 1 && isL0CompactionInProg() ) {
        return Status::OPERATION_IN_PROGRESS;
    }

    uint64_t wss = 0,
             total = 0,
             max_stack = 0;

    bool force_interlevel = false;
    // TODO: If we want to force inter-level compaction
    //       for intermediate levels, need to add some
    //       logic to make this flag true here.
    (void)force_interlevel;

    // Check if inter-level merge is needed.
    if ( !isLevelLocked(level) ) {
        s = getLevelSize(level, wss, total, max_stack);
        if (!s) return s;

        // FIXME:
        //   Only one inter-level compaction is allowed at a time now.
        //   If not, data loss will happen as different tables will
        //   have empty key as their min keys, at the same time.
        uint64_t level_limit = getLevelSizeLimit(level);
        if (wss > level_limit) {
            _log_info(myLog, "[INTERLEVEL] level %zu wss %zu limit %zu",
                      level, wss, level_limit);
            s_out = TableMgr::INTERLEVEL;
            victim_table_out = nullptr;
            return Status();
        }
    }

    // Find table to split first (when WSS > 1.5x table limit).
    TableMgr::VictimPolicy v_policy = TableMgr::WORKING_SET_SIZE_SPLIT;
    TableInfo* victim_table = nullptr;
    wss = total = 0;
    s = pickVictimTable( level, v_policy, true,
                         victim_table, wss, total );
    if (s && victim_table) {
        s_out = force_interlevel ? TableMgr::INTERLEVEL : TableMgr::SPLIT;
        victim_table_out = victim_table;
        return s;
    }

    // Not found, then find table to (in-place) compact
    // (when TOTAL > WSS * C).
    v_policy = TableMgr::STALE_RATIO;
    victim_table = nullptr;
    wss = total = 0;
    s = pickVictimTable( level, v_policy, true,
                         victim_table, wss, total );
    if (s && victim_table) {
        s_out = force_interlevel ? TableMgr::INTERLEVEL : TableMgr::INPLACE;
        victim_table_out = victim_table;
        return s;
    }

    // At lowest priority, find table to merge.
    v_policy = TableMgr::SMALL_WORKING_SET;
    victim_table = nullptr;
    wss = total = 0;
    s = pickVictimTable( level, v_policy, true,
                         victim_table, wss, total );
    if (s && victim_table) {
        s_out = TableMgr::MERGE;
        victim_table_out = victim_table;
        return s;
    }

    return Status::TABLE_NOT_FOUND;
}

}

