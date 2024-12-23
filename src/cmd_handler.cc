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

#include "cmd_handler.h"

#include "db_mgr.h"
#include "db_internal.h"
#include "log_mgr.h"
#include "skiplist.h"
#include "table_mgr.h"

#include <third_party/forestdb/include/libforestdb/forestdb.h>

#include <iostream>
#include <sstream>

// LCOV_EXCL_START

namespace jungle {

CmdHandler::CmdHandler( const std::string& _w_name,
                        const GlobalConfig& _config )
{
    workerName = _w_name;
    gConfig = _config;
    CmdHandlerOptions options;
    options.sleepDuration_ms = 1000;
    options.worker = this;
    curOptions = options;
    handle = std::thread(WorkerBase::loop, &curOptions);
}

CmdHandler::~CmdHandler() {
}

void CmdHandler::work(WorkerOptions* opt_base) {
    Status s;

    DBMgr* dbm = DBMgr::getWithoutInit();
    if (!dbm) return;

    dbm->updateGlobalTime();
    // For the case when there is no traffic.
    dbm->updateOpHistory(0);
    bool new_idle_status = dbm->determineIdleStatus();
    if (dbm->setIdleStatus(new_idle_status)) {
        if (new_idle_status) {
            _log_info(dbm->getLogger(), "   === Enter idle traffic mode ===");
        } else {
            _log_info(dbm->getLogger(), "   === Enter normal traffic mode ===");
        }
    }

    DBWrap* target_dbw = nullptr;

    {   std::lock_guard<std::mutex> l(dbm->dbMapLock);

        std::vector<DBWrap*> dbs_to_check;
        skiplist_node* cursor = skiplist_begin(&dbm->dbMap);
        while (cursor) {
            DBWrap* dbwrap = _get_entry(cursor, DBWrap, snode);
            dbs_to_check.push_back(dbwrap);
            cursor = skiplist_next(&dbm->dbMap, cursor);
            skiplist_release_node(&dbwrap->snode);
        }
        if (cursor) skiplist_release_node(cursor);

        for (DBWrap* dbw: dbs_to_check) {
            if (FileMgr::exist(dbw->path + "/jungle_cmd")) {
                target_dbw = dbw;
                break;
            }
        }
    }

    if (target_dbw) {
        handleCmd(target_dbw);
        FileMgr::remove(target_dbw->path + "/jungle_cmd");
    }

    {   // Remove pending files if exist (spend up to 1 second).
        Timer tt;
        tt.setDurationMs(1000);
        while (!tt.timeout()) {
            std::string full_path;
            s = dbm->popFileToRemove(full_path);
            if (!s) break;
            Timer tt;
            FileMgr::remove(full_path);
            _log_info(dbm->getLogger(),
                      "removed pending file %s, %zu us",
                      full_path.c_str(), tt.getUs());
        }
    }
}

#define HANDLER_BINDER(f) \
    std::bind(f, this, std::placeholders::_1, std::placeholders::_2 )

void CmdHandler::handleCmd(DBWrap* target_dbw) {
    std::string cmd_file = target_dbw->path + "/jungle_cmd";
    std::string ret_file = target_dbw->path + "/jungle_cmd_result";

    std::ifstream fs;
    fs.open(cmd_file);
    if (!fs.good()) return;

    std::stringstream ss;
    ss << fs.rdbuf();
    fs.close();

    if (ss.str().empty()) return;

    std::string ret_str;
    std::vector<std::string> tokens =
        StrHelper::tokenize( StrHelper::trim(ss.str()), " " );

    using handler_func =
        std::function< std::string(DBWrap*, const std::vector<std::string>&) >;
    static const std::unordered_map<std::string, handler_func> handlers(
        { { "getstats", HANDLER_BINDER( &CmdHandler::hGetStats ) },
          { "loglevel", HANDLER_BINDER( &CmdHandler::hLogLevel ) },
          { "logcachestats", HANDLER_BINDER( &CmdHandler::hLogCacheStats ) },
          { "getrecord", HANDLER_BINDER( &CmdHandler::hDumpKv ) },
          { "getmeta", HANDLER_BINDER( &CmdHandler::hDumpKv ) },
          { "dumpvalue2file", HANDLER_BINDER( &CmdHandler::hDumpKv ) },
          { "compactupto", HANDLER_BINDER( &CmdHandler::hCompactUpto ) },
          { "tableinfo", HANDLER_BINDER( &CmdHandler::hTableInfo ) }
        } );

    auto entry = handlers.find(tokens[0]);
    if (entry != handlers.end()) {
        ret_str = entry->second(target_dbw, tokens);
    }

    std::ofstream ofs;
    ofs.open(ret_file);
    if (ret_str.empty()) {
        ofs << "failed" << std::endl;
    } else {
        ofs << ret_str;
    }
    ofs.close();
    return;
}

std::string CmdHandler::hGetStats(DBWrap* target_dbw,
                                  const std::vector<std::string>& tokens)
{
    DBStats stats_out;
    target_dbw->db->getStats(stats_out);

    std::stringstream ss;
    ss << "num_records"
       << " " << stats_out.numKvs << std::endl;
    ss << "working_set_size"
       << " " << stats_out.workingSetSizeByte << std::endl;
    ss << "num_index_nodes"
       << " " << stats_out.numIndexNodes
       << " " << stats_out.numIndexNodes * 4096
       << std::endl;
    ss << "cache"
       << " " << stats_out.cacheUsedByte
       << " " << stats_out.cacheSizeByte << std::endl;
    ss << "num_open_memtables"
       << " " << stats_out.numOpenMemtables << std::endl;
    ss << "num_bg_tasks"
       << " " << stats_out.numBgTasks << std::endl;
    ss << "min_table_index"
       << " " << stats_out.minTableIndex << std::endl;
    ss << "max_table_index"
       << " " << stats_out.maxTableIndex << std::endl;

    return ss.str();
}

std::string CmdHandler::hLogLevel(DBWrap* target_dbw,
                                  const std::vector<std::string>& tokens)
{
    std::stringstream ss;
    int prev_lv = target_dbw->db->getLogLevel();

    if (tokens.size() == 1) {
        // Get log level.
        ss << "log_level"
           << " " << prev_lv  << std::endl;

    } else {
        // Set log level.
        int new_lv = atoi(tokens[1].c_str());
        if (new_lv < -1 || new_lv > 6) {
            ss << "invalid level: " << tokens[1] << std::endl;
            return ss.str();
        }

        target_dbw->db->setLogLevel(new_lv);

        ss << "log_level"
           << " " << prev_lv
           << " " << new_lv << std::endl;
    }
    return ss.str();
}

std::string CmdHandler::hLogCacheStats(DBWrap* target_dbw,
                                       const std::vector<std::string>& tokens)
{
    DBMgr* dbm = DBMgr::getWithoutInit();
    if (!dbm) return "DB manager not found";

    fdb_print_cache_stats();
    return "done, path: " + dbm->getGlobalConfig()->globalLogPath;
}

std::string CmdHandler::hDumpKv(DBWrap* target_dbw,
                                const std::vector<std::string>& tokens)
{
    std::stringstream ss;
    if (tokens.size() < 2) {
        ss << "too few arguments: dumpkv <KEY> [<OPTIONS>]\n";
        return ss.str();
    }

    static const size_t ITERATE_NUMBER_LIMIT = 100;

    // Options.
    bool hex_key = false;
    bool prefix_match = false;
    bool iterate = false;
    size_t iterate_number = 1;
    bool iterate_by_seqnum = false;
    uint64_t seqnum_start = -1, seqnum_end = -1;
    for (size_t ii=1; ii<tokens.size(); ++ii) {
        if (tokens[ii] == "-h" || tokens[ii] == "--hex") {
            hex_key = true;
        }
        if (tokens[ii] == "-p" || tokens[ii] == "--prefix") {
            prefix_match = true;
        }
        if ( ii + 1 < tokens.size() &&
             ( tokens[ii] == "-i" || tokens[ii] == "--iterate") ) {
            iterate = true;
            iterate_number = std::atoll(tokens[++ii].c_str());
        }
        if (ii + 1 < tokens.size() && tokens[ii] == "-s") {
            iterate_by_seqnum = true;
            seqnum_start = std::atoll(tokens[++ii].c_str());
            if (ii + 1 < tokens.size()) {
                uint64_t end = std::atoll(tokens[++ii].c_str());
                if (end < seqnum_end) {
                    seqnum_end = end;
                }
            }
            iterate_number = seqnum_end - seqnum_start + 1;
        }
    }

    if (tokens[0] == "dumpvalue2file") {
        if (!FileMgr::exist(target_dbw->db->getPath() + "/dump")) {
            FileMgr::mkdir(target_dbw->db->getPath() + "/dump");
        }
    }

    const std::string& given_key = tokens[1];
    SizedBuf key_buf;
    SizedBuf::Holder h_key_buf(key_buf);
    if (hex_key) {
        key_buf = HexDump::hexStr2bin(given_key);
        if (key_buf.empty()) {
            ss << "incorrect hex value: " << given_key << std::endl;
            return ss.str();
        }
    } else {
        key_buf.set(given_key);
    }

    auto print_rec = [&](Record& rec_out, size_t& count) {
        ss << "[" << count << "]" << std::endl;
        ss << "key: " << HexDump::toString(rec_out.kv.key) << std::endl;
        ss << "sequence number: " << rec_out.seqNum << std::endl;
        ss << "type: " << (int)rec_out.type << std::endl;
        ss << "meta: " << HexDump::toString(rec_out.meta) << std::endl;
        if (tokens[0] == "getmeta") {
            ss << "value: " << rec_out.kv.value.size << " bytes" << std::endl;
        } else if (tokens[0] == "getrecord") {
            ss << "value: " << HexDump::toString(rec_out.kv.value) << std::endl;
        } else if (tokens[0] == "dumpvalue2file") {
            char filename_raw[256];
            sprintf(filename_raw, "dump_%08zu", count);
            std::string filename = target_dbw->db->getPath() + "/dump/" +
                                   filename_raw;
            ss << "value: " << rec_out.kv.value.size << " bytes, "
               << filename << std::endl;
            std::ofstream fs;
            fs.open(filename);
            fs << rec_out.kv.value.toString();
            fs.close();
        }
        ss << std::endl;
        count++;
    };

    size_t count = 0;
    Status s;
    if (prefix_match || iterate || iterate_by_seqnum) {
        jungle::Iterator itr;
        if (iterate_by_seqnum) {
            s = itr.initSN(target_dbw->db, seqnum_start, seqnum_end);
        } else {
            s = itr.init(target_dbw->db, key_buf);
        }
        if (!s) {
            ss << "iterator init failed: " << (int)s << std::endl;
            itr.close();
            return ss.str();;
        }

        do {
            Record rec_out;
            Record::Holder h_rec_out(rec_out);
            s = itr.get(rec_out);
            if (!s) break;

            if (prefix_match) {
                if ( rec_out.kv.key.size >= key_buf.size &&
                     SizedBuf::cmp( key_buf,
                                    SizedBuf( key_buf.size,
                                              rec_out.kv.key.data ) ) != 0 ) {
                    break;
                }
            }
            print_rec(rec_out, count);

            if (iterate && count >= iterate_number) {
                break;
            }
            if (count >= ITERATE_NUMBER_LIMIT) {
                // To be safe, we limit the count by 100.
                ss << "more records may exist ..." << std::endl;
                break;
            }
        } while (itr.next().ok());
        itr.close();

    } else {
        Record rec_out;
        Record::Holder h_rec_out(rec_out);
        s = target_dbw->db->getRecordByKey(key_buf, rec_out);
        if (!s) {
            ss << "get record failed: " << (int)s << std::endl;
            return ss.str();
        }
        print_rec(rec_out, count);
    }

    return ss.str();
}

std::string CmdHandler::hCompactUpto(DBWrap* target_dbw,
                                     const std::vector<std::string>& tokens)
{
    std::stringstream ss;
    if (tokens.size() < 2) {
        ss << "too few arguments: compactupto <TABLE INDEX> "
              "[<EXPIRY SECONDS>]\n";
        return ss.str();
    }

    uint64_t table_idx_upto = std::stoul(tokens[1]);
    uint64_t expiry_sec = 0;
    if (tokens.size() >= 3) {
        expiry_sec = std::stoul(tokens[2]);
    }

    target_dbw->db->compactIdxUpto(CompactOptions(),
                                   table_idx_upto,
                                   expiry_sec);

    ss << "set urgent compaction table index upto " << table_idx_upto
       << ", expiry " << expiry_sec << " seconds"
       << std::endl;
    return ss.str();
}

std::string CmdHandler::hTableInfo(DBWrap* target_dbw,
                                   const std::vector<std::string>& tokens)
{
    std::stringstream ss;
    int target_level = -1;
    if (tokens.size() < 2) {
        ss << "No target level specified. All levels will be shown\n";
    } else {
        target_level = std::atoi(tokens[1].c_str());
        ss << "Target level: " << target_level << "\n";
        if (target_level < 0) {
            ss << "Wrong target level: "
               << "Target level should not be smaller than 0\n";
            return ss.str();
        }
    }

    // Number of levels.
    size_t num_levels = target_dbw->db->p->tableMgr->getNumLevels();
    ss << "number of levels: " << num_levels
       << " (bottommost level " << num_levels - 1 << ")\n";

    if (target_level >= (int)num_levels) {
        ss << "Wrong target level: "
           << "Target level should not be larger than bottommost level\n";
        return ss.str();
    }

    for (size_t ii=0; ii<num_levels; ++ii) {
        if (target_level >= 0 && target_level != (int)ii) continue;

        ss << "  level " << ii << ":\n";
        std::list<TableInfo*> tables;
        target_dbw->db->p->tableMgr->mani->getTablesRange
                                               ( ii, SizedBuf(), SizedBuf(), tables );
        for (auto& entry: tables) {
            TableInfo*& t_info = entry;
            TableStats t_stats;
            t_info->file->getStats(t_stats);

            ss << "    table " << t_info->number << ":\n";
            if (ii == 0) {
                // L0: hash partition.
                ss << "      hash: " << t_info->hashNum << "\n";
            } else {
                ss << "      min key: " << HexDump::toString(t_info->minKey);

                SizedBuf max_key_out;
                SizedBuf::Holder h(max_key_out);
                t_info->file->getMaxKey(max_key_out);
                ss << "      max key: " << HexDump::toString(max_key_out);
            }
            ss << "      number of records: " << t_stats.numKvs << "\n";
            ss << "      last sequence number: " << t_stats.lastSeqnum << "\n";
            ss << "      space: " << t_stats.workingSetSizeByte
               << " / " << t_stats.totalSizeByte
               << ", " << Formatter::sizeToString(t_stats.workingSetSizeByte)
               << " / " << Formatter::sizeToString(t_stats.totalSizeByte)
               << "\n";

            double space_perc = 0.0;
            if (t_stats.workingSetSizeByte) {
                space_perc = (double)t_stats.numIndexNodes * 4096 * 100;
                space_perc /= (double)t_stats.workingSetSizeByte;
            }
            ss << "      space by index: " << t_stats.numIndexNodes << " nodes, "
               << t_stats.numIndexNodes * 4096 << " / " << t_stats.workingSetSizeByte
               << ", " << std::fixed << std::setprecision(1) << space_perc
               << "%%\n";

            ss << "      block reuse cycle: " << t_stats.blockReuseCycle << "\n";
            ss << "      status: " << t_info->status.load() << "\n";

            t_info->done();
            ss << std::endl;
        }
        ss << std::endl;
    }

    return ss.str();
}

} // namespace jungle

// LCOV_EXCL_STOP

