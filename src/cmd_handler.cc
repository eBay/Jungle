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

    if ( tokens[0] == "getstats" ) {
        ret_str = hGetStats(target_dbw, tokens);

    } else if ( tokens[0] == "loglevel" ) {
        ret_str = hLogLevel(target_dbw, tokens);

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
    ss << "cache"
       << " " << stats_out.cacheUsedByte
       << " " << stats_out.cacheSizeByte << std::endl;

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

} // namespace jungle

// LCOV_EXCL_STOP

