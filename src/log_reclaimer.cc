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

#include "db_mgr.h"
#include "db_internal.h"
#include "internal_helper.h"
#include "log_mgr.h"
#include "log_reclaimer.h"
#include "skiplist.h"

#include <list>

#include _MACRO_TO_STR(LOGGER_H)

#include <pthread.h>

namespace jungle {

LogReclaimer::LogReclaimer(const std::string& _w_name,
                          const GlobalConfig& _config)
{
    workerName = _w_name;
    gConfig = _config;
    LogReclaimerOptions options;
    options.sleepDuration_ms = gConfig.logFileReclaimerSleep_sec * 1000;
    options.worker = this;
    curOptions = options;
    handle = std::thread(WorkerBase::loop, &curOptions);
}

LogReclaimer::~LogReclaimer() {}

void LogReclaimer::work(WorkerOptions* opt_base) {
    Status s;

    DBMgr* dbm = DBMgr::getWithoutInit();
    if (!dbm) return;

    std::list<DB*> target_dbs;

    {   // Check DB map.
        std::lock_guard<std::mutex> l(dbm->dbMapLock);

        skiplist_node* cursor = skiplist_begin(&dbm->dbMap);
        while (cursor) {
            DBWrap* dbwrap = _get_entry(cursor, DBWrap, snode);
            if (dbwrap->db->p->dbConfig.logSectionOnly) {
                target_dbs.push_back(dbwrap->db);
            }
            cursor = skiplist_next(&dbm->dbMap, cursor);
            skiplist_release_node(&dbwrap->snode);
        }
        if (cursor) skiplist_release_node(cursor);
    }

    if (target_dbs.empty()) return;

    for (auto& entry: target_dbs) {
        DB* db = entry;
        db->p->logMgr->doLogReclaim();
    }
}

} // namespace jungle

