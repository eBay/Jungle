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
#include "flusher.h"
#include "internal_helper.h"
#include "log_mgr.h"
#include "skiplist.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <pthread.h>

namespace jungle {

FlusherQueue::~FlusherQueue() {
    std::lock_guard<std::mutex> l(queueLock);
    for (auto& entry: queue) {
        FlusherQueueElem*& elem = entry;
        delete elem;
    }
}

void FlusherQueue::push(FlusherQueueElem* elem) {
    std::unique_lock<std::mutex> l(queueLock);
    // Find existing request for the same DB.
    for (auto& entry: queue) {
        FlusherQueueElem*& elem_entry = entry;
        if (elem_entry->targetDb == elem->targetDb) {
            // Merge handler list.
            for (auto& he: elem->handlers) {
                elem_entry->handlers.push_back(he);
            }
            // Set up-to-date info.
            elem_entry->fOptions = elem->fOptions;
            elem_entry->seqUpto = elem->seqUpto;
            _log_debug(elem_entry->targetDb->p->myLog,
                       "Overwrote existing req %p by %p.",
                       elem_entry, elem);

            // Delete newly given one.
            delete elem;
            return;
        }
    }
    // Not found. Add new.
    queue.push_back(elem);
    _log_debug(elem->targetDb->p->myLog,
               "Inserted new req %p into flusher queue.", elem);
    l.unlock();
}

FlusherQueueElem* FlusherQueue::pop() {
    std::lock_guard<std::mutex> l(queueLock);
    auto entry = queue.begin();
    if (entry != queue.end()) {
        FlusherQueueElem* elem = *entry;
        queue.pop_front();
        return elem;
    }
    return nullptr;
}

size_t FlusherQueue::size() const {
    std::lock_guard<std::mutex> l(queueLock);
    return queue.size();
}


Flusher::Flusher(const std::string& _w_name,
                 const GlobalConfig& _config)
    : lastCheckedFileIndex(0xffff) // Any big number to start from 0.
{
    workerName = _w_name;
    gConfig = _config;
    handleAsyncReqs = true;
    FlusherOptions options;
    options.sleepDuration_ms = gConfig.flusherSleepDuration_ms;
    options.worker = this;
    curOptions = options;
    handle = std::thread(WorkerBase::loop, &curOptions);
}

Flusher::~Flusher() {
}

void Flusher::work(WorkerOptions* opt_base) {
    Status s;

    DBMgr* dbm = DBMgr::getWithoutInit();
    if (!dbm) return;

    DB* target_db = nullptr;

    FlusherQueueElem* elem = nullptr;
    if (handleAsyncReqs) {
        elem = dbm->flusherQueue()->pop();
    }

    if (elem) {
        // User assigned work check if it is already closed.
        std::lock_guard<std::mutex> l(dbm->dbMapLock);
        skiplist_node* cursor = skiplist_begin(&dbm->dbMap);
        while (cursor) {
            DBWrap* dbwrap = _get_entry(cursor, DBWrap, snode);
            if (dbwrap->db == elem->targetDb) {
                target_db = elem->targetDb;
                target_db->p->incBgTask();
                break;
            }
            cursor = skiplist_next(&dbm->dbMap, cursor);
            skiplist_release_node(&dbwrap->snode);
        }
        if (cursor) skiplist_release_node(cursor);

    } else {
        // Otherwise: check DB map.
        std::lock_guard<std::mutex> l(dbm->dbMapLock);

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
            if (dbwrap->db->p->logMgr->checkTimeToFlush(gConfig)) {
                target_db = dbwrap->db;
                target_db->p->incBgTask();
                break;
            }
        }
    }

    if (target_db) {
        _log_debug(target_db->p->myLog,
                   "DB %p is selected for flushing: req %p.",
                   target_db, elem);

        bool call_fsync = false;
        bool sync_only = false;
        if (elem) {
            if (elem->fOptions.callFsync) call_fsync = true;
            if (elem->fOptions.syncOnly) sync_only = true;
        }

        if (gConfig.flusherAutoSync || sync_only) {
            s = target_db->sync(call_fsync);
        }
        if (s) {
            FlushOptions f_options;
            uint64_t seq_upto = NOT_INITIALIZED;
            if (elem) {
                // Requested by user.
                f_options = elem->fOptions;
                if (valid_number(elem->seqUpto)) seq_upto = elem->seqUpto;

            } else {
                // Auto flush.
                f_options.numFilesLimit = 8;
                f_options.beyondLastSync = !gConfig.flusherAutoSync;
                if (target_db->p->dbConfig.nextLevelExtension) {
                    // In LSM mode, bigger batch is always better.
                    f_options.numFilesLimit = 16;
                }
            }

            if (!sync_only) {
                s = target_db->flushLogs(f_options, seq_upto);
            }

            if (s && !elem) {
                // Successful flush + auto flush mode
                //  = do not sleep next time (continuously work).
                doNotSleepNextTime = true;
            }
        }

    } else {
        s = Status::DB_HANDLE_NOT_FOUND;
    }

    bool delayed_task = false;
    if (elem) {
        size_t elem_count = 0, handler_count = 0;
        for (auto& entry: elem->handlers) {
            FlusherQueueElem::HandlerElem& he = entry;
            if (he.handler) {
                he.handler(s, he.ctx);
                handler_count++;
            }
            elem_count++;
        }
        if (target_db) {
            _log_debug(target_db->p->myLog,
                       "total %zu handlers out of %zu requests "
                       "have been invoked together",
                       handler_count, elem_count);
        } else {
            _log_info(dbm->getLogger(),
                      "got stale request %p, target DB doesn't exist",
                      elem);
        }
        if (elem->fOptions.execDelayUs) delayed_task = true;
        delete elem;
    }

    // WARNING:
    //   We should decrease reference counter AFTER
    //   user handler finishes its job.
    if (target_db) {
        target_db->p->decBgTask();
    }

    if ( dbm->flusherQueue()->size() &&
         !delayed_task ) {
        doNotSleepNextTime = true;
    }
}

} // namespace jungle

