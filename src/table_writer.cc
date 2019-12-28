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

#include "table_writer.h"

#include "db_mgr.h"
#include "internal_helper.h"
#include "table_mgr.h"

#include <pthread.h>

namespace jungle {

TableWriterArgs::TableWriterArgs()
    : writerId(0)
    , stopSignal(false)
    , myLog(nullptr)
    {}

void TableWriterArgs::invoke() {
    awaiter.invoke();
}

TableWriterPkg::TableWriterPkg()
    : tHandle(nullptr)
    , inUse(false)
    {}

void TableWriterPkg::init(size_t writer_id,
                          TableWriterMgr* tw_mgr,
                          SimpleLogger* logger)
{
    writerArgs.writerId = writer_id;
    writerArgs.myLog = logger;
    tHandle = new std::thread
              ( &TableWriterMgr::tableWriterLoop, tw_mgr, &writerArgs );
}

void TableWriterPkg::shutdown() {
    writerArgs.stopSignal = true;
    writerArgs.invoke();
    if (tHandle && tHandle->joinable()) {
        tHandle->join();
    }
    DELETE(tHandle);
}


TableWriterMgr::~TableWriterMgr() {
    shutdown();
}

Status TableWriterMgr::init() {
    DBMgr* mgr = DBMgr::getWithoutInit();
    assert(mgr);

    GlobalConfig* g_config = mgr->getGlobalConfig();

    // Spawn level-0 writers.
    size_t num_writers = g_config->numTableWriters;

    SimpleLogger* my_log = mgr->getLogger();

    for (size_t ii=0; ii<num_writers; ++ii) {
        TableWriterPkg* pp = new TableWriterPkg();
        pp->init(ii, this, my_log);
        tableWriters.push_back(pp);
    }

    return Status();
}

Status TableWriterMgr::shutdown() {
    for (auto& entry: tableWriters) {
        TableWriterPkg*& pp = entry;
        pp->shutdown();
        DELETE(pp);
    }
    tableWriters.clear();

    return Status();
}

std::vector<TableWriterPkg*> TableWriterMgr::leaseWriters(size_t num) {
    std::vector<TableWriterPkg*> ret;
    if (!num) return ret;

    std::lock_guard<std::mutex> l(globalLock);

    // Search idle writers upto `num`.
    for (auto& entry: tableWriters) {
        TableWriterPkg* pp = entry;
        if (!pp->inUse) {
            pp->inUse = true;
            ret.push_back(pp);
            if (ret.size() >= num) break;
        }
    }
    // Even though there is no idle writer, just return empty list.
    return ret;
}

void TableWriterMgr::returnWriters(const std::vector<TableWriterPkg*> writers) {
    std::lock_guard<std::mutex> l(globalLock);
    for (auto& entry: writers) {
        TableWriterPkg* pp = entry;
        tableWriters[pp->writerArgs.writerId]->inUse = false;
    }
}

void TableWriterMgr::tableWriterLoop(TableWriterArgs* args) {
#ifdef __linux__
    std::string thread_name = "j_twriter_" + std::to_string(args->writerId);
    pthread_setname_np(pthread_self(), thread_name.c_str());
#endif

    _log_info( args->myLog, "table Writer initiated (%zu)",
               args->writerId );

    while (!args->stopSignal) {
        args->awaiter.wait_ms(1000);
        args->awaiter.reset();

        if (args->stopSignal) break;
        if (args->payload.isEmpty()) continue;

        doTableWrite(args);

        // Clear the workload.
        args->payload.reset();

        // Invoke table mgr.
        args->callerAwaiter.invoke();
    }

    _log_info( args->myLog, "table Writer terminated (%zu)",
               args->writerId );
}

void TableWriterMgr::doTableWrite(TableWriterArgs* args) {
    if (args->payload.batch) {
        // Batch is given.
        SizedBuf empty_key;
        args->payload.targetTableMgr->setTableFile
            ( *args->payload.batch,
              *args->payload.checkpoints,
              args->payload.bulkLoadMode,
              args->payload.targetTableFile,
              args->payload.targetHash,
              empty_key, empty_key );

    } else if (args->payload.sourceTableFile) {
        // Offset is given.
        args->payload.targetTableMgr->setTableFileOffset
            ( *args->payload.checkpoints,
              args->payload.sourceTableFile,
              args->payload.targetTableFile,
              *args->payload.offsets,
              args->payload.startIdx,
              args->payload.count );
    }
}


}; // namespace jungle

