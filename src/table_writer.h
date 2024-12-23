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

#pragma once

#include "event_awaiter.h"
#include "internal_helper.h"

#include <libjungle/jungle.h>

#include <cassert>
#include <list>
#include <mutex>
#include <vector>

class SimpleLogger;

namespace jungle {

class TableFile;
class TableMgr;
class TableWriterGroup;
class TableWriterMgr;

struct TableWritePayload {
    TableWritePayload()
        : payloadReady(false)
        , targetTableMgr(nullptr)
        , batch(nullptr)
        , offsets(nullptr)
        , startIdx(0)
        , count(0)
        , checkpoints(nullptr)
        , sourceTableFile(nullptr)
        , targetTableFile(nullptr)
        , targetHash(_SCU32(-1))
        , bulkLoadMode(false)
        {}

    // When records are given in advance.
    TableWritePayload(TableMgr* _table_mgr,
                      std::list<Record*>* _batch,
                      std::list<uint64_t>* _checkpoints,
                      TableFile* _target_table_file,
                      uint32_t _target_hash,
                      bool _bulk_load_mode)
        : payloadReady(false)
        , targetTableMgr(_table_mgr)
        , batch(_batch)
        , offsets(nullptr)
        , startIdx(0)
        , count(0)
        , checkpoints(_checkpoints)
        , sourceTableFile(nullptr)
        , targetTableFile(_target_table_file)
        , targetHash(_target_hash)
        , bulkLoadMode(_bulk_load_mode)
    {
        payloadReady = true;
    }

    // When only offset is known.
    TableWritePayload(TableMgr* _table_mgr,
                      std::vector<uint64_t>* _offsets,
                      uint64_t s_idx,
                      uint64_t c,
                      std::list<uint64_t>* _checkpoints,
                      TableFile* _source_table_file,
                      TableFile* _target_table_file)
        : payloadReady(false)
        , targetTableMgr(_table_mgr)
        , batch(nullptr)
        , offsets(_offsets)
        , startIdx(s_idx)
        , count(c)
        , checkpoints(_checkpoints)
        , sourceTableFile(_source_table_file)
        , targetTableFile(_target_table_file)
        , targetHash(_SCU32(-1))
        , bulkLoadMode(false)
    {
        payloadReady = true;
    }

    TableWritePayload& operator=(const TableWritePayload& src) {
        std::lock_guard<std::mutex> l(lock);
        assert(payloadReady.load() == false);

        targetTableMgr = src.targetTableMgr;
        batch = src.batch;
        offsets = src.offsets;
        startIdx = src.startIdx;
        count = src.count;
        checkpoints = src.checkpoints;
        sourceTableFile = src.sourceTableFile;
        targetTableFile = src.targetTableFile;
        targetHash = src.targetHash;
        bulkLoadMode = src.bulkLoadMode;

        payloadReady = true;
        return *this;
    }

    void reset() {
        std::lock_guard<std::mutex> l(lock);

        targetTableMgr = nullptr;
        batch = nullptr;
        offsets = nullptr;
        startIdx = 0;
        count = 0;
        checkpoints = nullptr;
        sourceTableFile = nullptr;
        targetTableFile = nullptr;
        targetHash = _SCU32(-1);
        bulkLoadMode = false;

        // WARNING:
        //   There are many threads monitoring
        //   `payloadReady` flag. We SHOULD clear
        //   this flag at the end this function.
        payloadReady = false;
    }

    bool isEmpty() const { return !payloadReady.load(); }

    std::mutex lock;
    std::atomic<bool> payloadReady;
    TableMgr* targetTableMgr;
    std::list<Record*>* batch;
    std::vector<uint64_t>* offsets;
    uint64_t startIdx;
    uint64_t count;
    std::list<uint64_t>* checkpoints;
    TableFile* sourceTableFile;
    TableFile* targetTableFile;
    uint32_t targetHash;
    bool bulkLoadMode;
};

struct TableWriterArgs {
    TableWriterArgs();

    void invoke();

    uint32_t writerId;
    EventAwaiter awaiter;
    EventAwaiter callerAwaiter;
    std::atomic<bool> stopSignal;
    TableWritePayload payload;
    SimpleLogger* myLog;
    std::atomic<bool> adjustingNumL0;
};

struct TableWriterPkg {
    TableWriterPkg();

    void init(size_t writer_id,
              TableWriterMgr* tw_mgr,
              SimpleLogger* logger);

    void shutdown();

    std::thread* tHandle;
    TableWriterArgs writerArgs;
    std::atomic<bool> inUse;
};

class TableWriterMgr {
public:
    TableWriterMgr() {}

    ~TableWriterMgr();

    Status init();

    Status shutdown();

    void tableWriterLoop(TableWriterArgs* args);

    static void doTableWrite(TableWriterArgs* args);

    std::vector<TableWriterPkg*> leaseWriters(size_t num);

    void returnWriters(const std::vector<TableWriterPkg*> writers);

    // Lock for both writer groups and waiting queue.
    std::mutex globalLock;

    std::vector<TableWriterPkg*> tableWriters;

};

class TableWriterHolder {
public:
    TableWriterHolder(TableWriterMgr* _mgr,
                      size_t num_requested)
        : mgr(_mgr)
    {
        leasedWriters = mgr->leaseWriters(num_requested);
    }

    ~TableWriterHolder() {
        mgr->returnWriters(leasedWriters);
    }

    TableWriterMgr* mgr;
    std::vector<TableWriterPkg*> leasedWriters;
};


}; // namespace jungle

