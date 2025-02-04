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

#include "internal_helper.h"
#include "worker_mgr.h"

#include <libjungle/jungle.h>

#include <list>

namespace jungle {

struct FlusherQueueElem {
    FlusherQueueElem()
        : targetDb(nullptr)
        , seqUpto(NOT_INITIALIZED)
        {}

    FlusherQueueElem(DB* _db,
                     const FlushOptions& _f_options,
                     const uint64_t _seq_upto,
                     UserHandler _handler,
                     void* _ctx)
        : targetDb(_db)
        , fOptions(_f_options)
        , seqUpto(_seq_upto)
    {
        handlers.push_back( HandlerElem(_handler, _ctx) );
    }

    struct HandlerElem {
        HandlerElem(UserHandler h = nullptr, void* c = nullptr)
            : handler(h), ctx(c)
            {}
        UserHandler handler;
        void* ctx;
    };

    DB* targetDb;
    FlushOptions fOptions;
    uint64_t seqUpto;
    std::list<HandlerElem> handlers;
};

class FlusherQueue {
public:
    FlusherQueue() {}
    ~FlusherQueue();

    void push(FlusherQueueElem* elem);
    FlusherQueueElem* pop();
    size_t size() const;

private:
    mutable std::mutex queueLock;
    std::list<FlusherQueueElem*> queue;
};

class Flusher : public WorkerBase {
public:
    enum FlusherType { GENERIC = 0x0, FLUSH_ON_DEMAND = 0x1, FLUSH_ON_CONDITION = 0x2 };

    struct FlusherOptions : public WorkerOptions {
    };

    Flusher(const std::string& w_name,
            const GlobalConfig& g_config,
            FlusherType f_type = FlusherType::GENERIC);
    ~Flusher();
    void work(WorkerOptions* opt_base);

    void calcGlobalThrottling(size_t total_num_log_files);

    GlobalConfig gConfig;
    size_t lastCheckedFileIndex;
    FlusherType type;
};


} // namespace jungle
