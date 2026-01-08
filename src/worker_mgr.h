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

#include <libjungle/jungle.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace jungle {

// Base class of child worker classes.
class WorkerBase {
public:
    enum WStatus {
        // Thread is not created yet.
        NO_INSTANCE = 0,
        // Not running, cannot work.
        STOP = 1,
        // Running but idle, can work.
        IDLE = 2,
        // Working.
        WORKING = 3,
        // Waiting for termination.
        TERMINATING = 4,
    };

    struct DestroyOptions {
        DestroyOptions() : wait(true) {}
        bool wait;
    };

    struct WorkerOptions {
        WorkerOptions()
            : sleepDurationMs(1000)
            , worker(nullptr) {}
        size_t sleepDurationMs;
        WorkerBase* worker;
    };

    WorkerBase();
    virtual ~WorkerBase();

    virtual void run();
    virtual void stop();
    virtual void invoke();
    virtual void work() = 0;
    virtual void destroy(const DestroyOptions& options);

    virtual bool isIdle() const { return status == IDLE; }
    std::string name() const { return workerName; }

    // To avoid any race condition, it will apply new global config after
    // finishing current task.
    void updateGlobalConfig(const GlobalConfig& g_config);

    // Each worker may have different ways to apply new global config,
    // so we let them implement this function.
    virtual void applyNewGlobalConfig(const GlobalConfig& g_config) = 0;

    void loop();

    std::thread handle;
    std::string workerName;
    WorkerOptions curOptions;
    std::atomic<WStatus> status;
    std::atomic<bool> doNotSleepNextTime;
    GlobalConfig gConfig;

    std::unique_ptr<GlobalConfig> newGConfig;
    std::mutex newGConfigLock;

    EventAwaiter ea;
};

class WorkerMgr {
public:
    WorkerMgr();
    ~WorkerMgr();

    Status addWorker(WorkerBase* worker);
    Status invokeWorker(const std::string& prefix, bool invoke_all = false);
    WorkerBase* getWorker(const std::string& name);

    Status updateGlobalConfig(const GlobalConfig& new_config);

private:
    std::mutex workersLock;
    std::unordered_map<std::string, WorkerBase*> workers;
};


} // namespace jungle


