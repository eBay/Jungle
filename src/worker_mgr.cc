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

#include "worker_mgr.h"

namespace jungle {

WorkerBase::WorkerBase()
    : workerName("noname")
    , status(NO_INSTANCE)
    , doNotSleepNextTime(false)
{}

WorkerBase::~WorkerBase() {
}

void WorkerBase::loop(WorkerOptions* opt) {
    if (!opt || !opt->worker) return;

    WorkerBase* worker = opt->worker;
#ifdef __linux__
    std::string thread_name = "j_" + worker->workerName;
    thread_name = thread_name.substr(0, 15);
    pthread_setname_np(pthread_self(), thread_name.c_str());
#endif

    for (;;) {
        // Sleep if IDLE or STOP.
        if ( (worker->status == IDLE || worker->status == STOP) &&
             !worker->doNotSleepNextTime.load() ) {
            worker->ea.wait_ms(opt->sleepDuration_ms);
            worker->ea.reset();
        }

        if (worker->status == TERMINATING) {
            // Terminate.
            break;
        }

        // Work.
        // IDLE --> WORKING.
        WStatus exp = IDLE;
        WStatus val = WORKING;
        if (worker->status.compare_exchange_weak(exp, val)) {
            worker->doNotSleepNextTime = false;
            worker->work(opt);
            // WORKING --> IDLE.
            exp = WORKING;
            val = IDLE;
            worker->status.compare_exchange_weak(exp, val);
        }
    }
    worker->status = NO_INSTANCE;
}

void WorkerBase::run() {
    WStatus exp = STOP;
    WStatus val = IDLE;
    if (status.compare_exchange_weak(exp, val)) {
    } else {
    }
}

void WorkerBase::stop() {
    status = STOP;
}

void WorkerBase::invoke() {
    ea.invoke();
}

void WorkerBase::destroy(const DestroyOptions& options) {
    status = TERMINATING;
    if (handle.joinable()) {
        if (options.wait) {
            handle.join();
        } else {
            ea.invoke();
        }
    }
}




WorkerMgr::WorkerMgr() {
}

WorkerMgr::~WorkerMgr() {
    std::lock_guard<std::mutex> ll(workersLock);
    for (auto& entry: workers) {
        WorkerBase* w = entry.second;
        WorkerBase::DestroyOptions d_opt;
        d_opt.wait = false;
        w->destroy(d_opt);

        // Wait until the worker normally finishes its job.
        while (w->status != WorkerBase::NO_INSTANCE) {
            std::this_thread::yield();
        }
        if (w->handle.joinable()) {
            w->handle.join();
        }
        delete w;
    }
}

Status WorkerMgr::addWorker(WorkerBase* worker)
{
    std::lock_guard<std::mutex> ll(workersLock);

    auto itr = workers.find(worker->name());
    if (itr != workers.end()) {
        return Status::ALREADY_EXIST;
    }
    workers.insert( std::make_pair(worker->name(), worker) );
    worker->stop();

    return Status();
}

Status WorkerMgr::invokeWorker(const std::string& prefix, bool invoke_all) {
    std::lock_guard<std::mutex> ll(workersLock);
    for (auto& entry: workers) {
        WorkerBase* worker = entry.second;
        if ( worker->name().find(prefix) != std::string::npos ) {
            worker->invoke();
            if (!invoke_all) break;
        }
    }

    return Status();
}

WorkerBase* WorkerMgr::getWorker(const std::string& name) {
    std::lock_guard<std::mutex> ll(workersLock);
    for (auto& entry: workers) {
        WorkerBase* worker = entry.second;
        if ( worker->name() == name ) {
            return worker;
        }
    }

    return nullptr;
}

} // namespace jungle
