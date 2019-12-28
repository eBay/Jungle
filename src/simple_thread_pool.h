/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright 2019 Jung-Sang Ahn
See URL: https://github.com/greensky00/simple_thread_pool
         (v0.1.2)

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

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

namespace simple_thread_pool {

class EventAwaiter {
private:
    enum class AS {
        idle    = 0x0,
        ready   = 0x1,
        waiting = 0x2,
        done    = 0x3
    };

public:
    EventAwaiter() : status(AS::idle) {}

    void reset() {
        status.store(AS::idle);
    }

    void wait() {
        wait_us(0);
    }

    void wait_ms(size_t time_ms) {
        wait_us(time_ms * 1000);
    }

    void wait_us(size_t time_us) {
        AS expected = AS::idle;
        if (status.compare_exchange_strong(expected, AS::ready)) {
            // invoke() has not been invoked yet, wait for it.
            std::unique_lock<std::mutex> l(cvLock);
            expected = AS::ready;
            if (status.compare_exchange_strong(expected, AS::waiting)) {
                if (time_us) {
                    cv.wait_for(l, std::chrono::microseconds(time_us));
                } else {
                    cv.wait(l);
                }
                status.store(AS::done);
            } else {
                // invoke() has grabbed `cvLock` earlier than this.
            }
        } else {
            // invoke() already has been called earlier than this.
        }
    }

    void invoke() {
        AS expected = AS::idle;
        if (status.compare_exchange_strong(expected, AS::done)) {
            // wait() has not been invoked yet, do nothing.
            return;
        }

        std::unique_lock<std::mutex> l(cvLock);
        expected = AS::ready;
        if (status.compare_exchange_strong(expected, AS::done)) {
            // wait() has been called earlier than invoke(),
            // but invoke() has grabbed `cvLock` earlier than wait().
            // Do nothing.
        } else {
            // wait() is waiting for ack.
            cv.notify_all();
        }
    }

private:
    std::atomic<AS> status;
    std::mutex cvLock;
    std::condition_variable cv;
};

enum TaskType {
    ONE_TIME = 0x0,
    RECURRING = 0x1,
};

enum TaskStatus {
    WAITING = 0x0,
    RUNNING = 0x1,
    DONE = 0x2,
};

class TaskResult {
public:
    enum Value {
        OK = 0,
        CANCELED = -1,
        FAILED = -32768,
    };

    TaskResult() : val(OK) {}
    TaskResult(Value v) : val(v) {}
    TaskResult(int v) : val((Value)v) {}
    inline explicit operator bool() { return ok(); }
    inline operator int() const { return (int)val; }
    inline bool ok() const { return (val == OK); }
    inline Value value() const { return val; }

private:
    Value val;
};

using TaskHandler = std::function< void(const TaskResult&) >;

class ThreadHandle;
class ThreadPoolMgrBase {
    friend class TaskHandle;
    friend class ThreadHandle;

public:
    ThreadPoolMgrBase() {}

    virtual bool isStopped() const = 0;

    virtual void invoke() = 0;

    virtual void returnThread(const std::shared_ptr<ThreadHandle>& t_handle) = 0;

    virtual bool invokeCanceledTask() const = 0;
};

class TaskHandle {
public:
    TaskHandle(ThreadPoolMgrBase* m,
               const TaskHandler& h,
               uint64_t interval_us,
               TaskType tt)
        : type(tt)
        , mgr(m)
        , intervalUs(interval_us)
        , handler(h)
        , status(WAITING)
    {
        reschedule(intervalUs);
    }

    /**
     * Check if this task is eligible to execute now.
     *
     * @param[out] time_left_us_out
     *     `0` if we can execute this task right now.
     *     Non-zero if we should wait more.
     * @return `true` if we can execute this task.
     */
    bool timeToFire(uint64_t& time_left_us_out) {
        auto cur = std::chrono::system_clock::now();

        std::lock_guard<std::mutex> l(tRegisteredLock);
        std::chrono::duration<double> elapsed = cur - tRegistered;
        if (intervalUs < elapsed.count() * 1000000) {
            time_left_us_out = 0;
            return true;
        }
        time_left_us_out = intervalUs - ( elapsed.count() * 1000000 );
        return false;
    }

    /**
     * Execute this task.
     * If the task already has been executed, this function will
     * do nothing.
     *
     * @param ret Result value that will be passed to the callback function.
     * @return `true` if successfully executed.
     */
    bool execute(const TaskResult& ret) {
        TaskStatus exp = WAITING;
        TaskStatus desired = RUNNING;
        if (status.compare_exchange_strong(exp, desired)) {
            handler(ret);
            if (type == ONE_TIME) {
                status = DONE;
            } else {
                exp = RUNNING;
                desired = WAITING;
                status.compare_exchange_strong(exp, desired);
            }
            return true;
        }
        return false;
    }

    /**
     * Cancel this task.
     *
     * @return `true` if successfully canceled.
     *         `false` if the task already has been executed.
     */
    bool cancel() {
        if (type == ONE_TIME) {
            TaskStatus exp = WAITING;
            TaskStatus desired = DONE;
            if (status.compare_exchange_strong(exp, desired)) {
                if (mgr->invokeCanceledTask()) {
                    handler( TaskResult(TaskResult::CANCELED) );
                }
                return true;
            }
            return false;
        }
        status = DONE;
        return true;
    }

    /**
     * Reschedule this task.
     *
     * @param new_interval_us New interval in microseconds.
     *                        If negetive value, will use existing inteval.
     */
    void reschedule(int64_t new_interval_us = -1) {
        auto cur = std::chrono::system_clock::now();
        {   std::lock_guard<std::mutex> l(tRegisteredLock);
            tRegistered = cur;
            if (new_interval_us >= 0) {
                intervalUs = new_interval_us;
            }
        }
        mgr->invoke();
    }

    /**
     * Check if this task is one-time job.
     *
     * @return `true` if one-time job.
     */
    bool isOneTime() const { return (type == ONE_TIME); }

    /**
     * Check if this task has been executed.
     *
     * @return `true` if already executed.
     */
    bool isDone() const { return (status == DONE); }

private:
    // Type of this task (one time or recurring).
    TaskType type;

    // Parent manager.
    ThreadPoolMgrBase* mgr;

    // Time when this task is initiated.
    std::chrono::time_point<std::chrono::system_clock> tRegistered;
    std::mutex tRegisteredLock;

    // Interval if this is timer task.
    uint64_t intervalUs;

    // Callback function.
    TaskHandler handler;

    // Current running status.
    std::atomic<TaskStatus> status;
};

class ThreadHandle {
public:
    ThreadHandle(ThreadPoolMgrBase* m,
                 size_t id)
        : myId(id)
        , mgr(m)
        , myself(nullptr)
        , tHandle(nullptr)
        , assignedTask(nullptr)
    {
        (void)myId;
    }


    void init(const std::shared_ptr< ThreadHandle >& itself) {
        myself = itself;
        tHandle = std::shared_ptr<std::thread>
                  ( new std::thread(&ThreadHandle::loop, this) );
    }

    void shutdown() {
        eaLoop.invoke();
        if (tHandle && tHandle->joinable()) {
            tHandle->join();
        }
        myself.reset();
        tHandle.reset();
        assignedTask.reset();
    }

    void loop() {
#ifdef __linux__
        std::string thread_name = "stp_" + std::to_string(myId);
        pthread_setname_np(pthread_self(), thread_name.c_str());
#endif
        while (!mgr->isStopped()) {
            eaLoop.wait();
            eaLoop.reset();
            if (mgr->isStopped()) break;

            if (assignedTask) {
                assignedTask->execute(TaskResult());
            }
            assignedTask.reset();

            mgr->returnThread(myself);
            mgr->invoke();
        }
    }

    void assign(const std::shared_ptr<TaskHandle>& handle) {
        assignedTask = handle;
        eaLoop.invoke();
    }

private:
    // Thread ID.
    size_t myId;

    // Parent manager.
    ThreadPoolMgrBase* mgr;

    // Instance of itself.
    std::shared_ptr< ThreadHandle > myself;

    // Thread.
    std::shared_ptr< std::thread > tHandle;

    // Assigned task to execute.
    std::shared_ptr< TaskHandle > assignedTask;

    // Condition variable for thread loop.
    EventAwaiter eaLoop;
};

struct ThreadPoolOptions {
    ThreadPoolOptions()
        : numInitialThreads(4)
        , busyWaitingIntervalUs(100)
        , invokeCanceledTask(false)
        {}

    // Number of threads in the pool.
    // If 0, the main coordinator loop will execute the task,
    // which may block other pending request.
    size_t numInitialThreads;

    // Time interval to do busy waiting instead of sleeping.
    // Higher number will provide better accuracy, but
    // will consume more CPU.
    size_t busyWaitingIntervalUs;

    // If `true`, will invoke task handler with `CANCELED` result code.
    bool invokeCanceledTask;
};

class ThreadPoolMgr : public ThreadPoolMgrBase {
    friend class TaskHandle;
    friend class ThreadHandle;

public:
    ThreadPoolMgr()
        : ThreadPoolMgrBase()
        , stopSignal(false)
        , loopThread(nullptr)
        {}

    ~ThreadPoolMgr() {
        shutdown();
    }

    /**
     * Initialize the thread pool with given options.
     *
     * @param opt Options.
     */
    void init(const ThreadPoolOptions& opt) {
        myOpt = opt;
        stopSignal = false;

        loopThread = std::shared_ptr<std::thread>
                     ( new std::thread( &ThreadPoolMgr::loop, this ) );

        {   std::lock_guard<std::mutex> l(idleThreadsLock);
            for (size_t ii = 0; ii < myOpt.numInitialThreads; ++ii) {
                std::shared_ptr<ThreadHandle> t_handle( new ThreadHandle(this, ii) );
                idleThreads.push_back(t_handle);
                t_handle->init(t_handle);
            }
        }
    }

    /**
     * Step down the thread pool.
     */
    void shutdown() {
        stopSignal = true;
        if (loopThread && loopThread->joinable()) {
            eaLoop.invoke();
            loopThread->join();
        }
        loopThread.reset();

        do {
            std::shared_ptr<ThreadHandle> t_handle_to_free = nullptr;
            {   std::lock_guard<std::mutex> l(idleThreadsLock);
                auto entry = idleThreads.begin();
                if (entry == idleThreads.end()) break;
                t_handle_to_free = *entry;
                idleThreads.erase(entry);
            }
            t_handle_to_free->shutdown();
        } while (true);

        std::list< std::shared_ptr<TaskHandle> > tasks_to_cancel;
        {   std::lock_guard<std::mutex> l(timedTasksLock);
            for (auto& entry: timedTasks) tasks_to_cancel.push_back(entry);
            timedTasks.clear();
        }
        {   std::lock_guard<std::mutex> l(normalTasksLock);
            for (auto& entry: normalTasks) tasks_to_cancel.push_back(entry);
            normalTasks.clear();
        }
        TaskResult tr(TaskResult::CANCELED);
        for (auto& entry: tasks_to_cancel) {
            std::shared_ptr<TaskHandle>& tt = entry;
            tt->cancel();
        }
    }

    /**
     * Register an async task.
     *
     * @param handler Callback function to execute.
     * @param interval_us Interval in microseconds, if given task
     *                    is a timer work.
     * @param type ONE_TIME or RECURRING.
     * @return Handle of registered task.
     */
    std::shared_ptr<TaskHandle> addTask(const TaskHandler& handler,
                                        uint64_t interval_us = 0,
                                        TaskType type = TaskType::ONE_TIME)
    {
        std::shared_ptr<TaskHandle>
            new_task( new TaskHandle(this, handler, interval_us, type) );

        if (interval_us) {
            std::lock_guard<std::mutex> l(timedTasksLock);
            timedTasks.push_back(new_task);
        } else {
            std::lock_guard<std::mutex> l(normalTasksLock);
            normalTasks.push_back(new_task);
        }
        eaLoop.invoke();

        return new_task;
    }

    /**
     * Check if thread pool manager is stopped.
     *
     * @return `true` if stopped.
     */
    bool isStopped() const { return stopSignal; }

    /**
     * Manually invoke the main coordination loop.
     */
    void invoke() { eaLoop.invoke(); }

private:
    void returnThread(const std::shared_ptr<ThreadHandle>& t_handle) {
        std::lock_guard<std::mutex> l(idleThreadsLock);
        idleThreads.push_front( t_handle );
    }

    bool invokeCanceledTask() const {
        return myOpt.invokeCanceledTask;
    }

    void loop() {
#ifdef __linux__
        pthread_setname_np(pthread_self(), "stp_coord");
#endif
        const uint64_t MAX_SLEEP_US = 1000000;
        uint64_t next_sleep_us = MAX_SLEEP_US;

        while (!stopSignal) {
            if (next_sleep_us > myOpt.busyWaitingIntervalUs) {
                eaLoop.wait_us(next_sleep_us - myOpt.busyWaitingIntervalUs);
            } else {
                // Otherwise: busy waiting.
            }
            if (stopSignal) break;

            eaLoop.reset();
            next_sleep_us = MAX_SLEEP_US;

            std::shared_ptr<ThreadHandle> thread_to_assign = nullptr;

            if (myOpt.numInitialThreads) {
                // Thread pool exists, pick an idle thread.
                {   std::lock_guard<std::mutex> l(idleThreadsLock);
                    auto entry = idleThreads.begin();
                    if (entry != idleThreads.end()) {
                        thread_to_assign = *entry;
                        idleThreads.erase(entry);
                    }
                }
                if (!thread_to_assign) {
                    // All threads are busy, skip.
                    continue;
                }
            }
            // Otherwise (empty thread pool),
            // this loop thread will do execution.

            // Check timer task first (higher priority).
            std::shared_ptr<TaskHandle> task_to_run = nullptr;
            {   std::lock_guard<std::mutex> l(timedTasksLock);
                auto entry = timedTasks.begin();
                while (entry != timedTasks.end()) {
                    std::shared_ptr<TaskHandle>& tt = *entry;
                    uint64_t remaining_us = 0;
                    if (tt->timeToFire(remaining_us)) {
                        task_to_run = tt;
                        if ( task_to_run->isOneTime() ||
                             task_to_run->isDone() ) {
                            entry = timedTasks.erase(entry);
                        } else {
                            task_to_run->reschedule();
                        }

                        if (task_to_run->isDone()) task_to_run.reset();
                        else break;
                    }
                    if (!task_to_run) {
                        entry++;
                        // Adjust next sleep time.
                        next_sleep_us = std::min(next_sleep_us, remaining_us);
                        next_sleep_us = std::min(next_sleep_us, MAX_SLEEP_US);
                    }
                }
            }

            if (!task_to_run) {
                // If there is no timer task to be fired for now,
                // pick a normal task.
                std::lock_guard<std::mutex> l(normalTasksLock);
                auto entry = normalTasks.begin();
                if (entry != normalTasks.end()) {
                    task_to_run = *entry;
                    normalTasks.erase(entry);
                }

                if (normalTasks.size()) {
                    // Still have pending task(s). Do not sleep.
                    next_sleep_us = 0;
                }
            }

            if (!task_to_run) {
                // No task to run, skip.
                if (thread_to_assign) {
                    std::lock_guard<std::mutex> l(idleThreadsLock);
                    idleThreads.push_front(thread_to_assign);
                }
                continue;
            }

            if (myOpt.numInitialThreads) {
                // Assign the task to picked thread.
                thread_to_assign->assign(task_to_run);
            } else {
                // Empty thread pool, execute here.
                task_to_run->execute(TaskResult());
            }
        }
    }

    // Options.
    ThreadPoolOptions myOpt;

    // `true` if system is being stopped.
    std::atomic<bool> stopSignal;

    // Condition variable for main coordination loop.
    EventAwaiter eaLoop;

    // Main coordination loop thread.
    std::shared_ptr< std::thread > loopThread;

    // List of timer tasks.
    std::list< std::shared_ptr<TaskHandle> > timedTasks;
    std::mutex timedTasksLock;

    // List of normal tasks.
    std::list< std::shared_ptr<TaskHandle> > normalTasks;
    std::mutex normalTasksLock;

    // List of idle threads.
    std::list< std::shared_ptr<ThreadHandle> > idleThreads;
    std::mutex idleThreadsLock;
};

};

