/**
 * Copyright (C) 2017-present Jung-Sang Ahn <jungsang.ahn@gmail.com>
 * All rights reserved.
 *
 * https://github.com/greensky00
 *
 * Simple Logger
 * Version: 0.4.0
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <signal.h>
#include <stdarg.h>
#if defined(__linux__) || defined(__APPLE__)
    #include <sys/time.h>
#endif

// To suppress false alarms by thread sanitizer,
// add -DSUPPRESS_TSAN_FALSE_ALARMS=1 flag to CXXFLAGS.
// #define SUPPRESS_TSAN_FALSE_ALARMS (1)

// 0: System  [====]
// 1: Fatal   [FATL]
// 2: Error   [ERRO]
// 3: Warning [WARN]
// 4: Info    [INFO]
// 5: Debug   [DEBG]
// 6: Trace   [TRAC]


// printf style log macro
#define _log_(level, l, ...)                                            \
    if (l && l->checkWhetherToLog(level, __FILE__, __func__, __LINE__)) \
        (l)->put(level, __FILE__, __func__, __LINE__, __VA_ARGS__)

#define _log_sys(l, ...)    _log_(SimpleLogger::SYS,     l, __VA_ARGS__)
#define _log_fatal(l, ...)  _log_(SimpleLogger::FATAL,   l, __VA_ARGS__)
#define _log_err(l, ...)    _log_(SimpleLogger::ERROR,   l, __VA_ARGS__)
#define _log_warn(l, ...)   _log_(SimpleLogger::WARNING, l, __VA_ARGS__)
#define _log_info(l, ...)   _log_(SimpleLogger::INFO,    l, __VA_ARGS__)
#define _log_debug(l, ...)  _log_(SimpleLogger::DEBUG,   l, __VA_ARGS__)
#define _log_trace(l, ...)  _log_(SimpleLogger::TRACE,   l, __VA_ARGS__)


// stream log macro
#define _stream_(level, l)                                              \
    if (l && l->checkWhetherToLog(level, __FILE__, __func__, __LINE__)) \
        l->eos() = l->stream(level, l, __FILE__, __func__, __LINE__)

#define _s_sys(l)   _stream_(SimpleLogger::SYS,     l)
#define _s_fatal(l) _stream_(SimpleLogger::FATAL,   l)
#define _s_err(l)   _stream_(SimpleLogger::ERROR,   l)
#define _s_warn(l)  _stream_(SimpleLogger::WARNING, l)
#define _s_info(l)  _stream_(SimpleLogger::INFO,    l)
#define _s_debug(l) _stream_(SimpleLogger::DEBUG,   l)
#define _s_trace(l) _stream_(SimpleLogger::TRACE,   l)


// Do printf style log, but print logs in `lv1` level during normal time,
// once in given `interval_ms` interval, print a log in `lv2` level.
// The very first log will be printed in `lv2` level.
//
// This function is global throughout the process, so that
// multiple threads will share the interval.
#define _timed_log_g(l, interval_ms, lv1, lv2, ...)                     \
{                                                                       \
    _timed_log_definition(static);                                      \
    _timed_log_body(l, interval_ms, lv1, lv2, __VA_ARGS__);             \
}

// Same as `_timed_log_g` but per-thread level.
#define _timed_log_t(l, interval_ms, lv1, lv2, ...)                     \
{                                                                       \
    _timed_log_definition(thread_local);                                \
    _timed_log_body(l, interval_ms, lv1, lv2, __VA_ARGS__);             \
}

#define _timed_log_definition(prefix)                                   \
    prefix std::mutex timer_lock;                                       \
    prefix bool first_event_fired = false;                              \
    prefix std::chrono::system_clock::time_point last_timeout =         \
        std::chrono::system_clock::now();                               \

#define _timed_log_body(l, interval_ms, lv1, lv2, ...)                  \
    std::chrono::system_clock::time_point cur =                         \
        std::chrono::system_clock::now();                               \
    bool timeout = false;                                               \
    {   std::lock_guard<std::mutex> __ll__(timer_lock);                 \
        std::chrono::duration<double> elapsed = cur - last_timeout;     \
        if ( elapsed.count() * 1000 > interval_ms ||                    \
             !first_event_fired ) {                                     \
            cur = std::chrono::system_clock::now();                     \
            elapsed = cur - last_timeout;                               \
            if ( elapsed.count() * 1000 > interval_ms ||                \
                 !first_event_fired ) {                                 \
                timeout = first_event_fired = true;                     \
                last_timeout = cur;                                     \
            }                                                           \
        }                                                               \
    }                                                                   \
    if (timeout) {                                                      \
        _log_(lv2, l, __VA_ARGS__);                                     \
    } else {                                                            \
        _log_(lv1, l, __VA_ARGS__);                                     \
    }

class SimpleLoggerImpl;

// Part of the constructor's signature. Bump the version if a change makes
// code compiled with an older copy of this header unable to work with this
// implementation, so that such code fails to link instead of misbehaving.
template<int VERSION> struct SimpleLoggerAbiTag {};
using SimpleLoggerAbi = SimpleLoggerAbiTag<2>;

// NOTE: Other libraries (e.g., Jungle) may be compiled with a different copy
//       of this header, while linked with this implementation. Hence,
//         * `SimpleLogger` should have no data member other than `impl`,
//           and no inline function accessing its state. Everything else
//           goes to `SimpleLoggerImpl` (logger_impl.h).
//         * Do not change the signature of existing functions, or the
//           layout of the types defined in this header
//           (`LoggerStream`, `SimpleLoggerLocalCtx`, etc.).
//         * Adding a new non-inline member function is fine.
class SimpleLogger {
public:
    static const int MSG_SIZE = 4096;
    static const std::memory_order MOR = std::memory_order_relaxed;

    using CtxMap = std::unordered_map<std::string, std::list<std::string>>;

    // This callback determines whether to log or not.
    // If it returns `false`, the log will be skipped.
    using PreCbType = std::function<bool(
        int level,
        const char* source_file,
        const char* func_name,
        size_t line_number,
        const CtxMap& ctx_map
    )>;

    // This callback is called before the log is written.
    // If this callback returns `false`, the log will not be written.
    using CbType = std::function<bool(
        int level,
        const char* source_file,
        const char* func_name,
        size_t line_number,
        uint64_t time_since_epoch_us,
        uint32_t tid,
        size_t msg_len,
        const char* msg,
        const CtxMap& ctx_map
    )>;

    enum Levels {
        SYS         = 0,
        FATAL       = 1,
        ERROR       = 2,
        WARNING     = 3,
        INFO        = 4,
        DEBUG       = 5,
        TRACE       = 6,
        UNKNOWN     = 99,
    };

    class LoggerStream : public std::ostream {
    public:
        LoggerStream() : std::ostream(&buf), level(0), logger(nullptr)
                       , file(nullptr), func(nullptr), line(0) {}

        template<typename T>
        inline LoggerStream& operator<<(const T& data) {
            sStream << data;
            return *this;
        }

        using MyCout = std::basic_ostream< char, std::char_traits<char> >;
        typedef MyCout& (*EndlFunc)(MyCout&);
        inline LoggerStream& operator<<(EndlFunc func) {
            func(sStream);
            return *this;
        }

        inline void put() {
            if (logger) {
                logger->put( level, file, func, line,
                             "%s", sStream.str().c_str() );
            }
        }

        inline void setLogInfo(int lv,
                               SimpleLogger* lg,
                               const char* fl,
                               const char* fc,
                               size_t ln)
        {
            sStream.str(std::string());
            level = lv;
            logger = lg;
            file = fl;
            func = fc;
            line = ln;
        }

    private:
        std::stringbuf buf;
        std::stringstream sStream;
        int level;
        SimpleLogger* logger;
        const char* file;
        const char* func;
        size_t line;
    };

    class EndOfStmt {
    public:
        EndOfStmt() {}
        EndOfStmt(LoggerStream& src) { src.put(); }
        EndOfStmt& operator=(LoggerStream& src) { src.put(); return *this; }
    };

    LoggerStream& stream( int level,
                          SimpleLogger* logger,
                          const char* file,
                          const char* func,
                          size_t line );

    EndOfStmt& eos();

    SimpleLogger(const std::string& file_path,
                 size_t max_log_elems           = 4096,
                 uint64_t log_file_size_limit   = 32*1024*1024,
                 uint32_t max_log_files         = 16,
                 SimpleLoggerAbi abi            = SimpleLoggerAbi());
    ~SimpleLogger();

    SimpleLogger(const SimpleLogger&) = delete;
    SimpleLogger& operator=(const SimpleLogger&) = delete;

    static void setCriticalInfo(const std::string& info_str);
    static void setCrashDumpPath(const std::string& path,
                                 bool origin_only = true);
    static void setStackTraceOriginOnly(bool origin_only);
    static void logStackBacktrace();

    static void shutdown();
    static std::string replaceString(const std::string& src_str,
                                     const std::string& before,
                                     const std::string& after);

    int start();
    int stop();

    bool traceAllowed() const;
    bool debugAllowed() const;

    void setLogLevel(int level);
    void setDispLevel(int level);
    void setMaxLogFiles(size_t max_log_files);

    int getLogLevel() const;
    int getDispLevel() const;

    void put(int level,
             const char* source_file,
             const char* func_name,
             size_t line_number,
             const char* format,
             ...);
    // If `skip_if_clean` is true, skip the ring buffer scan when no
    // log has been written since the last flush.
    void flushAll(bool skip_if_clean = false);

    void setGlobalCallback(PreCbType pre_cb, CbType cb);

    bool checkWhetherToLog(int level,
                           const char* source_file,
                           const char* func_name,
                           size_t line_number);

private:
    SimpleLoggerImpl* impl;
};

static_assert( sizeof(SimpleLogger) == sizeof(void*),
               "SimpleLogger should have no data member other than `impl`" );

// Singleton class
class SimpleLoggerMgr {
public:
    struct CompElem;

    struct TimeInfo {
        TimeInfo(std::tm* src);
        TimeInfo(std::chrono::system_clock::time_point now);
        int year;
        int month;
        int day;
        int hour;
        int min;
        int sec;
        int msec;
        int usec;
    };

    struct RawStackInfo {
        RawStackInfo() : tidHash(0), kernelTid(0), crashOrigin(false) {}
        uint32_t tidHash;
        uint64_t kernelTid;
        std::vector<void*> stackPtrs;
        bool crashOrigin;
    };

    static SimpleLoggerMgr* init();
    static SimpleLoggerMgr* get();
    static SimpleLoggerMgr* getWithoutInit();
    static void destroy();
    static int getTzGap();
    static void handleSegFault(int sig);
    static void handleSegAbort(int sig);
#if defined(__linux__) || defined(__APPLE__)
    static void handleStackTrace(int sig, siginfo_t* info, void* secret);
#endif
    static void flushWorker();
    static void compressWorker();

    void logStackBacktrace(size_t timeout_ms = 60*1000);
    void flushCriticalInfo();
    void enableOnlyOneDisplayer();
    void flushAllLoggers() { flushAllLoggers(0, std::string()); }
    void flushAllLoggers(int level,
                         const std::string& msg,
                         bool skip_clean_loggers = false);
    void addLogger(SimpleLoggerImpl* logger);
    void removeLogger(SimpleLoggerImpl* logger);
    void addThread(uint64_t tid);
    void removeThread(uint64_t tid);
    void addCompElem(SimpleLoggerMgr::CompElem* elem);
    void sleepFlusher(size_t ms);
    void sleepCompressor(size_t ms);
    bool chkTermination() const;
    void setCriticalInfo(const std::string& info_str);
    void setCrashDumpPath(const std::string& path,
                          bool origin_only);
    void setStackTraceOriginOnly(bool origin_only);

    /**
     * Set the flag regarding exiting on crash.
     * If flag is `true`, custom segfault handler will not invoke
     * original handler so that process will terminate without
     * generating core dump.
     * The flag is `false` by default.
     *
     * @param exit_on_crash New flag value.
     * @return void.
     */
    void setExitOnCrash(bool exit_on_crash);

    const std::string& getCriticalInfo() const;

    static std::mutex display_lock;

private:
    // Copy is not allowed.
    SimpleLoggerMgr(const SimpleLoggerMgr&) = delete;
    SimpleLoggerMgr& operator=(const SimpleLoggerMgr&) = delete;

    static const size_t STACK_TRACE_BUFFER_SIZE = 65536;

    // Singleton instance and lock.
    static std::atomic<SimpleLoggerMgr*> instance;
    static std::mutex instance_lock;

    SimpleLoggerMgr();
    ~SimpleLoggerMgr();

    void flushStackTraceBufferInternal(size_t buffer_len,
                                       uint32_t tid_hash,
                                       uint64_t kernel_tid,
                                       bool crash_origin);
    void flushStackTraceBuffer(RawStackInfo& stack_info);
    void flushRawStack(RawStackInfo& stack_info);
    void addRawStackInfo(bool crash_origin = false);
    void logStackBackTraceOtherThreads();

    bool chkExitOnCrash();

    // Must be called under `loggersLock`.
    void mergePendingLoggers();

    std::mutex loggersLock;
    std::unordered_set<SimpleLoggerImpl*> loggers;

    // Loggers added by `addLogger()` but not yet merged into `loggers`,
    // so that `addLogger()` does not wait for an ongoing flush pass.
    // Lock order: `loggersLock` -> `pendingLoggersLock`.
    std::mutex pendingLoggersLock;
    std::vector<SimpleLoggerImpl*> pendingLoggers;

    std::mutex activeThreadsLock;
    std::unordered_set<uint64_t> activeThreads;

    // Periodic log flushing thread.
    std::thread tFlush;

    // Old log file compression thread.
    std::thread tCompress;

    // List of files to be compressed.
    std::list<CompElem*> pendingCompElems;

    // Lock for `pendingCompFiles`.
    std::mutex pendingCompElemsLock;

    // Condition variable for BG flusher.
    std::condition_variable cvFlusher;
    std::mutex cvFlusherLock;

    // Condition variable for BG compressor.
    std::condition_variable cvCompressor;
    std::mutex cvCompressorLock;

    // Termination signal.
    std::atomic<bool> termination;

    // Original segfault handler.
    void (*oldSigSegvHandler)(int);

    // Original abort handler.
    void (*oldSigAbortHandler)(int);

    // Critical info that will be displayed on crash.
    std::string globalCriticalInfo;

    // Reserve some buffer for stack trace.
    char* stackTraceBuffer;

    // TID of thread where crash happens.
    std::atomic<uint64_t> crashOriginThread;

    std::string crashDumpPath;
    std::ofstream crashDumpFile;

    // If `true`, generate stack trace only for the origin thread.
    // Default: `true`.
    bool crashDumpOriginOnly;

    // If `true`, do not invoke original segfault handler
    // so that process just terminates.
    // Default: `false`.
    bool exitOnCrash;

    std::atomic<uint64_t> abortTimer;

    // Assume that only one thread is updating this.
    std::vector<RawStackInfo> crashDumpThreadStacks;
};

class SimpleLoggerLocalCtx {
public:
    SimpleLoggerLocalCtx(SimpleLogger* l,
                         const SimpleLogger::CtxMap& ctx,
                         SimpleLogger::PreCbType pre_cb = nullptr,
                         SimpleLogger::CbType cb = nullptr);

    ~SimpleLoggerLocalCtx();

private:
    SimpleLogger* logger;
    SimpleLogger::CbType callback;
    SimpleLogger::PreCbType preCallback;
    SimpleLogger::CtxMap localCtx;
};
