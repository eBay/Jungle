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

#include "logger.h"

#include <stdarg.h>

// Internal implementation of `SimpleLogger`.
// This header is not exposed, so its layout can be changed freely.
class SimpleLoggerImpl {
    friend class SimpleLoggerMgr;
public:
    using CtxMap = SimpleLogger::CtxMap;
    using PreCbType = SimpleLogger::PreCbType;
    using CbType = SimpleLogger::CbType;

    static const int MSG_SIZE = SimpleLogger::MSG_SIZE;
    static const std::memory_order MOR = SimpleLogger::MOR;

    SimpleLoggerImpl(const std::string& file_path,
                     size_t max_log_elems,
                     uint64_t log_file_size_limit,
                     uint32_t max_log_files);
    ~SimpleLoggerImpl();

    int start();
    int stop();

    void setLogLevel(int level);
    void setDispLevel(int level);
    void setMaxLogFiles(size_t max_log_files);

    inline int getLogLevel()  const { return curLogLevel.load(MOR); }
    inline int getDispLevel() const { return curDispLevel.load(MOR); }

    void put(int level,
             const char* source_file,
             const char* func_name,
             size_t line_number,
             const char* format,
             ...);
    void putV(int level,
              const char* source_file,
              const char* func_name,
              size_t line_number,
              const char* format,
              va_list args);

    // If `skip_if_clean` is true, skip the ring buffer scan when no
    // log has been written since the last flush.
    void flushAll(bool skip_if_clean = false);

    void setGlobalCallback(PreCbType pre_cb, CbType cb);

    bool checkWhetherToLog(int level,
                           const char* source_file,
                           const char* func_name,
                           size_t line_number);

    // Thread-local contexts and callbacks set by `SimpleLoggerLocalCtx`.
    static CtxMap& getLocalCtxMap();
    static std::list<CbType>& getLocalCbList();
    static std::list<PreCbType>& getLocalPreCbList();

private:
    struct LogElem {
        enum Status {
            CLEAN       = 0,
            WRITING     = 1,
            DIRTY       = 2,
            FLUSHING    = 3,
        };

        LogElem();

        // True if dirty.
        bool needToFlush();

        int write(size_t len, char* msg);
        int flush(std::ofstream& fs);

        size_t len;
        char ctx[MSG_SIZE];
        std::atomic<Status> status;
    };

    void findMinMaxRevNum(size_t& min_revnum_out,
                          size_t& max_revnum_out);
    void findMinMaxRevNumInternal(bool& min_revnum_initialized,
                                  size_t& min_revnum,
                                  size_t& max_revnum,
                                  std::string& f_name);
    std::string getLogFilePath(size_t file_num) const;
    void execCmd(const std::string& cmd);
    void doCompression(size_t file_num);
    bool flush(size_t start_pos);

    std::string filePath;
    size_t minRevnum;
    size_t curRevnum;
    std::atomic<size_t> maxLogFiles;
    std::ofstream fs;

    uint64_t maxLogFileSize;
    std::atomic<uint32_t> numCompJobs;

    // Log up to `curLogLevel`, default: 6.
    // Disable: -1.
    std::atomic<int> curLogLevel;

    // Display (print out on terminal) up to `curDispLevel`,
    // default: 4 (do not print debug and trace).
    // Disable: -1.
    std::atomic<int> curDispLevel;

    std::mutex displayLock;

    int tzGap;
    std::atomic<uint64_t> cursor;
    std::vector<LogElem> logs;
    std::mutex flushingLogs;

    // Set (after publishing DIRTY) by writers, and cleared by flushers
    // after acquiring `flushingLogs` and before scanning `logs`.
    // False positives only cause a redundant scan, but false negatives
    // are not allowed.
    std::atomic<bool> hasDirtyLogs;

    PreCbType globalPreCallback;
    CbType globalCallback;
};
