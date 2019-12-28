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

#include <libjungle/jungle.h>

#include <string>

#include <stdint.h>
#include <stddef.h>
#include <fcntl.h>

namespace jungle {

using cs_off_t = int64_t;

class FileOps;
class FileHandle {
public:
    FileHandle() : fOps(nullptr) { }
    virtual ~FileHandle() { }

    FileOps* ops() const { return fOps; }

    virtual bool isOpened() { return false; }

protected:
    FileOps* fOps;
};

class FileOps {
public:
    virtual ~FileOps() { }

    enum Mode {
        NORMAL = 0x0,
        READ_ONLY = 0x1,
    };

    static int getFlags(FileOps::Mode mode) {
        int flags = 0;
        if (mode == FileOps::NORMAL) {
            flags = O_CREAT | O_RDWR;
        } else {
            flags = O_RDONLY;
        }

        return flags;
    }

    static bool supportDirectIO() {
        // Can't define both __APPLE__ and __linux__
#if defined(__APPLE__) && defined(__linux__)
        return false;
#endif

#ifdef __APPLE__
    #ifdef F_NOCACHE
        return true;
    #else
        return false;
    #endif
#elif defined(__linux__)
    #ifndef _GNU_SOURCE
        #define _GNU_SOURCE
    #endif

    #ifdef O_DIRECT
        return true;
    #else
        return false;
    #endif
#else
        return false;
#endif
    }

    Status open(FileHandle** fhandle_out,
                const std::string& pathname) {
        return open(fhandle_out, pathname, NORMAL);
    }

    virtual Status open(FileHandle** fhandle_out,
                        const std::string& pathname,
                        FileOps::Mode mode) = 0;

    virtual Status close(FileHandle* fhandle) = 0;

    virtual Status pread(FileHandle* fhandle,
                         void* buf,
                         size_t count,
                         cs_off_t offset) = 0;

    virtual Status pwrite(FileHandle* fhandle,
                          const void *buf,
                          size_t count,
                          cs_off_t offset) = 0;

    virtual Status append(FileHandle* fhandle,
                          const void* buf,
                          size_t count) = 0;

    virtual cs_off_t eof(FileHandle* fhandle) = 0;

    virtual Status flush(FileHandle* fhandle) = 0;

    virtual Status fsync(FileHandle* fhandle) = 0;

    virtual Status ftruncate(FileHandle* fhandle,
                             cs_off_t length) = 0;

    virtual Status mkdir(const std::string& path) = 0;

    virtual bool exist(const std::string& path) = 0;

    virtual Status remove(const std::string& path) = 0;
};

} // namespace jungle

