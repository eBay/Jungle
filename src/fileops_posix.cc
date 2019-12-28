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

#include "fileops_posix.h"

#include "internal_helper.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace jungle {

#define MAX_TRIES (100)
#define MAX_SLEEP_US (500000) // 500 ms

class FileHandlePosix : public FileHandle {
public:
    FileHandlePosix(int _fd, FileOps* _ops)
        : fd(_fd) {
        fOps = _ops;
    }

    ~FileHandlePosix() {
        if (isOpened()) {
            ::close(fd);
        }
    }

    bool isOpened() { return (fd > 0); }

    int fd;
};


static FileHandlePosix* getHandle(FileHandle* fhandle) {
    return static_cast<FileHandlePosix*>(fhandle);
}

FileOpsPosix::FileOpsPosix() { }
FileOpsPosix::~FileOpsPosix() { }

Status FileOpsPosix::open(FileHandle** fhandle_out,
                          const std::string& pathname,
                          FileOps::Mode mode)
{
    int flags = getFlags(mode);

    int r = ::open(pathname.c_str(), flags, 0644);
    if (r <= 0) return Status::ERROR;

    FileHandlePosix* fhandle = new FileHandlePosix(r, this);
    *fhandle_out = fhandle;

    return Status();
}

Status FileOpsPosix::close(FileHandle* fhandle) {
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;

    int r = ::close(phandle->fd);
    if (r < 0) return Status::ERROR;

    phandle->fd = -1;
    return Status();
}

Status FileOpsPosix::pread(FileHandle* fhandle,
                           void *buf,
                           size_t count,
                           cs_off_t offset)
{
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (count == 0) return Status::OK;

    if (!buf) return Status::INVALID_PARAMETERS;

    size_t num_tries = 0;
    size_t sleep_time_us = 1;
    ssize_t r = -1;
    do {
        r = ::pread(phandle->fd, buf, count, offset);
        if (r == (ssize_t)count) break;

        Timer::sleepUs(sleep_time_us);
        sleep_time_us = std::min(sleep_time_us * 2, (size_t)MAX_SLEEP_US);
    } while (r != (ssize_t)count && ++num_tries < MAX_TRIES);
    if (r != (ssize_t)count) return Status::FILE_READ_SIZE_MISMATCH;

    return Status();
}

Status FileOpsPosix::pwrite(FileHandle* fhandle,
                            const void *buf,
                            size_t count,
                            cs_off_t offset)
{
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (count == 0) return Status::OK;


    size_t num_tries = 0;
    size_t sleep_time_us = 1;
    ssize_t r = -1;
    do {
        r = ::pwrite(phandle->fd, buf, count, offset);
        if (r == (ssize_t)count) break;

        Timer::sleepUs(sleep_time_us);
        sleep_time_us = std::min(sleep_time_us * 2, (size_t)MAX_SLEEP_US);
    } while (r != (ssize_t)count && ++num_tries < MAX_TRIES);
    if (r != (ssize_t)count) return Status::FILE_WRITE_SIZE_MISMATCH;

    return Status();
}

Status FileOpsPosix::append(FileHandle* fhandle,
                            const void* buf,
                            size_t count)
{
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (count == 0) return Status::OK;

    size_t num_tries = 0;
    size_t sleep_time_us = 1;
    ssize_t r = -1;
    off_t offset = ::lseek(phandle->fd, 0, SEEK_END);
    do {
        r = ::pwrite(phandle->fd, buf, count, offset);
        if (r == (ssize_t)count) break;

        Timer::sleepUs(sleep_time_us);
        sleep_time_us = std::min(sleep_time_us * 2, (size_t)MAX_SLEEP_US);
    } while (r != (ssize_t)count && ++num_tries < MAX_TRIES);
    if (r != (ssize_t)count) return Status::FILE_WRITE_SIZE_MISMATCH;

    return Status();
}

cs_off_t FileOpsPosix::eof(FileHandle* fhandle) {
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return -1;
    if (phandle->fd <= 0) return -1;

    return ::lseek(phandle->fd, 0, SEEK_END);
}

Status FileOpsPosix::flush(FileHandle* fhandle) {
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    return Status();
}

Status FileOpsPosix::fsync(FileHandle* fhandle) {
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;

    int r = ::fsync(phandle->fd);
    if (r < 0) return Status::ERROR;

    return Status();
}

Status FileOpsPosix::ftruncate(FileHandle* fhandle,
                               cs_off_t length)
{
    FileHandlePosix* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;

    int r = ::ftruncate(phandle->fd, length);
    if (r < 0) return Status::ERROR;

    return Status();
}

Status FileOpsPosix::mkdir(const std::string& path) {
    int r = ::mkdir(path.c_str(), 0755);
    if (r < 0) return Status::ERROR;

    return Status();
}

Status FileOpsPosix::remove(const std::string& path) {
    int r = ::remove(path.c_str());
    if (r < 0) return Status::ERROR;

    return Status();
}

bool FileOpsPosix::exist(const std::string& path) {
    struct stat st;
    int result = stat(path.c_str(), &st);
    return (result == 0);
}


} // namespace jungle
