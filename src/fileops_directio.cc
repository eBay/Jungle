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

#include "fileops_directio.h"

#include <cerrno>
#include <cstdlib>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace jungle {

#define MAX_TRIES (100)
#define MAX_SLEEP_US (500000) // 500 ms

class FileHandleDirectIO : public FileHandle {
public:
    FileHandleDirectIO(std::string _pathname,
                       int _fd,
                       FileOps* _ops,
                       void* _alignedBuf)
        : pathname(std::move(_pathname))
        , fd(_fd)
        , filePos(0)
        , flushedFilePos(0)
        , fsyncedFilePos(0)
        , bufPtr(_alignedBuf)
        , bufPos(0)
    {
        fOps = _ops;
        bufCharPtr = static_cast<uint8_t*>(bufPtr);
    }

    ~FileHandleDirectIO() override {
        if (isOpened()) {
            ::close(fd);
            filePos = 0;
            flushedFilePos = 0;
            fsyncedFilePos = 0;
        }
        if (bufPtr) {
            free(bufPtr);
            bufPtr = nullptr;
            bufCharPtr = nullptr;
            bufPos = 0;
        }
    }

    bool isOpened() override { return (fd > 0); }

    std::string pathname;
    int fd;
    size_t filePos;
    size_t flushedFilePos;
    size_t fsyncedFilePos;
    void* bufPtr;
    uint8_t* bufCharPtr;
    size_t bufPos;
};

struct AlignedMemoryHolder {
    explicit AlignedMemoryHolder(void* _bufPtr) : bufPtr(_bufPtr) {}
    ~AlignedMemoryHolder() {
        if (bufPtr) {
            free(bufPtr);
            bufPtr = nullptr;
        }
    }
    void* bufPtr;
};

static FileHandleDirectIO* getHandle(FileHandle* fhandle) {
    return dynamic_cast<FileHandleDirectIO*>(fhandle);
}

FileOpsDirectIO::FileOpsDirectIO(SimpleLogger* log) : myLog(log) {}
FileOpsDirectIO::~FileOpsDirectIO() {}

Status FileOpsDirectIO::allocAlignedBuf(void** aligned_buf) {
    int mr = posix_memalign(aligned_buf,
                            ALIGNMENT,
                            ALIGNED_BUFFER_SIZE + ALIGNMENT);
    if (mr) {
        _log_err(myLog,
                 "failed to allocate aligned buffer with error code %d, "
                 "aligned size %d, buffer size %d",
                 mr, ALIGNMENT, ALIGNED_BUFFER_SIZE);
        return Status::DIRECT_IO_NOT_SUPPORTED;
    }
    // One ALIGNMENT more for padding
    memset(*aligned_buf, 0x0, ALIGNED_BUFFER_SIZE + ALIGNMENT);
    return Status::OK;
}

Status FileOpsDirectIO::readInternal(const std::string &pathname,
                                     int fd,
                                     void* buf,
                                     size_t nbyte,
                                     off_t offset)
{
    size_t num_tries = 0;
    size_t sleep_time_us = 1;
    ssize_t r = -1;
    size_t read_size = nbyte;
    size_t need_align = nbyte % ALIGNMENT;
    if (need_align > 0) {
        // This is the case that reading the tail of the file with un-aligned size
        // Extend the read size even it may be larger than file size
        // The size of bytes returned should be exactly equal to nbyte
        read_size += (ALIGNMENT - need_align);
    }

    do {
        r = ::pread(fd, buf, read_size, offset);
        if (r == (ssize_t) nbyte) break;
        int n = errno;
        num_tries++;
        _log_err(myLog,
                 "failed to read log file %s, offset %ld, "
                 "bytes %zu, errno %d, msg %s, retrying %d...",
                 pathname.c_str(), offset,
                 nbyte, n, strerror(n), num_tries);
        Timer::sleepUs(sleep_time_us);
        sleep_time_us = std::min(sleep_time_us * 2, (size_t) MAX_SLEEP_US);
    } while (r != (ssize_t) nbyte && ++num_tries < MAX_TRIES);
    if (r != (ssize_t) nbyte) {
        _log_err(myLog,
                 "failed to read log file %s after %d retries, "
                 "offset %ld, bytes %zu",
                 pathname.c_str(), num_tries, offset, nbyte);
        return Status::FILE_READ_SIZE_MISMATCH;
    } else if (num_tries > 0) {
        _log_warn(myLog,
                  "read log file %s succeed after %d retries, "
                  "offset %ld, bytes %zu",
                  pathname.c_str(), num_tries, offset, nbyte);
    }
    return Status::OK;
}

Status FileOpsDirectIO::open(FileHandle** fhandle_out,
                             const std::string &pathname,
                             FileOps::Mode mode)
{
    if (!supportDirectIO()) return Status::DIRECT_IO_NOT_SUPPORTED;

    int flags = getFlags(mode);
#ifdef __linux__
    flags |= O_DIRECT;
    _log_debug(myLog,
               "open log file %s with flags O_CREAT|O_RDWR|O_DIRECT",
               pathname.c_str());
#endif

    int r = ::open(pathname.c_str(), flags, 0644);
    if (r <= 0) {
        int n = errno;
        _log_err(myLog,
                 "failed to open log file %s with errno %d, msg %s",
                 pathname.c_str(), n, strerror(n));
        return Status::ERROR;
    }

#ifdef __APPLE__
    int nr = fcntl(r, F_NOCACHE, 1);
    if (-1 == nr) {
        int n = errno;
        _log_err(myLog,
                 "failed to set log file %s F_NOCACHE with errno %d, msg %s",
                 pathname.c_str(), n, strerror(n));
        return Status::DIRECT_IO_NOT_SUPPORTED;
    }
    _log_debug(myLog,
               "open log file %s with flags O_CREAT|O_RDWR|F_NOCACHE",
               pathname.c_str());
#endif

    void* aligned_buf = nullptr;

    Status s;
    EP(allocAlignedBuf(&aligned_buf));

    auto* fhandle = new FileHandleDirectIO(pathname, r, this, aligned_buf);

    // Set the aligned write buffer
    cs_off_t file_size_tmp = eof(fhandle);
    if (file_size_tmp < 0) return Status::ERROR;
    auto file_size = static_cast<size_t>(file_size_tmp);

    size_t need_align = file_size % ALIGNMENT;
    if (need_align == 0) {
        fhandle->filePos = file_size;
        fhandle->flushedFilePos = file_size;
        fhandle->fsyncedFilePos = file_size;
        fhandle->bufPos = 0;
    } else {
        // Read the last un-aligned data
        EP(readInternal(pathname,
                        r,
                        aligned_buf,
                        need_align,
                        file_size - need_align));
        fhandle->filePos = file_size - need_align;
        fhandle->flushedFilePos = file_size;
        fhandle->fsyncedFilePos = file_size;
        fhandle->bufPos = need_align;
    }

    _log_debug(myLog,
               "log file %s is opened, size %zu, file_pos %zu, buf_pos %zu",
               pathname.c_str(), file_size, fhandle->filePos, fhandle->bufPos);

    *fhandle_out = fhandle;
    return Status();
}

Status FileOpsDirectIO::close(FileHandle* fhandle) {
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;

    Status s = flush(fhandle);
    if (!s) {
        _log_err(myLog,
                 "failed to close log file %s due to flush failure %d",
                 phandle->pathname.c_str(), s);
        return s;
    }

    cs_off_t file_size = eof(fhandle);

    int r = ::close(phandle->fd);
    if (r) {
        int n = errno;
        _log_err(myLog,
                 "failed to close log file %s size %ld with errno %d, msg %s",
                 phandle->pathname.c_str(), file_size, n, strerror(n));
        return Status::ERROR;
    }

    phandle->fd = -1;

    _log_debug(myLog,
               "log file %s is closed, file_pos %zu, "
               "buf_pos %zu, file_size %ld",
               phandle->pathname.c_str(), phandle->filePos,
               phandle->bufPos, file_size);
    return Status();
}

Status FileOpsDirectIO::pread(FileHandle* fhandle,
                              void* buf,
                              size_t count,
                              cs_off_t _offset)
{
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (count == 0) return Status::OK;

    if (!buf || _offset < 0) return Status::INVALID_PARAMETERS;
    auto offset = static_cast<size_t>(_offset);

    cs_off_t file_size_tmp = eof(fhandle);
    if (file_size_tmp < 0) return Status::ERROR;
    auto file_size = static_cast<size_t>(file_size_tmp);
    if (offset + count > file_size) return Status::FILE_SIZE_MISMATCH;

    // Aligned buffer to direct read
    thread_local void* aligned_buf = nullptr;
    thread_local Status aligned_buf_status = allocAlignedBuf(&aligned_buf);
    thread_local auto* aligned_buf_char = static_cast<uint8_t*>(aligned_buf);
    if (!aligned_buf_status) return aligned_buf_status;
    assert(aligned_buf != nullptr && aligned_buf_char != nullptr);
    thread_local AlignedMemoryHolder buf_holder(aligned_buf);
    (void) buf_holder;

    Status s;
    size_t aligned_file_offset = offset;
    size_t need_align = offset % ALIGNMENT;
    if (need_align > 0) {
        aligned_file_offset -= need_align;
    }

    auto* buf_char = static_cast<uint8_t*>(buf);
    while (aligned_file_offset < offset + count) {
        size_t aligned_slice;
        {
            size_t slice = std::min((size_t) (offset + count - aligned_file_offset),
                                    (size_t) ALIGNED_BUFFER_SIZE);
            aligned_slice = slice;
            need_align = slice % ALIGNMENT;
            if (need_align > 0) {
                aligned_slice = slice + (ALIGNMENT - need_align);
            }
            assert(aligned_slice > 0 && aligned_slice <= ALIGNED_BUFFER_SIZE);
            // The tail of file is allowed to be un-aligned size
            if (aligned_file_offset + aligned_slice > file_size) {
                aligned_slice = file_size - aligned_file_offset;
            }
        }

        s = readInternal(phandle->pathname,
                         phandle->fd,
                         aligned_buf,
                         aligned_slice,
                         aligned_file_offset);
        if (!s) break;

        memcpy(buf_char + (aligned_file_offset > offset
                           ? (aligned_file_offset - offset)
                           : 0),
               aligned_buf_char + (offset > aligned_file_offset
                                   ? (offset - aligned_file_offset)
                                   : 0),
               std::min(offset + count, aligned_file_offset + aligned_slice)
                   - std::max(offset, aligned_file_offset));
        aligned_file_offset += aligned_slice;
    }

    return s;
}

Status FileOpsDirectIO::pwrite(FileHandle* fhandle,
                               const void* buf,
                               size_t count,
                               cs_off_t offset)
{
    return Status::NOT_IMPLEMENTED;
}

Status FileOpsDirectIO::append(FileHandle* fhandle,
                               const void* buf,
                               size_t count)
{
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (count == 0) return Status::OK;

    size_t sleep_time_us = 1;
    ssize_t r = -1;

    size_t source_pos = 0;

    const auto* buf_char = static_cast<const uint8_t*>(buf);
    while (source_pos < count) {
        size_t write_buf_size = ALIGNED_BUFFER_SIZE - phandle->bufPos;
        size_t left_source_size = count - source_pos;
        size_t slice = std::min(left_source_size, write_buf_size);

        assert(slice > 0 && slice <= ALIGNED_BUFFER_SIZE);
        memcpy(phandle->bufCharPtr + phandle->bufPos,
               buf_char + source_pos,
               slice);

        phandle->bufPos += slice;
        source_pos += slice;

        // Flush, once write buffer is full
        if (ALIGNED_BUFFER_SIZE <= phandle->bufPos) {
            size_t num_tries = 0;
            do {
                r = ::pwrite(phandle->fd,
                             phandle->bufPtr,
                             ALIGNED_BUFFER_SIZE,
                             phandle->filePos);
                if (r == (ssize_t) ALIGNED_BUFFER_SIZE) break;
                int n = errno;
                num_tries++;
                _log_err(myLog,
                         "failed to write log file %s, offset %zu, "
                         "bytes %d, errno %d, msg %s, retrying %d...",
                         phandle->pathname.c_str(), phandle->filePos,
                         ALIGNED_BUFFER_SIZE, n, strerror(n), num_tries);
                Timer::sleepUs(sleep_time_us);
                sleep_time_us = std::min(sleep_time_us * 2, (size_t) MAX_SLEEP_US);
            } while (r != (ssize_t) ALIGNED_BUFFER_SIZE && num_tries < MAX_TRIES);
            if (r != (ssize_t) ALIGNED_BUFFER_SIZE) {
                _log_err(myLog,
                         "failed to write log file %s after %d retries, "
                         "offset %zu, bytes %d",
                         phandle->pathname.c_str(), num_tries,
                         phandle->filePos, ALIGNED_BUFFER_SIZE);
                return Status::FILE_WRITE_SIZE_MISMATCH;
            } else if (num_tries > 0) {
                _log_warn(myLog,
                          "Write log file %s succeed after %d retries, "
                          "offset %zu, bytes %d",
                          phandle->pathname.c_str(), num_tries,
                          phandle->filePos, ALIGNED_BUFFER_SIZE);
            }
            phandle->bufPos = 0;
            phandle->filePos += ALIGNED_BUFFER_SIZE;
            if (phandle->filePos > phandle->flushedFilePos) {
                phandle->flushedFilePos = phandle->filePos;
            }
        }
    }
    return Status();
}

cs_off_t FileOpsDirectIO::eof(FileHandle* fhandle) {
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return -1;
    if (phandle->fd <= 0) return -1;
    int r = ::lseek(phandle->fd, 0, SEEK_END);
    if (r < 0) {
        int n = errno;
        _log_err(myLog,
                 "failed to get size of log file %s with errno %d, msg %s",
                 phandle->pathname.c_str(), n, strerror(n));
    }
    return r;
}

Status FileOpsDirectIO::flush(FileHandle* fhandle) {
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (0 == phandle->bufPos) return Status();
    // Avoid duplicate flush
    if (phandle->flushedFilePos >= phandle->filePos + phandle->bufPos) {
        return Status();
    }

    size_t need_align = phandle->bufPos % ALIGNMENT;
    size_t aligned_pos = phandle->bufPos;
    if (need_align > 0) {
        size_t padding = ALIGNMENT - need_align;
        if (padding < 8) {
            // Enough room for padding flags
            padding += ALIGNMENT;
        }
        aligned_pos = phandle->bufPos + padding;

        // Fill zero
        memset(phandle->bufCharPtr + phandle->bufPos, 0x0, padding);
        // Set flag to indicate padding
        uint64_t offset = 0;
        append_mem_64(phandle->bufCharPtr + phandle->bufPos,
                      PADDING_HEADER_FLAG,
                      0,
                      offset);
    }
    assert(aligned_pos > 0 && aligned_pos <= ALIGNED_BUFFER_SIZE);

    size_t num_tries = 0;
    size_t sleep_time_us = 1;
    ssize_t r = -1;
    do {
        r = ::pwrite(phandle->fd,
                     phandle->bufPtr,
                     aligned_pos,
                     phandle->filePos);
        if (r == (ssize_t) aligned_pos) break;
        int n = errno;
        num_tries++;
        _log_err(myLog,
                 "failed to write log file %s, offset %zu, "
                 "bytes %zu, errno %d, msg %s, retrying %d...",
                 phandle->pathname.c_str(), phandle->filePos,
                 aligned_pos, n, strerror(n), num_tries);
        Timer::sleepUs(sleep_time_us);
        sleep_time_us = std::min(sleep_time_us * 2, (size_t) MAX_SLEEP_US);
    } while (r != (ssize_t) aligned_pos && num_tries < MAX_TRIES);
    if (r != (ssize_t) aligned_pos) {
        _log_err(myLog,
                 "failed to write log file %s after %d retries, "
                 "offset %zu, bytes %zu",
                 phandle->pathname.c_str(), num_tries,
                 phandle->filePos, aligned_pos);
        return Status::FILE_WRITE_SIZE_MISMATCH;
    } else if (num_tries > 0) {
        _log_warn(myLog,
                  "write log file %s succeed after %d retries, "
                  "offset %zu, bytes %zu",
                  phandle->pathname.c_str(), num_tries,
                  phandle->filePos, aligned_pos);
    }

    phandle->flushedFilePos = phandle->filePos + phandle->bufPos;
    // Don't need to change buf&file position
    // Wait for next writes to make buffer eventually alignment
    return Status();
}

Status FileOpsDirectIO::fsync(FileHandle* fhandle) {
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    // Avoid duplicate fsync
    if (phandle->fsyncedFilePos >= phandle->flushedFilePos) {
        return Status();
    }

    int r = ::fsync(phandle->fd);
    if (r) {
        int n = errno;
        _log_err(myLog,
                 "failed to fsync log file %s with errno %d, msg %s",
                 phandle->pathname.c_str(), n, strerror(n));
        return Status::ERROR;
    }
    phandle->fsyncedFilePos = phandle->flushedFilePos;
    return Status();
}

Status FileOpsDirectIO::ftruncate(FileHandle* fhandle,
                                  cs_off_t _length)
{
    FileHandleDirectIO* phandle = getHandle(fhandle);
    if (!phandle) return Status::NULL_FILEOPS_HANDLE;
    if (phandle->fd <= 0) return Status::INVALID_FILE_DESCRIPTOR;
    if (_length < 0) return Status::INVALID_PARAMETERS;
    auto length = static_cast<size_t>(_length);

    // Memtable will be synced before truncate,
    // so it's safe not to handle pending data in phandle->bufPtr.
    cs_off_t file_size_tmp = eof(fhandle);
    if (file_size_tmp < 0) return Status::ERROR;
    auto file_size = static_cast<size_t>(file_size_tmp);
    if (length >= file_size) return Status::OK;

    size_t aligned_length = length;
    size_t need_align = length % ALIGNMENT;
    if (need_align > 0) {
        aligned_length = length + (ALIGNMENT - need_align);
    }

    int r = ::ftruncate(phandle->fd, length);
    if (r) {
        int n = errno;
        _log_err(myLog,
                 "failed to truncate log file %s with errno %d, msg %s",
                 phandle->pathname.c_str(), n, strerror(n));
        return Status::ERROR;
    }

    // Set the aligned write buffer
    if (aligned_length == length) {
        phandle->filePos = aligned_length;
        phandle->flushedFilePos = aligned_length;
        phandle->bufPos = 0;
    } else {
        // Read the last un-aligned data
        Status s;
        EP(readInternal(phandle->pathname,
                        phandle->fd,
                        phandle->bufPtr,
                        need_align,
                        aligned_length - ALIGNMENT));
        phandle->filePos = aligned_length - ALIGNMENT;
        phandle->flushedFilePos = length;
        phandle->bufPos = need_align;
    }

    // Double check file size
    file_size_tmp = eof(fhandle);
    if (file_size_tmp < 0) return Status::ERROR;
    file_size = static_cast<size_t>(file_size_tmp);
    if (file_size != length) {
        _log_err(myLog,
                 "failed to truncate log file %s as "
                 "file size %zu is not equal to expected %zu",
                 phandle->pathname.c_str(), file_size, length);
        return Status::ERROR;
    }

    _log_debug(myLog,
               "log file %s is truncated, file_pos %zu, "
               "buf_pos %zu, file_size %zu",
               phandle->pathname.c_str(), phandle->filePos,
               phandle->bufPos, file_size);

    return Status();
}

Status FileOpsDirectIO::mkdir(const std::string &path) {
    int r = ::mkdir(path.c_str(), 0755);
    if (r < 0) return Status::ERROR;

    return Status();
}

Status FileOpsDirectIO::remove(const std::string &path) {
    int r = ::remove(path.c_str());
    if (r < 0) return Status::ERROR;

    return Status();
}

bool FileOpsDirectIO::exist(const std::string &path) {
    struct stat st;
    int result = stat(path.c_str(), &st);
    return (result == 0);
}

} // namespace jungle
