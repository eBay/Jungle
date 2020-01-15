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

#include "fileops_base.h"
#include "internal_helper.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

class FileOpsDirectIO : public FileOps {
public:
    FileOpsDirectIO(SimpleLogger* log,
                    size_t buffer_size = 16384,
                    size_t align_size = 512);
    ~FileOpsDirectIO();

    Status open(FileHandle** fhandle_out,
                const std::string& pathname,
                FileOps::Mode mode);

    Status close(FileHandle* fhandle);

    Status pread(FileHandle* fhandle,
                 void* buf,
                 size_t count,
                 cs_off_t offset);

    Status pwrite(FileHandle* fhandle,
                  const void* buf,
                  size_t count,
                  cs_off_t offset);

    Status append(FileHandle* fhandle,
                  const void* buf,
                  size_t count);

    cs_off_t eof(FileHandle* fhandle);

    // Flush internal aligned write buffer
    Status flush(FileHandle* fhandle);

    Status fsync(FileHandle* fhandle);

    Status ftruncate(FileHandle* fhandle,
                     cs_off_t length);

    Status mkdir(const std::string& path);

    bool exist(const std::string& path);

    Status remove(const std::string& path);
private:
    Status allocAlignedBuf(void** aligned_buf);
    Status readInternal(const std::string &pathname,
                        int fd,
                        void* buf,
                        size_t nbyte,
                        off_t offset);

    SimpleLogger* myLog;

    size_t mBufferSize;
    size_t mAlignSize;
};

} // namespace jungle
