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

#include "bench_config.h"
#include "json.hpp"

#include <list>
#include <string>

#include "string.h"

namespace jungle_bench {

class DbAdapter {
public:
    struct Buffer {
        Buffer() : data(nullptr), size(0) {}
        Buffer(void* ptr, size_t len) : data((uint8_t*)ptr), size(len) {}
        Buffer(const std::string& s) : data((uint8_t*)s.data()), size(s.size()) {}
        void alloc(size_t len) {
            data = (uint8_t*)malloc(len);
        }
        void free() {
            ::free(data);
        }
        uint8_t* data;
        size_t size;
    };

    struct KvElem {
        KvElem() {}
        Buffer key;
        Buffer value;
    };

    struct BatchOptions {
        BatchOptions() : sync(false) {}

        // If true, given batch should be durable on disk
        // before returning `setBatch()` API.
        bool sync;
    };

    DbAdapter() {}

    virtual ~DbAdapter() {}

    // Return a string that can identify the type and version of DB.
    virtual std::string getName() = 0;

    // Open a DB instance.
    virtual int open(const std::string& db_file,
                     const BenchConfig& bench_config,
                     json::JSON db_config) = 0;

    // Close a DB instance.
    virtual int close() = 0;

    // Shutdown process-wide engine stuffs (if necessary).
    virtual int shutdown() = 0;

    // Start bulk load mode (if necessary).
    virtual int startInitialLoad() = 0;

    // Finish bulk load mode (if necessary).
    virtual int endInitialLoad() = 0;

    // Single set.
    virtual int set(const KvElem& elem) = 0;

    // Batch set.
    virtual int setBatch(const std::list<KvElem>& batch,
                         const BatchOptions& opt) = 0;

    // Point query.
    // Underlying database should allocate a new memory
    // for returned `value_out`, and benchmark program
    // is responsible for freeing it.
    virtual int get(const Buffer& key,
                    Buffer& value_out) = 0;

    virtual std::string get(const std::string& key) {
        Buffer value_out;
        int ret = get(Buffer(key), value_out);
        if (ret != 0) return std::string();

        std::string ret_str((const char*)value_out.data, value_out.size);
        value_out.free();
        return ret_str;
    }

    // Range query returns KV pairs within [start, end].
    // Underlying database should allocate new memory blobs
    // for returned `kvs_out`, and benchmark program
    // is responsible for freeing them.
    virtual int getRange(const Buffer& start,
                         const Buffer& end,
                         std::list<KvElem>& kvs_out) = 0;
};

} // namespace jungle_bench;

