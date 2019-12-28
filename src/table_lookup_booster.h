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

#include "internal_helper.h"
#include "list.h"

#include <libjungle/jungle.h>

#include <mutex>
#include <unordered_set>

namespace jungle {

class TableMgr;
class TableFile;
class TableLookupBooster {
public:
    class Elem {
    public:
        Elem(uint32_t h = 0,
             uint64_t s = 0,
             uint64_t o = 0)
        {
            uint8_t* ptr = (uint8_t*)data;
            *((uint32_t*)(ptr +  0)) = h;
            *((uint64_t*)(ptr +  4)) = s;
            *((uint64_t*)(ptr + 12)) = o;
        }
        // Marshalling.
        inline uint32_t hValue() const {
            uint8_t* ptr = (uint8_t*)data;
            return *((uint32_t*)(ptr +  0));
        }
        inline uint64_t seqNum() const {
            uint8_t* ptr = (uint8_t*)data;
            return *((uint64_t*)(ptr +  4));
        }
        inline uint64_t offset() const {
            uint8_t* ptr = (uint8_t*)data;
            return *((uint64_t*)(ptr + 12));
        }
    private:
        char data[20];
    };

    TableLookupBooster(uint64_t limit,
                       const TableMgr* table_mgr,
                       const TableFile* table_file);

    ~TableLookupBooster();

    size_t size() const;

    Status get(uint32_t h_value, uint64_t& offset_out);

    Status setIfNew(const Elem& elem);

private:
// --- TYPES ---

// --- FUNCTIONS ---

// --- VARIABLES ---
    // Limit (the number of elems).
    uint64_t curLimit;

    // Table manager.
    const TableMgr* tMgr;

    // Table file.
    const TableFile* tFile;

    // Element array.
    std::vector<Elem> elems;

    // Lock for `elems`.
    mutable std::vector<std::mutex> elemsLock;
};

}

