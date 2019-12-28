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

#include "event_awaiter.h"
#include "internal_helper.h"

#include <libjungle/jungle.h>

#include <cstdint>
#include <list>
#include <vector>

class SimpleLogger;

namespace jungle {

class TableFile;
class TableMgr;
struct TableInfo;

void hex_dump_key(const SizedBuf& key, size_t maxlen, char** bp, size_t* len);

std::string hex_dump_to_string(const SizedBuf& key, size_t max_len = 256);

int cmp_records(const DBConfig* db_config,
                const Record* l,
                const Record* r);

bool cmp_records_bool(const DBConfig* db_config,
                      const Record* l,
                      const Record* r);

std::vector<uint64_t> table_list_to_number(const std::list<TableInfo*> src);

class TableLockHolder {
public:
    TableLockHolder(TableMgr* t_mgr,
                    const std::vector<uint64_t>& tables);
    ~TableLockHolder();
    bool ownsLock() const;
    void unlock();

private:
    TableMgr* tMgr;
    bool fOwnsLock;
    std::vector<uint64_t> holdingTables;
};

class LevelLockHolder {
public:
    LevelLockHolder(TableMgr* t_mgr,
                    size_t level_to_lock);
    ~LevelLockHolder();
    bool ownsLock() const;
    void unlock();
private:
    TableMgr* tMgr;
    bool fOwnsLock;
    size_t levelLocked;
};

}; // namespace jungle

