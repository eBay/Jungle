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

#include "table_helper.h"

#include "hex_dump.h"
#include "table_file.h"
#include "table_manifest.h"
#include "table_mgr.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

inline void hex_dump_key(const SizedBuf& key, size_t maxlen, char** bp, size_t* len) {
    print_hex_options hex_opt = PRINT_HEX_OPTIONS_INITIALIZER;
    hex_opt.enable_colors = 0;
    hex_opt.actual_address = 0;
    print_hex_to_buf(bp, len,
                     key.data, std::min((size_t)key.size, (size_t)maxlen),
                     hex_opt);
}

std::string hex_dump_to_string(const SizedBuf& key, size_t max_len) {
    char* bp = nullptr;
    size_t bp_size = 0;
    hex_dump_key(key, max_len, &bp, &bp_size);
    std::string ret(bp, bp_size);
    free(bp);
    return ret;
}

int cmp_records(const DBConfig* db_config,
                const Record* l,
                const Record* r)
{
    // Rigth is null, pick Left.
    if ( l && !r) return -1;
    // Left is null, pick Right.
    if (!l &&  r) return 1;

    // Both are null, shouldn't happen.
    if (!l && !r) assert(0);

    if (db_config->cmpFunc) {
        // Custom cmp mode.
        CustomCmpFunc func = db_config->cmpFunc;
        void* param = db_config->cmpFuncParam;
        return func(l->kv.key.data, l->kv.key.size,
                    r->kv.key.data, r->kv.key.size, param);
    }
    return SizedBuf::cmp(l->kv.key, r->kv.key);
}

bool cmp_records_bool(const DBConfig* db_config,
                      const Record* l,
                      const Record* r)
{
    return (cmp_records(db_config, l, r) < 0);
}

std::vector<uint64_t> table_list_to_number(const std::list<TableInfo*> src) {
    std::vector<uint64_t> ret(src.size());

    size_t idx = 0;
    for (auto& entry: src) {
        TableInfo* t_info = entry;
        ret[idx++] = t_info->number;
    }
    return std::move(ret);
}

bool TableMgr::isTableLocked(uint64_t t_number) {
    std::lock_guard<std::mutex> l(lockedTablesLock);
    auto found = lockedTables.find( t_number );
    return (found != lockedTables.end());
}

bool TableMgr::isLevelLocked(uint64_t l_number) {
    std::lock_guard<std::mutex> l(lockedLevelsLock);
    auto found = lockedLevels.find( l_number );
    return (found != lockedLevels.end());
}


// ==========

TableLockHolder::TableLockHolder
    ( TableMgr* t_mgr,
      const std::vector<uint64_t>& tables )
    : tMgr(t_mgr), fOwnsLock(false)
{
    if (!tables.size()) {
        fOwnsLock = true;
        return;
    }

    std::string msg = "try to lock table ";
    for (auto& entry: tables) msg += std::to_string(entry) + " ";

    {   std::lock_guard<std::mutex> l(tMgr->lockedTablesLock);
        for (auto& entry: tables) {
            auto found = tMgr->lockedTables.find(entry);
            if (found != tMgr->lockedTables.end()) {
                // Already locked, fail.
                _s_info(tMgr->myLog)
                    << msg << "=> locking failed due to table " << entry;
                return;
            }
        }

        // No existing locked table. Lock them all.
        for (auto& entry: tables) tMgr->lockedTables.insert(entry);
        // Clone.
        holdingTables = tables;
        fOwnsLock = true;
    }
    _s_info(tMgr->myLog) << msg << "=> succeeded";
}

TableLockHolder::~TableLockHolder() {
    unlock();
}

bool TableLockHolder::ownsLock() const {
    return fOwnsLock;
}

void TableLockHolder::unlock() {
    if (!fOwnsLock) return;
    if (!holdingTables.size()) return;

    std::string msg = "unlock table ";
    for (auto& entry: holdingTables) msg += std::to_string(entry) + " ";
    _s_info(tMgr->myLog) << msg;

    std::lock_guard<std::mutex> l(tMgr->lockedTablesLock);
    for (auto& entry: holdingTables) {
        auto found = tMgr->lockedTables.find(entry);
        // If not exist, that must be a bug.
        assert(found != tMgr->lockedTables.end());
        tMgr->lockedTables.erase(found);
    }
    holdingTables.clear();
    fOwnsLock = false;
}


// =========

LevelLockHolder::LevelLockHolder(TableMgr* t_mgr,
                                 size_t level_to_lock)
    : tMgr(t_mgr)
    , fOwnsLock(false)
    , levelLocked(level_to_lock)
{
    if (level_to_lock >= tMgr->mani->getNumLevels()) return;


    std::string msg = "try to lock level " + std::to_string(level_to_lock);
    {   std::lock_guard<std::mutex> l(tMgr->lockedLevelsLock);
        auto entry = tMgr->lockedLevels.find(level_to_lock);
        if (entry != tMgr->lockedLevels.end()) {
            _s_info(tMgr->myLog) << msg << " => locking failed";
            return;
        }

        tMgr->lockedLevels.insert(level_to_lock);
        fOwnsLock = true;
    }
    _s_info(tMgr->myLog) << msg << " => succeeded";
}

LevelLockHolder::~LevelLockHolder() {
    unlock();
}

bool LevelLockHolder::ownsLock() const {
    return fOwnsLock;
}

void LevelLockHolder::unlock() {
    if (!fOwnsLock) return;

    std::string msg = "unlock level " + std::to_string(levelLocked);
    _s_info(tMgr->myLog) << msg;

    std::lock_guard<std::mutex> l(tMgr->lockedLevelsLock);
    auto entry = tMgr->lockedLevels.find(levelLocked);
    // If not exist, that must be a bug.
    assert(entry != tMgr->lockedLevels.end());
    tMgr->lockedLevels.erase(entry);

    fOwnsLock = false;
}

}; // namespace jungle

