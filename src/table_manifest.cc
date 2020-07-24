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

#include "table_manifest.h"

#include "crc32.h"
#include "internal_helper.h"
#include "table_mgr.h"

#include <atomic>

namespace jungle {

static uint8_t TABMANI_FOOTER[8] = {0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xab, 0xef};
static uint32_t TABMANI_VERSION = 0x1;


struct TableManifest::LevelInfo {
    LevelInfo()
        : numTables(0)
    {
        tables = new skiplist_raw();
        skiplist_init(tables, TableInfo::cmp);
    }

    ~LevelInfo() {
        if (tables) {
            skiplist_node* cursor = skiplist_begin(tables);
            while (cursor) {
                TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
                cursor = skiplist_next(tables, cursor);

                TableStack* stack = t_info->stack;
                if (stack) {
                    std::lock_guard<std::mutex> l(stack->lock);
                    for (TableInfo* tt: stack->tables) {
                        delete tt->file;
                        delete tt;
                    }
                }

                delete t_info->file;
                delete t_info;
            }
            skiplist_free(tables);
            delete tables;
        }
    }

    std::atomic<size_t> numTables;
    skiplist_raw* tables;
};


TableManifest::TableManifest(const TableMgr* table_mgr,
                             FileOps* _f_ops)
    : tableMgr(table_mgr)
    , fOps(_f_ops)
    , mFile(nullptr)
    , maxTableNum(NOT_INITIALIZED)
    , myLog(nullptr)
{
    levels.reserve(NUM_RESERVED_LEVELS);
}

TableManifest::~TableManifest() {
    if (mFile) {
        // `delete` will close file if opened.
        delete mFile;
    }
    for (size_t ii=0; ii<levels.size(); ++ii) {
        LevelInfo* l_info = levels[ii];
        delete l_info;
    }
}

Status TableManifest::create(const std::string& path,
                             const std::string& filename)
{
    if (!fOps) return Status::NOT_INITIALIZED;
    if (fOps->exist(filename.c_str())) return Status::ALREADY_EXIST;
    if (filename.empty()) return Status::INVALID_PARAMETERS;

    dirPath = path;
    mFileName = filename;

    // Create a new file.
    Status s;
    EP(fOps->open(&mFile, mFileName.c_str()));
    if (!s) return s;

    // Initially there is only one level (level-0).
    levels.resize(1);
    levels[0] = new LevelInfo();

    // Store initial data.
    EP( store(false) );

    return Status();
}

Status TableManifest::load(const std::string& path,
                           const uint64_t prefix_num,
                           const std::string& filename)
{
    if (!fOps) return Status::NOT_INITIALIZED;
    if (!fOps->exist(filename.c_str())) return Status::FILE_NOT_EXIST;
    if (filename.empty()) return Status::INVALID_PARAMETERS;

    dirPath = path;
    mFileName = filename;

    Status s;
    EP( fOps->open(&mFile, mFileName.c_str()) );

   try {
    // File should be bigger than 16 bytes (FOOTER + version + CRC32).
    size_t file_size = fOps->eof(mFile);
    if (file_size < 16) throw Status(Status::FILE_CORRUPTION);

    // Footer check
    RwSerializer ss(fOps, mFile);
    uint8_t footer_file[8];
    ss.pos(file_size - 16);
    ss.get(footer_file, 8);
    if (memcmp(TABMANI_FOOTER, footer_file, 8) != 0) {
        throw Status(Status::FILE_CORRUPTION);
    }

    // Version check
    uint32_t ver_file = ss.getU32(s);
    (void)ver_file;

    // CRC check
    uint32_t crc_file = ss.getU32(s);

    SizedBuf chk_buf(file_size - 4);
    SizedBuf::Holder h_chk_buf(chk_buf);
    ss.pos(0);
    ss.get(chk_buf.data, chk_buf.size);
    uint32_t crc_local = crc32_8(chk_buf.data, chk_buf.size, 0);
    if (crc_local != crc_file) throw Status(Status::CHECKSUM_ERROR);

    // Max (latest) table file number.
    ss.pos(0);
    maxTableNum.store(ss.getU64(s), MOR);

    // Number of levels.
    uint32_t num_levels = ss.getU32(s);
    levels.resize(num_levels);

    for (size_t ii=0; ii<num_levels; ++ii) {
        // For each level
        LevelInfo* l_info = new LevelInfo();
        levels[ii] = l_info;

        uint32_t num_tables = ss.getU32(s);
        l_info->numTables.store(num_tables, MOR);

        for (size_t jj=0; jj<num_tables; ++jj) {
            // For each table
            uint64_t table_number = ss.getU64(s);
            TableInfo* t_info = nullptr;

            uint32_t k_size = ss.getU32(s);

            if (ii == 0) {
                // Level-0:
                uint32_t hash_num = 0;
                uint32_t status = 0;

                // NOTE:
                //  if k_size == 0, assume that hash_num is also 0
                //  (compatiable with old file format).
                if (k_size >= 4) hash_num = ss.getU32(s);
                if (k_size >= 8) status = ss.getU32(s);

                t_info = new TableInfo(ii, table_number, hash_num);
                if (status) t_info->status = (TableInfo::Status)status;

            } else {
                // Level-1+: read min key.
                t_info = new TableInfo(ii, table_number, 0);
                if (k_size) {
                    t_info->minKey.alloc(k_size, nullptr);
                    s = ss.get(t_info->minKey.data, k_size);
                }
            }

            TableFile* t_file = new TableFile(tableMgr);
            t_file->setLogger(myLog);
            std::string t_filename = TableFile::getTableFileName
                                     ( dirPath, prefix_num, table_number );

            TableFileOptions t_opt;
            t_file->load(ii, table_number, t_filename, fOps, t_opt);

            // Checkpoints
            t_file->loadCheckpoints(ss);

            t_info->file = t_file;
            t_info->file->setTableInfo(t_info);

            bool add_to_skiplist = true;
            if (add_to_skiplist) {
                skiplist_insert(l_info->tables, &t_info->snode);
            }
        }
    }

    return Status();

   } catch (Status s) {
    fOps->close(mFile);
    DELETE(mFile);
    return s;
   }
}

Status TableManifest::store(bool call_fsync) {
    if (mFileName.empty() || !fOps) return Status::NOT_INITIALIZED;

    Status s;

    RwSerializer ss(fOps, mFile);

    //   << Table manifest file format >>
    // Latest table file number,        8 bytes
    // Current number of levels (N),    4 bytes
    // +---
    // | Level entry,                   ...
    // +--- N times
    ss.putU64(maxTableNum.load(MOR));
    uint32_t num_levels = levels.size();
    ss.putU32(num_levels);

    for (size_t ii=0; ii<num_levels; ++ii) {
        //   << Level entry format >>
        // Number of tables (M),    4 bytes
        // +---
        // | Table entry,           ...
        // +--- M times
        LevelInfo* l_info = levels[ii];
        ss.putU32(l_info->numTables.load(MOR));
        skiplist_node* cursor = skiplist_begin(l_info->tables);
        while (cursor) {
            //   << Table entry format >>
            // Table number,                        8 bytes
            // Table min key length (L),            4 bytes
            // Table min key (L0: hash num + a),    L bytes
            // Number of checkpoints (K),           4 bytes
            // +---
            // | Checkpoint seq num,                8 bytes
            // | ForestDB seq num,                  8 bytes
            // +--- K times
            TableInfo* t_info = _get_entry(cursor, TableInfo, snode);

            // Do not save if this file is COMPACT_DST:
            //  it means that this file is incomplete yet.
            if (t_info->isValid()) {
                ss.putU64(t_info->number);
                if ( t_info->level == 0 ) {
                    // Level-0 (hash):
                    // hash num             4 bytes
                    // file status + flags  4 bytes
                    ss.putU32(sizeof(t_info->hashNum) + sizeof(uint32_t));
                    ss.putU32(t_info->hashNum);
                    ss.putU32((uint32_t)t_info->status.load());

                } else {
                    // Level-1+: append min key.
                    ss.putU32(t_info->minKey.size);
                    ss.put(t_info->minKey.data, t_info->minKey.size);
                }
                t_info->file->appendCheckpoints(ss);
            }

            cursor = skiplist_next(l_info->tables, cursor);
            skiplist_release_node(&t_info->snode);
        }
        if (cursor) skiplist_release_node(cursor);
    }

    // Footer.
    ss.put(TABMANI_FOOTER, 8);

    // Version.
    ss.putU32(TABMANI_VERSION);

    // CRC32
    size_t ctx_size = ss.pos();

    // Read whole file.
    RwSerializer ss_read(fOps, mFile);
    SizedBuf temp_buf(ctx_size);
    SizedBuf::Holder h_temp_buf(temp_buf);
    ss_read.get(temp_buf.data, temp_buf.size);
    uint32_t crc_val = crc32_8(temp_buf.data, temp_buf.size, 0);

    ss.putU32(crc_val);

    // Should truncate tail.
    fOps->ftruncate(mFile, ss.pos());

    if (call_fsync) {
        s = fOps->fsync(mFile);
        if (s) {
            // Same as that in log manifest.

            // After success, make a backup file one more time,
            // using the latest data.
            EP( BackupRestore::backup(fOps, mFileName, call_fsync) );
        }
    }

    return s;
}

Status TableManifest::storeTableStack(RwSerializer& ss,
                                      TableInfo* base_table)
{
    if ( !base_table ||
         !base_table->baseTable ||
         !base_table->stack ) return Status();

    TableStack* stack = base_table->stack;

    std::lock_guard<std::mutex> l(stack->lock);
    for (auto& entry: stack->tables) {
        TableInfo* cur_table = entry;
        ss.putU64(cur_table->number);
        ss.putU32(cur_table->minKey.size);
        ss.put(cur_table->minKey.data, cur_table->minKey.size);
        cur_table->file->appendCheckpoints(ss);
    }
    return Status();
}

Status TableManifest::extendLevel() {
    size_t new_level = 0;
    {   std::lock_guard<std::mutex> l(levelsLock);
        new_level = levels.size();
        levels.resize(new_level + 1);
        levels[new_level] = new LevelInfo();
    }
    uint64_t level_limit = tableMgr->getLevelSizeLimit(new_level);
    uint64_t table_limit = tableMgr->getDbConfig()->getMaxTableSize(new_level);

    _log_info( myLog,
               "NEW LEVEL: %zu, table size limit %zu (%s), "
               "level size limit %zu (%s)",
               new_level,
               table_limit, Formatter::sizeToString(table_limit, 2).c_str(),
               level_limit, Formatter::sizeToString(level_limit, 2).c_str() );
    return Status();
}

Status TableManifest::issueTableNumber(uint64_t& new_table_number) {
    uint64_t expected = NOT_INITIALIZED;
    uint64_t val = 0;
    if (maxTableNum.compare_exchange_weak(expected, val)) {
        // The first table file, number 0.
    } else {
        // Otherwise: current max + 1.
        do {
            expected = maxTableNum;
            val = maxTableNum + 1;
        } while (!maxTableNum.compare_exchange_weak(expected, val));
    }
    new_table_number = val;
    return Status();
}

Status TableManifest::addTableFile(size_t level,
                                   uint32_t hash_num,
                                   SizedBuf min_key,
                                   TableFile* t_file,
                                   bool allow_stacking)
{
    if (level >= levels.size()) return Status::INVALID_LEVEL;

    // New table corresponding to the number.
    TableInfo* t_info = new TableInfo(level, t_file->getNumber(), hash_num);
    t_info->file = t_file;
    t_info->file->setTableInfo(t_info);
    t_info->file->setLogger(myLog);
    min_key.copyTo(t_info->minKey);

    LevelInfo* cur_level = levels[level];

    // Non-tiering mode:
    //   Duplicate min-key SHOULD NOT exist.
    skiplist_insert(cur_level->tables, &t_info->snode);
    cur_level->numTables.fetch_add(1);
    _log_info( myLog, "level %zu: added table %zu_%zu (hash %zu) to manifest, "
               "min key %s, num tables %zu",
               level,
               tableMgr->getTableMgrOptions()->prefixNum,
               t_file->getNumber(), hash_num,
               t_info->minKey.toReadableString().c_str(),
               cur_level->numTables.load() );
    return Status();
}

Status TableManifest::removeTableFile(size_t level,
                                      TableInfo* table_to_remove)
{
    if (level >= levels.size()) return Status::INVALID_LEVEL;

    LevelInfo* cur_level = levels[level];
    if (!table_to_remove->baseTable) {
        // Stack of other table. Do nothing on skiplist.
        table_to_remove->setRemoved();
        cur_level->numTables.fetch_sub(1);
        _log_info(myLog, "level %zu: removed table %zu_%zu from stack "
                         "ref count %zu, num tables %zu",
                  level,
                  tableMgr->getTableMgrOptions()->prefixNum,
                  table_to_remove->number,
                  table_to_remove->refCount.load(),
                  cur_level->numTables.load());

        return Status();
    }

    uint64_t table_num = table_to_remove->number;

    skiplist_node* cursor = skiplist_begin(cur_level->tables);
    while (cursor) {
        TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
        if (t_info->number == table_num) {
            skiplist_erase_node(cur_level->tables, &t_info->snode);
            skiplist_release_node(&t_info->snode);
            skiplist_wait_for_free(&t_info->snode);

            // NOTE: the last done() call will kill itself (suicide).
            t_info->setRemoved();
            cur_level->numTables.fetch_sub(1);

            _log_info( myLog, "level %zu: removed table %zu_%zu from manifest "
                       "min key %s, ref count %zu, num tables %zu",
                       level,
                       tableMgr->getTableMgrOptions()->prefixNum,
                       table_num,
                       t_info->minKey.toReadableString().c_str(),
                       t_info->refCount.load(),
                       cur_level->numTables.load() );
            return Status();
        }
        cursor = skiplist_next(cur_level->tables, cursor);
        skiplist_release_node(&t_info->snode);
    }

    return Status();
}

bool TableManifest::doesHashPartitionExist(uint32_t hash_num) {
    LevelInfo* l_info = levels[0];
    skiplist_node* cursor = skiplist_begin(l_info->tables);
    if (!cursor) return false;

    while (cursor) {
        TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
        if ( t_info->hashNum == hash_num &&
             t_info->isValid() ) {
            // Match
            skiplist_release_node(&t_info->snode);
            return true;
        }
        cursor = skiplist_next(l_info->tables, cursor);
        skiplist_release_node(&t_info->snode);
    }
    return false;
}

Status TableManifest::getTablesByHash(LevelInfo* l_info,
                                      uint32_t target_hash_num,
                                      std::list<TableInfo*>& tables_out)
{
    //  Find the smallest number NORMAL log file.
    size_t num_partitions = tableMgr->getNumL0Partitions();

    // NOTE: only for level-0.
    skiplist_node* cursor = skiplist_begin(l_info->tables);
    if (!cursor) return Status::TABLE_NOT_FOUND;

    while (cursor) {
        TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
        // If 1) 1) invalid hash value (implies return all), OR
        //       2) the file's hash value is matched with given target hash.
        //     AND
        //    2) the file is valid (NORMAL or COMPACT_SRC), AND
        //    3) the file is not removed.
        if ( ( target_hash_num >= num_partitions ||
               t_info->hashNum == target_hash_num ) &&
             t_info->isValid() &&
             !t_info->isRemoved() ) {
            // Match
            t_info->grab();
            tables_out.push_back(t_info);
        }
        cursor = skiplist_next(l_info->tables, cursor);
        skiplist_release_node(&t_info->snode);
    }
    if (tables_out.empty()) return Status::TABLE_NOT_FOUND;

    return Status();
}

Status TableManifest::getL0Tables(uint32_t target_hash_num,
                                  std::list<TableInfo*>& tables_out)
{
    LevelInfo* l_info = levels[0];
    return getTablesByHash(l_info, target_hash_num, tables_out);
}

Status TableManifest::getTablesPoint(const size_t level,
                                     const SizedBuf& key,
                                     std::list<TableInfo*>& tables_out)
{
    if (level >= levels.size()) return Status::INVALID_LEVEL;

    LevelInfo* l_info = levels[level];

    if ( level == 0 ) {
        // Level-0: hash partition
        size_t num_partitions = tableMgr->getNumL0Partitions();
        uint32_t target_hash = getMurmurHash(key, num_partitions);
        return getTablesByHash(l_info, target_hash, tables_out);

    } else {
        // Otherwise: range-based partition (key)
        TableInfo query(level, 0, 0, true);
        query.minKey.referTo(key);
        skiplist_node* cursor = skiplist_find_smaller_or_equal
                                ( l_info->tables, &query.snode );
        if (!cursor) return Status::TABLE_NOT_FOUND;

        TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
        t_info->grab();
        pushTablesInStack(t_info, tables_out);
        tables_out.push_back(t_info);

        skiplist_release_node(&t_info->snode);
    }

    return Status();
}

void TableManifest::pushTablesInStack(TableInfo* t_info,
                                      std::list<TableInfo*>& tables_out)
{
    // If tiering mode, push all stack in reversed order
    // (newer -> older -> base (oldest)).
    TableStack* stack = t_info->stack;
    if (t_info->baseTable && stack) {
        std::lock_guard<std::mutex> l(stack->lock);

        auto entry = stack->tables.rbegin();
        while (entry != stack->tables.rend()) {
            TableInfo* tt = *entry;
            tt->grab();
            tables_out.push_back(tt);
            entry++;
        }
    }
}

Status TableManifest::getTablesRange(const size_t level,
                                     const SizedBuf& min_key,
                                     const SizedBuf& max_key,
                                     std::list<TableInfo*>& tables_out)
{
    if (level >= levels.size()) return Status::INVALID_LEVEL;

    LevelInfo* l_info = levels[level];

    if ( level == 0 ) {
        // Level-0: hash partition
        //  Ignore the given range and return all.
        size_t num_partitions = tableMgr->getNumL0Partitions();
        return getTablesByHash(l_info, num_partitions, tables_out);

    } else {
        TableInfo query(level, 0, 0, true);
        query.minKey.referTo(min_key);
        skiplist_node* cursor = skiplist_find_smaller_or_equal
                                ( l_info->tables, &query.snode );
        if (!cursor) cursor = skiplist_begin(l_info->tables);

        while (cursor) {
            TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
            if ( max_key.empty() ||
                 t_info->minKey <= max_key ) {
                t_info->grab();
                pushTablesInStack(t_info, tables_out);
                tables_out.push_back(t_info);
            }

            cursor = skiplist_next(l_info->tables, cursor);
            skiplist_release_node(&t_info->snode);
        }
        if (cursor) skiplist_release_node(cursor);
    }

    return Status();
}

void TableManifest::getTableNumbers(std::set<uint64_t>& numbers_out) {
    for (auto& entry: levels) {
        LevelInfo*& l_info = entry;
        skiplist_node* cursor = skiplist_begin(l_info->tables);
        while (cursor) {
            TableInfo* t_info = _get_entry(cursor, TableInfo, snode);

            TableStack* stack = t_info->stack;
            if (stack) {
                std::lock_guard<std::mutex> l(stack->lock);
                for (TableInfo* tt: stack->tables) {
                    numbers_out.insert(tt->number);
                }
            }

            numbers_out.insert(t_info->number);
            cursor = skiplist_next(l_info->tables, cursor);
            skiplist_release_node(&t_info->snode);
        }
        if (cursor) skiplist_release_node(cursor);
    }
}

size_t TableManifest::getNumLevels() {
    std::lock_guard<std::mutex> l(levelsLock);
    return levels.size();
}

Status TableManifest::getNumTables(size_t level, size_t& num_tables_out) const {
    if (level >= levels.size()) return Status::INVALID_LEVEL;

    LevelInfo* l_info = levels[level];
    num_tables_out = l_info->numTables;
    return Status();
}

Status TableManifest::getSmallestTableIdx(uint64_t& idx_out) {
    uint64_t cur_min = std::numeric_limits<uint64_t>::max();

    for (auto& entry: levels) {
        LevelInfo*& l_info = entry;
        skiplist_node* cursor = skiplist_begin(l_info->tables);
        while (cursor) {
            TableInfo* t_info = _get_entry(cursor, TableInfo, snode);
            cur_min = std::min(t_info->number, cur_min);

            cursor = skiplist_next(l_info->tables, cursor);
            skiplist_release_node(&t_info->snode);
        }
        if (cursor) skiplist_release_node(cursor);
    }

    idx_out = cur_min;
    return Status();
}

} // namespace jungle

