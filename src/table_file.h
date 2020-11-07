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
#include "table_lookup_booster.h"

#include <libjungle/jungle.h>
#include <third_party/forestdb/include/libforestdb/forestdb.h>

#include <list>
#include <map>
#include <mutex>
#include <unordered_map>

class BloomFilter;
class SimpleLogger;

namespace jungle {

class TableFileOptions {
public:
    TableFileOptions()
        : minBlockReuseFileSize(64*1024*1024) // 64 MB.
        , bloomFilterSize(0)
        {}

    uint64_t minBlockReuseFileSize;

    // Pre-defined bloom filter size for this table
    // (the total number of bits).
    // If zero, it will be automatically calculated.
    uint64_t bloomFilterSize;
};

class RwSerializer;
struct TableInfo;
class TableMgr;
class TableStats;
class TableFile {
    struct FdbHandle;

public:
    static const TableFileOptions DEFAULT_OPT;

    TableFile(const TableMgr* table_mgr);

    ~TableFile();

    void fdbLogCb(int level,
                  int ec,
                  const char* file,
                  const char* func,
                  size_t line,
                  const char* err_msg,
                  void* ctx);

    static std::string getTableFileName(const std::string& path,
                                        uint64_t prefix_num,
                                        uint64_t table_file_num);

    static uint64_t getBfSizeByLevel(const DBConfig* db_config,
                                     size_t level);

    static uint64_t getBfSizeByWss(const DBConfig* db_config,
                                   uint64_t wss);

    FdbHandle* getIdleHandle();

    void returnHandle(FdbHandle* f_handle);

    Status openFdbHandle(const DBConfig* db_config,
                         const std::string& f_name,
                         FdbHandle* f_handle);

    uint64_t getBfSize() const;

    Status create(size_t level,
                  uint64_t table_number,
                  const std::string& filename,
                  FileOps* f_ops,
                  const TableFileOptions& opt = DEFAULT_OPT);

    Status load(size_t level,
                uint64_t table_number,
                const std::string& f_name,
                FileOps* f_ops,
                const TableFileOptions& opt = DEFAULT_OPT);

    Status loadBloomFilter(const std::string& filename,
                           BloomFilter*& bf_out);

    Status saveBloomFilter(const std::string& filename,
                           BloomFilter* bf,
                           bool call_fsync);

    Status changeOptions(const TableFileOptions& new_opt);

    Status openSnapshot(DB* snap_handle,
                        const uint64_t checkpoint);
    Status closeSnapshot(DB* snap_handle);

    Status sync();

    Status setSingle(uint32_t key_hash_val,
                     const Record& rec,
                     uint64_t& offset_out,
                     bool set_as_it_is = false);

    Status setBatch(std::list<Record*>& batch,
                    std::list<uint64_t>& checkpoints,
                    const SizedBuf& min_key,
                    const SizedBuf& min_key_next_table,
                    uint32_t target_hash = _SCU32(-1),
                    bool bulk_load_mode = false);

    Status get(DB* snap_handle,
               Record& rec_inout,
               bool meta_only = false);

    Status getNearest(DB* snap_handle,
                      const SizedBuf& key,
                      Record& rec_out,
                      SearchOptions s_opt,
                      bool meta_only = false);

    Status getByOffset(DB* snap_handle,
                       uint64_t offset,
                       Record& rec_out);

    Status appendCheckpoints(RwSerializer& file_s);

    Status loadCheckpoints(RwSerializer& file_s);

    Status getAvailCheckpoints(std::list<uint64_t>& chk_out);

    Status getCheckpointSeqnum(uint64_t chk, uint64_t& seqnum_out);

    bool isFdbDocTombstone(fdb_doc* doc);

    Status compactTo(const std::string& dst_filename,
                     const CompactOptions& options);

    Status mergeCompactTo(const std::string& file_to_merge,
                          const std::string& dst_filename,
                          const CompactOptions& options);

    Status destroySelf();

    Status getLatestSnapMarker(uint64_t& last_snap_seqnum);

    Status getSnapMarkerUpto(uint64_t upto,
                             uint64_t& snap_seqnum_out);

    Status getOldestSnapMarker(uint64_t& oldest_snap_seqnum);

    void addCheckpoint(uint64_t chk, uint64_t commit_seqnum);

    void setLogger(SimpleLogger* logger) { myLog = logger; }

    void setTableInfo(TableInfo* t_info) { tableInfo = t_info; }

    SimpleLogger* getLogger() const { return myLog; }

    Status getStats(TableStats& stats_out);

    const std::string& getName() const { return filename; }

    uint64_t getNumber() const { return myNumber; }

    Status getMaxKey(SizedBuf& max_key_out);

    class Snapshot {
    public:
        Snapshot(TableFile* t_file,
                 fdb_kvs_handle* fdb_snap,
                 uint64_t fdb_seqnum)
            : tFile(t_file)
            , fdbSnap(fdb_snap)
            , fdbSeqnum(fdb_seqnum)
            , refCount(1)
            {}
        TableFile* tFile;
        fdb_kvs_handle* fdbSnap;
        uint64_t fdbSeqnum;
        uint32_t refCount;
    };

    class Iterator {
    public:
        Iterator();
        ~Iterator();

        enum SeekOption {
            GREATER = 0,
            SMALLER = 1,
        };

        Status init(DB* snap_handle,
                    TableFile* t_file,
                    const SizedBuf& start_key,
                    const SizedBuf& end_key);
        Status initSN(DB* snap_handle,
                      TableFile* t_file,
                      const uint64_t min_seq,
                      const uint64_t max_seq);
        Status get(Record& rec_out);
        Status getMeta(Record& rec_out,
                       size_t& valuelen_out,
                       uint64_t& offset_out);
        Status prev();
        Status next();
        Status seek(const SizedBuf& key, SeekOption opt = GREATER);
        Status seekSN(const uint64_t seqnum, SeekOption opt = GREATER);
        Status gotoBegin();
        Status gotoEnd();
        Status close();

        enum Type {
            BY_KEY = 0,
            BY_SEQ = 1,
        } type;
        TableFile* tFile;
        // Snapshot of Table file.
        Snapshot* tFileSnap;
        // Snapshot handle of ForestDB.
        fdb_kvs_handle* fdbSnap;
        // Iterator handle of ForestDB, derived from `fdbSnap`.
        fdb_iterator* fdbItr;
        uint64_t minSeq;
        uint64_t maxSeq;
    };

    Status updateSnapshot();

    Status leaseSnapshot(Snapshot*& snapshot_out);

    Status returnSnapshot(Snapshot* snapshot);

private:
// === TYPES
    /**
     * Compaction is triggered only when file size is bigger than 4MB.
     */
    static const uint64_t MIN_COMPACT_FILE_SIZE = 4*1024*1024;

    /**
     * Compact when `active size < file size * 50 %`.
     */
    static const uint64_t COMPACT_RATIO = 50;

    /**
     * Additional size added to user meta.
     */
    static const size_t META_ADD_SIZE = 9;

    static const uint32_t TF_FLAG_TOMBSTONE    = 0x1;

    static const uint32_t TF_FLAG_COMPRESSED   = 0x2;

    struct FdbHandle {
        FdbHandle(TableFile* _parent,
                  const DBConfig* db_config,
                  const TableFileOptions& t_file_opt);
        ~FdbHandle();

        fdb_config getFdbSettings(const DBConfig* db_config);
        fdb_kvs_config getKvsSettings();
        void refreshSettings();
        Status open(const std::string& filename);
        Status openCustomCmp(const std::string& filename,
                             fdb_custom_cmp_variable cmp_func,
                             void* cmp_func_param);
        Status commit();
        Status close();

        TableFile* parent;
        const DBConfig* dbConfig;
        const TableFileOptions& tFileOpt;
        fdb_file_handle* dbFile;
        fdb_kvs_handle* db;
        fdb_config config;
        fdb_kvs_config kvsConfig;
    };

    struct FdbHandleGuard {
        FdbHandleGuard(TableFile* _t_file, FdbHandle* _handle);
        ~FdbHandleGuard();
        TableFile* tFile;
        FdbHandle* handle;
    };

    struct InternalMeta {
        InternalMeta()
            : isTombstone(false), isCompressed(false), originalValueLen(0)
            {}
        bool isTombstone;
        bool isCompressed;
        uint32_t originalValueLen;
    };

// === FUNCTIONS
    static void userMetaToRawMeta(const SizedBuf& user_meta,
                                  const InternalMeta& internal_meta,
                                  SizedBuf& raw_meta_out);

    static void rawMetaToUserMeta(const SizedBuf& raw_meta,
                                  InternalMeta& internal_meta_out,
                                  SizedBuf& user_meta_out);

    static uint32_t tfExtractFlags(const SizedBuf& raw_meta);

    static bool tfIsTombstone(uint32_t flags);

    static bool tfIsCompressed(uint32_t flags);

    static size_t getInternalMetaLen(const InternalMeta& meta);

    Status decompressValue(DB* parent_db,
                           const DBConfig* db_config,
                           Record& rec_io,
                           const InternalMeta& i_meta);

    void initBooster(size_t level, const DBConfig* db_config);

    Status setCheckpoint(Record* rec,
                         uint64_t prev_seqnum,
                         std::list<uint64_t>& checkpoints,
                         bool remaining_all = false);

    Status compactToManully(FdbHandle* compact_handle,
                            const std::string& dst_filename,
                            const CompactOptions& options);

// === VARIABLES
    // File name.
    std::string filename;

    // Table number.
    uint64_t myNumber;

    // File operations.
    FileOps* fOps;

    // File options.
    TableFileOptions myOpt;

    // Parent table manager.
    const TableMgr* tableMgr;

    // Corresponding table info.
    TableInfo* tableInfo;

    // ForestDB writer handles
    FdbHandle* writer;

    // List of ForestDB reader handles
    std::list<FdbHandle*> readers;

    // Lock for `readers`.
    std::mutex readersLock;

    // Map {Checkpoint seqnum, ForestDB commit num}
    std::map<uint64_t, uint64_t> chkMap;

    // Lock for `chkMap`.
    std::mutex chkMapLock;

    // Map {Jungle snapshot handle, ForestDB snapshot handles}
    std::unordered_map<DB*, fdb_kvs_handle*> snapHandles;

    // Lock of `snapHandles`.
    std::mutex snapHandlesLock;

    // Pre-opened the snapshot of the latest Table.
    // The first one is the newest one.
    std::list<Snapshot*> latestSnapshot;

    // Lock of `latestSnapshot`.
    std::mutex latestSnapshotLock;

    // Bloom filter for key.
    BloomFilter* bfByKey;

    // Lookup booster.
    TableLookupBooster* tlbByKey;

    // Logger.
    SimpleLogger* myLog;
};


} // namespace jungle

