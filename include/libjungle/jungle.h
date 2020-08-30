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

#include "db_config.h"
#include "db_stats.h"
#include "global_batch.h"
#include "iterator.h"
#include "keyvalue.h"
#include "params.h"
#include "record.h"
#include "status.h"

#include <functional>
#include <list>
#include <string>

namespace jungle {

using UserHandler = std::function< void(Status, void*) >;

// Opaque class definition
namespace checker {
    class Checker;
};
class DB {
    friend class checker::Checker;
    friend class Compactor;
    friend class DBMgr;
    friend class DBGroup;
    friend class GlobalBatchExecutor;
    friend class Iterator;
    friend class Flusher;
    friend class FlusherQueue;
    friend class LogMgr;
    friend class LogReclaimer;
    friend class MemTable;
    friend class TableMgr;

public:
    /**
     * Null sequence number.
     */
    static const uint64_t NULL_SEQNUM = static_cast<uint64_t>(-1);

    /**
     * Default flush options.
     */
    static const FlushOptions DEFAULT_FLUSH_OPTIONS;

    /**
     * Initialize process-wide global resources such as
     * block cache and background workers.
     *
     * @param global_config Global configurations.
     * @return OK on success.
     */
    static Status init(const GlobalConfig& global_config);

    /**
     * Release process-wide global resources.
     *
     * @return OK on success.
     */
    static Status shutdown();

    /**
     * Open a Jungle instance in the given path.
     * If the given path is empty, create a new one.
     *
     * @param[out] ptr_out Pointer to the instance as a result of this API call.
     * @param path Path to open (or to create) an instance.
     * @param db_config DB configurations.
     * @return OK on success.
     */
    static Status open(DB** ptr_out,
                       const std::string& path,
                       const DBConfig& db_config);

    /**
     * Close a Jungle instance.
     * Note that the allocated memory pointed to by the given instance
     * will be released, so that user does not need to explicitly call `delete`.
     *
     * @param db Pointer to the instance to be closed.
     * @return OK on success.
     */
    static Status close(DB* db);

    /**
     * Check if the Jungle instance at the given path is in log section mode or not,
     * without opening the instance itself.
     *
     * @param path Path to an instance to check.
     * @return `true` if the instance at the path is for log section mode.
     */
    static bool isLogSectionMode(const std::string& path);

    /**
     * Check if the Jungle instance is in read-only mode.
     *
     * @param path Path to an instance to check.
     * @return `true` if it is in read-only mode..
     */
    bool isReadOnly() const;

    /**
     * Open a snapshot.
     *
     * @param[out] snap_out
     *     Pointer to the snapshot instance as a result of this API call.
     * @param checkpoint
     *     Checkpoint number of the snapshot to open. If `0`, will open a
     *     snapshot based on the latest image.
     * @return OK on success.
     */
    Status openSnapshot(DB** snap_out,
                        const uint64_t checkpoint = 0);

    /**
     * Rollback the given instance to the given sequence number.
     * Only supported in log section mode now.
     *
     * @param seqnum_upto Rollback point (exclusive). This sequence number will be
     *                    preserved.
     * @return OK on success.
     */
    Status rollback(uint64_t seqnum_upto);

    /**
     * Set (upsert) a key-value pair.
     * Sequence number will be automatically assigned.
     *
     * @param kv Key-value pair to set.
     * @return OK on success.
     */
    Status set(const KV& kv);

    /**
     * Set (upsert) a key-value pair, with custom sequence number.
     * Sequence number does not need to be consecutive,
     * but should be in an increasing order and also should be unique.
     *
     * @param seq_num Custom sequence number.
     * @param kv Key-value pair to set.
     * @return OK on success.
     */
    Status setSN(const uint64_t seq_num, const KV& kv);

    /**
     * Set (upsert) a record, with custom metadata and sequence number.
     * Sequence number does not need to be consecutive,
     * but should be in an increasing order and also should be unique.
     *
     * @param rec Record to set.
     * @return OK on success.
     */
    Status setRecord(const Record& rec);

    /**
     * Set (upsert) a record, with custom metadata.
     * Sequence number will be automatically assigned.
     *
     * @param rec Record to set.
     * @return OK on success.
     */
    Status setRecordByKey(const Record& rec);

    /**
     * Set (upsert) a set of records in batch.
     * The batch will be applied atomically (all or nothing).
     * `seqNum` part should be 1) all `NOT_INITIALIZED`, or 2) in
     * increasing order without duplicate numbers (doesn't need to be
     * consecutive).
     * In case of 1), if `seqNum` part is not set, Jungle will
     * automatically assign it.
     *
     * @param batch Set of records.
     * @return OK on success.
     */
    Status setRecordBatch(const std::list<Record>& batch);

    /**
     * Get the value corresponding to the given key.
     *
     * User is responsible for freeing the memory of `value_out`,
     * by using `SizedBuf::free()` or `SizedBuf::Holder`.
     *
     * @param key Key to find.
     * @param[out] value Reference to the buffer where the result will be stored.
     * @return OK on success.
     */
    Status get(const SizedBuf& key, SizedBuf& value_out);

    /**
     * Get the key-value pair corresponding to the given sequence number.
     * Only key-value pairs in log section will be visible.
     *
     * User is responsible for freeing the memory of `kv_out`,
     * by using `KV::free()` or `KV::Holder`.
     *
     * @param seq_num Sequence number to find.
     * @param[out] kv_out Reference to the key-value buffer
     *                    where the result will be stored.
     * @return OK on success.
     */
    Status getSN(const uint64_t seq_num, KV& kv_out);

    /**
     * Get the record corresponding to the given sequence number.
     * Only key-value pairs in log section will be visible.
     *
     * User is responsible for freeing the memory of `rec_out`,
     * by using `Record::free()` or `Record::Holder`.
     *
     * @param seq_num Sequence number to find.
     * @param[out] rec_out Reference to the record
     *                     where the result will be stored.
     * @return OK on success.
     */
    Status getRecord(const uint64_t seq_num, Record& rec_out);

    /**
     * Get the record corresponding to the given key.
     *
     * User is responsible for freeing the memory of `rec_out`,
     * by using `Record::free()` or `Record::Holder`.
     *
     * @param key Key to find.
     * @param[out] rec_out Reference to the record
     *                     where the result will be stored.
     * @param meta_only
     *     If `true`,
     *       1) value part will not be retrieved, and
     *       2) removed record will be searched unless
     *          they are already compacted and purged.
     *
     * @return OK on success.
     */
    Status getRecordByKey(const SizedBuf& key,
                          Record& rec_out,
                          bool meta_only = false);

    /**
     * Delete the key-value pair corresponding to the given key.
     *
     * @param key Key to delete.
     * @return OK on success.
     */
    Status del(const SizedBuf& key);

    /**
     * Delete the key-value pair corresponding to the given key,
     * with custom sequence number. Sequence number given to this API is
     * a number corresponding to delete operation itself, which will
     * be used as a tombstone.
     *
     * Sequence number does not need to be consecutive,
     * but should be in an increasing order and also should be unique.
     *
     * @param seq_num Custom sequence number.
     * @param key Key to delete.
     * @return OK on success.
     */
    Status delSN(const uint64_t seq_num, const SizedBuf& key);

    /**
     * Delete a record, with custom metadata and sequence number.
     * Sequence number given to this API is a number corresponding
     * to delete operation itself, which will be used as a tombstone.
     *
     * Sequence number does not need to be consecutive,
     * but should be in an increasing order and also should be unique.
     *
     * @param rec Record to set.
     * @return OK on success.
     */
    Status delRecord(const Record& rec);

    /**
     * Get the maximum sequence number in the log section.
     *
     * @param[out] seq_num_out Reference to sequence number as a result.
     * @return OK on success.
     */
    Status getMaxSeqNum(uint64_t& seq_num_out);

    /**
     * Get the minimum sequence number in the log section.
     *
     * @param[out] seq_num_out Reference to sequence number as a result.
     * @return OK on success.
     */
    Status getMinSeqNum(uint64_t& seq_num_out);

    /**
     * Get the last flushed sequence number.
     * "Flush" means:
     *   - Normal mode: merging into L0+ tables.
     *   - Log section mode: log compaction.
     *
     * @param[out] seq_num_out Reference to sequence number as a result.
     * @return OK on success.
     */
    Status getLastFlushedSeqNum(uint64_t& seq_num_out);

    /**
     * Get the last synced (written to file) sequence number.
     *
     * @param[out] seq_num_out Reference to sequence number as a result.
     * @return OK on success.
     */
    Status getLastSyncedSeqNum(uint64_t& seq_num_out);

    /**
     * Get the list of checkpoint markers.
     *
     * @param[out] chk_out Checkpoint markers.
     * @return OK on success.
     */
    Status getCheckpoints(std::list<uint64_t>& chk_out);

    /**
     * Do sync (writing to file).
     * Only one thread can execute this operation at a time, and other threads
     * will be blocked.
     *
     * @param call_fsync If `true`, call `fsync` for each file
     *                   after writing data is done.
     * @return OK on success.
     */
    Status sync(bool call_fsync = true);

    /**
     * Do sync (writing to file).
     * Only one thread can execute this operation at a time, and other threads
     * will return immediately, without waiting.
     *
     * @param call_fsync If `true`, call `fsync` for each file
     *                   after writing data is done.
     * @return OK on success.
     *         OPERATION_IN_PROGRESS if other thread is working on it.
     */
    Status syncNoWait(bool call_fsync = true);

    /**
     * Flush logs and merge them into table up to given sequence number.
     * In log section mode, this API is used for log compaction, which is
     * the same as `purgeOnly = true` option.
     *
     * Only one thread can execute this operation at a time, and other threads
     * will return immediately, without waiting.
     *
     * @param options Flush operation options.
     * @param seq_num Max sequence number to flush.
     *                If not given, it will flush all logs.
     * @return OK on success.
     */
    Status flushLogs(const FlushOptions& options = DEFAULT_FLUSH_OPTIONS,
                     const uint64_t seq_num = -1);

    /**
     * Flush logs asynchronously.
     * This API can be used to call `sync` API asynchronously as well.
     *
     * @param options Flush operation options.
     * @param handler Handler that will be invoked after the request is done.
     * @param ctx Generic pointer that will be passed to handler.
     * @param seq_num Max sequence number to flush.
     *                If not given, it will flush all logs.
     * @return OK on success.
     */
    Status flushLogsAsync(const FlushOptions& options,
                          UserHandler handler,
                          void* ctx,
                          const uint64_t seq_num = -1);

    /**
     * [Experimental]
     * Discard dirty key-value items that are not synced yet.
     *
     * @param seq_num_begin Starting sequence number to discard (inclusive).
     * @return OK on success.
     */
    Status discardDirty(uint64_t seq_num_begin);

    /**
     * Add a checkpoint marker.
     * This API will internally call `sync` operation.
     *
     * @param[out] seq_num_out
     *     Sequence number that will be used as a checkpoint marker.
     * @param call_fsync
     *     If `true`, call `fsync` for each file after writing data is done.
     * @return OK on success.
     */
    Status checkpoint(uint64_t& seq_num_out, bool call_fsync = true);

    /**
     * Do compaction on the table for given hash number in level-0.
     *
     * @param options Compaction options.
     * @param hash_num Hash partition number.
     * @return OK on success.
     */
    Status compactL0(const CompactOptions& options,
                     uint32_t hash_num);

    /**
     * Do inter-level compaction on a table in the given level,
     * except for level-0. This API will internally find the most
     * suitable table to compact.
     *
     * @param options Compaction options.
     * @param level Level to compact.
     * @return OK on success.
     *         TABLE_NOT_FOUND if there is no table to compact.
     */
    Status compactLevel(const CompactOptions& options,
                        size_t level);

    /**
     * Do in-place compaction on a table in the given level,
     * except for level-0. This API will internally find the most
     * suitable table to compact.
     *
     * @param options Compaction options.
     * @param level Level to compact.
     * @return OK on success.
     *         TABLE_NOT_FOUND if there is no table to compact.
     */
    Status compactInplace(const CompactOptions& options,
                          size_t level);

    /**
     * Do in-place compaction on tables whose index number is
     * equal to or smaller than the given parameter.
     * If zero, no more compaction will be triggered.
     *
     * @param options Compaction options.
     * @param idx_upto Upper bound of table index number to compact.
     * @return OK on success.
     */
    Status compactIdxUpto(const CompactOptions& options,
                          size_t idx_upto);

    /**
     * Do split on a table in the given level, except for level-0.
     * This API will internally find the most suitable table to compact.
     *
     * @param options Compaction options.
     * @param level Level to compact.
     * @return OK on success.
     *         TABLE_NOT_FOUND if there is no table to split.
     */
    Status splitLevel(const CompactOptions& options,
                      size_t level);

    /**
     * Do merge of two arbitrary adjacent tables in the given level,
     * except for level-0. This API will internally find the most
     * suitable table to compact.
     *
     * @param options Compaction options.
     * @param level Level to merge.
     * @return OK on success.
     *         TABLE_NOT_FOUND if there is no table to split.
     */
    Status mergeLevel(const CompactOptions& options,
                      size_t level);

    /**
     * Get the current statistics of the Jungle instance.
     *
     * @param[out] stats_out Stats as a result of this API call.
     * @return OK on success.
     */
    Status getStats(DBStats& stats_out);

    /**
     * Set debugging parameters.
     *
     * @param to New debugging parameters to set.
     * @param effective_time_sec
     *     Effective time duration in seconds. After it expires, debugging
     *     parameters will not have any impact.
     * @return OK on success.
     */
    static void setDebugParams(const DebugParams& to,
                               size_t effective_time_sec = 3600);

    /**
     * Get the current debugging parameters.
     *
     * @return The current debugging parameters.
     */
    static DebugParams getDebugParams();

    /**
     * Set the level of debugging log file (i.e., `system_logs.log`).
     *
     * Log level follows that in SimpleLogger:
     *   https://github.com/greensky00/simple_logger
     *
     *   0: System  [====]
     *   1: Fatal   [FATL]
     *   2: Error   [ERRO]
     *   3: Warning [WARN]
     *   4: Info    [INFO]
     *   5: Debug   [DEBG]
     *   6: Trace   [TRAC]
     *
     *   Default: 4 (Info).
     *
     * @param new_level New level to set.
     */
    void setLogLevel(int new_level);

    /**
     * Get the current level of debugging log file.
     *
     * @return Log level.
     */
    int getLogLevel() const;

    /**
     * Get the DB path of this instance.
     *
     * @return DB path.
     */
    std::string getPath() const;

private:
    DB();
    DB(DB* _parent, uint64_t last_flush, uint64_t checkpoint);
    ~DB();

    class DBInternal;
    DBInternal* const p;

    class SnapInternal;
    SnapInternal* const sn;
};

class DBGroup {
public:
    DBGroup();
    ~DBGroup();

    static Status open(DBGroup** ptr_out,
                       std::string path,
                       const DBConfig& db_config);
    static Status close(DBGroup* db_group);

    Status openDefaultDB(DB** ptr_out);

    Status openDB(DB** ptr_out,
                  std::string db_name);

    Status openDB(DB** ptr_out,
                  std::string db_name,
                  const DBConfig& db_config);

private:
    class DBGroupInternal;
    DBGroupInternal* const p;
};


// Global management functions. =====

/**
 * Initialize process-wide global resources such as
 * block cache and background workers.
 *
 * @param global_config Global configurations.
 * @return OK on success.
 */
static inline Status init(const GlobalConfig& global_config) {
    (void)init;
    return DB::init(global_config);
}

/**
 * Release process-wide global resources.
 *
 * @return OK on success.
 */
static inline Status shutdown() {
    (void)shutdown;
    return DB::shutdown();
}

/**
 * Set debugging parameters.
 *
 * @param to New debugging parameters to set.
 * @param effective_time_sec
 *     Effective time duration in seconds. After it expires, debugging
 *     parameters will not have any impact.
 * @return OK on success.
 */
static inline void setDebugParams(const DebugParams& to,
                                  size_t effective_time_sec = 3600) {
    (void)setDebugParams;
    DB::setDebugParams(to, effective_time_sec);
}

/**
 * Get the current debugging parameters.
 *
 * @return The current debugging parameters.
 */
static inline DebugParams getDebugParams() {
    (void)getDebugParams;
    return DB::getDebugParams();
}

} // namespace jungle

