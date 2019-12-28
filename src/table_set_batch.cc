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

#include "table_mgr.h"

#include "db_internal.h"
#include "internal_helper.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

void TableMgr::setTableFile( std::list<Record*>& batch,
                             std::list<uint64_t>& checkpoints,
                             bool bulk_load_mode,
                             TableFile* table_file,
                             uint32_t target_hash,
                             const SizedBuf& min_key,
                             const SizedBuf& next_min_key )
{
    table_file->setBatch( batch,
                          checkpoints,
                          min_key,
                          next_min_key,
                          target_hash,
                          bulk_load_mode );

    if (myLog->debugAllowed()) {
        _log_debug( myLog,
                    "Set batch table num %zu, hash %zu, "
                    "key1: %s key2: %s",
                    table_file->getNumber(), target_hash,
                    min_key.toReadableString().c_str(),
                    next_min_key.toReadableString().c_str() );
    }
}

void TableMgr::setTableFileOffset( std::list<uint64_t>& checkpoints,
                                   TableFile* src_file,
                                   TableFile* dst_file,
                                   std::vector<uint64_t>& offsets,
                                   uint64_t start_index,
                                   uint64_t count )
{
    const DBConfig* db_config = getDbConfig();
    (void)db_config;
    DBMgr* mgr = DBMgr::getWithoutInit();
    DebugParams d_params = mgr->getDebugParams();

    Status s;
    TableFile::Iterator itr;
    SizedBuf empty_key;
    std::list<Record*> recs_batch;

   try {
    for (uint64_t ii = start_index; ii < start_index + count; ++ii) {
        if (!isCompactionAllowed()) {
            throw Status(Status::COMPACTION_CANCELLED);
        }

        Record rec_out;
        Record::Holder h_rec_out(rec_out);
        s = src_file->getByOffset(nullptr, offsets[ii], rec_out);
        if (!s) {
            _log_fatal(myLog, "failed to read record at %zu", offsets[ii]);
            assert(0);
            continue;
        }
        uint32_t key_hash_val = getMurmurHash32(rec_out.kv.key);;
        uint64_t offset_out = 0; // not used.
        dst_file->setSingle(key_hash_val, rec_out, offset_out);

        if (d_params.compactionItrScanDelayUs) {
            // If debug parameter is given, sleep here.
            Timer::sleepUs(d_params.compactionItrScanDelayUs);
        }
    }

    // Final commit, and generate snapshot on it.
    setTableFileItrFlush(dst_file, recs_batch, false);
    _log_info(myLog, "(end of batch) set total %zu records", count);

   } catch (Status s) { // -----------------------------------
    _log_err(myLog, "got error: %d", (int)s);
    itr.close();

    for (Record* rr: recs_batch) {
        rr->free();
        delete rr;
    }
   }
}

void TableMgr::setTableFileItrFlush(TableFile* dst_file,
                                    std::list<Record*>& recs_batch,
                                    bool without_commit)
{
    SizedBuf empty_key;
    std::list<uint64_t> dummy_chk;

    dst_file->setBatch( recs_batch, dummy_chk,
                        empty_key, empty_key, _SCU32(-1),
                        without_commit );
    for (Record* rr: recs_batch) {
        rr->free();
        delete rr;
    }
    recs_batch.clear();
}

Status TableMgr::setBatch(std::list<Record*>& batch,
                          std::list<uint64_t>& checkpoints,
                          bool bulk_load_mode)
{
    // NOTE:
    //  This function deals with level-0 tables only,
    //  which means that it is always hash-partitioning.
    const DBConfig* db_config = getDbConfig();
    if (db_config->logSectionOnly) return Status::TABLES_ARE_DISABLED;

    std::unique_lock<std::mutex> l(L0Lock);
    Timer tt;
    Status s;

    // Not pure LSM: L0 is hash partitioned.
    EP( setBatchHash(batch, checkpoints, bulk_load_mode) );

    uint64_t elapsed_us = tt.getUs();
    _log_info(myLog, "L0 write done: %zu records, %zu us, %.1f iops",
              batch.size(), elapsed_us,
              (double)batch.size() * 1000000 / elapsed_us);

    return Status();
}

Status TableMgr::setBatchHash( std::list<Record*>& batch,
                               std::list<uint64_t>& checkpoints,
                               bool bulk_load_mode )
{
    Status s;
    std::list<TableInfo*> target_tables;

    DBMgr* db_mgr = DBMgr::getWithoutInit();
    DebugParams d_params = db_mgr->getDebugParams();

    _log_info(myLog, "Records: %zu", batch.size());

    // NOTE: write in parallel.
    size_t num_partitions = getNumL0Partitions();
    size_t max_writers = getDbConfig()->getMaxParallelWriters();

    // For the case where `num_partitions > num_writers`.
    for (size_t ii = 0; ii < num_partitions; ) {
        size_t upto_orig = std::min(ii + max_writers, num_partitions);

        // NOTE: request `req_writers - 1`, as the other one is this thread.
        size_t req_writers = upto_orig - ii;
        TableWriterHolder twh(db_mgr->tableWriterMgr(), req_writers - 1);

        // Lease may not succeed, adjust `upto`.
        size_t leased_writers = twh.leasedWriters.size();
        size_t upto = ii + leased_writers + 1;

        for (size_t jj = ii; jj < upto; ++jj) {
            size_t worker_idx = jj - ii;
            bool leased_thread = (jj + 1 < upto);

            TableWriterArgs local_args;
            local_args.myLog = myLog;

            TableWriterArgs* w_args = (leased_thread)
                                      ? &twh.leasedWriters[worker_idx]->writerArgs
                                      : &local_args;
            w_args->callerAwaiter.reset();

            // Find target table for the given hash number.
            std::list<TableInfo*> tables;
            s = mani->getL0Tables(jj, tables);
            if (!s) continue;

            TableInfo* target_table = getSmallestNormalTable(tables, jj);
            if (!target_table) {
                // Target table does not exist, skip.
                for (TableInfo*& entry: tables) entry->done();
                continue;
            }

            w_args->payload = TableWritePayload( this,
                                                 &batch,
                                                 &checkpoints,
                                                 target_table->file,
                                                 jj,
                                                 bulk_load_mode );
            // Release all tables except for the target.
            for (TableInfo*& entry: tables) {
                if (entry != target_table) entry->done();
            }
            target_tables.push_back(target_table);

            if (leased_thread) {
                // Leased threads.
                w_args->invoke();
            } else {
                // This thread.
                TableWriterMgr::doTableWrite(w_args);
            }
        }

        // Wait for each worker.
        for (size_t jj = ii; jj < upto - 1; ++jj) {
            size_t worker_idx = jj - ii;
            TableWriterArgs* w_args = &twh.leasedWriters[worker_idx]->writerArgs;
            while ( !w_args->payload.isEmpty() ) {
                w_args->callerAwaiter.wait_ms(1000);
                w_args->callerAwaiter.reset();
            }
        }

        if (d_params.tableSetBatchCb) {
            DebugParams::GenericCbParams p;
            d_params.tableSetBatchCb(p);
        }

        ii += leased_writers + 1;
    }

    // Release all target tables.
    for (TableInfo*& entry: target_tables) entry->done();

    return Status();
}

} // namespace jungle

