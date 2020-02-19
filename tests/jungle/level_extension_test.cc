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

#include "jungle_test_common.h"

#include "internal_helper.h"

#include <algorithm>
#include <fstream>
#include <numeric>
#include <random>

#include <stdio.h>

namespace level_extension_test {

int next_level_basic_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 10000;
    const char V_FMT[] = "v%0100zu";

    // Write even numbers.
    CHK_Z(_set_keys(db, 0, NUM, 2, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Write odd numbers.
    CHK_Z(_set_keys(db, 1, NUM, 2, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Point query check.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));

    // Range query check.
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Compact more.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Now L1 compaction (split).
    CHK_Z( db->splitLevel(c_opt, 1) );

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

struct WorkerArgs : TestSuite::ThreadArgs {
    enum CompactionType {
        INTERLEVEL = 0x0,
        INPLACE = 0x1,
        SPLIT = 0x2,
        MERGE = 0x3,
        L0 = 0x4,
    };

    WorkerArgs()
        : db(nullptr)
        , type(INTERLEVEL)
        , hashNum(0)
        {}
    jungle::DB* db;
    CompactionType type;
    size_t hashNum;
    jungle::Status expResult;
};

int compaction_worker(TestSuite::ThreadArgs* t_args) {
    WorkerArgs* args = static_cast<WorkerArgs*>(t_args);
    jungle::CompactOptions c_opt;
    jungle::Status s;
    switch (args->type) {
    case WorkerArgs::INTERLEVEL:
        s = args->db->compactLevel(c_opt, 1);
        break;
    case WorkerArgs::INPLACE:
        s = args->db->compactInplace(c_opt, 1);
        break;
    case WorkerArgs::SPLIT:
        s = args->db->splitLevel(c_opt, 1);
        break;
    case WorkerArgs::MERGE:
        s = args->db->mergeLevel(c_opt, 1);
        break;
    case WorkerArgs::L0:
        s = args->db->compactL0(c_opt, args->hashNum);
        break;
    default: break;
    }

    CHK_EQ( args->expResult, s );
    return 0;
}

int interlevel_compaction_cancel_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 10000;
    const char V_FMT_ORIG[] = "vo%0100zu";
    const char V_FMT[] = "v%0100zu";

    // Write KVs.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT_ORIG));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Do initial interlevel compaction to extend level.
    CHK_Z( db->compactLevel(c_opt, 1) );

    // Write more keys.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Set delay for split.
    jungle::DebugParams d_params;
    // 150 ms per record.
    d_params.compactionDelayUs = 150*1000;
    jungle::setDebugParams(d_params);

    // Inter-level compaction.
    WorkerArgs w_args;
    w_args.db = db;
    w_args.type = WorkerArgs::INTERLEVEL;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_sec(1, "wait for worker to start");

    // Close DB, it should cancel the split.
    CHK_Z(jungle::DB::close(db));
    h.join();
    CHK_Z(h.getResult());

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Again, set delay to 2nd phase at this time.
    d_params.compactionDelayUs = 0;
    d_params.compactionItrScanDelayUs = 150*1000;
    jungle::setDebugParams(d_params);

    w_args.db = db;
    w_args.type = WorkerArgs::INTERLEVEL;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h2(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_sec(1, "wait for worker to start");

    // Close DB, it should cancel the split.
    CHK_Z(jungle::DB::close(db));
    h2.join();
    CHK_Z(h2.getResult());

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Inter-level compaction should succeed even after cancel.
    d_params.compactionItrScanDelayUs = 0; // Cancel delay.
    jungle::setDebugParams(d_params);
    CHK_Z( db->compactLevel(c_opt, 1) );

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int inplace_compaction_cancel_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    config.minFileSizeToCompact = 128 * 1024;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 10000;
    const char V_FMT[] = "v%0100zu";

    // Write KVs.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Write more KVs, flush, and compact to L1,
    // in order to make stale data for in-place compaction.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) CHK_Z(db->compactL0(c_opt, ii));

    // Set delay for split.
    jungle::DebugParams d_params;
    // 150 ms per record.
    d_params.compactionDelayUs = 150*1000;
    jungle::setDebugParams(d_params);

    // Inter-level compaction.
    WorkerArgs w_args;
    w_args.db = db;
    w_args.type = WorkerArgs::INPLACE;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_sec(1, "wait for worker to start");

    // Close DB, it should cancel the split.
    CHK_Z(jungle::DB::close(db));
    h.join();
    CHK_Z(h.getResult());

    // NOTE: In-place compaction doesn't have 2nd phase iteration.

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // In-place compaction should succeed even after cancel.
    d_params.compactionDelayUs = 0; // Cancel delay.
    jungle::setDebugParams(d_params);
    CHK_Z( db->compactInplace(c_opt, 1) );

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int split_cancel_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 10000;
    const char V_FMT[] = "v%0100zu";

    // Write KVs.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Set delay for split.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 0;
    d_params.compactionItrScanDelayUs = 150*1000;
    jungle::setDebugParams(d_params);

    WorkerArgs w_args;
    w_args.db = db;
    w_args.type = WorkerArgs::SPLIT;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h2(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_sec(1, "wait for worker to start");

    // Close DB, it should cancel the split.
    CHK_Z(jungle::DB::close(db));
    h2.join();
    CHK_Z(h2.getResult());

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Split should succeed even after cancel.
    d_params.compactionItrScanDelayUs = 0; // Cancel delay.
    jungle::setDebugParams(d_params);
    CHK_Z( db->splitLevel(c_opt, 1) );

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int merge_cancel_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.minFileSizeToCompact = 64 * 1024;
    config.maxL0TableSize = 64 * 1024;
    config.maxL1TableSize = 64 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 10000;
    const char V_FMT[] = "v%0250zu";

    // Write KVs.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Delete KVs.
    CHK_Z(_del_keys(db, 0, NUM/2, 1, "k%06zu"));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // L1 in-place compaction.
    for (size_t ii=0; ii<4; ++ii) {
        db->compactInplace(c_opt, 1);
    }

    // Check.
    CHK_Z(_non_existing_keys(db, 0, NUM/2, 1, "k%06zu"));
    CHK_Z(_get_keys(db, NUM/2, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, NUM/2, NUM-1, 1, "k%06zu", V_FMT));

    // Set delay for merge.
    jungle::DebugParams d_params;
    d_params.compactionItrScanDelayUs = 500*1000;
    d_params.forceMerge = true;
    jungle::setDebugParams(d_params);

    // Merge.
    WorkerArgs w_args;
    w_args.db = db;
    w_args.type = WorkerArgs::MERGE;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_ms(200, "wait for worker to start");

    // Close DB, it should cancel the merge.
    CHK_Z(jungle::DB::close(db));
    h.join();
    CHK_Z(h.getResult());

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_non_existing_keys(db, 0, NUM/2, 1, "k%06zu"));
    CHK_Z(_get_keys(db, NUM/2, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, NUM/2, NUM-1, 1, "k%06zu", V_FMT));

    // Merge should succeed even after cancel.
    d_params.compactionItrScanDelayUs = 0; // Cancel delay.
    jungle::setDebugParams(d_params);
    CHK_Z( db->mergeLevel(c_opt, 1) );

    // Check again.
    CHK_Z(_non_existing_keys(db, 0, NUM/2, 1, "k%06zu"));
    CHK_Z(_get_keys(db, NUM/2, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, NUM/2, NUM-1, 1, "k%06zu", V_FMT));

    // Merge should fail if we clear force flag.
    d_params.forceMerge = false;
    jungle::setDebugParams(d_params);
    CHK_GT( 0, db->mergeLevel(c_opt, 1) );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int single_file_compaction_cancel_mode_change_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB with non-level-extension mode.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 10000;
    const char V_FMT[] = "v%0100zu";

    // Write KVs.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for split.
    jungle::DebugParams d_params;
    // 10 ms per record.
    d_params.compactionDelayUs = 10*1000;
    jungle::setDebugParams(d_params);

    // L0 compaction.
    WorkerArgs w_args;
    w_args.db = db;
    w_args.type = WorkerArgs::L0;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_sec(1, "wait for worker to start");

    // Close DB, it should cancel the split.
    CHK_Z(jungle::DB::close(db));
    h.join();
    CHK_Z(h.getResult());

    // Open DB with level-extension mode.
    config.nextLevelExtension = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Remove delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Inter-level compaction should succeed even after cancel.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(c_opt, ii) );
    }

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    // Close & reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check again.
    CHK_Z(_get_keys(db, 0, NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, NUM-1, 1, "k%06zu", V_FMT));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int dual_file_compaction_cancel_mode_change_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB with non-level-extension mode.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 5000;
    const char V_FMT[] = "v%0100zu";

    // Write KVs.
    CHK_Z(_set_keys(db, 0, NUM, 1, "k%06zu", V_FMT));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Write more KVs.
    CHK_Z(_set_keys(db, NUM, 2*NUM, 1, "k%06zu", V_FMT));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for split.
    jungle::DebugParams d_params;
    // 10 ms per record.
    d_params.compactionDelayUs = 10*1000;
    jungle::setDebugParams(d_params);

    // L0 compaction.
    WorkerArgs w_args;
    w_args.db = db;
    w_args.type = WorkerArgs::L0;
    w_args.expResult = jungle::Status::COMPACTION_CANCELLED;
    TestSuite::ThreadHolder h(&w_args, compaction_worker, nullptr);
    TestSuite::sleep_sec(1, "wait for worker to start");

    // Close DB, it should cancel the split.
    CHK_Z(jungle::DB::close(db));
    h.join();
    CHK_Z(h.getResult());

    // Open DB with level-extension mode.
    config.nextLevelExtension = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check.
    CHK_Z(_get_keys(db, 0, 2*NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, 2*NUM-1, 1, "k%06zu", V_FMT));

    // Remove delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Inter-level compaction should succeed even after cancel.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(c_opt, ii) );
    }

    // Check again.
    CHK_Z(_get_keys(db, 0, 2*NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, 2*NUM-1, 1, "k%06zu", V_FMT));

    // Close & reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check again.
    CHK_Z(_get_keys(db, 0, 2*NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, 2*NUM-1, 1, "k%06zu", V_FMT));

    // Compact L0.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(c_opt, ii) );
    }

    // Check again.
    CHK_Z(_get_keys(db, 0, 2*NUM, 1, "k%06zu", V_FMT));
    CHK_Z(_iterate_keys(db, 0, 2*NUM-1, 1, "k%06zu", V_FMT));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int seq_itr_test(bool with_opened_itr) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB with non-level-extension mode.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 2000;
    const char K_FMT[] = "k%06zu";
    const char V_FMT[] = "%08zu_v%0100zu";

    std::vector<size_t> key_arr(NUM);
    std::iota(key_arr.begin(), key_arr.end(), 0);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(key_arr.begin(), key_arr.end(), g);

    // Write KVs.
    size_t seqnum = 0;
    char key_raw[256];
    char val_raw[256];
    for (size_t ii=0; ii<NUM; ++ii) {
        size_t idx = key_arr[ii];
        sprintf(key_raw, K_FMT, idx);
        sprintf(val_raw, V_FMT, ++seqnum, idx);
        db->set( jungle::KV(key_raw, val_raw) );
        if (ii % (NUM / 10) == 0) {
            CHK_Z(db->sync(false));
        }
    }
    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Open an iterator and keep it to block table file removal.
    jungle::Iterator initial_itr;
    if (with_opened_itr) {
        CHK_Z(initial_itr.initSN(db));
    }

    auto initial_itr_check = [&s, NUM, &initial_itr]() -> int {
        size_t count = 0;
        initial_itr.gotoBegin();
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            s = initial_itr.get(rec);
            if (!s) break;

            count++;
            std::string val_seq = rec.kv.value.toString().substr(0, 8);
            uint64_t cur_seq = std::atol(val_seq.c_str());
            CHK_EQ(count, cur_seq);
        } while (initial_itr.next().ok());
        CHK_EQ(NUM, count);
        return 0;
    };
    if (with_opened_itr) {
        CHK_Z(initial_itr_check());
    }

    // L0 -> L1 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }
    if (with_opened_itr) {
        CHK_Z(initial_itr_check());
    }

    // Write more KVs (even numbers).
    for (size_t ii=0; ii<NUM; ii+=2) {
        size_t idx = key_arr[ii];
        sprintf(key_raw, K_FMT, idx);
        sprintf(val_raw, V_FMT, ++seqnum, idx);
        db->set( jungle::KV(key_raw, val_raw) );
        if (ii % (NUM / 10) == 0) {
            CHK_Z(db->sync(false));
        }
    }
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    if (with_opened_itr) {
        CHK_Z(initial_itr_check());
    }

    {   // Do seq iteration.
        jungle::Iterator itr;
        itr.initSN(db);
        size_t count = 0;
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            s = itr.get(rec);
            if (!s) break;

            count++;
            std::string val_seq = rec.kv.value.toString().substr(0, 8);
            uint64_t cur_seq = std::atol(val_seq.c_str());
            CHK_EQ(count, cur_seq);

        } while (itr.next().ok());
        CHK_Z(itr.close());
        CHK_EQ( NUM * 3 / 2, count );
    }

    // Write more KVs (odd numbers).
    for (size_t ii=1; ii<NUM; ii+=2) {
        size_t idx = key_arr[ii];
        sprintf(key_raw, K_FMT, idx);
        sprintf(val_raw, V_FMT, ++seqnum, idx);
        db->set( jungle::KV(key_raw, val_raw) );
    }
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    if (with_opened_itr) {
        CHK_Z(initial_itr_check());
    }

    auto seq_check = [&s, NUM, db]() -> int {
        jungle::Iterator itr;
        itr.initSN(db);
        uint64_t prev_seq = 0;
        size_t count = 0;
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            s = itr.get(rec);
            if (!s) break;

            count++;
            std::string val_seq = rec.kv.value.toString().substr(0, 8);
            uint64_t cur_seq = std::atol(val_seq.c_str());
            CHK_GT(cur_seq, prev_seq);
            prev_seq = cur_seq;
        } while (itr.next().ok());
        CHK_Z(itr.close());
        CHK_GTEQ(count, NUM);
        return 0;
    };

    // Do L0 -> L1 compaction, and check in between.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(seq_check());
        CHK_Z(db->compactL0(c_opt, ii));
        if (with_opened_itr) {
            CHK_Z(initial_itr_check());
        }
    }
    // Final check.
    CHK_Z(seq_check());

    if (with_opened_itr) {
        CHK_Z(initial_itr.close());
    }
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db = nullptr;

    // Disable background threads.
    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.numCompactorThreads = 0;
    jungle::init(g_config);

    // Open DB with non-level-extension mode.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    config.bloomFilterBitsPerUnit = 10.0;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM = 2000;
    const char K_FMT[] = "k%06zu";
    const char V_FMT[] = "%08zu_v%0100zu";

    std::vector<size_t> key_arr(NUM);
    std::iota(key_arr.begin(), key_arr.end(), 0);

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(key_arr.begin(), key_arr.end(), g);

    // Write KVs.
    size_t seqnum = 0;
    char key_raw[256];
    char val_raw[256];
    for (size_t ii=0; ii<NUM; ++ii) {
        size_t idx = key_arr[ii];
        sprintf(key_raw, K_FMT, idx);
        sprintf(val_raw, V_FMT, ++seqnum, idx);
        db->set( jungle::KV(key_raw, val_raw) );
    }
    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // L0 -> L1 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Write more KVs (even numbers).
    for (size_t ii=0; ii<NUM; ii+=2) {
        size_t idx = key_arr[ii];
        sprintf(key_raw, K_FMT, idx);
        sprintf(val_raw, V_FMT, ++seqnum, idx);
        db->set( jungle::KV(key_raw, val_raw) );
    }
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Open a snapshot (latest one).
    jungle::DB* snap = nullptr;
    CHK_Z(db->openSnapshot(&snap));

    auto snap_check = [&s, NUM, snap]() -> int {
        jungle::Iterator itr;
        itr.initSN(snap);
        size_t count = 0;
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            s = itr.get(rec);
            if (!s) break;

            count++;
            std::string val_seq = rec.kv.value.toString().substr(0, 8);
            uint64_t cur_seq = std::atol(val_seq.c_str());
            CHK_EQ(count, cur_seq);

        } while (itr.next().ok());
        CHK_Z(itr.close());
        CHK_EQ( NUM * 3 / 2, count );
        return 0;
    };
    CHK_Z(snap_check());

    // Write more KVs (odd numbers).
    for (size_t ii=1; ii<NUM; ii+=2) {
        size_t idx = key_arr[ii];
        sprintf(key_raw, K_FMT, idx);
        sprintf(val_raw, V_FMT, ++seqnum, idx);
        db->set( jungle::KV(key_raw, val_raw) );
    }
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    CHK_Z(snap_check());

    // Do L0 -> L1 compaction, and check in between.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(snap_check());
        CHK_Z(db->compactL0(c_opt, ii));
    }
    // Final check.
    CHK_Z(snap_check());

    CHK_Z(jungle::DB::close(snap));
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace level_extension_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("next level basic test",
              next_level_basic_test);

    ts.doTest("interlevel compaction cancel test",
              interlevel_compaction_cancel_test);

    ts.doTest("inplace compaction cancel test",
              inplace_compaction_cancel_test);

    ts.doTest("split cancel test",
              split_cancel_test);

    ts.doTest("merge cancel test",
              merge_cancel_test);

    ts.doTest("single file compaction cancel with mode change test",
              single_file_compaction_cancel_mode_change_test);

    ts.doTest("dual file compaction cancel with mode change test",
              dual_file_compaction_cancel_mode_change_test);

    ts.doTest("sequence iterator test",
              seq_itr_test,
              TestRange<bool>({false, true}));

    ts.doTest("snapshot test",
              snapshot_test);

    return 0;
}

