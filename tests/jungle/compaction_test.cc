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

#include <fstream>

#include <stdio.h>

namespace compaction_test {

int single_thread_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 100;

    // Write even numbers.
    CHK_Z(_set_keys(db, 0, num, 2, "k%06zu", "v%06zu"));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Write odd numbers.
    CHK_Z(_set_keys(db, 1, num, 2, "k%06zu", "v%06zu"));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Point query check.
    CHK_Z(_get_keys(db, 0, num, 1, "k%06zu", "v%06zu"));

    // Range query check.
    CHK_Z(_iterate_keys(db, 0, num-1, 1, "k%06zu", "v%06zu"));

    // Compact more.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Check again.
    CHK_Z(_get_keys(db, 0, num, 1, "k%06zu", "v%06zu"));
    CHK_Z(_iterate_keys(db, 0, num-1, 1, "k%06zu", "v%06zu"));

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Check again.
    CHK_Z(_get_keys(db, 0, num, 1, "k%06zu", "v%06zu"));
    CHK_Z(_iterate_keys(db, 0, num-1, 1, "k%06zu", "v%06zu"));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

struct WriterArgs : TestSuite::ThreadArgs {
    WriterArgs()
        : durationSec(1)
        , num(1000)
        , db(nullptr)
        , stopSignal(false)
        , rptResult(0)
        {}
    size_t durationSec;
    size_t num;
    jungle::DB* db;
    std::atomic<bool> stopSignal;
    size_t rptResult;
};

int writer(TestSuite::ThreadArgs* t_args) {
    WriterArgs* args = static_cast<WriterArgs*>(t_args);
    TestSuite::Timer tt(args->durationSec * 1000);

    size_t rpt = 0;
    while (!tt.timeover() && !args->stopSignal) {
        rpt++;
        CHK_Z( _set_keys( args->db, 0, args->num, 1,
                         "k%06zu", "v%06zu_" + std::to_string(rpt) ) );
        // Sync & flush.
        CHK_Z(args->db->sync(false));
        CHK_Z(args->db->flushLogs(jungle::FlushOptions()));
    }
    args->rptResult = rpt;
    return 0;
}

int concurrent_writer_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.nextLevelExtension = false;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t duration_sec = 1;
    size_t num = 1000;

    WriterArgs w_args;
    w_args.db = db;
    w_args.num = num;
    w_args.durationSec = duration_sec;
    TestSuite::ThreadHolder h(&w_args, writer, nullptr);

    TestSuite::Timer tt(duration_sec * 1000);
    while (!tt.timeover()) {
        jungle::CompactOptions c_opt;
        for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
            CHK_Z(db->compactL0(c_opt, ii));
        }
        TestSuite::sleep_ms(100);
    }

    h.join();
    CHK_Z(h.getResult());

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Check.
    size_t rpt = w_args.rptResult;
    CHK_Z(_get_keys(db, 0, num, 1, "k%06zu", "v%06zu_" + std::to_string(rpt)));
    CHK_Z(_iterate_keys(db, 0, num-1, 1, "k%06zu", "v%06zu_" + std::to_string(rpt)));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int irrelevant_table_file_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 100;

    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Create dummy table files: 4 and 5.
    for ( std::string suffix:
          {"/table0000_00000004", "/table0000_00000005"} ) {
        std::string table_file = filename + suffix;
        std::ofstream fs_out;
        fs_out.open(table_file);
        CHK_OK(fs_out.good());
        fs_out.write("garbage", 7);
        fs_out.close();
    }

    // Do compaction.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    // Close.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int auto_compactor_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numCompactorThreads = 1;
    g_config.compactorSleepDuration_ms = 5000;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 400000;

    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Sleep 10 secs.
    TestSuite::sleep_sec(20, "compactor");

    // Close.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

jungle::CompactionCbDecision callback(const jungle::CompactionCbParams& params) {
    // Drops all odd number KVs.
    std::string num_str = std::string((char*)params.rec.kv.key.data + 1,
                                      params.rec.kv.key.size - 1);
    size_t num = atoi(num_str.c_str());
    if (num % 2 == 1) {
        return jungle::CompactionCbDecision::DROP;
    }
    return jungle::CompactionCbDecision::KEEP;
}

int callback_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    // This callback function will drop all odd number KVs.
    config.compactionCbFunc = callback;
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 100;

    CHK_Z(_set_keys(db, 0, num, 1, "k%06zu", "v%06zu"));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    for (size_t ii=0; ii<num; ++ii) {
        char key_str[256];
        sprintf(key_str, "k%06zu", ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val;
        jungle::SizedBuf::Holder h_val(val);
        if (ii % 2 == 0) {
            CHK_Z( db->get(key, val) );
        } else {
            CHK_NOT( db->get(key, val) );
        }
    }

    jungle::Iterator itr;
    CHK_Z( itr.init(db) );
    size_t idx = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h_rec(rec);
        s = itr.get(rec);
        if (!s) break;

        char key_str[256];
        sprintf(key_str, "k%06zu", idx);
        jungle::SizedBuf key(key_str);

        CHK_EQ(key, rec.kv.key);
        idx += 2;

    } while (itr.next().ok());
    CHK_Z( itr.close() );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int callback_l1_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::GlobalConfig g_config;
    g_config.compactorSleepDuration_ms = 100;
    g_config.numCompactorThreads = 4;
    jungle::init(g_config);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)

    // This callback function will drop all odd number KVs.
    bool callback_invoked = false;
    bool min_max_key_check = true;
    auto cb_func = [&](const jungle::CompactionCbParams& params) {
        if (params.maxKey.empty() || params.minKey.empty()) {
            min_max_key_check = false;
        }

        // Drops all odd number KVs.
        callback_invoked = true;
        std::string num_str = std::string((char*)params.rec.kv.key.data + 1,
                                          params.rec.kv.key.size - 1);
        size_t num = atoi(num_str.c_str());
        if (num % 2 == 1) {
            return jungle::CompactionCbDecision::DROP;
        }
        return jungle::CompactionCbDecision::KEEP;
    };

    config.compactionCbFunc = cb_func;
    config.numL0Partitions = 4;
    config.compactionFactor = 150;
    config.nextLevelExtension = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 1000;
    jungle::CompactOptions c_opt;

    CHK_Z(_set_keys(db, 0, num, 1, "k%06zu", "v%06zu"));

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Callback shouldn't be invoked for L0 compaction if `nextLevelExtension` is true.
    CHK_FALSE(callback_invoked);

    // Do full compaction.
    jungle::DBStats db_stats;
    CHK_Z(db->getStats(db_stats));
    uint64_t cur_max_table_idx = db_stats.maxTableIndex;

    CHK_Z(db->compactIdxUpto(c_opt, cur_max_table_idx));

    // Wait until min table index becomes greater than the current max index.
    size_t tick = 0;
    const size_t MAX_TICK = 10;
    do {
        jungle::DBStats db_stats;
        CHK_Z(db->getStats(db_stats));
        if (db_stats.minTableIndex > cur_max_table_idx) {
            break;
        }
        std::stringstream ss;
        ss << "min table index: " << db_stats.minTableIndex
           << ", max table index: " << cur_max_table_idx
           << ", tick: " << tick;
        TestSuite::sleep_ms(500, ss.str());
        tick++;
    } while (tick < MAX_TICK);
    CHK_SM(tick, MAX_TICK);

    // Min max keys should have been given.
    CHK_TRUE(min_max_key_check);

    for (size_t ii=0; ii<num; ++ii) {
        char key_str[256];
        sprintf(key_str, "k%06zu", ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val;
        jungle::SizedBuf::Holder h_val(val);
        if (ii % 2 == 0) {
            CHK_Z( db->get(key, val) );
        } else {
            CHK_NOT( db->get(key, val) );
        }
    }

    jungle::Iterator itr;
    CHK_Z( itr.init(db) );
    size_t idx = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h_rec(rec);
        s = itr.get(rec);
        if (!s) break;

        char key_str[256];
        sprintf(key_str, "k%06zu", idx);
        jungle::SizedBuf key(key_str);

        CHK_EQ(key, rec.kv.key);
        idx += 2;

    } while (itr.next().ok());
    CHK_Z( itr.close() );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int tombstone_compaction_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Delete key 50
    jungle::SizedBuf key_to_del("k000050");
    CHK_Z( db->del(key_to_del) );
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Check before compaction.
    for (size_t ii=0; ii<num; ++ii) {
        char key_str[256];
        sprintf(key_str, "k%06zu", ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val;
        jungle::SizedBuf::Holder h_val(val);
        if (ii != 50) {
            CHK_Z( db->get(key, val) );
        } else {
            CHK_NOT( db->get(key, val) );
        }
    }

    // Compaction again.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Check after copmaction.
    for (size_t ii=0; ii<num; ++ii) {
        char key_str[256];
        sprintf(key_str, "k%06zu", ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val;
        jungle::SizedBuf::Holder h_val(val);
        if (ii != 50) {
            CHK_Z( db->get(key, val) );
        } else {
            CHK_NOT( db->get(key, val) );
        }
    }

    jungle::Iterator itr;
    CHK_Z( itr.init(db) );
    size_t idx = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h_rec(rec);
        s = itr.get(rec);
        if (!s) break;

        char key_str[256];
        sprintf(key_str, "k%06zu", idx);
        jungle::SizedBuf key(key_str);

        CHK_EQ(key, rec.kv.key);
        if (idx == 49) idx += 2;
        else idx++;

    } while (itr.next().ok());
    CHK_Z( itr.close() );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;

    return 0;
}

struct CompactorArgs : TestSuite::ThreadArgs {
    CompactorArgs()
        : db(nullptr)
        , hashNum(0)
        , numPartitions(4)
        , expResult(jungle::Status::COMPACTION_CANCELLED) {}
    jungle::DB* db;
    size_t hashNum;
    size_t numPartitions;
    jungle::Status expResult;
};

int compactor(TestSuite::ThreadArgs* t_args) {
    CompactorArgs* args = static_cast<CompactorArgs*>(t_args);
    jungle::CompactOptions c_opt;

    if (args->hashNum != _SCU32(-1)) {
        // Hash number is given.
        jungle::Status s = args->db->compactL0(c_opt, args->hashNum);
        CHK_EQ(args->expResult, s);

    } else {
        // Not given, compact all.
        for (size_t ii=0; ii<args->numPartitions; ++ii) {
            jungle::Status s = args->db->compactL0(c_opt, ii);
            CHK_EQ(args->expResult, s);
        }
    }
    return 0;
}

int compaction_cancel_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 500000; // 0.5 sec per record.
    jungle::setDebugParams(d_params);

    // Do compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);

    TestSuite::sleep_sec(1, "Wait for compaction to start");

    // Close DB while copmaction is in progress,
    // compaction should be cancelled immediately.
    CHK_Z(jungle::DB::close(db));

    h.join();
    CHK_Z(h.getResult());

    // Re-open.
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Remove compaction delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Compaction should work this time.
    c_args.db = db;
    c_args.expResult = jungle::Status::OK;
    CHK_Z( compactor(&c_args) );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    CHK_Z( jungle::DB::close(db) );

    // Re-open and check again.
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compaction_cancel_and_adjust_l0_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 500000; // 0.5 sec per record.
    jungle::setDebugParams(d_params);

    // Do compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);

    TestSuite::sleep_sec(1, "Wait for compaction to start");

    // Put more keys.
    CHK_Z( _set_keys(db, num, 2 * num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Close DB while copmaction is in progress,
    // compaction should be cancelled immediately.
    CHK_Z(jungle::DB::close(db));

    h.join();
    CHK_Z(h.getResult());

    // Remove compaction delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Re-open with different num L0.
    config.numL0Partitions = 1;
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, 2 * num, 1, "k%06zu", "v%06zu") );

    // Compaction should work this time.
    c_args.db = db;
    c_args.expResult = jungle::Status::OK;
    CHK_Z( compactor(&c_args) );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, 2 * num, 1, "k%06zu", "v%06zu") );

    CHK_Z( jungle::DB::close(db) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int adjust_l0_crash_and_retry_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = true;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    CHK_Z(jungle::DB::close(db));

    // Set callback for adjusting L0.
    size_t count = 0;
    std::string backup_path = filename + "_backup";
    std::string backup_path2 = filename + "_backup2";

    jungle::DebugParams d_params;
    d_params.adjustL0Cb =
        [&](const jungle::DebugParams::GenericCbParams& params) -> void {
        if (count == 0) {
            // Copy the DB file to mimic crash.
            TestSuite::copyfile(filename, backup_path);
            TestSuite::copyfile(filename, backup_path2);
        }
        count++;
    };
    jungle::setDebugParams(d_params);

    // Re-open with different L0.
    config.numL0Partitions = 1;
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    // Open the backup file with different num L0.
    config.numL0Partitions = 2;
    CHK_Z( jungle::DB::open(&db, backup_path, config) );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    // Open the backup file with the original num L0 to mimic cancelling adjustment.
    config.numL0Partitions = 4;
    CHK_Z( jungle::DB::open(&db, backup_path2, config) );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int merge_compaction_cancel_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction once.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 500000; // 0.5 sec per record.
    jungle::setDebugParams(d_params);

    // Do compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);

    TestSuite::sleep_sec(1, "Wait for compaction to start");

    // Close DB while copmaction is in progress,
    // compaction should be cancelled immediately.
    CHK_Z(jungle::DB::close(db));

    h.join();
    CHK_Z(h.getResult());

    // Re-open.
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Remove compaction delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Compaction should work this time.
    c_args.db = db;
    c_args.expResult = jungle::Status::OK;
    CHK_Z( compactor(&c_args) );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    CHK_Z( jungle::DB::close(db) );

    // Re-open and check again.
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

jungle::CompactionCbDecision
    callback2(const jungle::CompactionCbParams& params, bool* flag)
{
    // To check whether compaction happened or not.
    if (flag) *flag = true;
    return jungle::CompactionCbDecision::KEEP;
}

int compaction_cancel_recompact_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 500000; // 0.5 sec per record.
    jungle::setDebugParams(d_params);

    // Do compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);

    TestSuite::sleep_sec(1, "Wait for compaction to start");

    // Close DB while copmaction is in progress,
    // compaction should be cancelled immediately.
    CHK_Z(jungle::DB::close(db));

    h.join();
    CHK_Z(h.getResult());

    // Completely shutdown and re-open with background compactor.
    CHK_Z(jungle::shutdown());
    jungle::GlobalConfig g_conf;
    g_conf.compactorSleepDuration_ms = 500;
    g_conf.numCompactorThreads = 1;
    CHK_Z( jungle::init(g_conf) );

    bool compaction_happened = false;
    config.compactionCbFunc = std::bind( callback2,
                                         std::placeholders::_1,
                                         &compaction_happened );

    // Re-open.
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Remove compaction delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Compaction should work this time.
    TestSuite::sleep_sec(1, "Wait for bg compaction");

    CHK_TRUE( compaction_happened );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    CHK_Z( jungle::DB::close(db) );

    // Re-open and check again.
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int merge_compaction_cancel_recompact_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction once.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 500000; // 0.5 sec per record.
    jungle::setDebugParams(d_params);

    // Do compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);

    TestSuite::sleep_sec(1, "Wait for compaction to start");

    // Close DB while copmaction is in progress,
    // compaction should be cancelled immediately.
    CHK_Z(jungle::DB::close(db));

    h.join();
    CHK_Z(h.getResult());

    // Completely shutdown and re-open with background compactor.
    CHK_Z(jungle::shutdown());
    jungle::GlobalConfig g_conf;
    g_conf.compactorSleepDuration_ms = 500;
    g_conf.numCompactorThreads = 1;
    CHK_Z( jungle::init(g_conf) );

    bool compaction_happened = false;
    config.compactionCbFunc = std::bind( callback2,
                                         std::placeholders::_1,
                                         &compaction_happened );

    // Re-open.
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Remove compaction delay.
    d_params.compactionDelayUs = 0;
    jungle::setDebugParams(d_params);

    // Compaction should work this time.
    TestSuite::sleep_sec(1, "Wait for bg compaction");

    CHK_TRUE( compaction_happened );

    // All keys should be there.
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    CHK_Z( jungle::DB::close(db) );

    // Re-open and check again.
    CHK_Z( jungle::DB::open(&db, filename, config) );
    CHK_Z( _get_keys(db, 0, num, 1, "k%06zu", "v%06zu") );
    CHK_Z( jungle::DB::close(db) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int newest_value_after_compaction_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Do L0 compaction once.
    jungle::CompactOptions c_opt;
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Update even numbers.
    CHK_Z( _set_keys(db, 0, num, 2, "k%06zu", "even_%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    CHK_Z( _get_keys(db, 1, num, 2, "k%06zu", "v%06zu") );
    CHK_Z( _get_keys(db, 0, num, 2, "k%06zu", "even_%06zu") );

    // Do L0 compaction again.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(c_opt, ii));
    }

    // Update odd numbers.
    CHK_Z( _set_keys(db, 1, num, 2, "k%06zu", "odd_%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    CHK_Z( _get_keys(db, 1, num, 2, "k%06zu", "odd_%06zu") );
    CHK_Z( _get_keys(db, 0, num, 2, "k%06zu", "even_%06zu") );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int newest_value_during_compaction_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.nextLevelExtension = false;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 5000; // 5 ms per record.
    jungle::setDebugParams(d_params);

    // Do L0 compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    c_args.hashNum = _SCU32(-1);
    c_args.expResult = jungle::Status();

    // It will take 500 ms.
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);
    TestSuite::sleep_ms(100);

    // Update even numbers.
    CHK_Z( _set_keys(db, 0, num, 2, "k%06zu", "even_%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    CHK_Z( _get_keys(db, 1, num, 2, "k%06zu", "v%06zu") );
    CHK_Z( _get_keys(db, 0, num, 2, "k%06zu", "even_%06zu") );

    h.join();
    CHK_Z( h.getResult() );

    // Do L0 compaction again, it will take 500 ms.
    TestSuite::ThreadHolder h2(&c_args, compactor, nullptr);
    TestSuite::sleep_ms(100);

    // Update odd numbers.
    CHK_Z( _set_keys(db, 1, num, 2, "k%06zu", "odd_%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    CHK_Z( _get_keys(db, 1, num, 2, "k%06zu", "odd_%06zu") );
    CHK_Z( _get_keys(db, 0, num, 2, "k%06zu", "even_%06zu") );

    h2.join();
    CHK_Z( h2.getResult() );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace compaction_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("compaction single thread test",
              single_thread_test);

    ts.doTest("compaction with writer thread test",
              concurrent_writer_test);

    ts.doTest("irrelevant table file test",
              irrelevant_table_file_test);

    ts.doTest("callback test",
              callback_test);

    ts.doTest("callback L1 test",
              callback_l1_test);

    ts.doTest("tombstone compaction test",
              tombstone_compaction_test);

    ts.doTest("compaction cancel test",
              compaction_cancel_test);

    ts.doTest("compaction cancel and adjust L0 test",
              compaction_cancel_and_adjust_l0_test);

    ts.doTest("adjust L0 crash and retry test",
              adjust_l0_crash_and_retry_test);

    ts.doTest("merge-compaction cancel test",
              merge_compaction_cancel_test);

    ts.doTest("compaction cancel recompact test",
              compaction_cancel_recompact_test);

    ts.doTest("merge-compaction cancel recompact test",
              merge_compaction_cancel_recompact_test);

    ts.doTest("newest value after compaction test",
              newest_value_after_compaction_test);

    ts.doTest("newest value during compaction test",
              newest_value_during_compaction_test);

#if 0
    ts.doTest("auto compactor test",
              auto_compactor_test);
#endif

    return 0;
}

