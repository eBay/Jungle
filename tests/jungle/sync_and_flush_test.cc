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

#include <vector>

#include <stdio.h>

namespace sync_and_flush_test {

int async_flush_test_cb(size_t* counter,
                        size_t expected_count,
                        jungle::Status s,
                        void* ctx)
{
    CHK_Z(s);

    (*counter)++;
    if (*counter == expected_count) {
        EventAwaiter* ea = reinterpret_cast<EventAwaiter*>(ctx);
        ea->invoke();
    }
    return 0;
}

int async_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherSleepDuration_ms = 1000;
    jungle::init(g_config);

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync and async flush.
    CHK_Z(db->sync());

    size_t counter = 0;
    size_t expected_count = 5;
    EventAwaiter ea;

    for (size_t ii=0; ii<expected_count; ++ii) {
        CHK_Z( db->flushLogsAsync
                   ( jungle::FlushOptions(),
                     std::bind( async_flush_test_cb,
                                &counter,
                                expected_count,
                                std::placeholders::_1,
                                std::placeholders::_2 ),
                     &ea ) );
    }
    // Wait for handler.
    ea.wait();

    // All callbacks should have been invoked.
    CHK_EQ( expected_count, counter );

    // Get.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Invoke async flush and close DB without waiting.
    CHK_Z(db->flushLogsAsync(jungle::FlushOptions(), nullptr, nullptr));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int async_flush_verbose_test(bool debug_level) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherSleepDuration_ms = 1000;
    jungle::init(g_config);

    config.logSectionOnly = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    size_t NUM = 1000;
    std::vector<jungle::KV> kv(NUM);
    CHK_Z(_init_kv_pairs(NUM, kv, "key", "value"));

    CHK_EQ(4, db->getLogLevel());
    if (debug_level) {
        db->setLogLevel(5);
        CHK_EQ(5, db->getLogLevel());
    }

    const size_t EXP_COUNT = 11;
    for (size_t ii=0; ii<NUM; ii+=EXP_COUNT) {
        size_t counter = 0;
        EventAwaiter ea;

        size_t upto = std::min(ii+EXP_COUNT, NUM);
        for (size_t jj = ii; jj < upto; ++jj) {
            CHK_Z( db->setSN(jj+1, kv[jj]) );

            jungle::FlushOptions f_opt;
            f_opt.syncOnly = true;
            f_opt.callFsync = false;
            CHK_Z( db->flushLogsAsync
                       ( f_opt,
                         std::bind( async_flush_test_cb,
                                    &counter,
                                    upto - ii,
                                    std::placeholders::_1,
                                    std::placeholders::_2 ),
                         &ea,
                         jj ) );
        }

        // Wait for handler.
        ea.wait();
        // All callbacks should have been invoked.
        CHK_EQ( upto - ii, counter );
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    _free_kv_pairs(NUM, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int async_flush_verbose_with_delay_test(bool debug_level) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherSleepDuration_ms = 500;
    jungle::init(g_config);

    config.logSectionOnly = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    size_t NUM = 10000;
    std::vector<jungle::KV> kv(NUM);
    CHK_Z(_init_kv_pairs(NUM, kv, "key", "value"));

    CHK_EQ(4, db->getLogLevel());
    if (debug_level) {
        db->setLogLevel(5);
        CHK_EQ(5, db->getLogLevel());
    }

    TestSuite::Timer timer(1050);
    TestSuite::WorkloadGenerator wg(1000);
    for (size_t ii=0; ii<NUM; ) {
        if (timer.timeout()) break;

        size_t todo = wg.getNumOpsToDo();
        if (!todo) {
            TestSuite::sleep_ms(1);
            continue;
        }

        CHK_Z( db->setSN(ii+1, kv[ii]) );

        jungle::FlushOptions f_opt;
        f_opt.syncOnly = true;
        f_opt.callFsync = false;
        f_opt.execDelayUs = 100*1000;
        CHK_Z( db->flushLogsAsync( f_opt, nullptr, nullptr ) );

        wg.addNumOpsDone(1);
        ii += 1;
    }
    CHK_Z(jungle::DB::close(db));

    // Wait one more second to see if flusher can handle stale request.
    TestSuite::sleep_ms(1000);

    CHK_Z(jungle::shutdown());
    _free_kv_pairs(NUM, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int flush_beyond_sync_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 7;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t NUM = 100;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "key" + std::to_string(ii);
        std::string val_str = "val" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        if (ii == NUM/2) {
            // Sync logs in the middle.
            CHK_Z( db->sync(false) );
        }
    }

    for (size_t ii=0; ii<NUM; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        std::string key_str = "key" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        std::string value_exp = "val" + std::to_string(ii);
        CHK_Z( db->get( key_str, value_out) );
        CHK_EQ( value_exp, value_out.toString() );
    }

    // Normal flush (upto last sync).
    jungle::FlushOptions f_opt;
    CHK_Z( db->flushLogs(f_opt) );

    // Flush should have done upto the last sync.
    uint64_t seq_num_out = 0;
    CHK_Z( db->getLastFlushedSeqNum(seq_num_out) );
    CHK_EQ(NUM/2 + 1, seq_num_out);

    // Flush beyond sync (upto the latest).
    f_opt.beyondLastSync = true;
    CHK_Z( db->flushLogs(f_opt) );

    // Flush should have done upto the latest.
    CHK_Z( db->getLastFlushedSeqNum(seq_num_out) );
    CHK_EQ(NUM, seq_num_out);

    // Close DB without sync.
    CHK_Z( jungle::DB::close(db) );

    // Reopen.
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Put more logs.
    for (size_t ii=NUM; ii<NUM+3; ++ii) {
        std::string key_str = "key" + std::to_string(ii);
        std::string val_str = "val" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Check, all data should exist.
    for (size_t ii=0; ii<NUM+3; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        std::string key_str = "key" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        std::string value_exp = "val" + std::to_string(ii);
        CHK_Z( db->get( key_str, value_out) );
        CHK_EQ( value_exp, value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );

    // Free all resources for jungle.
    jungle::shutdown();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int common_prefix_l1_flush_by_fast_scan_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.maxEntriesInLogFile = 1000;
    config.bloomFilterBitsPerUnit = 10;
    config.minFileSizeToCompact = 65536;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 10000;

    auto get_key = [&](size_t idx) -> std::string {
        std::string key_str;
        if (idx < 2500) {
            key_str = "prefix1_user_transaction_" + TestSuite::lzStr(9, idx);
        } else if (idx < 5000) {
            key_str = "prefix2_user_sessions_2021-01-01_" +
                      TestSuite::lzStr(9, idx);
        } else if (idx < 7500) {
            key_str = "prefix2_user_sessions_2021-01-02_" +
                      TestSuite::lzStr(9, idx);
        } else {
            key_str = "prefix2_user_sessions_2021-01-03_" +
                      TestSuite::lzStr(9, idx);
        }
        return key_str;
    };

    // Insert key-value pair.
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t idx = ii;
        std::string key_str = get_key(idx);
        std::string val_str = "val" + TestSuite::lzStr(9, idx);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    auto verify_func = [&](bool verify_new_key) -> int {
        for (size_t ii = 0; ii < NUM; ++ii) {
            TestSuite::setInfo("old key %zu", ii);
            std::string key_str = get_key(ii);;
            std::string val_str = "val" + TestSuite::lzStr(9, ii);

            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get(key_str, value_out) );
            CHK_EQ(val_str, value_out.toString());
        }

        if (verify_new_key) {
            for (size_t ii = 0; ii < NUM; ++ii) {
                TestSuite::setInfo("new key %zu", ii);
                std::string key_str = "prefix2_user_sessions_2021-01-04_" +
                                      TestSuite::lzStr(9, ii);
                std::string val_str = "val" + TestSuite::lzStr(9, ii);

                jungle::SizedBuf value_out;
                jungle::SizedBuf::Holder h(value_out);
                CHK_Z( db->get(key_str, value_out) );
                CHK_EQ(val_str, value_out.toString());
            }
        }

        return 0;
    };

    // Flush to L0.
    db->sync(false);
    db->flushLogs();

    // Flush to L1 and verify.
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }
    // Verify keys.
    CHK_Z(verify_func(false));

    // Close and reopen with fast index scan option.
    CHK_Z(jungle::DB::close(db));
    config.fastIndexScan = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Insert new key sets with newer date.
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t idx = ii;
        std::string key_str = "prefix2_user_sessions_2021-01-04_" +
                              TestSuite::lzStr(9, idx);
        std::string val_str = "val" + TestSuite::lzStr(9, idx);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    // Flush to L0.
    db->sync(false);
    db->flushLogs();

    // Flush to L1 and verify.
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }
    // Verify keys, including newly inserted ones.
    CHK_Z(verify_func(true));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_flush_add_new_file_race_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    auto insert_keys = [&](size_t from, size_t to) {
        for (size_t ii = from; ii < to; ++ii) {
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii);
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        }
        return 0;
    };

    CHK_Z( insert_keys(0, 30) );

    EventAwaiter ea_add_new_file;
    EventAwaiter ea_main;
    const size_t WAIT_TIME_MS = 10 * 1000;

    // Enable debugging hook for new log file and log flush.
    jungle::DebugParams dp;
    dp.addNewLogFileCb = [&](const jungle::DebugParams::GenericCbParams& pp) {
        std::thread tt([&]() {
            // Right after adding a new log file and right before
            // pushing a new record to the new file, initiate log flushing.
            jungle::FlushOptions f_opt;
            f_opt.beyondLastSync = true;
            db->flushLogs(f_opt);
        });
        tt.detach();
        ea_add_new_file.wait_ms(WAIT_TIME_MS);
    };
    dp.logFlushCb = [&](const jungle::DebugParams::GenericCbParams& pp) {
        // Right after log flushing, add a few more records.
        ea_add_new_file.invoke();
        insert_keys(31, 35);
        ea_main.invoke();
    };
    jungle::DB::setDebugParams(dp);
    jungle::DB::enableDebugCallbacks(true);

    // Insert one more record, it will trigger above debug callbacks.
    CHK_Z( insert_keys(30, 31) );

    ea_main.wait_ms(WAIT_TIME_MS);

    auto verify = [&](size_t upto) {
        for (size_t ii = 0; ii < upto; ++ii) {
            TestSuite::setInfo("ii=%zu", ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii);
            CHK_Z( db->get(jungle::SizedBuf(key_str), value_out) );
            CHK_EQ(val_str, value_out.toString());
        }
        return 0;
    };
    CHK_Z( verify(35) );

    // Flush to L0 and verify.
    do {
        s = db->sync(false);
        if (s.ok()) break;
        TestSuite::sleep_ms(100);
    } while (s == jungle::Status::OPERATION_IN_PROGRESS);
    CHK_Z( s );
    CHK_Z( db->flushLogs() );
    CHK_Z( verify(35) );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int serialized_sync_and_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.serializeMultiThreadedLogFlush = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    auto insert_keys = [&](size_t from, size_t to) {
        for (size_t ii = from; ii < to; ++ii) {
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii);
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        }
        return 0;
    };

    CHK_Z( insert_keys(0, 10) );

    const size_t WAIT_TIME_MS = 3600 * 1000;
    EventAwaiter ea_main;

    // Enable debugging hook for new log file and log flush.
    jungle::DebugParams dp;
    std::thread* t1(nullptr);
    std::thread* t2(nullptr);
    jungle::Status t1_res(jungle::Status::ERROR), t2_res(jungle::Status::ERROR);
    std::atomic<bool> sync_cb_invoked(false), flush_cb_invoked(false);

    dp.syncCb = [&](const jungle::DebugParams::GenericCbParams& pp) {
        bool exp = false;
        bool des = true;
        // This CB is executed only once, so only 2 threads are racing.
        if (sync_cb_invoked.compare_exchange_strong(exp, des)) {
            t1 = new std::thread([&]() {
                // Let another thread sync simultaneously.
                // This thread should be blocked by the main thread.
                t1_res = db->sync(false);

                // Once it's done, resume the main thread flow.
                ea_main.invoke();
            });
        }
    };

    dp.logFlushCb = [&](const jungle::DebugParams::GenericCbParams& pp) {
        bool exp = false;
        bool des = true;
        // This CB is executed only once, so only 2 threads are racing.
        if (flush_cb_invoked.compare_exchange_strong(exp, des)) {
            t2 = new std::thread([&]() {
                // Let another thread flush simultaneously.
                // This thread should be blocked by the main thread.
                jungle::FlushOptions f_opt;
                t2_res = db->flushLogs(f_opt);

                // Once it's done, resume the main thread flow.
                ea_main.invoke();
            });
        }
    };
    jungle::DB::setDebugParams(dp);
    jungle::DB::enableDebugCallbacks(true);

    // Do sync.
    CHK_Z(db->sync(false));
    ea_main.wait_ms(WAIT_TIME_MS);
    ea_main.reset();
    // Both thread should succeed without `OPERATION_IN_PROGRESS`.
    CHK_Z(t1_res);

    // Do flush.
    jungle::FlushOptions f_opt;
    CHK_Z(db->flushLogs(f_opt));
    ea_main.wait_ms(WAIT_TIME_MS);
    ea_main.reset();
    // Both thread should succeed without `OPERATION_IN_PROGRESS`.
    CHK_Z(t2_res.getValue());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    if (t1->joinable()) {
        t1->join();
    }
    delete t1;
    if (t2->joinable()) {
        t2->join();
    }
    delete t2;

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int disabling_auto_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db1 = nullptr;
    jungle::DB* db2 = nullptr;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherMinRecordsToTrigger = 100;
    jungle::init(g_config);

    config.maxEntriesInLogFile = 100;
    std::string db1_file = filename + "_db1";
    CHK_Z(jungle::DB::open(&db1, db1_file, config));

    config.autoLogFlush = false;
    std::string db2_file = filename + "_db2";
    CHK_Z(jungle::DB::open(&db2, db2_file, config));

    auto insert_keys = [&](size_t from, size_t to) {
        for (size_t ii = from; ii < to; ++ii) {
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii);
            CHK_Z( db1->set( jungle::KV(key_str, val_str) ) );
            CHK_Z( db2->set( jungle::KV(key_str, val_str) ) );
        }
        return 0;
    };

    CHK_Z( insert_keys(0, 200) );

    // Wait longer than flusher sleep time.
    TestSuite::sleep_ms(g_config.flusherSleepDuration_ms * 2, "wait for flusher..");

    uint64_t db1_flushed_seqnum = 0, db2_flushed_seqnum = 0;
    CHK_Z(db1->getLastFlushedSeqNum(db1_flushed_seqnum));
    CHK_EQ(jungle::Status::INVALID_SEQNUM,
           db2->getLastFlushedSeqNum(db2_flushed_seqnum).getValue());

    CHK_Z(jungle::DB::close(db1));
    CHK_Z(jungle::DB::close(db2));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int sync_flush_race_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db = nullptr;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherMinRecordsToTrigger = 100;
    jungle::init(g_config);

    config.maxEntriesInLogFile = 10;
    std::string db_file = filename + "_db1";
    CHK_Z(jungle::DB::open(&db, db_file, config));

    auto insert_keys = [&](size_t from, size_t to) {
        for (size_t ii = from; ii < to; ++ii) {
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii);
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        }
        return 0;
    };

    // Write 10 log files.
    CHK_Z( insert_keys(0, 100) );

    // Sync and flush (but 5 files only).
    CHK_Z( db->sync(false) );
    uint64_t last_synced_log_file_num = 0;
    CHK_Z( db->getLastSyncedLogFileNum(last_synced_log_file_num) );

    jungle::FlushOptions f_opt;
    f_opt.beyondLastSync = true;
    f_opt.numFilesLimit = 5;
    CHK_Z( db->flushLogs(f_opt) );

    uint64_t last_synced_log_file_num2 = 0;
    CHK_Z( db->getLastSyncedLogFileNum(last_synced_log_file_num2) );
    CHK_EQ(last_synced_log_file_num, last_synced_log_file_num2);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int sync_skip_and_reload_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db = nullptr;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherMinRecordsToTrigger = 100;
    jungle::init(g_config);

    config.maxEntriesInLogFile = 10;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    auto insert_keys = [&](size_t from, size_t to, size_t suffix) {
        for (size_t ii = from; ii < to; ++ii) {
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii) +
                                  "_" + std::to_string(suffix);
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        }
        return 0;
    };

    // Write 2 log files with suffix 1.
    CHK_Z( insert_keys(0, 20, 1) );

    // Sync those 2 files.
    CHK_Z( db->sync(true) );

    // Write 2 more log files with suffix 2.
    CHK_Z( insert_keys(0, 20, 2) );

    // Open a snapshot to avoid file removal.
    jungle::DB* snap = nullptr;
    CHK_Z( db->openSnapshot(&snap) );

    // Flush all 4 files.
    jungle::FlushOptions f_opt;
    f_opt.beyondLastSync = true;
    f_opt.numFilesLimit = 5;
    CHK_Z( db->flushLogs(f_opt) );

    // Write 2 more log files with different keys.
    CHK_Z( insert_keys(20, 40, 1) );
    CHK_Z( db->sync(true) );

    // Verify keys, they should return suffix 2.
    auto verify_keys = [](jungle::DB* db, size_t from, size_t to, size_t suffix) {
        for (size_t ii = from; ii < to; ++ii) {
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii) +
                                  "_" + std::to_string(suffix);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get(key_str, value_out) );
            CHK_EQ(val_str, value_out.toString());
        }
        return 0;
    };
    CHK_Z( verify_keys(db, 0, 20, 2) );

    // Copy file at this moment to mimic crash.
    std::string clone_path = filename + "_clone";
    TestSuite::copyfile(filename, clone_path);

    // Close snaphsot.
    CHK_Z( jungle::DB::close(snap) );

    // Close DB.
    CHK_Z( jungle::DB::close(db) );

    // Open clone.
    jungle::DB* db2 = nullptr;
    CHK_Z( jungle::DB::open(&db2, clone_path, config) );

    // Verify keys again, they should return suffix 2.
    CHK_Z( verify_keys(db2, 0, 20, 2) );

    CHK_Z(jungle::DB::close(db2));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

}
using namespace sync_and_flush_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("async flush test", async_flush_test);
    ts.doTest("async flush verbose test", async_flush_verbose_test,
              TestRange<bool>( {false, true} ) );
    ts.doTest("async flush verbose with delay test",
              async_flush_verbose_with_delay_test,
              TestRange<bool>( { true } ) );
    ts.doTest("flush beyond sync test", flush_beyond_sync_test);
    ts.doTest("common prefix L1 flush by fast scan test",
              common_prefix_l1_flush_by_fast_scan_test);
    ts.doTest("log flush add new file race test", log_flush_add_new_file_race_test);
    ts.doTest("serialized sync and flush test", serialized_sync_and_flush_test);
    ts.doTest("disabling auto flush test", disabling_auto_flush_test);
    ts.doTest("sync flush race test", sync_flush_race_test);
    ts.doTest("sync skip and reload test", sync_skip_and_reload_test);

    return 0;
}

