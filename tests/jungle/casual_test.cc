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
#include "latency_collector.h"
#include "latency_dump.h"

#include <vector>

#include <stdio.h>

LatencyCollector global_lat;

int bench_jungle() {
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1024*1024*1024; // 1GB
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    //config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    uint64_t idx = 0;
    char val_char[257];
    memset(val_char, 'x', 256);
    val_char[256] = 0;
    std::string val(val_char);

    std::vector<uint64_t> key_order(1000000);
    for (uint64_t ii=0; ii<1000000; ++ii) key_order[ii] = ii;
    for (uint64_t ii=0; ii<1000000; ++ii) {
        uint64_t r1 = rand() % 1000000;
        uint64_t r2 = rand() % 1000000;
        uint64_t temp = key_order[r1];
        key_order[r1] = key_order[r2];
        key_order[r2] = temp;
    }

    TestSuite::Progress pp(1000000, "populating");
    TestSuite::Timer tt;
    for (uint64_t ii=0; ii<1000000; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, key_order[ii]);
        { collectBlockLatency(&global_lat, "set");
          CHK_Z(db->set(jungle::KV(key, val)));
        }
        idx++;

        if (idx && idx % 4000 == 0 && true) {
            collectBlockLatency(&global_lat, "checkpoint");
            db->sync(false);
        }

        pp.update(idx);
    }
    pp.done();
    TestSuite::_msg("%ld\n", tt.getTimeUs());
    TestSuite::_msg("%ld writes\n", idx);

    TestSuite::_msg("press enter..\n");
    int a = getc(stdin); (void)a;

    // Close, reopen, verify (twice).
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    pp = TestSuite::Progress(idx, "verifying");
    tt.reset();
    for (uint64_t ii=0; ii<idx; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);

        jungle::SizedBuf key_req(key);
        jungle::SizedBuf value_out;
        { collectBlockLatency(&global_lat, "get");
          s = db->get(key_req, value_out);
        }
        CHK_Z(s);
        value_out.free();
        pp.update(ii);
    }
    pp.done();
    TestSuite::_msg("%ld\n", tt.getTimeUs());

    //TestSuite::_msg("%s\n", global_lat.dump().c_str());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

#include <third_party/forestdb/include/libforestdb/forestdb.h>

int bench_fdb() {

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    fdb_config config = fdb_get_default_config();
    config.seqtree_opt = FDB_SEQTREE_USE;
    config.buffercache_size = (uint64_t)1024*1024*1024;

    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();
    fdb_file_handle *dbfile;
    fdb_kvs_handle *db;
    fdb_status s;

    s = fdb_open(&dbfile, filename.c_str(), &config);
    s = fdb_kvs_open(dbfile, &db, NULL, &kvs_config);

    uint64_t idx = 0;
    char val_char[257];
    memset(val_char, 'x', 256);
    val_char[256] = 0;
    std::string val(val_char);

    std::vector<uint64_t> key_order(1000000);
    for (uint64_t ii=0; ii<1000000; ++ii) key_order[ii] = ii;
    for (uint64_t ii=0; ii<1000000; ++ii) {
        uint64_t r1 = rand() % 1000000;
        uint64_t r2 = rand() % 1000000;
        uint64_t temp = key_order[r1];
        key_order[r1] = key_order[r2];
        key_order[r2] = temp;
    }

    TestSuite::Progress pp(1000000, "populating");
    TestSuite::Timer tt;
    for (uint64_t ii=0; ii<1000000; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, key_order[ii]);
        { collectBlockLatency(&global_lat, "set");
          s = fdb_set_kv(db, key.c_str(), key.size(), val.c_str(), val.size());
        }
        idx++;

        if (idx && idx % 4000 == 0 && true) {
            collectBlockLatency(&global_lat, "checkpoint");
            fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);
        }
        pp.update(idx);
    }
    pp.done();
    TestSuite::_msg("%ld\n", tt.getTimeUs());
    TestSuite::_msg("%ld writes\n", idx);
    fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);

    pp = TestSuite::Progress(idx, "verifying");
    tt.reset();
    for (uint64_t ii=0; ii<idx; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);

        jungle::SizedBuf key_req(key);
        jungle::SizedBuf value_out;
        { collectBlockLatency(&global_lat, "get");
          void* value_out = nullptr;
          size_t valuelen_out = 0;
          s = fdb_get_kv(db, key.c_str(), key.size(), &value_out, &valuelen_out);
          free(value_out);
        }
        CHK_Z(s);
        pp.update(ii);
    }
    pp.done();
    TestSuite::_msg("%ld\n", tt.getTimeUs());

    //TestSuite::_msg("%s\n", global_lat.dump().c_str());

    s = fdb_close(dbfile);
    s = fdb_shutdown();

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

int long_log_read_speed(uint64_t num) {
    jungle::Status s;
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::GlobalConfig g_config;
    // Disable auto flushing.
    //g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1024*1024*1024; // 1GB
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    //config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    uint64_t idx = 0;
    char val_char[257];
    memset(val_char, 'x', 256);
    val_char[256] = 0;
    std::string val(val_char);

    std::vector<uint64_t> key_order(num);
    for (uint64_t ii=0; ii<num; ++ii) key_order[ii] = ii;
    for (uint64_t ii=0; ii<num; ++ii) {
        uint64_t r1 = rand() % num;
        uint64_t r2 = rand() % num;
        uint64_t temp = key_order[r1];
        key_order[r1] = key_order[r2];
        key_order[r2] = temp;
    }

    TestSuite::Progress pp(num, "populating");
    TestSuite::Timer tt;
    for (uint64_t ii=0; ii<num; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, key_order[ii]);
        { collectBlockLatency(&global_lat, "set");
          CHK_Z(db->set(jungle::KV(key, val)));
        }
        idx++;

        if (idx && idx % 4000 == 0 && true) {
            collectBlockLatency(&global_lat, "checkpoint");
            db->sync(false);
        }

        pp.update(idx);
    }
    pp.done();

    uint64_t time_us = 0;
    time_us = tt.getTimeUs();
    TestSuite::_msg("%s\n", TestSuite::usToString(time_us).c_str());
    TestSuite::_msg("%s ops/s\n",
                    TestSuite::throughputStr(num, time_us).c_str());

    TestSuite::_msg("press enter..\n");
    int a = getc(stdin); (void)a;

    // Close, reopen, verify (twice).
    CHK_Z(jungle::DB::close(db));

    tt.reset();
    CHK_Z(jungle::DB::open(&db, filename, config));
    time_us = tt.getTimeUs();
    TestSuite::_msg("%s\n", TestSuite::usToString(time_us).c_str());
    TestSuite::_msg("%s ops/s\n",
                    TestSuite::throughputStr(num, time_us).c_str());

    pp = TestSuite::Progress(idx, "verifying");
    tt.reset();
    for (uint64_t ii=0; ii<idx; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);

        jungle::SizedBuf key_req(key);
        jungle::SizedBuf value_out;
        { collectBlockLatency(&global_lat, "get");
          s = db->get(key_req, value_out);
        }
        CHK_Z(s);
        value_out.free();
        pp.update(ii);
    }
    pp.done();

    time_us = tt.getTimeUs();
    TestSuite::_msg("%s\n", TestSuite::usToString(time_us).c_str());
    TestSuite::_msg("%s ops/s\n", TestSuite::throughputStr(num, time_us).c_str());

    //TestSuite::_msg("%s\n", global_lat.dump().c_str());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int see_auto_flush(uint64_t num_records) {
    jungle::Status s;
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    TestSuite::Progress pp(num_records);
    for (size_t ii=0; ii<num_records; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);
        std::string val = "v" + TestSuite::lzStr(7, ii);
        CHK_Z( db->set(jungle::KV(key, val)) );
        pp.update(ii);
    }
    pp.done();

    TestSuite::sleep_sec(5, "flushing..");

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int load_and_print() {
    std::string filename = "./db_to_load";
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;
    jungle::Status s;
    CHK_Z(jungle::DB::open(&db, filename, config));

    jungle::Iterator itr;
    itr.init(db);
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;
        TestSuite::_msg("seq %zu %s\n",
            rec_out.seqNum,
            jungle::HexDump::toString(rec_out.kv.key.toString()).c_str());
    } while (itr.next().ok());
    itr.close();

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    return 0;
}

int flush_and_delete() {
    jungle::Status s;
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num_records = 10;

    for (size_t ii=0; ii<num_records; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);
        std::string val = "v" + TestSuite::lzStr(7, ii);
        CHK_Z( db->set(jungle::KV(key, val)) );
    }

    db->sync();
    db->flushLogs(jungle::FlushOptions());

    for (size_t ii=0; ii<num_records; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);
        CHK_Z( db->del(jungle::SizedBuf(key)) );
    }

    // Now log section has deletion markers only.

    for (size_t ii=0; ii<num_records; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);
        jungle::SizedBuf val_out;
        CHK_NOT( db->get(jungle::SizedBuf(key), val_out) );
        val_out.free();
    }

    jungle::Iterator itr;
    itr.init(db);
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;
        TestSuite::_msg("seq %zu %s\n",
            rec_out.seqNum,
            jungle::HexDump::toString(rec_out.kv.key.toString()).c_str());
    } while (itr.next().ok());
    itr.close();

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compaction_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.numCompactorThreads = 1;
    g_config.flusherSleepDuration_ms = 500;
    g_config.compactorSleepDuration_ms = 1000;
    jungle::DB::init(g_config);

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Sync & flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.compactionDelayUs = 100000000;
    d_params.urgentCompactionFilesize = 60000;
    jungle::setDebugParams(d_params);
/*
    // Do compaction in background.
    CompactorArgs c_args;
    c_args.db = db;
    TestSuite::ThreadHolder h(&c_args, compactor, nullptr);
*/
    TestSuite::sleep_sec(1, "Wait for compaction to start");

    // Put more.
    CHK_Z( _set_keys(db, 0, num, 1, "k%06zu", "v%06zu") );

    // Request async flush.
    CHK_Z( db->flushLogsAsync(jungle::FlushOptions(), nullptr, nullptr) );

    TestSuite::sleep_sec(3600, "Wait for compaction to start");

/*
    // Wait more.
    h.join();
    CHK_Z(h.getResult());
*/
    // Close DB while copmaction is in progress,
    // compaction should be cancelled immediately.
    CHK_Z(jungle::DB::close(db));

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int temp_compaction() {
    std::string filename = "./bed";

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<4; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    return 0;
}

int multi_db_compact_order_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.numCompactorThreads = 1;
    g_config.flusherSleepDuration_ms = 1000;
    g_config.compactorSleepDuration_ms = 3000;
    jungle::DB::init(g_config);

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    std::vector<jungle::DB*> dbs(4, nullptr);
    size_t cnt = 0;
    for (auto& entry: dbs) {
        jungle::DB* db = entry;
        std::string f_local = filename + "_" + std::to_string(cnt++);
        CHK_Z( jungle::DB::open(&db, f_local, config) );
        db->setLogLevel(5);
    }

    TestSuite::sleep_sec(3600, "Wait for compaction to start");

    for (jungle::DB* db: dbs) {
        CHK_Z(jungle::DB::close(db));
    }

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int split_testbed() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB with compaction callback function.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.maxL0TableSize = 64*1024*1024;
    config.maxL1TableSize = 64*1024*1024;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t NUM = 2000000;
    std::string key_payload(48, 'x');
    std::string val_payload(512, 'x');
    TestSuite::Progress pp(NUM);
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "k" +
                              TestSuite::lzStr(7, ii) +
                              key_payload;
        CHK_Z( db->set( jungle::KV(key_str, val_payload) ) );
        pp.update(ii);
    }
    pp.done();
    db->sync(false);
    db->flushLogs(jungle::FlushOptions());

    pp = TestSuite::Progress(4);
    for (size_t ii=0; ii<4; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
        pp.update(ii);
    }
    pp.done();

    CHK_Z(db->splitLevel(jungle::CompactOptions(), 1));

    TestSuite::sleep_sec(60);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int delete_compaction_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.numCompactorThreads = 2;
    g_config.compactorSleepDuration_ms = 1000;
    g_config.fdbCacheSize = (uint64_t)2*1024*1024*1024;
    jungle::DB::init(g_config);

    jungle::DBConfig d_config;
    d_config.maxL0TableSize = 128*1024*1024;
    d_config.maxL1TableSize = 128*1024*1024;
    jungle::DB* db = nullptr;
    CHK_Z( jungle::DB::open(&db, filename, d_config) );

    char val_str_raw[1024];
    memset(val_str_raw, 'x', 1024);

    const size_t NUM = 1000000;
    TestSuite::Progress pp(NUM, "insert");
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(7, ii);
        sprintf(val_str_raw, "v%07zu", ii);
        CHK_Z( db->set( jungle::KV( jungle::SizedBuf(key_str),
                                    jungle::SizedBuf(1024, val_str_raw) ) ) );
        pp.update(ii+1);
    }
    pp.done();

    // Flush all logs.
    db->sync(false);
    db->flushLogs(jungle::FlushOptions());
    // Wait.
    TestSuite::sleep_sec(5, "wait for flushing");

    // Compact L0.
    for (size_t ii=0; ii<4; ++ii) {
        db->compactL0(jungle::CompactOptions(), ii);
    }

    pp = TestSuite::Progress(NUM / 2, "delete");
    for (size_t ii=0; ii<NUM/2; ++ii) {
        if (ii % 100 != 0) {
            std::string key_str = "k" + TestSuite::lzStr(7, ii);
            CHK_Z( db->del( jungle::SizedBuf(key_str) ) );
        }
        pp.update(ii+1);
    }
    pp.done();

    // Flush all logs.
    db->sync(false);
    db->flushLogs(jungle::FlushOptions());
    // Wait.
    TestSuite::sleep_sec(5, "wait for flushing");

    // Compact L0.
    for (size_t ii=0; ii<4; ++ii) {
        db->compactL0(jungle::CompactOptions(), ii);
    }

    // Set urgent compaction.
    jungle::DebugParams d_params;
    d_params.urgentCompactionRatio = 120;
    jungle::setDebugParams(d_params);

    // Wait.
    TestSuite::sleep_sec(10, "wait for compaction");

    // Flush all logs.
    db->sync(false);
    db->flushLogs(jungle::FlushOptions());
    // Compact L0.
    for (size_t ii=0; ii<4; ++ii) {
        db->compactL0(jungle::CompactOptions(), ii);
    }

    // Check.
    pp = TestSuite::Progress(NUM, "verify");
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(7, ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h(value_out);
        s = db->get(jungle::SizedBuf(key_str), value_out);
        if (ii < NUM/2 && ii % 100 != 0) {
            CHK_FALSE(s);
        } else {
            CHK_OK(s);
        }
        pp.update(ii+1);
    }
    pp.done();

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

/*
int basic_test_template() {
    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    jungle::shutdown();
    TestSuite::clearTestFile(prefix);
    return 0;
}
*/

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.options.preserveTestFiles = true;

    ts.doTest("test", delete_compaction_test);
    return 0;

    ts.doTest("temp split", split_testbed);
    ts.doTest("multi db compact order test", multi_db_compact_order_test);

    ts.doTest( "bench jungle", bench_jungle );
    ts.doTest( "bench fdb", bench_fdb );
    ts.doTest( "long log read speed",
               long_log_read_speed,
               TestRange<uint64_t>({1000000}) );
    ts.doTest( "see auto flushing",
               see_auto_flush,
               TestRange<uint64_t>({100000}) );
    ts.doTest("load and print", load_and_print);
    ts.doTest("flush and delete", flush_and_delete);
    ts.doTest("multi db compact order test", multi_db_compact_order_test);

    return 0;
}
