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

#include "config_test_common.h"
#include "libjungle/jungle.h"
#include "test_common.h"

#include <atomic>
#include <vector>

#include <stdio.h>

struct FlushReaderArgs : TestSuite::ThreadArgs {
    FlushReaderArgs()
        : TestSuite::ThreadArgs()
        , lastInsertedIdx(0)
        , termSignal(false) {}
    jungle::DB* db;
    std::atomic<uint64_t> lastInsertedIdx;
    std::atomic<bool> termSignal;
};

int flush_stress_test_reader(TestSuite::ThreadArgs* base_args) {
    FlushReaderArgs* args = static_cast<FlushReaderArgs*>(base_args);
    uint64_t succ_count = 0;
    uint64_t fail_count = 0;

    while ( !args->termSignal ) {
        if (args->lastInsertedIdx < 1000) {
            std::this_thread::yield();
            continue;
        }

        uint64_t inserted = args->lastInsertedIdx;
        for (uint64_t ii=0; ii<inserted && !args->termSignal; ++ii) {
            jungle::Status s;
            std::string key = "k" + TestSuite::lzStr(7, ii);
            std::string val = "v" + TestSuite::lzStr(7, ii);

            jungle::SizedBuf key_req(key);
            jungle::SizedBuf value_out;
            s = args->db->get(key_req, value_out);
            CHK_Z(s);

            CHK_EQ(jungle::SizedBuf(val), value_out);
            value_out.free();
            succ_count++;
        }
    }
    TestSuite::_msg("%ld successful reads, %ld outdated reads\n", succ_count, fail_count);

    return 0;
}

int flusher_stress_basic_test(size_t dur_sec) {
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
    FlushReaderArgs args;
    args.db = db;

    TestSuite::ThreadHolder h(&args, flush_stress_test_reader, nullptr);

    TestSuite::Progress pp(dur_sec, "populating", "sec");
    TestSuite::Timer tt(dur_sec * 1000);
    while (!tt.timeover()) {
        std::string key = "k" + TestSuite::lzStr(7, idx);
        std::string val = "v" + TestSuite::lzStr(7, idx);
        CHK_Z(db->set(jungle::KV(key, val)));
        idx++;
        args.lastInsertedIdx = idx;

        if (idx && idx % 5000 == 0) {
            CHK_Z(db->flushLogsAsync(jungle::FlushOptions(), nullptr, nullptr));
        }

        uint64_t cur_sec = tt.getTimeUs() / 1000000;
        pp.update(cur_sec);
    }
    pp.done();
    TestSuite::_msg("%ld writes\n", idx);

    args.termSignal = true;
    h.join();
    CHK_Z(h.getResult());

    // Close, reopen, verify (twice).
    for (size_t ii=0; ii<2; ++ii) {
        CHK_Z(jungle::DB::close(db));
        CHK_Z(jungle::DB::open(&db, filename, config));

        pp = TestSuite::Progress(args.lastInsertedIdx, "verifying");
        for (uint64_t ii=0; ii<args.lastInsertedIdx; ++ii) {
            std::string key = "k" + TestSuite::lzStr(7, ii);
            std::string val = "v" + TestSuite::lzStr(7, ii);

            jungle::SizedBuf key_req(key);
            jungle::SizedBuf value_out;
            s = db->get(key_req, value_out);
            CHK_Z(s);

            CHK_EQ(jungle::SizedBuf(val), value_out);
            value_out.free();
            pp.update(ii);
        }
        pp.done();
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

int auto_flusher_stress_test(size_t dur_sec) {
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1024*1024*1024; // 1GB
    g_config.ltOpt.startNumLogs = 64;
    g_config.ltOpt.limitNumLogs = 128;
    g_config.ltOpt.maxSleepTimeMs = 1000;
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    uint64_t idx = 0;
    FlushReaderArgs args;
    args.db = db;

    TestSuite::ThreadHolder h(&args, flush_stress_test_reader, nullptr);

    TestSuite::Progress pp(dur_sec, "populating", "sec");
    TestSuite::Timer tt(dur_sec * 1000);
    auto run_writes = [&]() {
        while (!tt.timeover()) {
            std::string key = "k" + TestSuite::lzStr(7, idx);
            std::string val = "v" + TestSuite::lzStr(7, idx);
            CHK_Z(db->set(jungle::KV(key, val)));
            idx++;
            args.lastInsertedIdx = idx;

            uint64_t cur_sec = tt.getTimeUs() / 1000000;
            pp.update(cur_sec);
        }
        TestSuite::_msg("%ld writes\n", idx);
        return 0;
    };
    run_writes();

    g_config.ltOpt.startNumLogs = 64;
    g_config.ltOpt.limitNumLogs = 256;
    g_config.ltOpt.maxSleepTimeMs = 500;
    jungle::updateGlobalConfig(g_config);

    TestSuite::sleep_sec(1, "waiting for the new global config to take effect");

    tt.reset();
    run_writes();

    args.termSignal = true;
    h.join();
    CHK_Z(h.getResult());

    // Close, reopen, verify (twice).
    for (size_t ii=0; ii<2; ++ii) {
        CHK_Z(jungle::DB::close(db));
        CHK_Z(jungle::DB::open(&db, filename, config));

        pp = TestSuite::Progress(args.lastInsertedIdx, "verifying");
        for (uint64_t ii=0; ii<args.lastInsertedIdx; ++ii) {
            std::string key = "k" + TestSuite::lzStr(7, ii);
            std::string val = "v" + TestSuite::lzStr(7, ii);

            jungle::SizedBuf key_req(key);
            jungle::SizedBuf value_out;
            s = db->get(key_req, value_out);
            CHK_Z(s);

            CHK_EQ(jungle::SizedBuf(val), value_out);
            value_out.free();
            pp.update(ii);
        }
        pp.done();
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

int auto_flusher_many_db_test(size_t dur_sec) {
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);
    TestSuite::mkdir(filename);

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1024*1024*1024; // 1GB
    g_config.ltOpt.startNumLogs = 64;
    g_config.ltOpt.limitNumLogs = 128;
    g_config.ltOpt.maxSleepTimeMs = 1000;
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 1000;

    const size_t NUM_DBS = 32;

    std::vector<jungle::DB*> dbs;
    for (size_t ii=0; ii<NUM_DBS; ++ii) {
        jungle::DB* db;
        std::string path = filename + "/" + TestSuite::lzStr(2, ii);
        CHK_Z(jungle::DB::open(&db, path, config));
        dbs.push_back(db);
    }

    uint64_t idx = 0;
    uint64_t last_inserted_idx = 0;

    TestSuite::Progress pp(dur_sec, "populating", "sec");
    TestSuite::Timer tt(dur_sec * 1000);
    while (!tt.timeover()) {
        std::string key = "k" + TestSuite::lzStr(7, idx);
        std::string val = "v" + TestSuite::lzStr(7, idx);

        for (auto& db: dbs) {
            CHK_Z(db->set(jungle::KV(key, val)));
        }
        idx++;
        last_inserted_idx = idx;

        uint64_t cur_sec = tt.getTimeUs() / 1000000;
        pp.update(cur_sec);
    }
    TestSuite::_msg("%lu writes (* %zu = %lu)\n", idx, NUM_DBS, idx * NUM_DBS);

    // Close, reopen, verify (twice).
    for (size_t ii=0; ii<2; ++ii) {
        for (size_t ii=0; ii<NUM_DBS; ++ii) {
            pp = TestSuite::Progress(last_inserted_idx,
                                     "verifying " + TestSuite::lzStr(2, ii));

            std::string path = filename + "/" + TestSuite::lzStr(2, ii);
            CHK_Z(jungle::DB::close(dbs[ii]));
            CHK_Z(jungle::DB::open(&dbs[ii], path, config));

            for (uint64_t jj = 0; jj < last_inserted_idx; ++jj) {
                std::string key = "k" + TestSuite::lzStr(7, jj);
                std::string val = "v" + TestSuite::lzStr(7, jj);

                jungle::SizedBuf key_req(key);
                jungle::SizedBuf value_out;
                s = dbs[ii]->get(key_req, value_out);
                CHK_Z(s);

                CHK_EQ(jungle::SizedBuf(val), value_out);
                value_out.free();
                pp.update(jj);
            }

            pp.done();
        }
    }

    for (auto& db: dbs) {
        CHK_Z(jungle::DB::close(db));
    }
    CHK_Z(jungle::shutdown());

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.options.abortOnFailure = true;
    ts.doTest( "manual flusher stress test",
               flusher_stress_basic_test, TestRange<size_t>({10}) );
    ts.doTest( "auto flusher stress test",
               auto_flusher_stress_test, TestRange<size_t>({10}) );
    ts.doTest( "auto flusher many db test",
               auto_flusher_many_db_test, TestRange<size_t>({10}) );

    return 0;
}

