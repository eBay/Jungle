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

struct PurgeReaderArgs : TestSuite::ThreadArgs {
    PurgeReaderArgs()
        : TestSuite::ThreadArgs()
        , lastPurgedSeq(0)
        , lastInsertedSeq(0)
        , termSignal(false) {}
    jungle::DB* db;
    std::atomic<uint64_t> lastPurgedSeq;
    std::atomic<uint64_t> lastInsertedSeq;
    std::atomic<bool> termSignal;
};

int purge_stress_test_reader(TestSuite::ThreadArgs* base_args) {
    PurgeReaderArgs* args = static_cast<PurgeReaderArgs*>(base_args);
    uint64_t succ_count = 0;
    uint64_t fail_count = 0;

    while ( !args->termSignal ) {
        if (args->lastInsertedSeq < 1000) {
            std::this_thread::yield();
            continue;
        }

        uint64_t purged = args->lastPurgedSeq;
        uint64_t inserted = args->lastInsertedSeq;
        for (uint64_t ii=purged+1; ii<inserted; ++ii) {
            jungle::KV kv_out;
            jungle::Status s;
            s = args->db->getSN(ii, kv_out);
            if (!s) {
                fail_count++;
                purged = args->lastPurgedSeq;
                CHK_GTEQ(purged, ii);
                ii = purged + 1;
                continue;
            }

            std::string key = "k" + TestSuite::lzStr(7, ii);
            std::string val = "v" + TestSuite::lzStr(7, ii);
            jungle::KV kv_expected(key, val);
            CHK_EQ(kv_expected.key, kv_out.key);
            CHK_EQ(kv_expected.value, kv_out.value);
            kv_out.free();
            succ_count++;
        }
    }
    TestSuite::_msg("%ld successful reads, %ld outdated reads\n", succ_count, fail_count);

    return 0;
}

int purge_stress_basic_test(size_t dur_sec) {
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
    config.logSectionOnly = true;
    //config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    uint64_t idx = 1;
    PurgeReaderArgs args;
    args.db = db;

    TestSuite::ThreadHolder h(&args, purge_stress_test_reader, nullptr);

    TestSuite::Progress pp(dur_sec);
    TestSuite::Timer tt(dur_sec * 1000);
    while (!tt.timeover()) {
        std::string key = "k" + TestSuite::lzStr(7, idx);
        std::string val = "v" + TestSuite::lzStr(7, idx);
        CHK_Z( db->setSN(idx, jungle::KV(key, val)) );
        args.lastInsertedSeq = idx;
        idx++;

        if ( idx >= 10000 &&
             idx % 5000 == 0) {
            args.lastPurgedSeq = idx - 5000;
            CHK_Z( db->flushLogsAsync( jungle::FlushOptions(),
                                       nullptr, nullptr,
                                       idx - 5000 ) );
        }

        uint64_t cur_sec = tt.getTimeUs() / 1000000;
        pp.update(cur_sec);
    }

    args.termSignal = true;
    h.join();
    CHK_Z(h.getResult());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

int purge_with_auto_flusher_test(size_t dur_sec) {
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
    config.logSectionOnly = true;
    //config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    uint64_t idx = 1;
    PurgeReaderArgs args;
    args.db = db;

    TestSuite::ThreadHolder h(&args, purge_stress_test_reader, nullptr);

    TestSuite::Progress pp(dur_sec);
    TestSuite::Timer tt(dur_sec * 1000);
    while (!tt.timeover()) {
        std::string key = "k" + TestSuite::lzStr(7, idx);
        std::string val = "v" + TestSuite::lzStr(7, idx);
        CHK_Z( db->setSN(idx, jungle::KV(key, val)) );
        args.lastInsertedSeq = idx;
        idx++;

        uint64_t cur_sec = tt.getTimeUs() / 1000000;
        pp.update(cur_sec);
    }

    args.termSignal = true;
    h.join();
    CHK_Z(h.getResult());

    // Close, reopen, verify (twice).
    for (size_t kk=0; kk<2; ++kk) {
        CHK_Z(jungle::DB::close(db));
        CHK_Z(jungle::DB::open(&db, filename, config));

        pp = TestSuite::Progress(args.lastInsertedSeq, "verifying");
        for (uint64_t ii=1; ii<=args.lastInsertedSeq; ++ii) {
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

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.doTest( "purge stress test", purge_stress_basic_test, TestRange<size_t>({10}) );
    ts.doTest( "purge with auto flusher test",
               purge_with_auto_flusher_test, TestRange<size_t>({5}) );

    return 0;
}

