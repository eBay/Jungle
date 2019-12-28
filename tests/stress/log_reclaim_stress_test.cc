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
#include <fstream>

#include <stdio.h>

namespace log_reclaim_stress_test {

struct ReaderArgs : TestSuite::ThreadArgs {
    ReaderArgs()
        : durationSec(1)
        , db(nullptr)
        , stopSignal(false)
        , curNextSlot(nullptr)
        , curStartIdx(nullptr)
        {}
    size_t durationSec;
    jungle::DB* db;
    std::atomic<bool> stopSignal;
    std::atomic<uint64_t>* curNextSlot;
    std::atomic<uint64_t>* curStartIdx;
};

int purge_thread_func(TestSuite::ThreadArgs* t_args) {
    ReaderArgs* args = (ReaderArgs*)t_args;

    while (!args->stopSignal) {
        TestSuite::sleep_sec(1);
        uint64_t next_slot = args->curNextSlot->load();
        if (next_slot > 10000) {
            jungle::FlushOptions f_opt;
            f_opt.purgeOnly = true;
            args->db->flushLogs(f_opt, next_slot - 10000);
            args->curStartIdx->store(next_slot - 10000 + 1);
        }
    }

    return 0;
}

int point_query_thread_func(TestSuite::ThreadArgs* t_args) {
    ReaderArgs* args = (ReaderArgs*)t_args;

    while (!args->stopSignal) {
        uint64_t start_idx = args->curStartIdx->load();
        uint64_t next_slot = args->curNextSlot->load();
        uint64_t gap = std::min((uint64_t)5000, next_slot - start_idx);
        if (!gap) continue;

        uint64_t r = next_slot - (std::rand() % gap);
        for (size_t ii=r; ii<next_slot; ++ii) {
            jungle::KV kv_out;
            args->db->getSN(ii+1, kv_out);
            kv_out.free();
        }
    }

    return 0;
}

int range_query_thread_func(TestSuite::ThreadArgs* t_args) {
    ReaderArgs* args = (ReaderArgs*)t_args;

    while (!args->stopSignal) {
        TestSuite::sleep_sec(1);

        uint64_t start_idx = args->curStartIdx->load();
        uint64_t next_slot = args->curNextSlot->load();
        uint64_t gap = std::min((uint64_t)1000, next_slot - start_idx);
        if (!gap) continue;

        uint64_t r = next_slot - (std::rand() % gap);

        jungle::Iterator itr;
        itr.initSN(args->db, r);
        do {
            jungle::Record rec_out;
            jungle::Status s = itr.get(rec_out);
            if (!s) break;
            rec_out.free();
        } while (itr.next().ok());
        itr.close();
    }

    return 0;
}

int log_reclaim_with_queries_test(size_t dur_sec) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 500;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    std::vector<ReaderArgs> args(3);

    std::atomic<uint64_t> next_slot(0);
    std::atomic<uint64_t> start_idx(0);

    for (ReaderArgs& entry: args) {
        entry.durationSec = dur_sec;
        entry.db = db;
        entry.curNextSlot = &next_slot;
        entry.curStartIdx = &start_idx;
    }

    // 2K writes/sec
    TestSuite::WorkloadGenerator wg(2000.0);
    TestSuite::Timer tt(dur_sec * 1000);
    TestSuite::Progress pp(dur_sec);

    TestSuite::ThreadHolder purge_thread(&args[0], purge_thread_func, nullptr);
    TestSuite::ThreadHolder point_thread(&args[1], point_query_thread_func, nullptr);
    TestSuite::ThreadHolder range_thread(&args[2], range_query_thread_func, nullptr);

    char val_buf[256];
    memset(val_buf, 'x', 256);
    jungle::SizedBuf val(256, val_buf);
    while (!tt.timeover()) {
        if (!wg.getNumOpsToDo()) {
            TestSuite::sleep_us(500);
            continue;
        }

        std::string key_str = "seq" + TestSuite::lzStr(7, next_slot);
        jungle::SizedBuf key(key_str);
        CHK_Z( db->setSN(next_slot+1, jungle::KV(key, val)) );
        (void)db->sync(false); // Sync may fail.
        next_slot++;

        pp.update(tt.getTimeUs() / 1000000);
        wg.addNumOpsDone(1);
    }
    pp.done();
    for (ReaderArgs& entry: args) entry.stopSignal = true;

    purge_thread.join();
    point_thread.join();
    range_thread.join();

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace log_reclaim_stress_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.doTest("log reclaim with point and range queries test",
              log_reclaim_with_queries_test,
              TestRange<size_t>({10}));

    return 0;
}
