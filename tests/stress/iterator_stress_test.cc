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

namespace ns_iterator_stress_test {

struct IteratorArgs : TestSuite::ThreadArgs {
    IteratorArgs()
        : TestSuite::ThreadArgs()
        , db(nullptr)
        , maxNum(0)
        , batchSize(30)
        , lastFlushedSeq(0)
        , lastInsertedNumber(0)
        , termSignal(false) {}
    jungle::DB* db;
    size_t maxNum;
    size_t batchSize;
    std::atomic<uint64_t> lastFlushedSeq;
    std::atomic<uint64_t> lastInsertedNumber;
    std::atomic<bool> termSignal;
};

int iterator_worker(TestSuite::ThreadArgs* base_args) {
    IteratorArgs* args = static_cast<IteratorArgs*>(base_args);
    uint64_t succ_count = 0;
    uint64_t succ_get_count = 0;
    uint64_t fail_count = 0;
    jungle::Status s;

    while ( !args->termSignal ) {
        jungle::Iterator itr;
        size_t num_limit = args->lastInsertedNumber;
        if (!num_limit) {
            std::this_thread::yield();
            continue;
        }
        size_t rnd_start = std::rand() % num_limit;
        std::string key_s = "k" + TestSuite::lzStr(7, rnd_start);
        std::string key_e = "k" + TestSuite::lzStr(7, rnd_start + 100);

        s = itr.init(args->db, jungle::SizedBuf(key_s), jungle::SizedBuf(key_e));
        if (!s) {
            fail_count++;
            printf("%d %zu %lu\n", (int)s, rnd_start, num_limit);
            continue;
        }

        size_t batch_cnt = 0;
        do {
            jungle::Record rec_out;
            s = itr.get(rec_out);
            if (!s) break;
            succ_get_count++;

            //printf("%.*s\n", (int)rec_out.kv.key.size, rec_out.kv.key.data);
            rec_out.free();
            if (batch_cnt++ >= args->batchSize) break;
        } while (itr.next().ok());

        itr.close();
        succ_count++;
    }
    TestSuite::_msg("%zu %zu successful reads, %ld failed reads\n",
                    succ_count, succ_get_count, fail_count);

    return 0;
}

int itr_stress_test(size_t dur_sec) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1024*1024*1024; // 1GB
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    //config.numL0Partitions = 4;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t max_num = 1000000;
    size_t idx = 1;
    IteratorArgs args;
    args.db = db;
    args.maxNum = max_num;

    TestSuite::ThreadHolder h(&args, iterator_worker, nullptr);

    TestSuite::Progress pp(dur_sec);
    TestSuite::Timer tt(dur_sec * 1000);
    while (!tt.timeover()) {
        size_t number = (idx * 7) % max_num;
        std::string key = "k" + TestSuite::lzStr(7, number);
        std::string val = "v" + TestSuite::lzStr(7, number);
        CHK_Z( db->setSN(idx, jungle::KV(key, val)) );
        args.lastInsertedNumber = number;
        idx++;

        if ( idx >= 10000 &&
             idx % 1000 == 0) {
            args.lastFlushedSeq = idx - 1000;
            CHK_Z( db->flushLogsAsync( jungle::FlushOptions(),
                                       nullptr, nullptr,
                                       idx - 1000 ) );
        }

        uint64_t cur_sec = tt.getTimeUs() / 1000000;
        pp.update(cur_sec);
    }

    args.termSignal = true;
    h.join();
    CHK_Z(h.getResult());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

}
using namespace ns_iterator_stress_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.doTest( "iterator stress test", itr_stress_test, TestRange<size_t>({10}) );

    return 0;
}

