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

#include "latency_collector.h"
#include "latency_dump.h"
#include "internal_helper.h"

#include <atomic>
#include <string>
#include <vector>

#include <stdio.h>

namespace compactor_stress_test {

static LatencyCollector global_lat;

static std::string given_path;
static bool verify_at_the_end = true;
static int write_rate = 10000;

std::string get_key_str(uint64_t idx) {
    return "k" + TestSuite::lzStr(8, idx);
}

struct IteratorArgs : TestSuite::ThreadArgs {
    IteratorArgs()
        : TestSuite::ThreadArgs()
        , db(nullptr)
        , maxNum(0)
        , batchSize(30)
        , lastInsertedNumber(nullptr)
        , termSignal(false) {}
    jungle::DB* db;
    size_t maxNum;
    size_t batchSize;
    std::atomic<uint64_t>* lastInsertedNumber;
    std::atomic<bool> termSignal;
};

int iterator_worker(TestSuite::ThreadArgs* base_args) {
    IteratorArgs* args = static_cast<IteratorArgs*>(base_args);
    uint64_t succ_count = 0;
    uint64_t succ_get_count = 0;
    uint64_t fail_count = 0;
    jungle::Status s;
    jungle::Timer tt;

    while ( !args->termSignal ) {
        jungle::Iterator itr;
        size_t num_limit = args->lastInsertedNumber->load();
        if (!num_limit) {
            TestSuite::sleep_ms(1);
            continue;
        }

        size_t rnd_start = std::rand() % num_limit;
        std::string key_s = get_key_str(rnd_start);
        std::string key_e = get_key_str(rnd_start + args->batchSize);

        {   collectBlockLatency(&global_lat, "init");
            s = itr.init(args->db, jungle::SizedBuf(key_s), jungle::SizedBuf(key_e));
        }

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

            {   collectBlockLatency(&global_lat, "next");
                s = itr.next();
            }
        } while (s);

        itr.close();
        succ_count++;
    }
    TestSuite::_msg("%zu %zu successful reads, %ld failed reads\n",
                    succ_count, succ_get_count, fail_count);

    return 0;
}

int cpt_stress_test(size_t dur_sec) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    bool do_initial_load = true;
    if (!given_path.empty()) {
        filename = given_path;
        if ( TestSuite::exist(given_path) &&
             TestSuite::exist(given_path + "/db_manifest") ) {
            do_initial_load = false;
        }
    }

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1*1024*1024*1024; // 1GB

    g_config.numCompactorThreads = 1;
    g_config.compactorSleepDuration_ms = 1000; // 1 second
    g_config.flusherMinRecordsToTrigger = 8192;

    g_config.numTableWriters = 4;
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.compactionFactor = 200;
    //config.compactionFactor = 200;
    config.blockReuseFactor = 0;
    //config.blockReuseFactor = 0;
    config.minFileSizeToCompact = 4*1024*1024;
    config.minBlockReuseCycleToCompact = 0;
    config.maxBlockReuseCycle = 0;
    config.numL0Partitions = 4;

    config.nextLevelExtension = false;
    config.maxL0TableSize = (uint64_t)64*1024*1024;
    config.maxL1TableSize = (uint64_t)64*1024*1024;
    config.maxL1Size = (uint64_t)5*64*1024*1024;
    config.tableSizeRatio = {2.5};
    config.levelSizeRatio = {10.0};
    config.bloomFilterBitsPerUnit = 0.0;
    config.useBloomFilterForGet = true;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    //const size_t MAX_NUM = 250000;
    const size_t MAX_NUM = 400 * 1000;
    const size_t VAL_SIZE = 900;

    jungle::DebugParams d_params;
    d_params.urgentCompactionRatio = 120; (void)d_params;
    //jungle::setDebugParams(d_params, 60);

    char val_buf[1024];
    memset(val_buf, 'x', 1024);

    if (do_initial_load) {
        // Initial load
        TestSuite::Progress pp(MAX_NUM, "initial load");
        TestSuite::Timer tt;
        for (size_t ii=0; ii<MAX_NUM; ++ii) {
            std::string key_str = get_key_str(ii);
            jungle::SizedBuf key(key_str);

            sprintf(val_buf, "v%07zu", ii);
            jungle::SizedBuf val(VAL_SIZE, val_buf);

            CHK_Z( db->set(jungle::KV(key, val)) );
            pp.update(ii);
        }
        pp.done();
        db->sync(false);
        db->flushLogs(jungle::FlushOptions());

        TestSuite::_msg( "initial load rate: %s ops/sec\n",
                         TestSuite::throughputStr(MAX_NUM, tt.getTimeUs()).c_str() );
        TestSuite::sleep_sec(5, "sleep");

    } else {
        TestSuite::_msg("skip initial load\n");
    }


    size_t idx = 1;
    std::atomic<uint64_t> last_inserted_number(MAX_NUM);

    size_t NUM_ITR_THREADS = 2;
    std::vector<IteratorArgs> args_arr(NUM_ITR_THREADS);
    for (IteratorArgs& args: args_arr) {
        args.db = db;
        args.maxNum = MAX_NUM;
        args.lastInsertedNumber = &last_inserted_number;
    }

    std::vector<TestSuite::ThreadHolder*> hs(NUM_ITR_THREADS);
    for (size_t ii=0; ii<NUM_ITR_THREADS; ++ii) {
        hs[ii] = new TestSuite::ThreadHolder
                     ( &args_arr[ii], iterator_worker, nullptr );
    }

    TestSuite::Progress pp(dur_sec, "testing");
    TestSuite::Timer tt(dur_sec * 1000);
    TestSuite::WorkloadGenerator wg(write_rate);
    while (!tt.timeover()) {
        uint64_t cur_sec = tt.getTimeUs() / 1000000;
        pp.update(cur_sec);

        if (wg.getNumOpsToDo() == 0) {
            TestSuite::sleep_us(100);
            continue;
        }

        size_t number = (idx * 7) % MAX_NUM;

        std::string key_str = get_key_str(number);
        jungle::SizedBuf key(key_str);

        sprintf(val_buf, "v%07zu", number);
        jungle::SizedBuf val(VAL_SIZE, val_buf);

        CHK_Z( db->set(jungle::KV(key, val)) );
        //last_inserted_number = number;
        idx++;

        wg.addNumOpsDone(1);
    }
    pp.done();

    for (IteratorArgs& args: args_arr) {
        args.termSignal = true;
    }

    for (size_t ii=0; ii<NUM_ITR_THREADS; ++ii) {
        hs[ii]->join();
        CHK_Z(hs[ii]->getResult());
        delete hs[ii];
    }

    if (verify_at_the_end) {
        // Final verify
        {   TestSuite::Timer tt;
            TestSuite::Progress pp(MAX_NUM, "verifying (point)");
            for (size_t ii=0; ii<MAX_NUM; ++ii) {
                TestSuite::setInfo("ii=%zu", ii);
                std::string key = get_key_str(ii);
                jungle::SizedBuf value_out;
                CHK_Z( db->get(jungle::SizedBuf(key), value_out) );
                value_out.free();
                pp.update(ii);
            }
            pp.done();
            TestSuite::_msg( "point lookup rate: %s ops/sec\n",
                             TestSuite::throughputStr
                                        (MAX_NUM, tt.getTimeUs()).c_str() );
        }

        {   TestSuite::Timer tt;
            TestSuite::Progress pp(MAX_NUM, "verifying (iterator)");
            jungle::Iterator itr;
            itr.init(db);
            size_t ii = 0;
            do {
                TestSuite::setInfo("ii=%zu", ii);
                jungle::Record rec_out;
                jungle::Record::Holder h_rec_out(rec_out);
                CHK_Z( itr.get(rec_out) );

                std::string key = get_key_str(ii);
                CHK_EQ( jungle::SizedBuf(key), rec_out.kv.key );

                sprintf(val_buf, "v%07zu", ii);
                jungle::SizedBuf val(VAL_SIZE, val_buf);
                CHK_EQ( val, rec_out.kv.value );

                pp.update(ii);
                ii++;
            } while (itr.next());
            itr.close();
            pp.done();
            TestSuite::_msg( "range lookup rate: %s ops/sec\n",
                             TestSuite::throughputStr
                                        (MAX_NUM, tt.getTimeUs()).c_str() );
        }

        // Close & re-open & re-verify
        CHK_Z(jungle::DB::close(db));
        CHK_Z(jungle::DB::open(&db, filename, config));

        {   TestSuite::Timer tt;
            TestSuite::Progress pp(MAX_NUM, "re-verifying (point)");
            for (size_t ii=0; ii<MAX_NUM; ++ii) {
                TestSuite::setInfo("ii=%zu", ii);
                std::string key = get_key_str(ii);
                jungle::SizedBuf value_out;
                CHK_Z( db->get(jungle::SizedBuf(key), value_out) );
                value_out.free();
                pp.update(ii);
            }
            pp.done();
            TestSuite::_msg( "point lookup rate: %s ops/sec\n",
                             TestSuite::throughputStr
                                        (MAX_NUM, tt.getTimeUs()).c_str() );
        }

        {   TestSuite::Timer tt;
            TestSuite::Progress pp(MAX_NUM, "re-verifying (iterator)");
            jungle::Iterator itr;
            itr.init(db);
            size_t ii = 0;
            do {
                TestSuite::setInfo("ii=%zu", ii);
                jungle::Record rec_out;
                jungle::Record::Holder h_rec_out(rec_out);
                CHK_Z( itr.get(rec_out) );

                std::string key = get_key_str(ii);
                CHK_EQ( jungle::SizedBuf(key), rec_out.kv.key );

                sprintf(val_buf, "v%07zu", ii);
                jungle::SizedBuf val(VAL_SIZE, val_buf);
                CHK_EQ( val, rec_out.kv.value );

                pp.update(ii);
                ii++;
            } while (itr.next());
            itr.close();
            pp.done();
            TestSuite::_msg( "range lookup rate: %s ops/sec\n",
                             TestSuite::throughputStr
                                        (MAX_NUM, tt.getTimeUs()).c_str() );
        }
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    if (given_path.empty()) {
        TEST_SUITE_CLEANUP_PATH();
    }

    TestSuite::Msg msg_stream;
    LatencyDumpDefaultImpl dump_impl;
    msg_stream << std::endl << global_lat.dump(&dump_impl) << std::endl;

    return 0;
}

void check_args(int argc, char** argv) {
    for (int ii=0; ii<argc; ++ii) {
        std::string cur_str = argv[ii];
        if ( cur_str.substr(0, 7) == "--path=" ) {
            given_path = cur_str.substr(7);

        } else if ( cur_str.substr(0, 11) == "--no-verify" ) {
            verify_at_the_end = false;

        } else if ( ii+1 < argc &&
                    cur_str.substr(0, 2) == "-w" ) {
            write_rate = std::stoi( argv[ii+1] );
            ++ii;
        }
    }
}

}
using namespace compactor_stress_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    check_args(argc, argv);

    ts.options.printTestMessage = true;
    ts.doTest( "compactor stress test",
               cpt_stress_test,
               TestRange<size_t>({10}) );

    return 0;
}

