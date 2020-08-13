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

static size_t NUM_DBS = 3;
static size_t NUM_VERIFIER = 4;
static size_t NUM_KEYS = 10;

static std::atomic<uint64_t> batch_counter(1);

int do_atomic_batch(const std::vector< jungle::DB* >& db_vector) {
#if 1
    std::list<jungle::Record> recs_to_free;
    jungle::GlobalBatch g_batch;
    std::string counter_string = std::to_string( batch_counter.fetch_add(1) );
    for (auto& entry: db_vector) {
        jungle::DB* db = entry;
        std::list<jungle::Record> recs;
        for (size_t ii=0; ii<NUM_KEYS; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + TestSuite::lzStr(4, ii);
            std::string val_str = counter_string;
            rr.kv.alloc(key_str, val_str);
            recs.push_back(rr);
            recs_to_free.push_back(rr);
        }
        g_batch.addBatch(db, recs);
    }
    CHK_Z( g_batch.execute() );
    for (jungle::Record& rr: recs_to_free) {
        rr.free();
    }

#else
    // Same set but without global batch, should fail.
    std::string counter_string = std::to_string( batch_counter.fetch_add(1) );
    for (auto& entry: db_vector) {
        jungle::DB* db = entry;
        for (size_t ii=0; ii<NUM_KEYS; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + TestSuite::lzStr(4, ii);
            std::string val_str = counter_string;
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        }
    }
#endif
    return 0;
}

struct WorkerArgs : public TestSuite::ThreadArgs {
    std::vector< jungle::DB* >* dbVector;
    std::atomic<bool>* stopSignal;
};

int writer_thread(TestSuite::ThreadArgs* base_args) {
    WorkerArgs* args = static_cast<WorkerArgs*>(base_args);

    TestSuite::WorkloadGenerator wg(100);

    while (!args->stopSignal->load()) {
        if (!wg.getNumOpsToDo()) {
            TestSuite::sleep_ms(1);
            continue;
        }
        CHK_Z( do_atomic_batch(*args->dbVector) );
        wg.addNumOpsDone(1);
    }
    return 0;
}

int verifier_thread(TestSuite::ThreadArgs* base_args) {
    WorkerArgs* args = static_cast<WorkerArgs*>(base_args);

    uint64_t prev_value = 0;
    while (!args->stopSignal->load()) {
        auto entry = args->dbVector->begin();
        while (entry != args->dbVector->end()) {
            jungle::DB* db = *entry;

            // Pick any random key and check value.
            size_t rr = rand() % NUM_KEYS;
            std::string key_str = "k" + TestSuite::lzStr(4, rr);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get( jungle::SizedBuf(key_str), value_out ) );
            uint64_t cur_value = std::stol(value_out.toString());

            // `cur_value` shouldn't be smaller than prev_value
            CHK_GTEQ(cur_value, prev_value);
            prev_value = cur_value;

            entry++;
        }
    }
    return 0;
}

int global_batch_with_readers_test() {
    std::string filename_base;
    TEST_SUITE_PREPARE_PATH(filename_base);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    std::vector< jungle::DB* > db_vector(NUM_DBS);

    for (size_t ii=0; ii<NUM_DBS; ++ii) {
        std::string filename = filename_base + "_" + std::to_string(ii);
        CHK_Z( jungle::DB::open(&db_vector[ii], filename, config) );
    }

    // Do an initial batch.
    do_atomic_batch(db_vector);

    std::atomic<bool> stop_signal(false);

    WorkerArgs w_args;
    w_args.dbVector = &db_vector;
    w_args.stopSignal = &stop_signal;
    TestSuite::ThreadHolder w_holder(&w_args, writer_thread, nullptr);

    std::vector< TestSuite::ThreadHolder > v_holders(NUM_VERIFIER);
    for (auto& entry: v_holders) {
        TestSuite::ThreadHolder& hh = entry;
        hh.spawn(&w_args, verifier_thread, nullptr);
    }
    TestSuite::sleep_sec(5, "wait 5 seconds");

    stop_signal = true;
    w_holder.join();
    CHK_Z(w_holder.getResult());

    for (auto& entry: v_holders) {
        TestSuite::ThreadHolder& hh = entry;
        hh.join();
        CHK_Z(hh.getResult());
    }

    for (auto& entry: db_vector) {
        jungle::DB* db = entry;
        CHK_Z(jungle::DB::close(db));
    }
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("global batch with multiple readers test", global_batch_with_readers_test);

    return 0;
}

