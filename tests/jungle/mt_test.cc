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
#include "test_common.h"

#include "libjungle/jungle.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <stdio.h>
#include <unistd.h>

namespace mt_test {

struct mt1_args : public TestSuite::ThreadArgs {
    enum Type {
        READER = 0,
        WRITER = 1
    };

    mt1_args() {}
    mt1_args(jungle::DB* _db, Type _type, int _duration)
        : db(_db), type(_type), duration_ms(_duration)
        {}

    jungle::DB* db;
    Type type;
    int duration_ms;
    int ret;
};

int mt1_reader(TestSuite::ThreadArgs* t_args) {
    mt1_args* args = static_cast<mt1_args*>(t_args);
    TestSuite::Timer timer(args->duration_ms);
    do {
        jungle::Status s;
        uint64_t seq;
        s = args->db->getMaxSeqNum(seq);
        if (!s || !seq) continue;

        uint64_t r = (rand() % seq);
        jungle::Record rec;
        jungle::Record::Holder h_rec(rec);
        std::string key_str("k" + TestSuite::lzStr(6, r));
        s = args->db->getRecordByKey( jungle::SizedBuf(key_str), rec );
        CHK_OK(s);
    } while (!timer.timeover());
    return 0;
}

int mt1_writer(TestSuite::ThreadArgs* t_args) {
    mt1_args* args = static_cast<mt1_args*>(t_args);
    TestSuite::Timer timer(args->duration_ms);
    uint64_t count = 0;
    do {
        jungle::Status s;
        jungle::KV kv;
        std::string key_str("k" + TestSuite::lzStr(6, count));
        std::string val_str("v" + TestSuite::lzStr(6, count));
        kv.set(key_str, val_str);
        s = args->db->set(kv);
        CHK_OK(s);
        if (count % 1000 == 0) {
            s = args->db->sync();
            // NOTE: `s` may fail if there is concurrent flush
            //       by BG flusher.
            (void)s;
        }
        count++;
    } while (!timer.timeover());
    return 0;
}

int mt1_single_writer_multi_reader_seq(size_t duration_sec) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    jungle::DB::open(&db, filename, config);

    int num_reader = 2;

    TestSuite::ThreadHolder* w_holder = nullptr;
    mt1_args w_args;
    w_args = mt1_args(db, mt1_args::WRITER, 1000 * duration_sec);
    w_holder = new TestSuite::ThreadHolder(&w_args, mt1_writer, nullptr);

    mt1_args r_args[num_reader];
    std::vector<TestSuite::ThreadHolder*> r_holders(num_reader, nullptr);
    for (int i=0; i<num_reader; ++i) {
        r_args[i] = mt1_args(db, mt1_args::READER, 1000 * duration_sec);
        TestSuite::ThreadHolder*& r_holder = r_holders[i];
        r_holder = new TestSuite::ThreadHolder(&r_args[i], mt1_reader, nullptr);
    }

    w_holder->join();
    CHK_Z(w_holder->getResult());
    delete w_holder;

    for (int i=0; i<num_reader; ++i) {
        TestSuite::ThreadHolder*& r_holder = r_holders[i];
        r_holder->join();
        CHK_Z(r_holder->getResult());
        delete r_holder;
    }

    jungle::DB::close(db);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int flusher_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    jungle::init(g_config);

    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // 1 second.
    TestSuite::Timer timer(1000);
    uint64_t ii = 0;
    while (!timer.timeover()) {
        jungle::KV kv;
        std::string key_str("k" + TestSuite::lzStr(6, ii));
        std::string val_str("v" + TestSuite::lzStr(6, ii));
        kv.set(key_str, val_str);
        db->set(kv);
        ii++;

        // 5000 r/sec * 1 sec = 1000 records.
        TestSuite::sleep_us(200);
    }

    s = jungle::DB::close(db);
    CHK_OK(s);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace mt_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("single writer multi reader seq test",
              mt1_single_writer_multi_reader_seq,
              TestRange<size_t>({1}));

    ts.doTest("flusher test",
              flusher_test);

    return 0;
}
