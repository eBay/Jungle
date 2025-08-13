/************************************************************************
Copyright 2017-present eBay Inc.

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

int disabling_seq_index_basic_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.useSequenceIndex = false;
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 1000;
    std::string key_prefix = "key";
    std::string val_prefix = "val";

    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string key_str = key_prefix + TestSuite::lzStr(5, ii);
        std::string val_str = val_prefix + TestSuite::lzStr(5, ii);
        CHK_Z(db->set(jungle::KV(key_str, val_str)));
    }

    auto verify_point_get = [&]() -> int {
        for (size_t ii = 0; ii < NUM; ++ii) {
            std::string key_str = key_prefix + TestSuite::lzStr(5, ii);
            std::string val_str = val_prefix + TestSuite::lzStr(5, ii);
            jungle::SizedBuf val_out;
            jungle::SizedBuf::Holder h(val_out);
            CHK_Z(db->get(key_str, val_out));
            CHK_EQ(jungle::SizedBuf(val_str), val_out);
        }
        return 0;
    };

    auto verify_itr_get = [&]() -> int {
        jungle::Iterator itr;
        itr.init(db);
        size_t count = 0;
        do {
            jungle::Record rec_out;
            jungle::Record::Holder h(rec_out);
            s = itr.get(rec_out);
            if (!s.ok()) {
                break;
            }

            std::string key_str = key_prefix + TestSuite::lzStr(5, count);
            std::string val_str = val_prefix + TestSuite::lzStr(5, count);
            CHK_EQ(jungle::SizedBuf(key_str), rec_out.kv.key);
            CHK_EQ(jungle::SizedBuf(val_str), rec_out.kv.value);
            count++;
        } while (itr.next().ok());
        CHK_EQ(NUM, count);
        return 0;
    };

    // In-memory.
    verify_point_get();
    verify_itr_get();

    // Flush into table.
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs());
    verify_point_get();
    verify_itr_get();

    // Flush in to L1.
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }
    verify_point_get();
    verify_itr_get();

    jungle::Iterator itr;
    CHK_EQ(jungle::Status::SEQINDEX_UNAVAILABLE, itr.initSN(db).getValue());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int disabling_seq_index_and_reenabling_test(bool flush_after_reopen) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.useSequenceIndex = true;
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 1000;
    std::string key_prefix = "key";
    std::string val_prefix = "val";

    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string key_str = key_prefix + TestSuite::lzStr(5, ii);
        std::string val_str = val_prefix + TestSuite::lzStr(5, ii);
        CHK_Z(db->set(jungle::KV(key_str, val_str)));
    }

    auto verify_point_get = [&]() -> int {
        for (size_t ii = 0; ii < NUM; ++ii) {
            std::string key_str = key_prefix + TestSuite::lzStr(5, ii);
            std::string val_str = val_prefix + TestSuite::lzStr(5, ii);
            jungle::SizedBuf val_out;
            jungle::SizedBuf::Holder h(val_out);
            CHK_Z(db->get(key_str, val_out));
            CHK_EQ(jungle::SizedBuf(val_str), val_out);
        }
        return 0;
    };

    auto verify_keyitr_get = [&]() -> int {
        jungle::Iterator itr;
        itr.init(db);
        size_t count = 0;
        do {
            jungle::Record rec_out;
            jungle::Record::Holder h(rec_out);
            s = itr.get(rec_out);
            if (!s.ok()) {
                break;
            }

            std::string key_str = key_prefix + TestSuite::lzStr(5, count);
            std::string val_str = val_prefix + TestSuite::lzStr(5, count);
            CHK_EQ(jungle::SizedBuf(key_str), rec_out.kv.key);
            CHK_EQ(jungle::SizedBuf(val_str), rec_out.kv.value);
            count++;
        } while (itr.next().ok());
        CHK_EQ(NUM, count);
        return 0;
    };

    auto verify_seqitr_get = [&]() -> int {
        jungle::Iterator itr;
        itr.initSN(db);
        size_t count = 0;
        do {
            jungle::Record rec_out;
            jungle::Record::Holder h(rec_out);
            s = itr.get(rec_out);
            if (!s.ok()) {
                break;
            }

            std::string key_str = key_prefix + TestSuite::lzStr(5, count);
            std::string val_str = val_prefix + TestSuite::lzStr(5, count);
            CHK_EQ(jungle::SizedBuf(key_str), rec_out.kv.key);
            CHK_EQ(jungle::SizedBuf(val_str), rec_out.kv.value);
            count++;
        } while (itr.next().ok());
        CHK_EQ(NUM, count);
        return 0;
    };

    // Flush into table.
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs());
    verify_point_get();
    verify_keyitr_get();
    verify_seqitr_get();

    // Close and re-open it without seq index option.
    CHK_Z(jungle::DB::close(db));
    db = nullptr;
    config.useSequenceIndex = false;
    CHK_Z(jungle::DB::open(&db, filename, config));

    verify_point_get();
    verify_keyitr_get();

    if (flush_after_reopen) {
        // Flush in to L1.
        for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
            CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
        }
        verify_point_get();
        verify_keyitr_get();
    }

    jungle::Iterator itr;
    CHK_EQ(jungle::Status::SEQINDEX_UNAVAILABLE, itr.initSN(db).getValue());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int enabling_seq_index_and_disabling_test(bool flush_after_reopen) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.useSequenceIndex = false;
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 1000;
    std::string key_prefix = "key";
    std::string val_prefix = "val";

    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string key_str = key_prefix + TestSuite::lzStr(5, ii);
        std::string val_str = val_prefix + TestSuite::lzStr(5, ii);
        CHK_Z(db->set(jungle::KV(key_str, val_str)));
    }

    auto verify_point_get = [&]() -> int {
        for (size_t ii = 0; ii < NUM; ++ii) {
            std::string key_str = key_prefix + TestSuite::lzStr(5, ii);
            std::string val_str = val_prefix + TestSuite::lzStr(5, ii);
            jungle::SizedBuf val_out;
            jungle::SizedBuf::Holder h(val_out);
            CHK_Z(db->get(key_str, val_out));
            CHK_EQ(jungle::SizedBuf(val_str), val_out);
        }
        return 0;
    };

    auto verify_keyitr_get = [&]() -> int {
        jungle::Iterator itr;
        itr.init(db);
        size_t count = 0;
        do {
            jungle::Record rec_out;
            jungle::Record::Holder h(rec_out);
            s = itr.get(rec_out);
            if (!s.ok()) {
                break;
            }

            std::string key_str = key_prefix + TestSuite::lzStr(5, count);
            std::string val_str = val_prefix + TestSuite::lzStr(5, count);
            CHK_EQ(jungle::SizedBuf(key_str), rec_out.kv.key);
            CHK_EQ(jungle::SizedBuf(val_str), rec_out.kv.value);
            count++;
        } while (itr.next().ok());
        CHK_EQ(NUM, count);
        return 0;
    };

    auto verify_seqitr_get = [&](size_t expected_count) -> int {
        jungle::Iterator itr;
        itr.initSN(db);
        size_t count = 0;
        do {
            jungle::Record rec_out;
            jungle::Record::Holder h(rec_out);
            s = itr.get(rec_out);
            if (!s.ok()) {
                break;
            }

            std::string key_str = key_prefix + TestSuite::lzStr(5, count);
            std::string val_str = val_prefix + TestSuite::lzStr(5, count);
            CHK_EQ(jungle::SizedBuf(key_str), rec_out.kv.key);
            CHK_EQ(jungle::SizedBuf(val_str), rec_out.kv.value);
            count++;
        } while (itr.next().ok());
        CHK_EQ(expected_count, count);
        return 0;
    };

    // Flush into table.
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs());
    verify_point_get();
    verify_keyitr_get();

    jungle::Iterator itr;
    CHK_EQ(jungle::Status::SEQINDEX_UNAVAILABLE, itr.initSN(db).getValue());

    // Close and re-open it without seq index option.
    CHK_Z(jungle::DB::close(db));
    db = nullptr;
    config.useSequenceIndex = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    verify_point_get();
    verify_keyitr_get();
    // Now seq iterator should work, but it will not see any old records.
    verify_seqitr_get(0);

    if (flush_after_reopen) {
        // Flush in to L1.
        for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
            CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
        }
        verify_point_get();
        verify_keyitr_get();

        // After compaction, it should see all the records.
        verify_seqitr_get(NUM);
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("disabling seq index basic test", disabling_seq_index_basic_test);
    ts.doTest("disabling seq index and re-enabling test",
              disabling_seq_index_and_reenabling_test,
              TestRange<bool>({false, true}));
    ts.doTest("enabling seq index and disabling test",
              enabling_seq_index_and_disabling_test,
              TestRange<bool>({false, true}));

    return 0;
}