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

#include <numeric>
#include <random>
#include <vector>

#include <stdio.h>

int get_nearest_test(int flush_options) {
    // flush_options:
    //   0: all in the log.
    //   1: 3/4 in the log and 1/4 in the table.
    //   2: 1/2 in the log and 1/2 in the table.
    //   3: 1/4 in the log and 3/4 in the table.
    //   4: all in the table.

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.maxEntriesInLogFile = 20;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 100;

    // Shuffle (0 -- 99).
    std::vector<size_t> idx_arr(NUM);
    std::iota(idx_arr.begin(), idx_arr.end(), 0);
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t jj = std::rand() % NUM;
        std::swap(idx_arr[ii], idx_arr[jj]);
    }

    for (size_t kk = 0; kk < 2; ++kk) {
        for (size_t ii = 0; ii < NUM; ++ii) {
            size_t idx = idx_arr[ii] * 10;
            std::string key_str = "key" + TestSuite::lzStr(5, idx);
            std::string val_str = "val" + TestSuite::lzStr(2, kk) +
                                  "_" + TestSuite::lzStr(5, idx);
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );

            if ( (kk == 0 && ii == NUM/2 - 1 && flush_options == 1) ||
                 (kk == 1 && ii == NUM/2 - 1 && flush_options == 3) ) {
                CHK_Z( db->sync(false) );
                CHK_Z( db->flushLogs() );
            }
        }
        CHK_Z( db->sync(false) );
        if (kk == 0 && flush_options == 2) {
            CHK_Z( db->flushLogs() );
        }
    }
    CHK_Z( db->sync(false) );
    if (flush_options == 4) {
        CHK_Z( db->flushLogs() );
    }

    auto verify_func = [&]( size_t ii,
                            jungle::SearchOptions s_opt,
                            bool exact_query ) -> int {
        TestSuite::setInfo("ii %zu, s_opt %d, exact_query %d",
                           ii, s_opt, exact_query);

        int64_t idx = exact_query ? ii * 10 : ii * 10 + (s_opt.isGreater() ? 1 : -1);
        int64_t exp_idx = 0;

        if (exact_query) {
            if (s_opt.isExactMatchAllowed()) {
                exp_idx = idx;
            } else {
                exp_idx = (ii + (s_opt.isGreater() ? 1 : -1)) * 10;
            }
        } else {
            exp_idx = (ii + (s_opt.isGreater() ? 1 : -1)) * 10;
        }

        std::string key_str;
        if (idx >= 0) {
            key_str = "key" + TestSuite::lzStr(5, idx);
        } else {
            key_str = "000";
        }
        std::string val_str = "val" + TestSuite::lzStr(2, 1) +
                              "_" + TestSuite::lzStr(5, exp_idx);

        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        s = db->getNearestRecordByKey(jungle::SizedBuf(key_str), rec_out, s_opt);
        if (s_opt == jungle::SearchOptions::EQUAL) {
            // EQUAL should be denied.
            CHK_NOT(s);
            return 0;
        }

        bool exp_succ = false;
        if (exact_query) {
            if ( s_opt.isExactMatchAllowed() ||
                 (s_opt.isGreater() && ii < NUM - 1) ||
                 (s_opt.isSmaller() && ii > 0) ) {
                exp_succ = true;
            }
        } else {
            if ( (s_opt.isGreater() && ii < NUM - 1) ||
                 (s_opt.isSmaller() && ii > 0) ) {
                exp_succ = true;
            }
        }

        if (exp_succ) {
            CHK_Z(s);
            CHK_EQ( jungle::SizedBuf(val_str), rec_out.kv.value );
        } else  {
            CHK_NOT(s);
        }
        return 0;
    };

    using jungle::SearchOptions;
    for (size_t ii = 0; ii < NUM; ++ii) {
        for (bool exact_query: {true, false}) {
            for (SearchOptions s_opt: { SearchOptions::GREATER_OR_EQUAL,
                                        SearchOptions::GREATER,
                                        SearchOptions::SMALLER_OR_EQUAL,
                                        SearchOptions::SMALLER,
                                        SearchOptions::EQUAL }) {
                CHK_Z( verify_func(ii, s_opt, exact_query) );
            }
        }
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int get_nearest_on_deleted_keys_test(int flush_options) {
    // flush_options:
    //   0: all in the log.
    //   1: 3/4 in the log and 1/4 in the table.
    //   2: 1/2 in the log and 1/2 in the table.
    //   3: 1/4 in the log and 3/4 in the table.
    //   4: all in the table.

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.maxEntriesInLogFile = 20;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 100;

    // Shuffle (0 -- 99).
    std::vector<size_t> idx_arr(NUM);
    std::iota(idx_arr.begin(), idx_arr.end(), 0);
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t jj = std::rand() % NUM;
        std::swap(idx_arr[ii], idx_arr[jj]);
    }

    for (size_t kk = 0; kk < 2; ++kk) {
        for (size_t ii = 0; ii < NUM; ++ii) {
            size_t idx = idx_arr[ii] * 10;
            std::string key_str = "key" + TestSuite::lzStr(5, idx);
            std::string val_str = "val" + TestSuite::lzStr(2, kk) +
                                  "_" + TestSuite::lzStr(5, idx);

            if (kk == 0) {
                CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
            } else if (kk == 1) {
                CHK_Z( db->del( key_str ) );
            }

            if ( (kk == 0 && ii == NUM/2 - 1 && flush_options == 1) ||
                 (kk == 1 && ii == NUM/2 - 1 && flush_options == 3) ) {
                CHK_Z( db->sync(false) );
                CHK_Z( db->flushLogs() );
            }
        }
        CHK_Z( db->sync(false) );
        if (kk == 0 && flush_options == 2) {
            CHK_Z( db->flushLogs() );
        }
    }
    CHK_Z( db->sync(false) );
    if (flush_options == 4) {
        CHK_Z( db->flushLogs() );
    }

    auto verify_func = [&]( size_t ii,
                            jungle::SearchOptions s_opt,
                            bool exact_query ) -> int {
        TestSuite::setInfo("ii %zu, s_opt %d, exact_query %d",
                           ii, s_opt, exact_query);

        int64_t idx = exact_query ? ii * 10 : ii * 10 + (s_opt.isGreater() ? 1 : -1);

        std::string key_str;
        if (idx >= 0) {
            key_str = "key" + TestSuite::lzStr(5, idx);
        } else {
            key_str = "000";
        }

        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        s = db->getNearestRecordByKey(jungle::SizedBuf(key_str), rec_out, s_opt);
        if (s.ok()) {
            CHK_TRUE(rec_out.isDel());
        }

        return 0;
    };

    using jungle::SearchOptions;
    for (size_t ii = 0; ii < NUM; ++ii) {
        for (bool exact_query: {true, false}) {
            for (SearchOptions s_opt: { SearchOptions::GREATER_OR_EQUAL,
                                        SearchOptions::GREATER,
                                        SearchOptions::SMALLER_OR_EQUAL,
                                        SearchOptions::SMALLER,
                                        SearchOptions::EQUAL }) {
                CHK_Z( verify_func(ii, s_opt, exact_query) );
            }
        }
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int get_by_prefix_test(size_t hash_len) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.maxEntriesInLogFile = 20;
    config.customLenForHash = [hash_len](const jungle::HashKeyLenParams& p) -> size_t {
        return hash_len;
    };
    config.bloomFilterBitsPerUnit = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 100;

    // Shuffle (0 -- 99).
    std::vector<size_t> idx_arr(NUM);
    std::iota(idx_arr.begin(), idx_arr.end(), 0);
#if 0
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(idx_arr.begin(), idx_arr.end(), g);
#else
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t jj = std::rand() % NUM;
        std::swap(idx_arr[ii], idx_arr[jj]);
    }
#endif

    const size_t ROUND_MAX = 10;
    for (size_t round = 0; round < ROUND_MAX; ++round) {
        for (size_t ii = 0; ii < NUM; ++ii) {
            size_t idx = idx_arr[ii];
            std::string key_str = "key" + TestSuite::lzStr(5, idx) + "_" +
                                  TestSuite::lzStr(2, ROUND_MAX - round);
            std::string val_str = "val" + TestSuite::lzStr(5, idx) + "_" +
                                  TestSuite::lzStr(2, ROUND_MAX - round);
            CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        }
        CHK_Z( db->sync(false) );
        if (round == 0) {
            CHK_Z( db->flushLogs() );
        } else if (round >= 1 && round % 2 == 0 && round < ROUND_MAX - 2) {
            for (size_t hh = 0; hh < config.numL0Partitions; ++hh) {
                CHK_Z( db->compactL0(jungle::CompactOptions(), hh) );
            }
            CHK_Z( db->flushLogs() );
        }
    }

    // Get all records with the given prefix.
    for (size_t ii = 0; ii < NUM; ++ii) {
        TestSuite::setInfo("ii = %zu", ii);
        size_t idx = ii;
        std::string prefix_str = "key" + TestSuite::lzStr(5, idx);
        std::list<jungle::Record> recs_out;
        auto cb_func = [&](const jungle::SearchCbParams& params) ->
                       jungle::SearchCbDecision {
            jungle::Record dst;
            params.rec.copyTo(dst);
            recs_out.push_back(dst);
            return jungle::SearchCbDecision::NEXT;
        };
        CHK_Z( db->getRecordsByPrefix(jungle::SizedBuf(prefix_str), cb_func) );

        CHK_EQ(ROUND_MAX, recs_out.size());
        for (jungle::Record& rec: recs_out) rec.free();
    }

    // Same but stop in the middle.
    const size_t STOP_AT = 5;
    for (size_t ii = 0; ii < NUM; ++ii) {
        TestSuite::setInfo("ii = %zu", ii);
        size_t idx = ii;
        std::string prefix_str = "key" + TestSuite::lzStr(5, idx);
        std::list<jungle::Record> recs_out;
        auto cb_func = [&](const jungle::SearchCbParams& params) ->
                       jungle::SearchCbDecision {
            jungle::Record dst;
            params.rec.copyTo(dst);
            recs_out.push_back(dst);
            if (recs_out.size() >= STOP_AT) return jungle::SearchCbDecision::STOP;
            return jungle::SearchCbDecision::NEXT;
        };
        CHK_Z( db->getRecordsByPrefix(jungle::SizedBuf(prefix_str), cb_func) );

        CHK_EQ(STOP_AT, recs_out.size());
        for (jungle::Record& rec: recs_out) rec.free();
    }

    // Exact match should work.
    for (size_t round = 0; round < ROUND_MAX; ++round) {
        for (size_t ii = 0; ii < NUM; ++ii) {
            TestSuite::setInfo("round = %zu, ii = %zu", round, ii);
            size_t idx = idx_arr[ii];
            std::string key_str = "key" + TestSuite::lzStr(5, idx) + "_" +
                                  TestSuite::lzStr(2, ROUND_MAX - round);
            std::string val_str = "val" + TestSuite::lzStr(5, idx) + "_" +
                                  TestSuite::lzStr(2, ROUND_MAX - round);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get( jungle::SizedBuf(key_str), value_out ) );
            CHK_EQ( val_str, value_out.toString() );
        }
    }

    {   // Insert a record with new prefix, and keep it in log section.
        std::string key_str = "key_newprefix";
        std::string val_str = "val_newprefix";
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        CHK_Z( db->sync(false) );

        // Search keys with the new prefix, the API should return OK.
        bool cb_invoked = false;
        auto cb_func = [&](const jungle::SearchCbParams& params) ->
                       jungle::SearchCbDecision {
            cb_invoked = true;
            return jungle::SearchCbDecision::NEXT;
        };
        CHK_Z( db->getRecordsByPrefix(jungle::SizedBuf(key_str), cb_func) );
        CHK_TRUE( cb_invoked );

        // Flush it to table, now log section is empty.
        CHK_Z( db->flushLogs() );

        cb_invoked = false;
        CHK_Z( db->getRecordsByPrefix(jungle::SizedBuf(key_str), cb_func) );
        CHK_TRUE( cb_invoked );

        // Find non-existing key.
        std::string ne_key_str = "non_existing_key";
        cb_invoked = false;
        s = db->getRecordsByPrefix(jungle::SizedBuf(ne_key_str), cb_func);
        CHK_EQ( jungle::Status::KEY_NOT_FOUND, s.getValue() );
        CHK_FALSE( cb_invoked );

    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int get_by_prefix_frequent_update_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    std::string key_prefix = "key";
    std::string val_prefix = "val";
    for (size_t ii = 0; ii < 2; ++ii) {
        std::string key_str = key_prefix + TestSuite::lzStr(2, ii);
        std::string val_str = val_prefix + TestSuite::lzStr(2, ii);
        CHK_Z(db->set(jungle::KV(key_str, val_str)));

        if (ii == 0) {
            // Flush the first log.
            CHK_Z(db->sync());
            CHK_Z(db->flushLogs());
        }
    }

    size_t count = 0;
    auto cb_func = [&](const jungle::SearchCbParams& params)
                   -> jungle::SearchCbDecision {
        count++;
        return jungle::SearchCbDecision::NEXT;
    };
    CHK_Z(db->getRecordsByPrefix(jungle::SizedBuf(key_prefix), cb_func));

    // Two records should be found.
    CHK_EQ(2, count);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int get_by_prefix_out_of_order_delete_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 10;
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    std::string key_prefix = "key";
    std::string val_prefix = "val";

    // Insert keys in asc order.
    for (size_t ii = 0; ii < 10; ++ii) {
        std::string key_str = key_prefix + TestSuite::lzStr(3, ii);;
        std::string val_str = val_prefix + TestSuite::lzStr(3, ii);
        CHK_Z(db->set(jungle::KV(key_str, val_str)));
    }

    // Delete odd number keys in arbitrary order. They should be in the next log file.
    std::vector<size_t> del_keys = {5, 7, 3, 9, 1};
    for (size_t ii: del_keys) {
        std::string key_str = key_prefix + TestSuite::lzStr(3, ii);;
        CHK_Z(db->del(jungle::SizedBuf(key_str)));
    }

    size_t count = 0;
    auto cb_func = [&](const jungle::SearchCbParams& params)
                   -> jungle::SearchCbDecision {
        // Should be returned in asc order.
        TestSuite::_msg("key: %s, type: %d\n",
                        params.rec.kv.key.toReadableString().c_str(),
                        params.rec.type);
        std::string exp_key = key_prefix + TestSuite::lzStr(3, count);
        if (jungle::SizedBuf(exp_key) != params.rec.kv.key) {
            return jungle::SearchCbDecision::STOP;
        }
        if (count % 2 == 1) {
            if (!params.rec.isDel()) {
                return jungle::SearchCbDecision::STOP;
            }
        } else {
            if (params.rec.isDel()) {
                return jungle::SearchCbDecision::STOP;
            }
        }
        count++;
        return jungle::SearchCbDecision::NEXT;
    };
    CHK_Z(db->getRecordsByPrefix(jungle::SizedBuf(key_prefix), cb_func));

    // All records should be found.
    CHK_EQ(10, count);

    // Search with prefix smaller than key.
    CHK_EQ(jungle::Status::KEY_NOT_FOUND,
           db->getRecordsByPrefix(jungle::SizedBuf("a"), cb_func).getValue());

    // Search with prefix greater than key.
    CHK_EQ(jungle::Status::KEY_NOT_FOUND,
           db->getRecordsByPrefix(jungle::SizedBuf("l"), cb_func).getValue());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("get nearest test",
              get_nearest_test,
              TestRange<int>(0, 4, 1, StepType::LINEAR));
    ts.doTest("get nearest on deleted keys test",
              get_nearest_on_deleted_keys_test,
              TestRange<int>(0, 4, 1, StepType::LINEAR));
    ts.doTest("get by prefix test",
              get_by_prefix_test, TestRange<size_t>( {0, 8, 16} ));
    ts.doTest("get by prefix frequent update test",
              get_by_prefix_frequent_update_test);
    ts.doTest("get by prefix out of order delete test",
              get_by_prefix_out_of_order_delete_test);
    return 0;
}