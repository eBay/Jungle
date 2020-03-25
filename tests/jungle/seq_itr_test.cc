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

#include "libjungle/jungle.h"

#include <vector>

#include <stdio.h>

int itr_seq_empty() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 10;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Iterator on empty DB, should succeed.
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_Z(s);

    // All get, prev, next should fail.
    jungle::Record rec_out;
    CHK_NOT(itr.get(rec_out));
    CHK_NOT(itr.prev());
    CHK_NOT(itr.next());

    // Close iterator.
    itr.close();

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_basic() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Iterator.
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_Z(s);

    // prev() should fail.
    s = itr.prev();
    CHK_NOT(s);

    // Check returned records (forward).
    CHK_Z(_itr_check(0, n, itr, kv));
    // Check returned records (backward).
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    // Close iterator.
    itr.close();

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_purge() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync & partial flush.
    CHK_Z(db->sync(false));

    uint64_t flush_upto = 2;
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options, flush_upto));

    // Min seqnum == flush_upto + 1.
    { uint64_t seq;
      s = db->getMinSeqNum(seq);
      CHK_Z(s);
      CHK_EQ(flush_upto+1, seq); }

    // Flush seqnum == flush_upto.
    { uint64_t seq;
      s = db->getLastFlushedSeqNum(seq);
      CHK_Z(s);
      CHK_EQ(flush_upto, seq); }

    // Iterator.
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_Z(s);

    // prev() should fail.
    s = itr.prev();
    CHK_NOT(s);

    // Check returned records.
    CHK_Z(_itr_check(0, n, itr, kv));
    // Check returned records (backward).
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    // Close iterator.
    itr.close();

    // Flush all.
    CHK_Z(db->flushLogs(f_options));

    // Check again.
    CHK_Z(itr.initSN(db));
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));
    CHK_Z(itr.close());

    // Set more KV pairs.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key2_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));
    CHK_Z(db->sync(false));

    // Check again.
    CHK_Z(itr.initSN(db));
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check(0, n, itr, kv2));
    CHK_Z(_itr_check_bwd(0, n, itr, kv2));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));
    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_isolation() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Iterator.
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_Z(s);

    // Add more KVs
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key2_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Check returned records.
    // New KVs should not be visiable by this iterator.
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    // But visiable by normal get.
    CHK_Z(_get_bykey_check(0, n, db, kv2));

    // Close iterator.
    itr.close();

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_multiple_logs() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 10;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 100;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync & partial flush.
    CHK_Z(db->sync(false));

    uint64_t flush_upto = 150;
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options, flush_upto));

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Iterator.
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_Z(s);
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));
    s = itr.close();
    CHK_Z(s);

    // Flush all.
    CHK_Z(db->flushLogs(f_options));

    // Check again.
    CHK_Z(itr.initSN(db));
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));
    CHK_Z(itr.close());

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_preserve_log_file_test() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 10;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 100;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync & partial flush.
    CHK_Z(db->sync(false));

    // Open iterator.
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_Z(s);

    // Flush while the iterator is still alive.
    uint64_t flush_upto = n * 95 / 100;
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options, flush_upto));

    // Check.
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close iterator.
    s = itr.close();
    CHK_Z(s);

    // Re-open iterator.
    s = itr.initSN(db);
    CHK_Z(s);
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));
    s = itr.close();
    CHK_Z(s);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_seek() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 5;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 50;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync & partial flush.
    CHK_Z(db->sync(false));

    uint64_t flush_upto = 23;
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options, flush_upto));

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Iterator.
    jungle::Iterator itr;
    CHK_Z(itr.initSN(db));

    // Forward
    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seekSN(ii+1));
        CHK_Z(_itr_check(ii, n, itr, kv));
    }
    // Backward
    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seekSN(ii+1));
        CHK_Z(_itr_check_bwd(0, ii+1, itr, kv));
    }

    // Out-of-range: should return max seq.
    CHK_Z(itr.seekSN(n+1));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    // gotoBegin.
    CHK_Z(itr.gotoBegin());
    CHK_Z(_itr_check(0, n, itr, kv));

    // gotoEnd.
    CHK_Z(itr.gotoEnd());
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    CHK_Z(itr.close());

    // Flush all.
    CHK_Z(db->flushLogs(f_options));

    // Check again.
    CHK_Z(itr.initSN(db));
    // Forward
    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seekSN(ii+1));
        CHK_Z(_itr_check(ii, n, itr, kv));
    }
    // Backward
    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seekSN(ii+1));
        CHK_Z(_itr_check_bwd(0, ii+1, itr, kv));
    }

    // gotoBegin.
    CHK_Z(itr.gotoBegin());
    CHK_Z(_itr_check(0, n, itr, kv));

    // gotoEnd.
    CHK_Z(itr.gotoEnd());
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    CHK_Z(itr.close());

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_min_max_logs() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 5;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 50;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync.
    CHK_Z(db->sync(false));

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Iterator.
    for (size_t ii=1; ii<=50; ++ii) {
        for (size_t jj=ii; jj<=50; ++jj) {
            TestSuite::setInfo("ii: %zu, jj: %zu", ii, jj);
            jungle::Iterator itr;
            CHK_Z(itr.initSN(db, ii, jj));
            CHK_Z(_itr_check(ii-1, jj, itr, kv));
        }
    }

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_out_of_range_table() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 5;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int NUM = 100;
    std::vector<jungle::KV> kv(NUM);

    CHK_Z(_init_kv_pairs(NUM, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, NUM, 0, db, kv));

    // Sync and flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    // Iterator up to 50.
    jungle::Iterator itr;
    CHK_Z(itr.initSN(db, 1, 50));
    size_t count = 0;
    do {
        jungle::Record rec_out;
        jungle::Record::Holder h(rec_out);
        s = itr.get(rec_out);
        if (!s) break;
        count++;
    } while (itr.next().ok());
    itr.close();
    CHK_EQ(50, count);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(NUM, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_seq_out_of_range_snapshot() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 5;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int NUM = 100;
    std::vector<jungle::KV> kv(NUM);

    CHK_Z(_init_kv_pairs(NUM, kv, "key1_", "value1_"));
    CHK_Z(_set_byseq_kv_pairs(0, NUM, 0, db, kv));

    // Sync and flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    // Open snapshot and then iterator.
    // FIXME: If there is no write after
    //        flushing logs and then creating persistent checkpoint,
    //        opening snapshot fails.
    jungle::DB* snap_out = nullptr;
    CHK_Z(db->openSnapshot(&snap_out));

    jungle::Iterator itr;
    CHK_Z(itr.initSN(snap_out, 1, 50));
    size_t count = 0;
    do {
        jungle::Record rec_out;
        jungle::Record::Holder h(rec_out);
        s = itr.get(rec_out);
        if (!s) break;
        count++;
    } while (itr.next().ok());
    itr.close();
    CHK_EQ(50, count);

    // Close DB.
    CHK_Z(jungle::DB::close(snap_out));
    CHK_Z(jungle::DB::close(db));

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(NUM, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("seq empty itr", itr_seq_empty);
    ts.doTest("seq itr test", itr_seq_basic);
    ts.doTest("seq itr flush test", itr_seq_purge);
    ts.doTest("seq itr snapshot isolation test", itr_seq_isolation);
    ts.doTest("seq itr multiple log files test", itr_seq_multiple_logs);
    ts.doTest("seq itr preserve log files test", itr_seq_preserve_log_file_test);
    ts.doTest("seq itr seek test", itr_seq_seek);
    ts.doTest("seq itr min max test", itr_seq_min_max_logs);
    ts.doTest("seq itr out of range table test", itr_seq_out_of_range_table);
    ts.doTest("seq itr out of range snapshot test", itr_seq_out_of_range_snapshot);

    return 0;
}
