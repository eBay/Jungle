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

int cmp_double(void* a, size_t len_a,
               void* b, size_t len_b,
               void* param)
{
    CHK_NONNULL(param);

    double aa = *(double*)a;
    double bb = *(double*)b;
    if (aa < bb) return -1;
    else if (aa > bb) return 1;
    return 0;
}

static int _custom_cmp_set(jungle::DB* db,
                           std::vector<jungle::KV>& kv) {
    int n = kv.size();
    for (int ii=0; ii<n; ++ii) {
        double key = (double)ii * 100 / 13;
        double val = (double)ii * 100 / 17;
        jungle::KV& kv_ref = kv[ii];
        kv_ref.key.set(sizeof(key), &key);
        kv_ref.value.set(sizeof(val), &val);
        CHK_Z(db->set(kv_ref));
    }
    return 0;
}

static int _custom_cmp_get(jungle::DB* db,
                           std::vector<jungle::KV>& kv) {
    int n = kv.size();
    for (int ii=0; ii<n; ++ii) {
        double key = (double)ii * 100 / 13;
        double val = (double)ii * 100 / 17;
        jungle::SizedBuf key_buf(sizeof(key), &key);
        jungle::SizedBuf value_out;
        CHK_Z(db->get(key_buf, value_out));

        double val_out = *(double*)value_out.data;
        CHK_EQ(val, val_out);
        value_out.free();
    }
    return 0;
}

int cmp_log_only() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB with custom cmp (double).
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.cmpFunc = cmp_double;
    config.cmpFuncParam = (void*)&db;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    _custom_cmp_set(db, kv);

    // Just sync.
    CHK_Z(db->sync());

    // Point query check.
    _custom_cmp_get(db, kv);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TestSuite::clearTestFile(prefix);
    return 0;
}

int cmp_table_only() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB with custom cmp (double).
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.cmpFunc = cmp_double;
    config.cmpFuncParam = (void*)&db;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    _custom_cmp_set(db, kv);

    // Sync and flush.
    CHK_Z(db->sync());
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options));

    // Point query check.
    _custom_cmp_get(db, kv);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TestSuite::clearTestFile(prefix);
    return 0;
}

int cmp_mixed() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB with custom cmp (double).
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.cmpFunc = cmp_double;
    config.cmpFuncParam = (void*)&db;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 10;

    // Even numbers: in table.
    std::vector<jungle::KV> kv(n);
    for (int ii=0; ii<n; ii+=2) {
        double key = (double)ii * 100 / 13;
        double val = (double)ii * 100 / 17;
        jungle::KV& kv_ref = kv[ii];
        kv_ref.key.set(sizeof(key), &key);
        kv_ref.value.set(sizeof(val), &val);
        CHK_Z(db->set(kv_ref));
    }
    CHK_Z(db->sync());
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options));

    // Odd numbers: in log.
    for (int ii=1; ii<n; ii+=2) {
        double key = (double)ii * 100 / 13;
        double val = (double)ii * 100 / 17;
        jungle::KV& kv_ref = kv[ii];
        kv_ref.key.set(sizeof(key), &key);
        kv_ref.value.set(sizeof(val), &val);
        CHK_Z(db->set(kv_ref));
    }
    CHK_Z(db->sync());

    // Point query check.
    _custom_cmp_get(db, kv);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TestSuite::clearTestFile(prefix);
    return 0;
}

static int _custom_cmp_itr(jungle::Iterator& itr,
                           std::vector<jungle::KV>& kv) {
    int n = kv.size();
    int count = 0;
    do {
        jungle::Record rec_out;
        jungle::Status s = itr.get(rec_out);
        if (!s) break;

        double key = (double)count * 100 / 13;
        double key_out = *(double*)rec_out.kv.key.data;
        CHK_EQ(key, key_out);

        double val = (double)count * 100 / 17;
        double val_out = *(double*)rec_out.kv.value.data;
        CHK_EQ(val, val_out);

        rec_out.free();
        count++;
    } while (itr.next());
    CHK_EQ(n, count);

    return 0;
}

int cmp_itr_log_only() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB with custom cmp (double).
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.cmpFunc = cmp_double;
    config.cmpFuncParam = (void*)&db;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    _custom_cmp_set(db, kv);

    // Just sync.
    CHK_Z(db->sync());

    // Key iterator check.
    jungle::Iterator itr;
    CHK_Z(itr.init(db));
    CHK_Z(_custom_cmp_itr(itr, kv));
    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TestSuite::clearTestFile(prefix);
    return 0;
}

int cmp_itr_table_only() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB with custom cmp (double).
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.cmpFunc = cmp_double;
    config.cmpFuncParam = (void*)&db;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    _custom_cmp_set(db, kv);

    // Sync and flush.
    CHK_Z(db->sync());
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options));

    // Key iterator check.
    jungle::Iterator itr;
    CHK_Z(itr.init(db));
    CHK_Z(_custom_cmp_itr(itr, kv));
    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TestSuite::clearTestFile(prefix);
    return 0;
}

int cmp_itr_mixed() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB with custom cmp (double).
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.cmpFunc = cmp_double;
    config.cmpFuncParam = (void*)&db;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 10;

    // Even numbers: in table.
    std::vector<jungle::KV> kv(n);
    for (int ii=0; ii<n; ii+=2) {
        double key = (double)ii * 100 / 13;
        double val = (double)ii * 100 / 17;
        jungle::KV& kv_ref = kv[ii];
        kv_ref.key.set(sizeof(key), &key);
        kv_ref.value.set(sizeof(val), &val);
        CHK_Z(db->set(kv_ref));
    }
    CHK_Z(db->sync());
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options));

    // Odd numbers: in log.
    for (int ii=1; ii<n; ii+=2) {
        double key = (double)ii * 100 / 13;
        double val = (double)ii * 100 / 17;
        jungle::KV& kv_ref = kv[ii];
        kv_ref.key.set(sizeof(key), &key);
        kv_ref.value.set(sizeof(val), &val);
        CHK_Z(db->set(kv_ref));
    }
    CHK_Z(db->sync());

    // Key iterator check.
    jungle::Iterator itr;
    CHK_Z(itr.init(db));
    CHK_Z(_custom_cmp_itr(itr, kv));
    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TestSuite::clearTestFile(prefix);
    return 0;
}

/*
int basic_test_template() {
    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    jungle::shutdown();
    TestSuite::clearTestFile(prefix);
    return 0;
}
*/

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("custom cmp log only", cmp_log_only);
    ts.doTest("custom cmp table only", cmp_table_only);
    ts.doTest("custom cmp mixed", cmp_mixed);
    ts.doTest("custom cmp iteration log only", cmp_itr_log_only);
    ts.doTest("custom cmp iteration table only", cmp_itr_table_only);
    ts.doTest("custom cmp iteration mixed", cmp_itr_mixed);

    return 0;
}
