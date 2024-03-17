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
#include "table_helper.h"

#include <vector>

#include <stdio.h>

int itr_key_empty() {
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
    s = itr.init(db);
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

int itr_key_basic() {
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
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Update even number KV pairs.
    int seq_count = n;
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    for (int ii=0; ii<n; ii+=2) {
        CHK_Z(db->setSN(seq_count, kv2[ii]));
        seq_count++;
    }

    // Iterator.
    jungle::Iterator itr;
    s = itr.init(db);
    CHK_Z(s);

    // Check returned records.
    int count = 0;
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        if (count % 2 == 0) {
            CHK_EQ(kv2[count].key, rec_out.kv.key);
            CHK_EQ(kv2[count].value, rec_out.kv.value);
        } else {
            CHK_EQ(kv[count].key, rec_out.kv.key);
            CHK_EQ(kv[count].value, rec_out.kv.value);
        }

        rec_out.free();
        count++;
    } while (itr.next());
    CHK_EQ(n, count);

    // Backward
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        count--;
        if (count % 2 == 0) {
            CHK_EQ(kv2[count].key, rec_out.kv.key);
            CHK_EQ(kv2[count].value, rec_out.kv.value);
        } else {
            CHK_EQ(kv[count].key, rec_out.kv.key);
            CHK_EQ(kv[count].value, rec_out.kv.value);
        }

        rec_out.free();
    } while (itr.prev());
    CHK_EQ(0, count);

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

int itr_key_purge() {
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
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync and flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Update even number KV pairs.
    int seq_count = n + 1;
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key", "value2_"));
    for (int ii=0; ii<n; ii+=2) {
        CHK_Z(db->setSN(seq_count, kv2[ii]));
        seq_count++;
    }
    CHK_Z(db->sync(false));

    // Iterator.
    jungle::Iterator itr;
    s = itr.init(db);
    CHK_Z(s);

    // Check returned records.
    int count = 0;
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        if (count % 2 == 0) {
            CHK_EQ(kv2[count].key, rec_out.kv.key);
            CHK_EQ(kv2[count].value, rec_out.kv.value);
        } else {
            CHK_EQ(kv[count].key, rec_out.kv.key);
            CHK_EQ(kv[count].value, rec_out.kv.value);
        }

        rec_out.free();
        count++;
    } while (itr.next());
    CHK_EQ(n, count);

    // Backward
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        count--;
        if (count % 2 == 0) {
            CHK_EQ(kv2[count].key, rec_out.kv.key);
            CHK_EQ(kv2[count].value, rec_out.kv.value);
        } else {
            CHK_EQ(kv[count].key, rec_out.kv.key);
            CHK_EQ(kv[count].value, rec_out.kv.value);
        }

        rec_out.free();
    } while (itr.prev());
    CHK_EQ(0, count);

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

int itr_key_isolation() {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));

    // Set even number KV pairs.
    for (int ii=0; ii<10; ii+=2) {
        db->set(kv[ii]);
    }

    // Iterator.
    jungle::Iterator itr;
    s = itr.init(db);
    CHK_Z(s);

    // Set odd number KV pairs.
    for (int ii=1; ii<10; ii+=2) {
        db->set(kv[ii]);
    }

    // Only even numbers should be visible.
    int count = 0;
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        CHK_EQ(kv[count].key, rec_out.kv.key);
        CHK_EQ(kv[count].value, rec_out.kv.value);

        rec_out.free();
        count += 2;
    } while (itr.next());
    CHK_EQ(n, count);

    // But visiable by normal get.
    CHK_Z(_get_bykey_check(0, n, db, kv));

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

int itr_key_seek() {
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

    uint64_t flush_upto = 13;
    jungle::FlushOptions f_options;
    CHK_Z(db->flushLogs(f_options, flush_upto));

    // By-key check.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Iterator.
    jungle::Iterator itr;
    CHK_Z(itr.init(db));

    // Forward
    for (int ii=0; ii<n; ++ii) {
        TestSuite::setInfo("ii=%d", ii);
        CHK_Z(itr.seek(kv[ii].key));
        CHK_Z(_itr_check(ii, n, itr, kv));
        TestSuite::clearInfo();
    }
    // Backward
    for (int ii=0; ii<n; ++ii) {
        TestSuite::setInfo("ii=%d", ii);
        CHK_Z(itr.seek(kv[ii].key));
        CHK_Z(_itr_check_bwd(0, ii+1, itr, kv));
        TestSuite::clearInfo();
    }

    // Out-of-range: should return max seq.
    jungle::SizedBuf oor_key;
    oor_key.alloc("zzz");
    CHK_Z(itr.seek(oor_key));
    CHK_Z(_itr_check_bwd(0, n, itr, kv));
    oor_key.free();

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
    CHK_Z(itr.init(db));
    // Forward
    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seek(kv[ii].key));
        CHK_Z(_itr_check(ii, n, itr, kv));
    }
    // Backward
    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seek(kv[ii].key));
        CHK_Z(_itr_check_bwd(0, ii+1, itr, kv));
    }

    // gotoBegin.
    CHK_Z(itr.gotoBegin());
    CHK_Z(_itr_check(0, n, itr, kv));

    // gotoEnd.
    CHK_Z(itr.gotoEnd());
    CHK_Z(_itr_check_bwd(0, n, itr, kv));

    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_key_deleted() {
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
    for (int ii=0; ii<n; ii++) {
        CHK_Z(db->set(kv[ii]));
    }

    // Delete even numbers.
    for (int ii=0; ii<n; ii+=2) {
        CHK_Z(db->del(kv[ii].key));
    }

    // Sync & partial flush.
    CHK_Z(db->sync(false));

    // By-key check.
    for (int ii=0; ii<n; ++ii) {
        jungle::SizedBuf value_out;
        s = db->get(kv[ii].key, value_out);
        if (ii % 2 == 0) {
            CHK_NOT(s);
        } else {
            CHK_EQ(kv[ii].value, value_out);
        }
        value_out.free();
    }

    // Iterator.
    jungle::Iterator itr;
    CHK_Z(itr.init(db));

    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seek(kv[ii].key));
        CHK_Z(_itr_check_step(ii + ((ii%2 == 0)?1:0), n, 2, itr, kv));
    }

    for (int ii=0; ii<n; ++ii) {
        CHK_Z(itr.seek(kv[ii].key));
        CHK_Z(_itr_check_bwd_step(0, ii + ((ii%2 == 0)?1:0) + 1, 2, itr, kv));
    }

    // Out-of-range: should return max key.
    jungle::SizedBuf oor_key;
    oor_key.alloc("zzz");
    CHK_Z(itr.seek(oor_key));
    CHK_Z(_itr_check_bwd_step(0, n, 2, itr, kv));
    oor_key.free();

    // gotoBegin.
    CHK_Z(itr.gotoBegin());
    CHK_Z(_itr_check_step(1, n, 2, itr, kv));

    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_key_all_del_markers() {
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
    for (int ii=0; ii<n; ii++) {
        CHK_Z(db->del(kv[ii].key));
    }
    CHK_Z(db->sync(false));

    // By-key check.
    for (int ii=0; ii<n; ++ii) {
        jungle::SizedBuf value_out;
        s = db->get(kv[ii].key, value_out);
        CHK_NOT(s);
        value_out.free();
    }

    // Iterator.
    jungle::Iterator itr;
    jungle::Record rec;
    CHK_Z(itr.init(db));
    CHK_NOT(itr.get(rec));

    CHK_Z(itr.gotoBegin());
    CHK_NOT(itr.get(rec));
    CHK_Z(itr.gotoEnd());
    CHK_NOT(itr.get(rec));

    CHK_Z(itr.close());

    // Insert a single KV.
    CHK_Z(db->set(kv[n/2]));

    // Now iterator should return that key only.
    CHK_Z(itr.init(db));
    CHK_Z(itr.get(rec));
    CHK_EQ(kv[n/2].key, rec.kv.key);
    rec.free();
    CHK_NOT(itr.next());

    CHK_Z(itr.gotoBegin());
    CHK_Z(itr.get(rec));
    CHK_EQ(kv[n/2].key, rec.kv.key);
    rec.free();
    CHK_NOT(itr.next());

    CHK_Z(itr.gotoEnd());
    CHK_Z(itr.get(rec));
    CHK_EQ(kv[n/2].key, rec.kv.key);
    rec.free();
    CHK_NOT(itr.prev());

    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_key_insert_delete_all() {
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
    for (int ii=0; ii<n; ii++) {
        CHK_Z(db->set(kv[ii]));
    }
    for (int ii=0; ii<n; ii++) {
        CHK_Z(db->del(kv[ii].key));
    }
    CHK_Z(db->sync(false));

    // By-key check.
    for (int ii=0; ii<n; ++ii) {
        jungle::SizedBuf value_out;
        s = db->get(kv[ii].key, value_out);
        CHK_NOT(s);
        value_out.free();
    }

    // Iterator.
    jungle::Iterator itr;
    jungle::Record rec;
    CHK_Z(itr.init(db));
    CHK_NOT(itr.get(rec));

    CHK_Z(itr.gotoBegin());
    CHK_NOT(itr.get(rec));
    CHK_Z(itr.gotoEnd());
    CHK_NOT(itr.get(rec));

    CHK_Z(itr.close());

    // Insert a single KV.
    CHK_Z(db->set(kv[n/2]));

    // Now iterator should return that key only.
    CHK_Z(itr.init(db));
    CHK_Z(itr.get(rec));
    CHK_EQ(kv[n/2].key, rec.kv.key);
    rec.free();
    CHK_NOT(itr.next());

    CHK_Z(itr.gotoBegin());
    CHK_Z(itr.get(rec));
    CHK_EQ(kv[n/2].key, rec.kv.key);
    rec.free();
    CHK_NOT(itr.next());

    CHK_Z(itr.gotoEnd());
    CHK_Z(itr.get(rec));
    CHK_EQ(kv[n/2].key, rec.kv.key);
    rec.free();
    CHK_NOT(itr.prev());

    CHK_Z(itr.close());

    // Close DB.
    s = jungle::DB::close(db);
    CHK_Z(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_key_flush_and_delete_all(bool recreate = false) {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));

    if (!recreate) {
        // Open DB once.
        TestSuite::clearTestFile(filename);
        jungle::DBConfig config;
        config.maxEntriesInLogFile = 5;
        s = jungle::DB::open(&db, filename, config);
        CHK_Z(s);
    }

    for (int kk=0; kk<2*n; kk++) {
        if (recreate) {
            // Open DB in every run.
            TestSuite::clearTestFile(filename);
            jungle::DBConfig config;
            config.maxEntriesInLogFile = 5;
            s = jungle::DB::open(&db, filename, config);
            CHK_Z(s);
        }

        // kk: decide when to flush.
        int cnt = 0;

        // set all
        for (int ii=0; ii<n; ii++) {
            CHK_Z(db->set(kv[ii]));
            if (cnt++ == kk) {
                CHK_Z(db->sync(false));
                CHK_Z(db->flushLogs(jungle::FlushOptions()));
            }
        }

        // delete all
        for (int ii=0; ii<n; ii++) {
            CHK_Z(db->del(kv[ii].key));
            if (cnt++ == kk) {
                CHK_Z(db->sync(false));
                CHK_Z(db->flushLogs(jungle::FlushOptions()));
            }
        }

        CHK_Z(db->sync(false));

        // By-key check: nothing should be visible.
        for (int ii=0; ii<n; ++ii) {
            jungle::SizedBuf value_out;
            s = db->get(kv[ii].key, value_out);
            CHK_NOT(s);
            value_out.free();
        }

        // Iterator.
        jungle::Iterator itr;
        jungle::Record rec;
        CHK_Z(itr.init(db));
        CHK_NOT(itr.get(rec));

        CHK_Z(itr.gotoBegin());
        CHK_NOT(itr.get(rec));
        CHK_Z(itr.gotoEnd());
        CHK_NOT(itr.get(rec));

        CHK_Z(itr.close());

        CHK_Z(db->flushLogs(jungle::FlushOptions()));

        if (recreate) {
            // Close DB in every run.
            s = jungle::DB::close(db);
            CHK_Z(s);
        }
    }

    if (!recreate) {
        // Close DB once.
        s = jungle::DB::close(db);
        CHK_Z(s);
    }

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int itr_key_flush_and_delete_half_even(bool recreate = false) {
    jungle::DB* db;
    jungle::Status s;

    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename)

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));

    if (!recreate) {
        // Open DB once.
        TestSuite::clearTestFile(filename);
        jungle::DBConfig config;
        config.maxEntriesInLogFile = 5;
        s = jungle::DB::open(&db, filename, config);
        CHK_Z(s);
    }

    for (int kk=0; kk<15*n/10; kk++) {
        if (recreate) {
            // Open DB in every run.
            TestSuite::clearTestFile(filename);
            jungle::DBConfig config;
            config.maxEntriesInLogFile = 5;
            s = jungle::DB::open(&db, filename, config);
            CHK_Z(s);
        }

        // kk: decide when to flush.
        int cnt = 0;

        // set all
        for (int ii=0; ii<n; ii++) {
            CHK_Z(db->set(kv[ii]));
            if (cnt++ == kk) {
                CHK_Z(db->sync(false));
                CHK_Z(db->flushLogs(jungle::FlushOptions()));
            }
        }

        // delete even numbers
        for (int ii=0; ii<n; ii+=2) {
            CHK_Z(db->del(kv[ii].key));
            if (cnt++ == kk) {
                CHK_Z(db->sync(false));
                CHK_Z(db->flushLogs(jungle::FlushOptions()));
            }
        }

        CHK_Z(db->sync(false));

        // By-key check: only odd numbers should be visible.
        for (int ii=0; ii<n; ++ii) {
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            s = db->get(kv[ii].key, value_out);
            if (ii % 2 == 0) {
                // even
                CHK_NOT(s);
                value_out.free();
            } else {
                // odd
                CHK_Z(s);
                CHK_EQ(kv[ii].value, value_out);
            }
        }

        // Iterator.
        jungle::Iterator itr;
        CHK_Z(itr.init(db));

        for (int ii=0; ii<n; ++ii) {
            TestSuite::setInfo("kk=%d ii=%d", kk, ii);
            CHK_Z(itr.seek(kv[ii].key));
            CHK_Z(_itr_check_step(ii + ((ii%2 == 0)?1:0), n, 2, itr, kv));
        }

        for (int ii=0; ii<n; ++ii) {
            TestSuite::setInfo("kk=%d ii=%d", kk, ii);
            CHK_Z(itr.seek(kv[ii].key));
            CHK_Z(_itr_check_bwd_step(0, ii + ((ii%2 == 0)?1:0) + 1, 2, itr, kv));
        }

        // Out-of-range: should return max key.
        jungle::SizedBuf oor_key;
        oor_key.alloc("zzz");
        CHK_Z(itr.seek(oor_key));
        CHK_Z(_itr_check_bwd_step(0, n, 2, itr, kv));
        oor_key.free();

        // gotoBegin.
        CHK_Z(itr.gotoBegin());
        CHK_Z(_itr_check_step(1, n, 2, itr, kv));

        CHK_Z(itr.close());

        CHK_Z(db->flushLogs(jungle::FlushOptions()));

        if (recreate) {
            // Close DB in every run.
            s = jungle::DB::close(db);
            CHK_Z(s);
        }
    }

    if (!recreate) {
        // Close DB once.
        s = jungle::DB::close(db);
        CHK_Z(s);
    }

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int different_version_and_level_test(bool deletion) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    jungle::DB* db;

    config.maxL0TableSize = 1024 * 1024;
    config.maxL1TableSize = 1024 * 1024;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 2;

    // Put older version to L1, and nwer version to L0.
    // Key: `kkk`, value: `v0` and `v1`.
    // If `deletion == true`, issue a delete operation.
    for (size_t ii = 0; ii < NUM; ++ii) {
        std::string key_str = "kkk";
        std::string val_str = "v" + TestSuite::lzStr(1, ii);
        if (ii == 1 && deletion) {
            CHK_Z(db->del(key_str));
        } else {
            CHK_Z(db->set(jungle::KV(key_str, val_str)));
        }
        CHK_Z(db->sync());
        CHK_Z(db->flushLogs());

        if (ii == 0) {
            jungle::CompactOptions c_opt;
            for (size_t jj = 0; jj < config.numL0Partitions; ++jj) {
                CHK_Z(db->compactL0(c_opt, jj));
            }
        }
    }

    // Put a greater key to log.
    // Key: `kkkk`, value: `v`.
    std::string key_str = "kkkk";
    std::string val_str = "v";
    CHK_Z(db->set(jungle::KV(key_str, val_str)));
    CHK_Z(db->sync());

    // Reverse scan.
    jungle::Iterator itr;
    std::string s_key_str = "k";
    std::string e_key_str = "l";
    itr.init(db, s_key_str, e_key_str);
    itr.gotoEnd();

    size_t count = 0;
    do {
        jungle::Record rec_out;
        jungle::Record::Holder h(rec_out);
        s = itr.get(rec_out);
        if (!s) break;

        TestSuite::_msg("key: %s, value: %s\n",
                        rec_out.kv.key.toString().c_str(),
                        rec_out.kv.value.toString().c_str());

        if (count == 0) {
            CHK_EQ(jungle::SizedBuf("kkkk"), rec_out.kv.key);
            CHK_EQ(jungle::SizedBuf("v"), rec_out.kv.value);

        } else if (count == 1) {
            if (deletion) {
                return -1;
            } else {
                CHK_EQ(jungle::SizedBuf("kkk"), rec_out.kv.key);
                CHK_EQ(jungle::SizedBuf("v1"), rec_out.kv.value);
            }

        } else {
            return -1;
        }

        rec_out.free();
        count++;
    } while (itr.prev().ok());
    itr.close();

    if (deletion) {
        CHK_EQ(1, count);
    } else {
        CHK_EQ(2, count);
    }

    CHK_Z(jungle::DB::close(db));

    jungle::shutdown();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    //ts.options.abortOnFailure = true;
    ts.doTest("key empty itr", itr_key_empty);
    ts.doTest("key itr test", itr_key_basic);
    ts.doTest("key itr purge test", itr_key_purge);
    ts.doTest("key itr isolation test", itr_key_isolation);
    ts.doTest("key itr seek test", itr_key_seek);
    ts.doTest("key itr deleted test", itr_key_deleted);
    ts.doTest("key itr all deletion markers test", itr_key_all_del_markers);
    ts.doTest("key itr insert and then delete all test", itr_key_insert_delete_all);
    ts.doTest("key itr flush and delete all test",
              itr_key_flush_and_delete_all,
              TestRange<bool>({false, true}));
    ts.doTest("key itr flush and delete half even test",
              itr_key_flush_and_delete_half_even,
              TestRange<bool>({false, true}));
    ts.doTest("key itr different version and level test",
              different_version_and_level_test,
              TestRange<bool>({false, true}));
    return 0;
}
