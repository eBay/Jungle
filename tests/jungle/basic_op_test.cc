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

int basic_operations_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Min seqnum should fail.
    { uint64_t seq;
      s = db->getMinSeqNum(seq);
      CHK_NOT(s); }

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Overwriting existing seq num should fail.
    s = db->setSN(0, kv[0]);
    CHK_NOT(s);

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Min seqnum == 1.
    { uint64_t seq;
      s = db->getMinSeqNum(seq);
      CHK_OK(s);
      CHK_EQ(1, seq); }

    // Max seqnum == n.
    { uint64_t seqnum;
      s = db->getMaxSeqNum(seqnum);
      CHK_EQ(n, seqnum); }

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Sync again (nothing to sync).
    s = db->sync();
    CHK_OK(s);

    // Get KV pairs (after sync).
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Flush all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Min seqnum fail.
    { uint64_t seq;
      s = db->getMinSeqNum(seq);
      CHK_NOT(s); }

    // Flush seqnum == n.
    { uint64_t seq;
      s = db->getLastFlushedSeqNum(seq);
      CHK_OK(s);
      CHK_EQ(n, seq); }

    // Flush again (nothing to flush).
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs (after purge).
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int many_logs_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 7;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t NUM = 100;
    size_t PRIME = 17;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "key" + std::to_string(ii % PRIME);
        std::string val_str = "val" + std::to_string(ii / PRIME);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    for (size_t ii=0; ii<PRIME; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        std::string key_str = "key" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        std::string value_exp = (ii < 15) ? "val5" : "val4";
        CHK_Z( db->get( key_str, value_out) );
        CHK_EQ( value_exp, value_out.toString() );
    }

    // Close DB.
    CHK_Z( jungle::DB::close(db) );

    // Free all resources for jungle.
    jungle::shutdown();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int overwrite_seq() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.allowOverwriteSeqNum = true;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Max seqnum == n.
    uint64_t seqnum;
    s = db->getMaxSeqNum(seqnum);
    CHK_EQ(n, (int)seqnum);

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Another KV with duplicate seq nums.
    std::vector<jungle::KV> kv2(n);

    CHK_Z(_init_kv_pairs(n, kv2, "key2_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv2));

    // Sync again.
    s = db->sync();
    CHK_OK(s);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // It shouldn't be log mode.
    CHK_FALSE( jungle::DB::isLogSectionMode(filename) );

    // Reload DB.
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Write more.
    std::vector<jungle::KV> kv3(n+2);

    CHK_Z(_init_kv_pairs(n+2, kv3, "key3_", "value3_"));
    CHK_Z(_set_byseq_kv_pairs(n-2, n+2, 0, db, kv3));

    // Sync again.
    s = db->sync();
    CHK_OK(s);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);
    _free_kv_pairs(n+2, kv3);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int overwrite_seq_last_record() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.allowOverwriteSeqNum = true;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Max seqnum == n.
    uint64_t seqnum;
    CHK_Z(db->getMaxSeqNum(seqnum));
    CHK_EQ(n, (int)seqnum);

    // Sync.
    CHK_Z(db->sync());

    // Overwrite the last one.
    jungle::KV new_kv("new_key", "new_value");
    CHK_Z( db->setSN(n, new_kv) );

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n-1, 0, db, kv));
    {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(n, kv_out) );
        CHK_EQ(new_kv.key, kv_out.key);
        CHK_EQ(new_kv.value, kv_out.value);
    }

    // Sync again.
    CHK_Z(db->sync());

    // Close DB.
    CHK_Z(jungle::DB::close(db));

    // Reload DB.
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n-1, 0, db, kv));
    {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(n, kv_out) );
        CHK_EQ(new_kv.key, kv_out.key);
        CHK_EQ(new_kv.value, kv_out.value);
    }

    // Close DB.
    CHK_Z(jungle::DB::close(db));

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int load_db_sync() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Reopen.
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Put more keys.
    std::vector<jungle::KV> kv_second(n);
    CHK_Z(_init_kv_pairs(n, kv_second, "key_v2", "value_v2"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv_second));

    // Get KV pairs (before sync).
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_byseq_check(0, n, n, db, kv_second));
    CHK_Z(_get_bykey_check(0, n, db, kv_second));

    // 2nd sync.
    s = db->sync();
    CHK_OK(s);

    // Get KV pairs (after sync).
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_byseq_check(0, n, n, db, kv_second));
    CHK_Z(_get_bykey_check(0, n, db, kv_second));

    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv_second);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int load_db_flush() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Sync and flush
    s = db->sync();
    CHK_OK(s);
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Reopen
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Get KV pairs.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Put more keys.
    std::vector<jungle::KV> kv_second(n);
    CHK_Z(_init_kv_pairs(n, kv_second, "key_v2", "value_v2"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv_second));

    // Sync and flush.
    s = db->sync();
    CHK_OK(s);
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs.
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv_second));

    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv_second);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_dedup() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    int n = 5;
    std::vector<jungle::KV> kv(n);
    // Same key, different value.
    for (int i=0; i<n; ++i) {
        kv[i].alloc("key", "value" + TestSuite::lzStr(3, i));
        // Put
        db->setSN(i+1, kv[i]);
    }

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));

    jungle::SizedBuf value_out;
    db->get(kv[0].key, value_out);
    CHK_EQ(kv[n-1].value, value_out);
    value_out.free();

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Get KV pairs (after sync).
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));

    db->get(kv[0].key, value_out);
    CHK_EQ(kv[n-1].value, value_out);
    value_out.free();

    // Purge all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs (after purge).
    db->get(kv[0].key, value_out);
    CHK_EQ(kv[n-1].value, value_out);
    value_out.free();

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int deletion_op() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Delete some keys.
    int delete_upto_exclusive = 2;
    for (int i=0; i<delete_upto_exclusive; ++i) {
        s = db->delSN(n+i+1, kv[i].key);
        CHK_OK(s);
    }
    s = db->sync();
    CHK_OK(s);

    jungle::SizedBuf value_ret;
    CHK_Z(_get_bykey_check(0, delete_upto_exclusive, db, kv, false));
    CHK_Z(_get_bykey_check(delete_upto_exclusive, n, db, kv));

    // Should be able to get using `meta_only` flag.
    for (int ii=0; ii<delete_upto_exclusive; ++ii) {
        jungle::Record rec_out;
        jungle::Status s = db->getRecordByKey(kv[ii].key, rec_out, true);
        CHK_Z(s);
        CHK_OK(rec_out.isDel());
        CHK_EQ(kv[ii].key, rec_out.kv.key);
        rec_out.free();
    }

    // Purge all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    CHK_Z( _get_bykey_check(0, delete_upto_exclusive, db, kv, false) );
    CHK_Z( _get_bykey_check(delete_upto_exclusive, n, db, kv) );

    // Should be able to get using `meta_only` flag.
    for (int ii=0; ii<delete_upto_exclusive; ++ii) {
        jungle::Record rec_out;
        jungle::Status s = db->getRecordByKey(kv[ii].key, rec_out, true);
        CHK_Z(s);
        CHK_OK(rec_out.isDel());
        CHK_EQ(kv[ii].key, rec_out.kv.key);
        rec_out.free();
    }

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int multiple_log_files() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 100;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Get KV pairs (after sync).
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Purge all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs (after purge).
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Re-open
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Get KV pairs (after purge).
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int multiple_kvs() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DBGroup* group;
    jungle::DB* db;
    jungle::Status s;

    // Open DB group and default DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DBGroup::open(&group, filename, config);
    CHK_OK(s);
    s = group->openDefaultDB(&db);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Another DB
    jungle::DB* meta_store;
    s = group->openDB(&meta_store, "meta");
    CHK_OK(s);

    std::vector<jungle::KV> kv_another(n);
    CHK_Z(_init_kv_pairs(n, kv_another, "key", "value_another"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, meta_store, kv_another));
    s = meta_store->sync();
    CHK_OK(s);

    // Purge all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);
    s = meta_store->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs (after purge).
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, meta_store, kv_another));

    // Close DB.
    s = jungle::DB::close(meta_store);
    CHK_OK(s);
    s = jungle::DB::close(db);
    CHK_OK(s);
    s = jungle::DBGroup::close(group);
    CHK_OK(s);

    // reopen
    s = jungle::DBGroup::open(&group, filename, config);
    CHK_OK(s);
    s = group->openDefaultDB(&db);
    CHK_OK(s);
    s = group->openDB(&meta_store, "meta");
    CHK_OK(s);

    // Get KV pairs.
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, meta_store, kv_another));

    // Close DB.
    s = jungle::DB::close(meta_store);
    CHK_OK(s);
    s = jungle::DB::close(db);
    CHK_OK(s);
    s = jungle::DBGroup::close(group);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv_another);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int set_by_key() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv));

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Get KV pairs (after sync).
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Purge all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs (after purge).
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int command_marker() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Set a command marker.
    jungle::Record marker(jungle::Record::COMMAND);
    marker.kv.alloc("marker_key", "marker_value");
    s = db->setRecord(marker);
    CHK_OK(s);

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Get marker, by calling getRecord().
    int marker_seqnum = n+1;
    jungle::Record rec_out;
    s = db->getRecord(marker_seqnum, rec_out);
    CHK_OK(s);
    CHK_EQ(marker.kv.key, rec_out.kv.key);
    CHK_EQ(marker.kv.value, rec_out.kv.value);
    CHK_EQ(marker_seqnum, (int)rec_out.seqNum);
    rec_out.free();

    // Marker is invisible by getSN().
    jungle::KV kv_out;
    s = db->getSN(marker_seqnum, kv_out);
    CHK_NOT(s);

    // Marker is visible by iterator;
    jungle::Iterator itr;
    s = itr.initSN(db);
    CHK_OK(s);

    int count = 0;
    do {
        s = itr.get(rec_out);
        if (!s) break;

        if ((int)rec_out.seqNum == marker_seqnum) {
            CHK_EQ(marker.kv.key, rec_out.kv.key);
            CHK_EQ(marker.kv.value, rec_out.kv.value);
        }
        rec_out.free();
        count++;
    } while(itr.next());

    s = itr.close();
    CHK_OK(s);

    CHK_EQ(n+1, count);

    // Purge all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);

    // Marker is not a normal key-value pair.
    jungle::SizedBuf value_out;
    s = db->get(marker.kv.key, value_out);
    CHK_NOT(s);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    marker.free();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int multiple_handles() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DBGroup* group;
    jungle::DB *db, *db_another;
    jungle::DB *kvs, *kvs_another;

    // Open DB.
    s = jungle::DBGroup::open(&group, filename, config);
    CHK_OK(s);
    s = group->openDefaultDB(&db);
    CHK_OK(s);

    // Open the same DB using another handle.
    s = group->openDefaultDB(&db_another);
    CHK_OK(s);
    CHK_EQ((uint64_t)db, (uint64_t)db_another);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync.
    s = db->sync();
    CHK_OK(s);

    // Another handle can get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, db_another, kv));
    CHK_Z(_get_bykey_check(0, n, db_another, kv));

    // Another KVS
    s = group->openDB(&kvs, "meta");
    CHK_OK(s);

    s = group->openDB(&kvs_another, "meta");
    CHK_OK(s);

    std::vector<jungle::KV> kv_another(n);
    CHK_Z(_init_kv_pairs(n, kv_another, "key", "value_another"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, kvs, kv_another));

    // Sync.
    s = kvs->sync();
    CHK_OK(s);

    // Another handle can get KV pairs.
    CHK_Z(_get_byseq_check(0, n, 0, kvs_another, kv_another));
    CHK_Z(_get_bykey_check(0, n, kvs_another, kv_another));

    // Flush all.
    jungle::FlushOptions f_options;
    s = db->flushLogs(f_options);
    CHK_OK(s);
    s = kvs->flushLogs(f_options);
    CHK_OK(s);

    // Get KV pairs (after purge).
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, db_another, kv));
    CHK_Z(_get_bykey_check(0, n, kvs, kv_another));
    CHK_Z(_get_bykey_check(0, n, kvs_another, kv_another));

    // Close DB.
    s = jungle::DB::close(kvs_another);
    CHK_OK(s);
    s = jungle::DB::close(kvs);
    CHK_OK(s);
    s = jungle::DB::close(db_another);
    CHK_OK(s);
    s = jungle::DB::close(db);
    CHK_OK(s);
    s = jungle::DBGroup::close(group);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv_another);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int multiple_group_handles() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DBGroup *g1, *g2;
    jungle::DB *db1, *db2;
    jungle::DB *kvs1;

    // Open 1st group, default db, meta kvs.
    s = jungle::DBGroup::open(&g1, filename, config);
    CHK_OK(s);
    s = g1->openDefaultDB(&db1);
    CHK_OK(s);
    s = g1->openDB(&kvs1, "meta");
    CHK_OK(s);

    // Open 2nd group of the same file, and open default db only.
    s = jungle::DBGroup::open(&g2, filename, config);
    CHK_OK(s);
    CHK_EQ((uint64_t)g1, (uint64_t)g2);
    s = g2->openDefaultDB(&db2);
    CHK_OK(s);

    // Close 1st group.
    s = jungle::DB::close(kvs1);
    CHK_OK(s);
    s = jungle::DB::close(db1);
    CHK_OK(s);
    s = jungle::DBGroup::close(g1);
    CHK_OK(s);

    // Close 2nd group.
    s = jungle::DB::close(db2);
    CHK_OK(s);
    s = jungle::DBGroup::close(g2);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int group_handle_misuse() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DBGroup* group;
    jungle::DB* db;

    // Directly open DB first.
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Try to open group on the same file.
    s = jungle::DBGroup::open(&group, filename, config);
    // Should fail.
    CHK_NOT(s);

    // Close.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int purge_only_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 50;
    std::vector<jungle::KV> kv(n);

    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync.
    CHK_Z( db->sync() );

    // Flush (purge-only).
    jungle::FlushOptions options;
    options.purgeOnly = true;
    // purge upto key19.
    CHK_Z(db->flushLogs(options, 20));

    // key20 -> seq number 21.
    uint64_t seq_num_out;
    CHK_Z(db->getMinSeqNum(seq_num_out));
    CHK_EQ(21, seq_num_out);

    // purge all.
    CHK_Z(db->flushLogs(options));

    // All KVs are gone.
    CHK_Z(_get_bykey_check(0, n, db, kv, false));

    s = jungle::DB::close(db);
    CHK_Z(s);

    // Reopen, and they should not be visiable.
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Still all KVs should not be there.
    CHK_Z(_get_bykey_check(0, n, db, kv, false));

    s = jungle::DB::close(db);
    CHK_Z(s);

    jungle::shutdown();
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

static int _meta_test_deleted_check(jungle::DB* db,
                                    int n,
                                    std::vector<jungle::Record>& rec) {
    for (int ii=0; ii<n; ++ii) {
        jungle::Record& rr = rec[ii];
        jungle::Record rr_ret;

        if (ii % 2 == 0) {
            // Even number: deleted.
            // Normal get should fail.
            CHK_NOT(db->getRecordByKey(rr.kv.key, rr_ret));
            // Get meta should succeed.
            CHK_Z(db->getRecordByKey(rr.kv.key, rr_ret, true));
            std::string chk_str("meta_deleted" + TestSuite::lzStr(3, ii));
            jungle::SizedBuf exp_meta(chk_str);
            CHK_EQ(exp_meta, rr_ret.meta);
        } else {
            // Otherwise: normal.
            CHK_Z(db->getRecordByKey(rr.kv.key, rr_ret));
        }
        rr_ret.free();
    }
    return 0;
}

int meta_test_log() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::Record> rec(n);
    for (int ii=0; ii<n; ++ii) {
        jungle::Record& rr = rec[ii];
        rr.kv.alloc( "key"   + TestSuite::lzStr(3, ii),
                     "value" + TestSuite::lzStr(3, ii) );
        rr.meta.alloc("meta" + TestSuite::lzStr(3, ii));
        rr.seqNum = ii+1;
        CHK_Z(db->setRecord(rr));
    }

    // Sync.
    CHK_Z(db->sync());

    // Get.
    for (int ii=0; ii<n; ++ii) {
        jungle::Record& rr = rec[ii];
        jungle::Record rr_ret;
        CHK_Z(db->getRecordByKey(rr.kv.key, rr_ret));
        CHK_EQ(rr.meta, rr_ret.meta);
        rr_ret.free();
    }

    // Delete even numbers.
    for (int ii=0; ii<n; ii+=2) {
        jungle::Record rr;
        rr.kv.alloc( "key"   + TestSuite::lzStr(3, ii),
                     "value" + TestSuite::lzStr(3, ii) );
        rr.meta.alloc("meta_deleted" + TestSuite::lzStr(3, ii));
        CHK_Z( db->delRecord(rr) );
        rr.free();
    }
    CHK_Z(db->sync());

    // Deleted record meta check.
    CHK_Z(_meta_test_deleted_check(db, n, rec));

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Deleted record meta check: again.
    CHK_Z(_meta_test_deleted_check(db, n, rec));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    for (int ii=0; ii<n; ++ii) rec[ii].free();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int meta_test_table() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::Record> rec(n);
    for (int ii=0; ii<n; ++ii) {
        jungle::Record& rr = rec[ii];
        rr.kv.alloc( "key"   + TestSuite::lzStr(3, ii),
                     "value" + TestSuite::lzStr(3, ii) );
        rr.meta.alloc("meta" + TestSuite::lzStr(3, ii));
        rr.seqNum = ii+1;
        CHK_Z(db->setRecord(rr));
    }

    // Sync and flush.
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Get.
    for (int ii=0; ii<n; ++ii) {
        jungle::Record& rr = rec[ii];
        jungle::Record rr_ret;
        CHK_Z(db->getRecordByKey(rr.kv.key, rr_ret));
        CHK_EQ(rr.meta, rr_ret.meta);
        rr_ret.free();
    }

    // Delete even numbers.
    for (int ii=0; ii<n; ii+=2) {
        jungle::Record rr;
        rr.kv.alloc( "key"   + TestSuite::lzStr(3, ii),
                     "value" + TestSuite::lzStr(3, ii) );
        rr.meta.alloc("meta_deleted" + TestSuite::lzStr(3, ii));
        CHK_Z( db->delRecord(rr) );
        rr.free();
    }
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Deleted record meta check.
    CHK_Z(_meta_test_deleted_check(db, n, rec));

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Deleted record meta check: again.
    CHK_Z(_meta_test_deleted_check(db, n, rec));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    for (int ii=0; ii<n; ++ii) rec[ii].free();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int async_flush_test_cb(size_t* counter,
                        size_t expected_count,
                        jungle::Status s,
                        void* ctx)
{
    CHK_Z(s);

    (*counter)++;
    if (*counter == expected_count) {
        EventAwaiter* ea = reinterpret_cast<EventAwaiter*>(ctx);
        ea->invoke();
    }
    return 0;
}

int async_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherSleepDuration_ms = 1000;
    jungle::init(g_config);

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync and async flush.
    CHK_Z(db->sync());

    size_t counter = 0;
    size_t expected_count = 5;
    EventAwaiter ea;

    for (size_t ii=0; ii<expected_count; ++ii) {
        CHK_Z( db->flushLogsAsync
                   ( jungle::FlushOptions(),
                     std::bind( async_flush_test_cb,
                                &counter,
                                expected_count,
                                std::placeholders::_1,
                                std::placeholders::_2 ),
                     &ea ) );
    }
    // Wait for handler.
    ea.wait();

    // All callbacks should have been invoked.
    CHK_EQ( expected_count, counter );

    // Get.
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Invoke async flush and close DB without waiting.
    CHK_Z(db->flushLogsAsync(jungle::FlushOptions(), nullptr, nullptr));

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int async_flush_verbose_test(bool debug_level) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherSleepDuration_ms = 1000;
    jungle::init(g_config);

    config.logSectionOnly = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    size_t NUM = 1000;
    std::vector<jungle::KV> kv(NUM);
    CHK_Z(_init_kv_pairs(NUM, kv, "key", "value"));

    CHK_EQ(4, db->getLogLevel());
    if (debug_level) {
        db->setLogLevel(5);
        CHK_EQ(5, db->getLogLevel());
    }

    const size_t EXP_COUNT = 11;
    for (size_t ii=0; ii<NUM; ii+=EXP_COUNT) {
        size_t counter = 0;
        EventAwaiter ea;

        size_t upto = std::min(ii+EXP_COUNT, NUM);
        for (size_t jj = ii; jj < upto; ++jj) {
            CHK_Z( db->setSN(jj+1, kv[jj]) );

            jungle::FlushOptions f_opt;
            f_opt.syncOnly = true;
            f_opt.callFsync = false;
            CHK_Z( db->flushLogsAsync
                       ( f_opt,
                         std::bind( async_flush_test_cb,
                                    &counter,
                                    upto - ii,
                                    std::placeholders::_1,
                                    std::placeholders::_2 ),
                         &ea,
                         jj ) );
        }

        // Wait for handler.
        ea.wait();
        // All callbacks should have been invoked.
        CHK_EQ( upto - ii, counter );
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    _free_kv_pairs(NUM, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int async_flush_verbose_with_delay_test(bool debug_level) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.flusherSleepDuration_ms = 500;
    jungle::init(g_config);

    config.logSectionOnly = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    size_t NUM = 10000;
    std::vector<jungle::KV> kv(NUM);
    CHK_Z(_init_kv_pairs(NUM, kv, "key", "value"));

    CHK_EQ(4, db->getLogLevel());
    if (debug_level) {
        db->setLogLevel(5);
        CHK_EQ(5, db->getLogLevel());
    }

    TestSuite::Timer timer(1050);
    TestSuite::WorkloadGenerator wg(1000);
    for (size_t ii=0; ii<NUM; ) {
        if (timer.timeout()) break;

        size_t todo = wg.getNumOpsToDo();
        if (!todo) {
            TestSuite::sleep_ms(1);
            continue;
        }

        CHK_Z( db->setSN(ii+1, kv[ii]) );

        jungle::FlushOptions f_opt;
        f_opt.syncOnly = true;
        f_opt.callFsync = false;
        f_opt.execDelayUs = 100*1000;
        CHK_Z( db->flushLogsAsync( f_opt, nullptr, nullptr ) );

        wg.addNumOpsDone(1);
        ii += 1;
    }
    CHK_Z(jungle::DB::close(db));

    // Wait one more second to see if flusher can handle stale request.
    TestSuite::sleep_ms(1000);

    CHK_Z(jungle::shutdown());
    _free_kv_pairs(NUM, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int flush_beyond_sync_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 7;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t NUM = 100;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "key" + std::to_string(ii);
        std::string val_str = "val" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
        if (ii == NUM/2) {
            // Sync logs in the middle.
            CHK_Z( db->sync(false) );
        }
    }

    for (size_t ii=0; ii<NUM; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        std::string key_str = "key" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        std::string value_exp = "val" + std::to_string(ii);
        CHK_Z( db->get( key_str, value_out) );
        CHK_EQ( value_exp, value_out.toString() );
    }

    // Normal flush (upto last sync).
    jungle::FlushOptions f_opt;
    CHK_Z( db->flushLogs(f_opt) );

    // Flush should have done upto the last sync.
    uint64_t seq_num_out = 0;
    CHK_Z( db->getLastFlushedSeqNum(seq_num_out) );
    CHK_EQ(NUM/2 + 1, seq_num_out);

    // Flush beyond sync (upto the latest).
    f_opt.beyondLastSync = true;
    CHK_Z( db->flushLogs(f_opt) );

    // Flush should have done upto the latest.
    CHK_Z( db->getLastFlushedSeqNum(seq_num_out) );
    CHK_EQ(NUM, seq_num_out);

    // Close DB without sync.
    CHK_Z( jungle::DB::close(db) );

    // Reopen.
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Put more logs.
    for (size_t ii=NUM; ii<NUM+3; ++ii) {
        std::string key_str = "key" + std::to_string(ii);
        std::string val_str = "val" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Check, all data should exist.
    for (size_t ii=0; ii<NUM+3; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        std::string key_str = "key" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        std::string value_exp = "val" + std::to_string(ii);
        CHK_Z( db->get( key_str, value_out) );
        CHK_EQ( value_exp, value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );

    // Free all resources for jungle.
    jungle::shutdown();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int get_stat_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.fdbCacheSize = 128*1024*1024;
    jungle::init(g_config);

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync and async flush.
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    jungle::DBStats stats_out;
    CHK_Z(db->getStats(stats_out));
    CHK_EQ(n, stats_out.numKvs);
    CHK_GT(stats_out.workingSetSizeByte, n*10);
    CHK_EQ(stats_out.cacheSizeByte, g_config.fdbCacheSize);
    CHK_GT(stats_out.cacheUsedByte, 0);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int double_shutdown_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    CHK_Z(db->sync());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    s = jungle::shutdown();
    CHK_EQ(jungle::Status(jungle::Status::ALREADY_SHUTDOWN), s);

    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int reopen_empty_db_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    CHK_Z(jungle::DB::open(&db, filename, config));
    // Sync & close without any insert.
    s = db->sync(); // Will return error.
    CHK_Z(jungle::DB::close(db));

    // Reopen & set KV pairs.
    CHK_Z(jungle::DB::open(&db, filename, config));
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));
    CHK_Z(db->sync());
    CHK_Z(jungle::DB::close(db));

    // Reopen & check.
    CHK_Z(jungle::DB::open(&db, filename, config));
    CHK_Z(_get_byseq_check(0, n, 0, db, kv));
    CHK_Z(jungle::DB::close(db));

    CHK_Z(jungle::shutdown());
    _free_kv_pairs(n, kv);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int different_l0_partitions() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.numL0Partitions = 1;
    CHK_Z(jungle::DB::open(&db, filename, config));
    int n = 10;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key", "value"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv));
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs(jungle::FlushOptions()));
    CHK_Z(jungle::DB::close(db));

    // Change the number of partitions,
    // but it should be ignored internally.
    config.numL0Partitions = 4;

    // Reopen & check.
    CHK_Z(jungle::DB::open(&db, filename, config));
    CHK_Z(_get_bykey_check(0, n, db, kv));

    // Insert more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_new", "value_new"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv2));
    CHK_Z(db->sync());
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

    // Check both.
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv2));
    CHK_Z(jungle::DB::close(db));

    // Reopen & check.
    CHK_Z(jungle::DB::open(&db, filename, config));
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv2));
    CHK_Z(jungle::DB::close(db));

    CHK_Z(jungle::shutdown());
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int add_new_log_file_race_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    jungle::DebugParams dp;
    dp.addNewLogFileCb = [&db](const jungle::DebugParams::GenericCbParams& pp) {
        db->sync(false);
        uint64_t seq_num_out = 0;
        CHK_Z( db->getLastSyncedSeqNum(seq_num_out) );
        return 0;
    };
    jungle::DB::setDebugParams(dp);

    for (size_t ii=0; ii<11; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int async_remove_file_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    // Initialize Jungle without any flusher/compactor/writer.
    jungle::GlobalConfig g_conf;
    g_conf.numCompactorThreads = 0;
    g_conf.numFlusherThreads = 0;
    g_conf.numTableWriters = 0;
    jungle::init(g_conf);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Put 20 records.
    for (size_t ii=0; ii<20; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Flush up to 15.
    CHK_Z( db->sync() );
    CHK_Z( db->flushLogs(jungle::FlushOptions(), 15) );

    // Wait 2 seconds.
    TestSuite::sleep_sec(2);

    // `log0000_00000000` shouldn't exist.
    CHK_FALSE( TestSuite::exist(filename + "/log0000_00000000") );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int set_batch_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Put 5 records.
    for (size_t ii=0; ii<5; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Set debug callback.
    jungle::DebugParams dp;
    dp.newLogBatchCb =
        [db](const jungle::DebugParams::GenericCbParams& pp) -> int {
        for (size_t ii=0; ii<15; ++ii) {
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            jungle::Status s = db->get( jungle::SizedBuf(key_str), value_out );
            if (ii < 5) {
                CHK_Z(s);
                CHK_EQ( val_str, value_out.toString() );
            } else {
                // Otherwise they should not be visible yet.
                CHK_FALSE(s);
            }
        }

        // Same to iterator.
        jungle::Iterator itr;
        itr.init(db);
        size_t count = 0;
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            CHK_Z( itr.get(rec) );
            count++;
        } while (itr.next().ok());
        itr.close();
        CHK_EQ(5, count);
        return 0;
    };
    jungle::DB::setDebugParams(dp);

    // Put 10 records atomically.
    std::list<jungle::Record> recs;
    for (size_t ii=5; ii<15; ++ii) {
        jungle::Record rr;
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        rr.kv.alloc(key_str, val_str);
        recs.push_back(rr);
    }
    CHK_Z( db->setRecordBatch(recs) );
    for (jungle::Record& rr: recs) {
        rr.free();
    }

    // After batch update succeeds, we should see all records.
    for (size_t ii=0; ii<15; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out ) );
        CHK_EQ( val_str, value_out.toString() );
    }

    // Same to iterator.
    jungle::Iterator itr;
    itr.init(db);
    size_t count = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h(rec);
        CHK_Z( itr.get(rec) );
        count++;
    } while (itr.next().ok());
    itr.close();
    CHK_EQ(15, count);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int set_batch_invalid_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Case 1: mixed sequence number.
    {   std::list<jungle::Record> recs;
        for (size_t ii=0; ii<10; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            rr.kv.alloc(key_str, val_str);
            if (ii % 2 == 0) rr.seqNum = ii + 1;
            recs.push_back(rr);
        }
        CHK_GT( 0, db->setRecordBatch(recs) );
        for (jungle::Record& rr: recs) {
            rr.free();
        }
    }

    // Case 2: not increasing order.
    {   std::list<jungle::Record> recs;
        for (size_t ii=0; ii<10; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            rr.kv.alloc(key_str, val_str);
            rr.seqNum = 100 - ii;
            recs.push_back(rr);
        }
        CHK_GT( 0, db->setRecordBatch(recs) );
        for (jungle::Record& rr: recs) {
            rr.free();
        }
    }

    // Case 3: duplicate seq numbers.
    {   std::list<jungle::Record> recs;
        for (size_t ii=0; ii<10; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            rr.kv.alloc(key_str, val_str);
            rr.seqNum = 100 + ii / 2;
            recs.push_back(rr);
        }
        CHK_GT( 0, db->setRecordBatch(recs) );
        for (jungle::Record& rr: recs) {
            rr.free();
        }
    }

    // Normal records, should succeed.
    {   std::list<jungle::Record> recs;
        for (size_t ii=0; ii<10; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            rr.kv.alloc(key_str, val_str);
            rr.seqNum = 100 + ii;
            recs.push_back(rr);
        }
        CHK_Z( db->setRecordBatch(recs) );
        for (jungle::Record& rr: recs) {
            rr.free();
        }
    }

    // Case 4: seq number smaller than current max.
    {   std::list<jungle::Record> recs;
        for (size_t ii=0; ii<10; ++ii) {
            jungle::Record rr;
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            rr.kv.alloc(key_str, val_str);
            rr.seqNum = 10 + ii;
            recs.push_back(rr);
        }
        CHK_GT( 0, db->setRecordBatch(recs) );
        for (jungle::Record& rr: recs) {
            rr.free();
        }
    }

    // Only succeeded records should be visible.
    for (size_t ii=0; ii<10; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        jungle::Record rec_out;
        jungle::Record::Holder h(rec_out);
        CHK_Z( db->getRecordByKey(jungle::SizedBuf(key_str), rec_out) );
        CHK_EQ( val_str, rec_out.kv.value.toString() );
        CHK_EQ( 100 + ii, rec_out.seqNum );
    }

    // Same to iterator.
    jungle::Iterator itr;
    itr.init(db);
    size_t count = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h(rec);
        CHK_Z( itr.get(rec) );
        count++;
    } while (itr.next().ok());
    itr.close();
    CHK_EQ(10, count);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int empty_log_file_Test(size_t num_reopen) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 10;

    // Open and immediately close.
    for (size_t ii=0; ii<num_reopen; ++ii) {
        CHK_Z( jungle::DB::open(&db, filename, config) );
        CHK_Z( jungle::DB::close(db) );
    }

    // Re-open and put some records.
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t NUM = 50;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Get min seq number.
    uint64_t min_seq = 0;
    CHK_Z( db->getMinSeqNum(min_seq) );
    CHK_EQ(1, min_seq);

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Min seq number should be the same.
    CHK_Z( db->getMinSeqNum(min_seq) );
    CHK_EQ(1, min_seq);

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("basic operation test", basic_operations_test);
    ts.doTest("many logs test", many_logs_test);
    ts.doTest("overwrite sequence number test", overwrite_seq);
    ts.doTest("overwrite last sequence number test", overwrite_seq_last_record);
    ts.doTest("load existing db test (sync)", load_db_sync);
    ts.doTest("load existing db test (flush)", load_db_flush);
    ts.doTest("log deduplication test", log_dedup);
    ts.doTest("deletion test", deletion_op);
    ts.doTest("multiple log files test", multiple_log_files);
    ts.doTest("multiple KV Stores test", multiple_kvs);
    ts.doTest("set by key test", set_by_key);
    ts.doTest("command marker test", command_marker);
    ts.doTest("multiple handles test", multiple_handles);
    ts.doTest("multiple group handles test", multiple_group_handles);
    ts.doTest("group handle misuse test", group_handle_misuse);
    ts.doTest("purge only test", purge_only_test);
    ts.doTest("meta test log", meta_test_log);
    ts.doTest("meta test table", meta_test_table);
    ts.doTest("async flush test", async_flush_test);
    ts.doTest("async flush verbose test", async_flush_verbose_test,
              TestRange<bool>( {false, true} ) );
    ts.doTest("async flush verbose with delay test",
              async_flush_verbose_with_delay_test,
              TestRange<bool>( { true } ) );
    ts.doTest("flush beyond sync test", flush_beyond_sync_test);
    ts.doTest("get stat test", get_stat_test);
    ts.doTest("double shutdown test", double_shutdown_test);
    ts.doTest("reopen empty db test", reopen_empty_db_test);
    ts.doTest("different number of L0 partitions test", different_l0_partitions);
    ts.doTest("add new log file race test", add_new_log_file_race_test);
    ts.doTest("async remove file test", async_remove_file_test);
    ts.doTest("set batch test", set_batch_test);
    ts.doTest("set batch invalid test", set_batch_invalid_test);
    ts.doTest("empty log file test", empty_log_file_Test,
              TestRange<size_t>( {1, 10} ) );

    return 0;
}

