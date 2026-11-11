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

#include <map>
#include <set>
#include <vector>

#include <stdio.h>

namespace snapshot_test {

int checkpoint_basic_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);

    // 0 -- n-1: key_x value_x
    CHK_OK(_init_kv_pairs(n, kv, "key_", "value_") == 0);
    CHK_OK(_set_byseq_kv_pairs(0, n, 0, db, kv) == 0);

    // Checkpoint.
    uint64_t seq_num_out = 0;
    s = db->checkpoint(seq_num_out);
    CHK_OK(s);
    CHK_EQ(n, (int)seq_num_out);

    // n -- 2n-1: key2_x value2_x
    std::vector<jungle::KV> kv2(n);
    CHK_OK(_init_kv_pairs(n, kv2, "key2_", "value2_") == 0);
    CHK_OK(_set_byseq_kv_pairs(0, n, n, db, kv2) == 0);

    // Checkpoint again.
    s = db->checkpoint(seq_num_out);
    CHK_OK(s);
    CHK_EQ(2*n, (int)seq_num_out);

    // Flush.
    s = db->flushLogs();
    CHK_OK(s);

    // 2n -- 3n-1: key3_x value3_x
    std::vector<jungle::KV> kv3(n);
    CHK_OK(_init_kv_pairs(n, kv3, "key3_", "value3_") == 0);
    CHK_OK(_set_byseq_kv_pairs(0, n, 2*n, db, kv3) == 0);
    s = db->sync(false);

    // Get all.
    CHK_Z(_get_bykey_check(0, n, db, kv));
    CHK_Z(_get_bykey_check(0, n, db, kv2));
    CHK_Z(_get_bykey_check(0, n, db, kv3));

    s = jungle::DB::close(db);
    CHK_OK(s);

    jungle::shutdown();
    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);
    _free_kv_pairs(n, kv3);

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int checkpoint_marker_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 10;
    std::list<uint64_t> chk_local;

    for (size_t ii=0; ii<10; ++ii) {
        // Append `n` pairs.
        std::vector<jungle::KV> kv(n);
        CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
        CHK_Z(_set_byseq_kv_pairs(0, n, ii*n, db, kv));

        // Checkpoint.
        uint64_t seq_num_out = 0;
        CHK_OK(db->checkpoint(seq_num_out));
        chk_local.push_back(seq_num_out);

        _free_kv_pairs(n, kv);
    }

    // Get list of checkpoints.
    std::list<uint64_t> chk_out;
    s = db->getCheckpoints(chk_out);
    CHK_OK(s);

    // Should be identical.
    CHK_Z(_cmp_lists(chk_local, chk_out));

    s = jungle::DB::close(db);
    CHK_OK(s);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int checkpoint_marker_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 10;
    std::list<uint64_t> chk_local;

    for (size_t ii=0; ii<10; ++ii) {
        // Append `n` pairs.
        std::vector<jungle::KV> kv(n);
        CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
        CHK_Z(_set_byseq_kv_pairs(0, n, ii*n, db, kv));

        // Checkpoint.
        uint64_t seq_num_out = 0;
        CHK_OK(db->checkpoint(seq_num_out));
        chk_local.push_back(seq_num_out);

        _free_kv_pairs(n, kv);
    }

    // Flush half.
    CHK_OK(db->flushLogs(jungle::FlushOptions(), n*10/2));

    // Get list of checkpoints.
    std::list<uint64_t> chk_out;
    s = db->getCheckpoints(chk_out);
    CHK_OK(s);

    // Should be identical.
    CHK_Z(_cmp_lists(chk_local, chk_out));

    s = jungle::DB::close(db);
    CHK_OK(s);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int checkpoint_marker_purge_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 10;
    std::list<uint64_t> chk_local;

    for (size_t ii=0; ii<10; ++ii) {
        // Append `n` pairs.
        std::vector<jungle::KV> kv(n);
        CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
        CHK_Z(_set_byseq_kv_pairs(0, n, ii*n, db, kv));

        // Checkpoint.
        uint64_t seq_num_out = 0;
        CHK_OK(db->checkpoint(seq_num_out));
        if (seq_num_out > 50) chk_local.push_back(seq_num_out);

        _free_kv_pairs(n, kv);
    }

    // Purge half.
    jungle::FlushOptions f_opt;
    f_opt.purgeOnly = true;
    CHK_OK(db->flushLogs(f_opt, n*10/2));

    // Get list of checkpoints.
    std::list<uint64_t> chk_out;
    s = db->getCheckpoints(chk_out);
    CHK_OK(s);

    // Should be identical.
    CHK_Z(_cmp_lists(chk_local, chk_out));

    s = jungle::DB::close(db);
    CHK_OK(s);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int checkpoint_on_cold_start() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Sync and close.
    CHK_Z(db->sync(false));
    CHK_Z(jungle::DB::close(db));

    // Re-open.
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Add checkpoint.
    uint64_t seq_num_out;
    CHK_Z(db->checkpoint(seq_num_out, false));

    // New records.
    CHK_Z(_set_byseq_kv_pairs(0, n, 5, db, kv));

    // Sync and close.
    CHK_Z(db->sync(false));
    CHK_Z(jungle::DB::close(db));

    // Re-open.
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get checkpoint.
    std::list<uint64_t> chk_out;
    CHK_Z(db->getCheckpoints(chk_out));

    CHK_EQ(1, chk_out.size());
    CHK_EQ(seq_num_out, *chk_out.rbegin());

    CHK_Z(jungle::DB::close(db));
    jungle::shutdown();

    _free_kv_pairs(n, kv);
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int checkpoint_limit_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_conf;
    g_conf.numTableWriters = 0;
    jungle::init(g_conf);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 10;
    std::list<uint64_t> chk_local;

    for (size_t ii=0; ii<20; ++ii) {
        // Append `n` pairs.
        std::vector<jungle::KV> kv(n);
        CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
        CHK_Z(_set_bykey_kv_pairs(0, n, db, kv));

        // Checkpoint.
        uint64_t seq_num_out = 0;
        CHK_OK(db->checkpoint(seq_num_out));

        _free_kv_pairs(n, kv);
    }

    // Get the list of checkpoints, it should match the config.
    std::list<uint64_t> chk_out;
    TestSuite::_msg("before flush\n");
    CHK_Z( db->getCheckpoints(chk_out) );
    for (auto& entry: chk_out) TestSuite::_msg("%zu\n", entry);
    CHK_EQ( config.maxKeepingCheckpoints, chk_out.size() );

    // Flush.
    CHK_Z( db->sync() );
    CHK_Z( db->flushLogs() );

    // Get the list of checkpoints, it should not exceed the limit.
    CHK_Z( db->getCheckpoints(chk_out) );
    TestSuite::_msg("after flush\n");
    for (auto& entry: chk_out) TestSuite::_msg("%zu\n", entry);
    CHK_GTEQ( config.maxKeepingCheckpoints, chk_out.size() );

    CHK_Z( jungle::DB::close(db) );

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_basic_table_only_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Checkpoint.
    uint64_t chk = 0;
    CHK_OK(db->checkpoint(chk));

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Sync and flush.
    CHK_OK(db->sync(false));
    CHK_OK(db->flushLogs());

    // Open snapshot.
    jungle::DB* snap;
    CHK_OK(db->openSnapshot(&snap, chk));

    // Old value should be visible by snapshot.
    CHK_Z(_get_bykey_check(0, n, snap, kv));

    // New value should be visible by DB handle.
    CHK_Z(_get_bykey_check(0, n, db, kv2));

    // Close snapshot.
    CHK_OK(jungle::DB::close(snap));

    s = jungle::DB::close(db);
    CHK_OK(s);

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_basic_log_only_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Checkpoint.
    uint64_t chk = 0;
    CHK_OK(db->checkpoint(chk));

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Sync.
    CHK_OK(db->sync(false));

    // Open snapshot.
    jungle::DB* snap;
    CHK_OK(db->openSnapshot(&snap, chk));

    // Old value should be visible by snapshot.
    CHK_Z(_get_bykey_check(0, n, snap, kv));

    // New value should be visible by DB handle.
    CHK_Z(_get_bykey_check(0, n, db, kv2));

    // Close snapshot.
    CHK_OK(jungle::DB::close(snap));

    s = jungle::DB::close(db);
    CHK_OK(s);

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_basic_combined_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Checkpoint.
    uint64_t chk = 0;
    CHK_OK(db->checkpoint(chk));

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Sync and flush.
    CHK_OK(db->sync(false));
    CHK_OK(db->flushLogs());

    // Set more and checkpoint.
    std::vector<jungle::KV> kv3(n);
    CHK_Z(_init_kv_pairs(n, kv3, "key3_", "value3_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n*2, db, kv3));

    CHK_OK(db->checkpoint(chk));

    // Set more and sync.
    std::vector<jungle::KV> kv4(n);
    CHK_Z(_init_kv_pairs(n, kv4, "key3_", "value4_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n*3, db, kv4));
    CHK_OK(db->sync(false));

    // Open snapshot.
    jungle::DB* snap;
    CHK_OK(db->openSnapshot(&snap, chk));

    // Flush after that.
    CHK_OK(db->flushLogs());

    // Snapshot: sees kv2 and kv3.
    CHK_Z(_get_bykey_check(0, n, snap, kv2));
    CHK_Z(_get_bykey_check(0, n, snap, kv3));

    // DB: sees kv2 and kv4.
    CHK_Z(_get_bykey_check(0, n, db, kv2));
    CHK_Z(_get_bykey_check(0, n, db, kv4));

    // Close snapshot.
    CHK_OK(jungle::DB::close(snap));

    // Close DB.
    CHK_OK(jungle::DB::close(db));

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);
    _free_kv_pairs(n, kv3);
    _free_kv_pairs(n, kv4);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_iterator_table_only_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Checkpoint.
    uint64_t chk = 0;
    CHK_OK(db->checkpoint(chk));

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Sync and flush.
    CHK_OK(db->sync(false));
    CHK_OK(db->flushLogs());

    // Open snapshot.
    jungle::DB* snap;
    CHK_OK(db->openSnapshot(&snap, chk));

    jungle::Iterator itr;
    CHK_OK(itr.initSN(snap));

    // kv should be found.
    CHK_Z(_itr_check(0, n, itr, kv));

    // No more records.
    CHK_NOT(itr.next());

    CHK_OK(itr.close());

    // Close snapshot.
    CHK_OK(jungle::DB::close(snap));

    s = jungle::DB::close(db);
    CHK_OK(s);

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_iterator_log_only_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Checkpoint.
    uint64_t chk = 0;
    CHK_OK(db->checkpoint(chk));

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Sync.
    CHK_OK(db->sync(false));

    // Open snapshot.
    jungle::DB* snap;
    CHK_OK(db->openSnapshot(&snap, chk));

    jungle::Iterator itr;
    CHK_OK(itr.initSN(snap));

    // kv should be found.
    CHK_Z(_itr_check(0, n, itr, kv));

    // No more records.
    CHK_NOT(itr.next());

    CHK_OK(itr.close());

    // Close snapshot.
    CHK_OK(jungle::DB::close(snap));

    s = jungle::DB::close(db);
    CHK_OK(s);

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_iterator_combined_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    config.maxEntriesInLogFile = 16;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    int n = 5;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key_", "value_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, 0, db, kv));

    // Checkpoint.
    uint64_t chk = 0;
    CHK_OK(db->checkpoint(chk));

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key_", "value2_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n, db, kv2));

    // Sync and flush.
    CHK_OK(db->sync(false));
    CHK_OK(db->flushLogs());

    // Set more and checkpoint.
    std::vector<jungle::KV> kv3(n);
    CHK_Z(_init_kv_pairs(n, kv3, "key3_", "value3_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n*2, db, kv3));

    CHK_OK(db->checkpoint(chk));

    // Set more and sync.
    std::vector<jungle::KV> kv4(n);
    CHK_Z(_init_kv_pairs(n, kv4, "key3_", "value4_"));
    CHK_Z(_set_byseq_kv_pairs(0, n, n*3, db, kv4));
    CHK_OK(db->sync(false));

    // Open snapshot.
    jungle::DB* snap;
    CHK_OK(db->openSnapshot(&snap, chk));

    // Iterator.
    jungle::Iterator itr;
    CHK_OK(itr.initSN(snap));

    // Flush after that.
    CHK_OK(db->flushLogs());

    // Snapshot: sees kv2 and kv3.
    CHK_Z(_itr_check(0, n, itr, kv2));
    CHK_Z(_itr_check(0, n, itr, kv3));

    // No more records.
    CHK_NOT(itr.next());

    CHK_OK(itr.close());

    // Close snapshot.
    CHK_OK(jungle::DB::close(snap));

    // Close DB.
    CHK_OK(jungle::DB::close(db));

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);
    _free_kv_pairs(n, kv3);
    _free_kv_pairs(n, kv4);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_with_compaction_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    // Set KV pairs.
    for (size_t ii=0; ii<30; ii+=10) {
        CHK_Z( _set_keys(db, ii, ii+10, 1, "key_%04d", "val_%04d") );

        uint64_t seq_num_out;
        CHK_Z( db->checkpoint(seq_num_out, false) );
        TestSuite::_msg("%zu\n", seq_num_out);
    }

    // Flush.
    CHK_Z( db->flushLogs() );

    // Verify checkpoints.
    for (int chk: {10, 20, 30}) {
        jungle::DB* snap_out = nullptr;
        CHK_Z( db->openSnapshot(&snap_out, chk) );

        jungle::Iterator itr;
        CHK_Z( itr.initSN(snap_out) );

        size_t count = 0;
        do {
            jungle::Record rec_out;
            s = itr.get(rec_out);
            if (!s) break;

            rec_out.free();
            count++;
        } while (itr.next().ok());
        CHK_EQ(chk, count);
        CHK_Z( itr.close() );
        CHK_Z( jungle::DB::close(snap_out) );
    }

    // Compact.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }

    // Verify checkpoints:
    for (int chk: {10, 20, 30}) {
        jungle::DB* snap_out = nullptr;
        CHK_NOT( db->openSnapshot(&snap_out, chk).ok() );
    }

    // Put more.
    for (size_t ii=30; ii<50; ii+=10) {
        CHK_Z( _set_keys(db, ii, ii+10, 1, "key_%04d", "val_%04d") );

        uint64_t seq_num_out;
        CHK_Z( db->checkpoint(seq_num_out, false) );
        TestSuite::_msg("%zu\n", seq_num_out);
    }

    // Flush.
    CHK_Z( db->flushLogs() );

    // Verify checkpoints:
    //  10-30: invalid.
    //  40-50: valid.
    for (int chk: {10, 20, 30, 40, 50}) {
        jungle::DB* snap_out = nullptr;
        if (chk < 40) {
            CHK_NOT( db->openSnapshot(&snap_out, chk).ok() );
            continue;
        }
        CHK_Z( db->openSnapshot(&snap_out, chk) );

        jungle::Iterator itr;
        CHK_Z( itr.initSN(snap_out) );

        size_t count = 0;
        do {
            jungle::Record rec_out;
            s = itr.get(rec_out);
            if (!s) break;

            rec_out.free();
            count++;
        } while (itr.next().ok());
        CHK_EQ(chk, count);
        CHK_Z( itr.close() );
        CHK_Z( jungle::DB::close(snap_out) );
    }

    // Compact.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }

    for (int chk: {10, 20, 30, 40, 50}) {
        TestSuite::setInfo("chk %d", chk);
        jungle::DB* snap_out = nullptr;
        CHK_NOT( db->openSnapshot(&snap_out, chk).ok() );
    }

    // Put more.
    for (size_t ii=50; ii<70; ii+=10) {
        CHK_Z( _set_keys(db, ii, ii+10, 1, "key_%04d", "val_%04d") );

        uint64_t seq_num_out;
        CHK_Z( db->checkpoint(seq_num_out, false) );
        TestSuite::_msg("%zu\n", seq_num_out);
    }

    // Verify checkpoints:
    //  10-50: invalid.
    //  60-70: valid.
    for (int chk: {10, 20, 30, 40, 50, 60, 70}) {
        jungle::DB* snap_out = nullptr;
        if (chk < 60) {
            CHK_NOT( db->openSnapshot(&snap_out, chk).ok() );
            continue;
        }
        CHK_Z( db->openSnapshot(&snap_out, chk) );

        jungle::Iterator itr;
        CHK_Z( itr.initSN(snap_out) );

        size_t count = 0;
        do {
            jungle::Record rec_out;
            s = itr.get(rec_out);
            if (!s) break;

            rec_out.free();
            count++;
        } while (itr.next().ok());
        CHK_EQ(chk, count);
        CHK_Z( itr.close() );
        CHK_Z( jungle::DB::close(snap_out) );
    }

    // Flush.
    CHK_Z( db->flushLogs() );

    // Close DB.
    CHK_OK(jungle::DB::close(db));

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int latest_snapshot_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db = nullptr;

    config.maxEntriesInLogFile = 10;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Set KV pairs.
    int n = 25;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv));

    // Sync and flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key2_", "value2_"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv2));
    CHK_Z(db->sync(false));

    // Open the latest snapshot (checkpoint = 0).
    jungle::DB* snap = nullptr;
    CHK_Z(db->openSnapshot(&snap));

    // Set more KV pairs, using the same key.
    std::vector<jungle::KV> kv3(n);
    CHK_Z(_init_kv_pairs(n, kv3, "key1_", "value1_new_"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv3));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    std::vector<jungle::KV> kv4(n);
    CHK_Z(_init_kv_pairs(n, kv4, "key2_", "value2_new_"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv4));
    CHK_Z(db->sync(false));

    // Do compaction.
    for (size_t ii=0; ii<config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }

    // Now open iterator using the snapshot.
    jungle::Iterator itr;
    CHK_Z(itr.init(snap));

    // Should see kv and kv2 only.
    CHK_Z(_itr_check(0, n, itr, kv));
    CHK_Z(_itr_check(0, n, itr, kv2));

    // Create another iterator from the snapshot in parallel
    // and do the same thing.
    jungle::Iterator itr2;
    CHK_Z(itr2.init(snap));
    CHK_Z(_itr_check(0, n, itr2, kv));
    CHK_Z(_itr_check(0, n, itr2, kv2));

    // Create another iterator from the main DB in parallel.
    // Should see new kv.
    jungle::Iterator itr3;
    CHK_Z(itr3.init(db));
    CHK_Z(_itr_check(0, n, itr3, kv3));
    CHK_Z(_itr_check(0, n, itr3, kv4));

    // Close iterators.
    CHK_Z(itr.close());
    CHK_Z(itr2.close());
    CHK_Z(itr3.close());

    // Close snapshot.
    CHK_Z(jungle::DB::close(snap));

    // Close DB.
    CHK_Z(jungle::DB::close(db));

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);
    _free_kv_pairs(n, kv3);
    _free_kv_pairs(n, kv4);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int empty_db_snapshot_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db = nullptr;

    config.maxEntriesInLogFile = 10;
    s = jungle::DB::open(&db, filename, config);
    CHK_Z(s);

    // Open snapshot on empty DB, should succeed.
    jungle::DB* snap = nullptr;
    CHK_Z(db->openSnapshot(&snap));

    // Set KV pairs.
    int n = 25;
    std::vector<jungle::KV> kv(n);
    CHK_Z(_init_kv_pairs(n, kv, "key1_", "value1_"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv));

    // Sync and flush.
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    // Set more.
    std::vector<jungle::KV> kv2(n);
    CHK_Z(_init_kv_pairs(n, kv2, "key2_", "value2_"));
    CHK_Z(_set_bykey_kv_pairs(0, n, db, kv2));
    CHK_Z(db->sync(false));

    // Snapshot shouldn't see anything.
    CHK_Z(_get_bykey_check(0, n, snap, kv, false));
    CHK_Z(_get_bykey_check(0, n, snap, kv2, false));

    // Now open iterator using the snapshot.
    jungle::Iterator itr;
    CHK_Z(itr.init(snap));

    // Shouldn't see anything.
    jungle::Record rec_out;
    CHK_NOT(itr.get(rec_out));
    CHK_NOT(itr.next());
    CHK_NOT(itr.prev());

    // Close iterator.
    CHK_Z(itr.close());

    // Close snapshot.
    CHK_Z(jungle::DB::close(snap));

    // Close DB.
    CHK_Z(jungle::DB::close(db));

    _free_kv_pairs(n, kv);
    _free_kv_pairs(n, kv2);

    jungle::shutdown();
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_lookup_after_compaction_test(int lookup_type, bool level_extension) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    CHK_Z(jungle::init(g_config));

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 4;
    config.nextLevelExtension = level_extension;
    jungle::DB* db = nullptr;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t num = 32;
    CHK_Z(_set_keys(db, 0, num, 1, "key_%04zu", "old_%04zu"));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    // With level extension, the snapshot contains overlapping L0 and L1
    // versions. Point reads must select the newest captured version.
    CHK_Z(_set_keys(db, 0, num / 2, 1, "key_%04zu", "snap_%04zu"));
    CHK_Z(db->del(jungle::SizedBuf("key_0001")));
    // Rotate past all tested keys, including the deletion, so their reads
    // must reach the captured table snapshots rather than the final log file.
    CHK_Z(_set_keys(db, 0, 8, 1, "padding_%04zu", "padding_%04zu"));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    jungle::DB* snap = nullptr;
    CHK_Z(db->openSnapshot(&snap));
    uint64_t snapshot_seq = 0;
    CHK_Z(db->getMaxSeqNum(snapshot_seq));

    // Replace the live tables and change every value after capturing the
    // snapshot. Older keys cannot be satisfied by its retained log tail.
    CHK_Z(_set_keys(db, 0, num, 1, "key_%04zu", "live_%04zu"));
    CHK_Z(db->set(jungle::KV("future_key", "future_value")));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    for (size_t ii = 0; ii < num; ++ii) {
        std::string key = "key_" + TestSuite::lzStr(4, ii);
        std::string value = (ii < num / 2 ? "snap_" : "old_") +
                            TestSuite::lzStr(4, ii);
        TestSuite::setInfo("key %s, lookup %d, extension %d",
                           key.c_str(), lookup_type, level_extension);
        if (lookup_type == 0) {
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h_value_out(value_out);
            jungle::Status s = snap->get(jungle::SizedBuf(key), value_out);
            if (ii == 1) {
                CHK_OK(s == jungle::Status::KEY_NOT_FOUND);
            } else {
                CHK_Z(s);
                CHK_EQ(jungle::SizedBuf(value), value_out);
            }
            for (bool meta_only : {false, true}) {
                jungle::Record rec;
                jungle::Record::Holder h_rec(rec);
                s = snap->getRecordByKey(jungle::SizedBuf(key), rec, meta_only);
                if (ii == 1 && !meta_only) {
                    CHK_OK(s == jungle::Status::KEY_NOT_FOUND);
                    continue;
                }
                CHK_Z(s);
                CHK_EQ(ii == 1, rec.isDel());
                CHK_OK(rec.seqNum <= snapshot_seq);
                if (!meta_only) CHK_EQ(jungle::SizedBuf(value), rec.kv.value);
            }
        } else if (lookup_type == 1) {
            for (auto opt : {jungle::SearchOptions::GREATER_OR_EQUAL,
                             jungle::SearchOptions::SMALLER_OR_EQUAL}) {
                for (bool meta_only : {false, true}) {
                    jungle::Record rec;
                    jungle::Record::Holder h_rec(rec);
                    CHK_Z(snap->getNearestRecordByKey(jungle::SizedBuf(key),
                                                      rec, opt, meta_only));
                    CHK_EQ(jungle::SizedBuf(key), rec.kv.key);
                    CHK_EQ(ii == 1, rec.isDel());
                    CHK_OK(rec.seqNum <= snapshot_seq);
                    if (!meta_only && ii != 1) {
                        CHK_EQ(jungle::SizedBuf(value), rec.kv.value);
                    }
                    if (meta_only) CHK_OK(rec.kv.value.empty());
                }
            }
        } else {
            // Prefix lookup intentionally reports versions from multiple
            // tables. Check its newest result without changing that contract.
            uint64_t newest_seq = 0;
            std::string newest_value;
            bool newest_deleted = false;
            bool future_record = false;
            auto cb = [&](const jungle::SearchCbParams& params) {
                const jungle::Record& rec = params.rec;
                future_record |= rec.seqNum > snapshot_seq;
                if (rec.seqNum > newest_seq) {
                    newest_seq = rec.seqNum;
                    newest_value = rec.kv.value.toString();
                    newest_deleted = rec.isDel();
                }
                return jungle::SearchCbDecision::NEXT;
            };
            CHK_Z(snap->getRecordsByPrefix(jungle::SizedBuf(key), cb));
            CHK_NOT(future_record);
            CHK_OK(newest_seq > 0);
            CHK_EQ(ii == 1, newest_deleted);
            if (ii != 1) CHK_EQ(value, newest_value);
        }
    }

    jungle::SizedBuf value_out;
    jungle::SizedBuf::Holder h_value_out(value_out);
    CHK_OK(snap->get(jungle::SizedBuf("future_key"), value_out) ==
           jungle::Status::KEY_NOT_FOUND);
    CHK_Z(_get_keys(db, 0, num, 1, "key_%04zu", "live_%04zu"));

    CHK_Z(jungle::DB::close(snap));
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int snapshot_lookup_many_tables_test(bool use_checkpoint) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    CHK_Z(jungle::init(g_config));

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 1000;
    config.nextLevelExtension = true;
    config.maxL1TableSize = 1024 * 1024;
    jungle::DB* db = nullptr;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Snapshot image: even keys only, so that odd keys can be used as
    // keys in between, and as keys written after the snapshot.
    const size_t NUM = 20000;
    const char K_FMT[] = "key_%06zu";
    struct Expected {
        std::string value;
        bool deleted = false;
    };
    std::map<std::string, Expected> image;
    auto key_of = [&](size_t idx) {
        char buf[32];
        sprintf(buf, K_FMT, idx);
        return std::string(buf);
    };
    // Keys around the boundaries of level-1 tables.
    std::set<size_t> boundary_idxs;
    auto set_even_keys = [&](size_t step, const std::string& v_prefix) {
        for (size_t ii = 0; ii < NUM; ii += step) {
            if (boundary_idxs.count(ii)) continue;
            std::string key = key_of(ii);
            std::string value = v_prefix + std::string(450, 'x') +
                                TestSuite::lzStr(6, ii);
            CHK_Z(db->set(jungle::KV(key, value)));
            image[key].value = value;
        }
        return 0;
    };

    // Level-1 consisting of multiple tables.
    CHK_Z(set_even_keys(2, "old_"));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }
    CHK_Z(_split_level1(db, 4));

    // Newer versions in level-1.
    CHK_Z(set_even_keys(6, "mid_"));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    // Level-1 tables are not changed until the snapshot is opened.
    // Keep the keys around their boundaries only in level-1, so that
    // lookups there need the right level-1 tables.
    {
        jungle::DBStatsOptions opt;
        opt.getTableHierarchy = true;
        jungle::DBStats stats;
        CHK_Z(db->getStats(stats, opt));
        CHK_GTEQ(stats.tableHierarchy[1].size(), 4);
        for (auto& entry: stats.tableHierarchy[1]) {
            size_t idx = 0;
            std::string min_key = entry.minKey.toString();
            if (sscanf(min_key.c_str(), K_FMT, &idx) != 1) continue;
            for (size_t jj = (idx > 4 ? idx - 4 : 0); jj <= idx + 4; ++jj) {
                boundary_idxs.insert(jj);
            }
        }
        CHK_GTEQ(boundary_idxs.size(), 3 * 9);
    }

    // Newer versions and deletions in level-0.
    CHK_Z(set_even_keys(4, "new_"));
    for (size_t ii = 0; ii < NUM; ii += 10) {
        if (boundary_idxs.count(ii)) continue;
        CHK_Z(db->del(jungle::SizedBuf(key_of(ii))));
        image[key_of(ii)].deleted = true;
    }
    // Rotate past all tested keys, so that their reads must reach
    // the captured tables rather than the log files.
    CHK_Z(_set_keys(db, 0, config.maxEntriesInLogFile * 2, 1,
                    "padding_%06zu", "padding_%06zu"));
    uint64_t chk = 0;
    if (use_checkpoint) CHK_Z(db->checkpoint(chk));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    std::set<size_t> test_idxs = boundary_idxs;
    for (size_t ii = 0; ii <= NUM; ii += 97) test_idxs.insert(ii);

    jungle::DB* snap = nullptr;
    CHK_Z(db->openSnapshot(&snap, chk));

    // Replace the live tables, and write odd keys after the snapshot.
    CHK_Z(_set_keys(db, 0, NUM, 1, K_FMT, "live_%06zu"));
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < config.numL0Partitions; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }
    CHK_Z(_get_keys(db, 0, NUM, 1, K_FMT, "live_%06zu"));

    auto check_rec = [&](const jungle::Record& rec, bool meta_only) {
        std::string key = rec.kv.key.toString();
        auto entry = image.find(key);
        CHK_OK(entry != image.end());
        CHK_EQ(entry->second.deleted, rec.isDel());
        if (!meta_only && !rec.isDel()) {
            CHK_EQ(entry->second.value, rec.kv.value.toString());
        }
        return 0;
    };

    // Point and nearest lookups.
    for (size_t ii: test_idxs) {
        std::string key = key_of(ii);
        TestSuite::setInfo("key %s", key.c_str());
        auto entry = image.find(key);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        jungle::Status s = snap->get(jungle::SizedBuf(key), value_out);
        if (entry == image.end() || entry->second.deleted) {
            CHK_EQ(jungle::Status::KEY_NOT_FOUND, s.getValue());
        } else {
            CHK_Z(s);
            CHK_EQ(entry->second.value, value_out.toString());
        }

        if (entry != image.end()) {
            jungle::Record rec;
            jungle::Record::Holder h_rec(rec);
            CHK_Z(snap->getRecordByKey(jungle::SizedBuf(key), rec, true));
            CHK_EQ(entry->second.deleted, rec.isDel());
        }

        for (auto opt: { jungle::SearchOptions::GREATER,
                         jungle::SearchOptions::SMALLER }) {
            auto exp = (opt == jungle::SearchOptions::GREATER)
                       ? image.upper_bound(key)
                       : image.lower_bound(key);
            if (opt == jungle::SearchOptions::SMALLER) {
                exp = (exp == image.begin()) ? image.end() : std::prev(exp);
            }

            jungle::Record rec;
            jungle::Record::Holder h_rec(rec);
            s = snap->getNearestRecordByKey(jungle::SizedBuf(key), rec, opt);
            if (exp == image.end()) {
                // Out of the tested keys: nothing, or the padding keys.
                if (s) CHK_NEQ(std::string::npos, rec.kv.key.toString().find("padding_"));
                continue;
            }
            CHK_Z(s);
            CHK_EQ(exp->first, rec.kv.key.toString());
            CHK_Z(check_rec(rec, false));
        }
    }

    // Prefix lookups, spanning one to all level-1 tables.
    for (std::string prefix: {"key_0", "key_00", "key_01",
                              "key_000", "key_0123"}) {
        TestSuite::setInfo("prefix %s", prefix.c_str());
        // Prefix lookup reports versions from multiple tables.
        // Check the newest one of each key.
        std::map<std::string, jungle::Record*> newest;
        auto cb = [&](const jungle::SearchCbParams& params) {
            jungle::Record*& rec = newest[params.rec.kv.key.toString()];
            if (!rec || rec->seqNum < params.rec.seqNum) {
                if (rec) rec->free();
                delete rec;
                rec = new jungle::Record();
                params.rec.copyTo(*rec);
            }
            return jungle::SearchCbDecision::NEXT;
        };
        CHK_Z(snap->getRecordsByPrefix(jungle::SizedBuf(prefix), cb));

        size_t num_expected = 0;
        for (auto& entry: image) {
            if (entry.first.find(prefix) == 0) num_expected++;
        }
        CHK_EQ(num_expected, newest.size());
        for (auto& entry: newest) {
            CHK_Z(check_rec(*entry.second, false));
            entry.second->free();
            delete entry.second;
        }
    }

    // Keys out of range.
    for (std::string key: {"a", "key_", "z"}) {
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_EQ(jungle::Status::KEY_NOT_FOUND,
               snap->get(jungle::SizedBuf(key), value_out).getValue());
    }

    CHK_Z(jungle::DB::close(snap));
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int race_with_set_batch() {
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
        //jungle::DB* db;
        // Open a snapshot.
        jungle::DB* snap = nullptr;
        CHK_Z( db->openSnapshot(&snap) );

        for (size_t ii=0; ii<15; ++ii) {
            std::string key_str = "k" + std::to_string(ii);
            std::string val_str = "v" + std::to_string(ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            jungle::Status s = snap->get( jungle::SizedBuf(key_str), value_out );
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
        itr.init(snap);
        size_t count = 0;
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            CHK_Z( itr.get(rec) );
            count++;
        } while (itr.next().ok());
        itr.close();
        CHK_EQ(5, count);

        CHK_Z(jungle::DB::close(snap));
        return 0;
    };
    jungle::DB::setDebugParams(dp);
    jungle::DB::enableDebugCallbacks(true);

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

    // After batch update succeeds, and create snapshot.
    // We should see all records from the snapshot.
    jungle::DB* snap = nullptr;
    CHK_Z( db->openSnapshot(&snap) );
    for (size_t ii=0; ii<15; ++ii) {
        std::string key_str = "k" + std::to_string(ii);
        std::string val_str = "v" + std::to_string(ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h(value_out);
        CHK_Z( snap->get( jungle::SizedBuf(key_str), value_out ) );
        CHK_EQ( val_str, value_out.toString() );
    }

    // Same to iterator.
    jungle::Iterator itr;
    itr.init(snap);
    size_t count = 0;
    do {
        jungle::Record rec;
        jungle::Record::Holder h(rec);
        CHK_Z( itr.get(rec) );
        count++;
    } while (itr.next().ok());
    itr.close();
    CHK_EQ(15, count);

    CHK_Z(jungle::DB::close(snap));
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

}
using namespace snapshot_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("checkpoint basic test", checkpoint_basic_test);
    ts.doTest("checkpoint marker test", checkpoint_marker_test);
    ts.doTest("checkpoint marker flush test", checkpoint_marker_flush_test);
    ts.doTest("checkpoint marker purge test", checkpoint_marker_purge_test);
    ts.doTest("checkpoint on cold start test", checkpoint_on_cold_start);
    ts.doTest("checkpoint limit test", checkpoint_limit_test);
    ts.doTest("snapshot basic table only test", snapshot_basic_table_only_test);
    ts.doTest("snapshot basic log only test", snapshot_basic_log_only_test);
    ts.doTest("snapshot basic combined test", snapshot_basic_combined_test);
    ts.doTest("snapshot iterator table only test", snapshot_iterator_table_only_test);
    ts.doTest("snapshot iterator log only test", snapshot_iterator_log_only_test);
    ts.doTest("snapshot iterator combined test", snapshot_iterator_combined_test);
    ts.doTest("snapshot with compaction test", snapshot_with_compaction_test);
    ts.doTest("latest snapshot test", latest_snapshot_test);
    ts.doTest("empty db snapshot test", empty_db_snapshot_test);
    for (bool level_extension : {false, true}) {
        for (int lookup_type = 0; lookup_type < 3; ++lookup_type) {
            ts.doTest("snapshot lookup after compaction " +
                      std::to_string(lookup_type) +
                      (level_extension ? " with levels" : " without levels"),
                      [=]() {
                          return snapshot_lookup_after_compaction_test(
                              lookup_type, level_extension);
                      });
        }
    }
    ts.doTest("snapshot lookup many tables test",
              snapshot_lookup_many_tables_test,
              TestRange<bool>({false, true}));
    ts.doTest("race with set batch test", race_with_set_batch);

    return 0;
}
