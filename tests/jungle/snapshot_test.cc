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
    s = db->flushLogs(jungle::FlushOptions());
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
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

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
    CHK_OK(db->flushLogs(jungle::FlushOptions()));

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
    jungle::FlushOptions f_opt;
    CHK_OK(db->flushLogs(f_opt));

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
    CHK_OK(db->flushLogs(f_opt));

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
    CHK_OK(db->flushLogs(jungle::FlushOptions()));

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
    jungle::FlushOptions f_opt;
    CHK_OK(db->flushLogs(f_opt));

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
    CHK_OK(db->flushLogs(f_opt));

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
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

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
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

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
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

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
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

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
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

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
    CHK_Z(db->flushLogs(jungle::FlushOptions()));

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

    return 0;
}
