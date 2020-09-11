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

#include "internal_helper.h"

#include <fstream>
#include <vector>

#include <stdio.h>

namespace log_reclaim_test {

int basic_log_reclaim_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 1000;
    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    // Sync.
    CHK_Z(db->sync(false));

    TestSuite::sleep_sec(3, "waiting for reclaiming");

    // Purge.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), 500) );

    // Get min seq number.
    uint64_t min_seqnum = 0;
    CHK_Z(db->getMinSeqNum(min_seqnum));
    CHK_EQ(501, min_seqnum);

    // Point query something.
    for (size_t ii=501; ii<=1000; ii+=171) {
        char key_str[256];
        char val_str[256];
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii, kv_out));

        sprintf(key_str, "k%06zu", ii-1);
        sprintf(val_str, "v%06zu", ii-1);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val(val_str);
        CHK_EQ(key, kv_out.key);
        CHK_EQ(val, kv_out.value);
        kv_out.free();
    }

    // Seq iterator.
    jungle::Iterator itr;
    CHK_Z( itr.initSN(db, 751, 900) );
    size_t idx = 750;
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        char key_str[256];
        char val_str[256];
        sprintf(key_str, "k%06zu", idx);
        sprintf(val_str, "v%06zu", idx);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val(val_str);
        CHK_EQ(key, rec_out.kv.key);
        CHK_EQ(val, rec_out.kv.value);
        rec_out.free();
        idx++;
    } while (itr.next().ok());
    itr.close();

    TestSuite::sleep_sec(3, "waiting for reclaiming");

    // Purge more.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), 700) );

    // Get min seq number.
    min_seqnum = 0;
    CHK_Z(db->getMinSeqNum(min_seqnum));
    CHK_EQ(701, min_seqnum);

    CHK_Z(jungle::DB::close(db));

    // Offline checking if it is log section mode.
    CHK_TRUE( jungle::DB::isLogSectionMode(filename) );

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int multi_instances_reclaim_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    std::vector<jungle::DB*> dbs(10);

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    g_config.numFlusherThreads = 0;
    g_config.numCompactorThreads = 0;
    g_config.numTableWriters = 0;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 50;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    size_t count = 0;
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        CHK_Z( jungle::DB::open( &db,
                                 filename + "_" + std::to_string(count++),
                                 config) );
    }

    size_t num = 1000;
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");
    }

    // Sync.
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        CHK_Z( db->sync(false) );
    }

    TestSuite::sleep_sec(3, "waiting for reclaiming");

    // Purge.
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        CHK_Z( db->flushLogs(jungle::FlushOptions(), 500) );
    }

    // Get min seq number.
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        uint64_t min_seqnum = 0;
        CHK_Z(db->getMinSeqNum(min_seqnum));
        CHK_EQ(501, min_seqnum);
    }

    // Point query something.
    for (size_t ii=501; ii<=1000; ii+=171) {
        for (size_t jj=0; jj<10; ++jj) {
            jungle::DB*& db = dbs[jj];
            char key_str[256];
            char val_str[256];
            jungle::KV kv_out;
            jungle::KV::Holder h_kv_out(kv_out);
            if (ii == 843 && jj == 9) {
                int bp = 0; (void)bp;
            }
            s = db->getSN(ii, kv_out);
            if (!s) {
                int bp = 0; (void)bp;
            }

            sprintf(key_str, "k%06zu", ii-1);
            sprintf(val_str, "v%06zu", ii-1);
            jungle::SizedBuf key(key_str);
            jungle::SizedBuf val(val_str);
            CHK_EQ(key, kv_out.key);
            CHK_EQ(val, kv_out.value);
        }
    }

    // Seq iterator.
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        jungle::Iterator itr;
        CHK_Z( itr.initSN(db, 751, 900) );
        size_t idx = 750;
        do {
            jungle::Record rec_out;
            s = itr.get(rec_out);
            if (!s) break;

            char key_str[256];
            char val_str[256];
            sprintf(key_str, "k%06zu", idx);
            sprintf(val_str, "v%06zu", idx);
            jungle::SizedBuf key(key_str);
            jungle::SizedBuf val(val_str);
            CHK_EQ(key, rec_out.kv.key);
            CHK_EQ(val, rec_out.kv.value);
            rec_out.free();
            idx++;
        } while (itr.next().ok());
        itr.close();
    }

    TestSuite::sleep_sec(3, "waiting for reclaiming");

    // Purge more.
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        CHK_Z( db->flushLogs(jungle::FlushOptions(), 700) );
    }

    // Get min seq number.
    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        uint64_t min_seqnum = 0;
        CHK_Z(db->getMinSeqNum(min_seqnum));
        CHK_EQ(701, min_seqnum);
    }

    for (auto& entry: dbs) {
        jungle::DB*& db = entry;
        CHK_Z(jungle::DB::close(db));
    }
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int reload_log_in_reclaim_mode_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 1000;
    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    // Sync.
    CHK_Z(db->sync(false));

    // Purge.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), 500) );

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));

    // Offline checking if it is log section mode.
    CHK_TRUE( jungle::DB::isLogSectionMode(filename) );

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get min seq number.
    uint64_t min_seqnum = 0;
    CHK_Z(db->getMinSeqNum(min_seqnum));
    CHK_EQ(501, min_seqnum);

    // Point query something.
    for (size_t ii=501; ii<=1000; ii+=171) {
        char key_str[256];
        char val_str[256];
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii, kv_out));

        sprintf(key_str, "k%06zu", ii-1);
        sprintf(val_str, "v%06zu", ii-1);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val(val_str);
        CHK_EQ(key, kv_out.key);
        CHK_EQ(val, kv_out.value);
        kv_out.free();
    }

    // Seq iterator.
    jungle::Iterator itr;
    CHK_Z( itr.initSN(db, 751, 900) );
    size_t idx = 750;
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        char key_str[256];
        char val_str[256];
        sprintf(key_str, "k%06zu", idx);
        sprintf(val_str, "v%06zu", idx);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val(val_str);
        CHK_EQ(key, rec_out.kv.key);
        CHK_EQ(val, rec_out.kv.value);
        rec_out.free();
        idx++;
    } while (itr.next().ok());
    itr.close();

    TestSuite::sleep_sec(3, "waiting for reclaiming");

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int reload_with_empty_files_test_create() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10000;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 100;
    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    // Sync.
    CHK_Z(db->sync(false));

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));

    // Offline checking if it is log section mode.
    CHK_TRUE( jungle::DB::isLogSectionMode(filename) );

    CHK_Z(jungle::DB::open(&db, filename, config));

    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    TestSuite::sleep_sec(60, "kill me now");

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int reload_with_empty_files_test_load() {
    std::string filename = "./bed";

    jungle::Status s;
    jungle::DB* db;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10000;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;
    CHK_Z(jungle::DB::open(&db, filename, config));

    uint64_t seq_num_out = 0;
    db->getMaxSeqNum(seq_num_out);
    CHK_EQ(100, seq_num_out);

    size_t num = 100;
    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    return 0;
}

void async_callback(bool* invoked,
                    EventAwaiter* ea,
                    jungle::Status s,
                    void* ctx)
{
    if (invoked) *invoked = true;
    ea->invoke();
}

int async_sync_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.numCompactorThreads = 0;
    g_config.numTableWriters = 0;
    g_config.compactorSleepDuration_ms = 1000; // 1 second
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    size_t num = 100;
    _set_keys(db, 0, num, 1, "k%06zu", "v%06zu");

    // Async sync.
    jungle::FlushOptions f_opt;
    f_opt.syncOnly = true;
    f_opt.callFsync = true;

    bool callback_invoked = false;
    EventAwaiter ea;
    CHK_Z( db->flushLogsAsync
               ( f_opt,
                 std::bind( async_callback,
                            &callback_invoked,
                            &ea,
                            std::placeholders::_1,
                            std::placeholders::_2 ),
                 nullptr ) );

    ea.wait_ms(1000 * 5);
    CHK_TRUE( callback_invoked );

    uint64_t seq_num_out = 0;
    CHK_Z( db->getLastSyncedSeqNum(seq_num_out) );
    CHK_EQ( num, seq_num_out );

    // Put more keys.
    _set_keys(db, num, 2 * num, 1, "k%06zu", "v%06zu");

    // They are not synced.
    seq_num_out = 0;
    CHK_Z( db->getLastSyncedSeqNum(seq_num_out) );
    CHK_EQ( num, seq_num_out );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int crash_after_adding_new_log_file_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    _set_keys(db, 0, 5, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    _set_keys(db, 5, 25, 1, "k%06zu", "v%06zu");

    // Copy files somewhere.
    jungle::FileMgr::mkdir(filename + "/backup");
    jungle::FileMgr::copy(filename + "/log0000_*", filename + "/backup");

    CHK_Z( jungle::DB::close(db) );

    // Restore and re-open.
    jungle::FileMgr::copy(filename + "/backup/log0000_*", filename + "/");
    CHK_Z( jungle::DB::open(&db, filename, config) );

    uint64_t seq_num_out = 0;
    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( 5, seq_num_out );
    _set_keys(db, 5, 15, 1, "k%06zu", "v%06zu");

    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( 15, seq_num_out );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( 15, seq_num_out );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_rollback_basic_test(bool sync_before_rollback) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t N1 = 8, N2 = 15, ROLLBACK = 5;
    _set_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    if (sync_before_rollback) {
        CHK_Z( db->sync(false) );
    }

    _get_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    CHK_Z( db->rollback(ROLLBACK) );

    jungle::KV kv_out;
    uint64_t seq_num_out = 0;

    // Check rollback.
    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( ROLLBACK, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    for (size_t ii=ROLLBACK+1; ii<=N1; ++ii) CHK_FALSE( db->getSN(ii, kv_out) );

    CHK_Z( db->sync(false) );

    // Even after sync, the results should be the same.
    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( ROLLBACK, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    for (size_t ii=ROLLBACK+1; ii<=N1; ++ii) CHK_FALSE( db->getSN(ii, kv_out) );

    // Put more KVs.
    _set_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    // Old KVs (rolled-back) shouldn't be visible.
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( N2, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_rollback_multi_files_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t N1 = 55, N2 = 95, ROLLBACK = 25;
    _set_keys(db, 0, N1, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );
    _get_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    CHK_Z( db->rollback(ROLLBACK) );

    jungle::KV kv_out;
    uint64_t seq_num_out = 0;

    // Check rollback.
    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( ROLLBACK, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    for (size_t ii=ROLLBACK+1; ii<=N1; ++ii) CHK_FALSE( db->getSN(ii, kv_out) );

    CHK_Z( db->sync(false) );

    // Even after sync, the results should be the same.
    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( ROLLBACK, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    for (size_t ii=ROLLBACK+1; ii<=N1; ++ii) CHK_FALSE( db->getSN(ii, kv_out) );

    // Put more KVs.
    _set_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    // Old KVs (rolled-back) shouldn't be visible.
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( N2, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_rollback_and_reclaim_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t N1 = 99, N2 = 105, ROLLBACK = 98;
    _set_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    CHK_Z( db->sync(false) );

    _get_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    CHK_Z( db->rollback(ROLLBACK) );

    jungle::KV kv_out;
    uint64_t seq_num_out = 0;

    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( ROLLBACK, seq_num_out );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    for (size_t ii=ROLLBACK+1; ii<=N1; ++ii) CHK_FALSE( db->getSN(ii, kv_out) );

    // Put more KVs.
    _set_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");
    CHK_Z( db->sync(false) );

    // Old KVs (rolled-back) shouldn't be visible.
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    // Wait for reclaim of old log.
    TestSuite::sleep_ms(1500, "wait for reclaim");

    // Wait one more time.
    TestSuite::sleep_ms(1500, "wait for reclaim");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_rollback_with_concurrent_write_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 1;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t N1 = 99, N2 = 105, ROLLBACK = 98;
    _set_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    CHK_Z( db->sync(false) );

    _get_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    // Set delay for compaction.
    jungle::DebugParams d_params;
    d_params.rollbackDelayUs = 1000000; // 1 second delay.
    jungle::setDebugParams(d_params);

    std::thread t_rollback
        ( [db, ROLLBACK]() {
            (void)ROLLBACK;
            db->rollback(ROLLBACK);
        } );

    TestSuite::sleep_us(d_params.rollbackDelayUs / 2, "wait for rollback");

    // Write operations should fail.
    {
        char key_str[256];
        char val_str[256];
        for (size_t ii=ROLLBACK; ii<N2; ii++) {
            sprintf(key_str, "k%06zu", ii);
            sprintf(val_str, "v2_%06zu", ii);
            jungle::SizedBuf key(key_str);
            jungle::SizedBuf val(val_str);
            CHK_FALSE( db->set(jungle::KV(key, val)) );
        }
    }
    CHK_FALSE( db->sync(false) );
    CHK_FALSE( db->syncNoWait(false) );
    CHK_FALSE( db->flushLogsAsync(jungle::FlushOptions(), nullptr, nullptr) );

    if (t_rollback.joinable()) t_rollback.join();

    // After rollback is done, they should succeed.
    // Put more KVs.
    _set_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");
    CHK_Z( db->sync(false) );

    // Old KVs (rolled-back) shouldn't be visible.
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, N2, 1, "k%06zu", "v2_%06zu");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int urgent_reclaim_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 3600;
    config.maxKeepingMemtables = 5;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t N1  = 200;
    _set_keys(db, 0, N1, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    _get_keys(db, 0, N1, 1, "k%06zu", "v%06zu");

    TestSuite::sleep_sec(2, "wait for reclaiming..");

    // Read from the beginning, may cause reload and re-purge.
    uint64_t cur_seq = 1;
    for (size_t ii=0; ii<N1; ii++) {
        TestSuite::setInfo("ii=%zu", ii);
        jungle::KV kv_out;
        jungle::KV::Holder h_kv_out(kv_out);
        CHK_Z(db->getSN(cur_seq, kv_out));

        char key_str[256], val_str[256];
        sprintf(key_str, "k%06zu", ii);
        sprintf(val_str, "v%06zu", ii);
        jungle::SizedBuf key_exp(key_str);
        jungle::SizedBuf val_exp(val_str);
        CHK_EQ(key_exp, kv_out.key);
        CHK_EQ(val_exp, kv_out.value);
        cur_seq++;
    }

    TestSuite::sleep_sec(2, "wait for reclaiming..");

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_flush_zero_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 60;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t N1 = 100;
    _set_keys(db, 0, N1, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Flush with 0, should do nothing.
    db->flushLogs(jungle::FlushOptions(), 0);

    uint64_t seq_num_out = 0;
    CHK_Z( db->getMaxSeqNum(seq_num_out) );
    CHK_EQ( N1, seq_num_out );

    // Should fail.
    CHK_NEQ(0, db->getLastFlushedSeqNum(seq_num_out));

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int moving_to_next_log_without_sync_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.numL0Partitions = 4;
    config.maxLogFileSize = 1024*1024;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 60;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::string val_payload(1024, 'x');
    const size_t NUM = 2000;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = TestSuite::lzStr(6, ii);
        CHK_Z( db->setSN(ii+1, jungle::KV(key_str, val_payload)) );
    }
    // No sync to file, but `log0000_00000001` file should exist.
    CHK_TRUE( TestSuite::exist( filename + "/log0000_00000001" ) );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int immediate_log_purging_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    g_config.flusherSleepDuration_ms = 3600 * 1000;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxKeepingMemtables = 8;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 3600;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Create 200 log files.
    const size_t NUM = 2000;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = TestSuite::lzStr(6, ii);
        std::string val_str = TestSuite::lzStr(6, ii);
        CHK_Z( db->setSN(ii+1, jungle::KV(key_str, val_str)) );
        if (ii % config.maxEntriesInLogFile == 0) {
            CHK_Z( db->sync(false) );
        }
    }

    // Truncate half.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), 1000) );

    // Scan all logs which will cause burst memtable load.
    for (size_t ii=1001; ii<NUM; ++ii) {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(ii+1, kv_out) );
        TestSuite::sleep_ms(1);
    }

    jungle::DBStats stats;
    db->getStats(stats);
    // Number of open Memtables shouldn't exceed `numKeepingMemtables`.
    // But considering some timing issues, we will allow up to 2x.
    TestSuite::_msg("open memtables: %zu\n", stats.numOpenMemtables);
    CHK_SM(stats.numOpenMemtables, 2 * config.maxKeepingMemtables);

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Do the same thing.
    for (size_t ii=1001; ii<NUM; ++ii) {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(ii+1, kv_out) );
        TestSuite::sleep_ms(1);
    }

    db->getStats(stats);
    TestSuite::_msg("open memtables: %zu\n", stats.numOpenMemtables);
    CHK_SM(stats.numOpenMemtables, 2 * config.maxKeepingMemtables);

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int frequent_sync_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    g_config.flusherSleepDuration_ms = 3600 * 1000;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxKeepingMemtables = 8;
    config.maxEntriesInLogFile = 10;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 3600;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Create 200 log files, and sync everytime.
    const size_t NUM = 2000;
    for (size_t ii=0; ii<NUM/2; ++ii) {
        std::string key_str = TestSuite::lzStr(6, ii);
        std::string val_str = TestSuite::lzStr(6, ii);
        CHK_Z( db->setSN(ii+1, jungle::KV(key_str, val_str)) );
        CHK_Z( db->sync( (ii % 100 == 0) ) );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Check logs.
    for (size_t ii=0; ii<NUM/2; ++ii) {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(ii+1, kv_out) );
    }

    for (size_t ii=NUM/2; ii<NUM; ++ii) {
        std::string key_str = TestSuite::lzStr(6, ii);
        std::string val_str = TestSuite::lzStr(6, ii);
        CHK_Z( db->setSN(ii+1, jungle::KV(key_str, val_str)) );
        CHK_Z( db->sync( (ii % 100 == 0) ) );
    }

    // Truncate half.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), 1000) );

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Check logs.
    for (size_t ii=1001; ii<NUM; ++ii) {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(ii+1, kv_out) );
    }

    for (size_t ii=NUM; ii<NUM*2; ++ii) {
        std::string key_str = TestSuite::lzStr(6, ii);
        std::string val_str = TestSuite::lzStr(6, ii);
        CHK_Z( db->setSN(ii+1, jungle::KV(key_str, val_str)) );
        CHK_Z( db->sync( (ii % 100 == 0) ) );
    }
    CHK_Z( db->sync( true ) );

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    // Check logs.
    for (size_t ii=1001; ii<NUM*2; ++ii) {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(ii+1, kv_out) );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int overwrite_seq_multi_log_files_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.logFileReclaimerSleep_sec = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxLogFileSize = 1024;
    config.logSectionOnly = true;
    config.allowOverwriteSeqNum = true;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    const size_t NUM = 1000;
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(8, ii);
        std::string val_str = "v" + TestSuite::lzStr(16, ii);
        CHK_Z( db->setSN( ii+1, jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    // Even with `allowOverwriteSeqNum` option,
    // there should be no problem with creating new log files.
    jungle::DBStats stats_out;
    CHK_Z( db->getStats(stats_out) );
    CHK_NEQ(-1, stats_out.minLogIndex);
    CHK_NEQ(-1, stats_out.maxLogIndex);
    CHK_GT(stats_out.maxLogIndex, stats_out.minLogIndex);

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace log_reclaim_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("basic log reclaim test",
              basic_log_reclaim_test);

    ts.doTest("multi instances log reclaim test",
              multi_instances_reclaim_test);

    ts.doTest("reload log in reclaim mode test",
              reload_log_in_reclaim_mode_test);

    ts.doTest("async-sync test",
              async_sync_test);

    ts.doTest("crash after adding new log file test",
              crash_after_adding_new_log_file_test);

    ts.doTest("log rollback basic test",
              log_rollback_basic_test,
              TestRange<bool>( {true, false} ));

    ts.doTest("log rollback multi files test",
              log_rollback_multi_files_test);

    ts.doTest("log rollback and reclaim test",
              log_rollback_and_reclaim_test);

    ts.doTest("log rollback with concurrent write test",
              log_rollback_with_concurrent_write_test);

    ts.doTest("urgent reclaim test",
              urgent_reclaim_test);

    ts.doTest("log flush upto zero test",
              log_flush_zero_test);

    ts.doTest("moving to next log without sync test",
              moving_to_next_log_without_sync_test);

    ts.doTest("immediate log purging test",
              immediate_log_purging_test);

    ts.doTest("frequent sync test",
              frequent_sync_test);

    ts.doTest("overwrite seq multi log files test",
              overwrite_seq_multi_log_files_test);

#if 0
    ts.doTest("reload empty files test",
              reload_with_empty_files_test_load);
#endif

    return 0;
}
