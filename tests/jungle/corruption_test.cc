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

#include <stdio.h>

namespace corruption_test {

static int truncate_file(const std::string& filename,
                          size_t amount) {
    std::ifstream fs_in;
    fs_in.open(filename);
    CHK_OK(fs_in.good());

    fs_in.seekg(0, fs_in.end);
    size_t fs_size = fs_in.tellg();
    fs_in.seekg(0, fs_in.beg);
    char buffer[fs_size];
    fs_in.read(buffer, fs_size);
    fs_in.close();

    std::ofstream fs_out;
    fs_out.open(filename);
    CHK_OK(fs_out.good());
    fs_out.write(buffer, fs_size - amount);
    fs_out.close();
    return 0;
}

static int inject_crc_error(const std::string& filename,
                            size_t offset = 16) {
    std::ifstream fs_in;
    fs_in.open(filename);
    CHK_OK(fs_in.good());

    fs_in.seekg(0, fs_in.end);
    size_t fs_size = fs_in.tellg();
    fs_in.seekg(0, fs_in.beg);
    char buffer[fs_size];
    fs_in.read(buffer, fs_size);
    fs_in.close();

    // Flip.
    buffer[offset] = ~buffer[offset];

    std::ofstream fs_out;
    fs_out.open(filename);
    CHK_OK(fs_out.good());
    fs_out.write(buffer, fs_size);
    fs_out.close();
    return 0;
}

int log_file_truncation_test(size_t amount) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write something.
    size_t num = 101;
    std::vector<jungle::KV> kv(num);
    CHK_Z(_init_kv_pairs(num, kv, "key", "value"));

    for (size_t ii=0; ii<num; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));
    CHK_Z(jungle::DB::close(db));

    // Truncate file.
    CHK_Z(truncate_file(filename + "/log0000_00000000", amount));

    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t corrupted_idx = 0;
    for (size_t ii=0; ii<num; ++ii) {
        jungle::KV kv_out;
        s = db->getSN(ii+1, kv_out);
        if (!s) {
            corrupted_idx = ii;
            break;
        }
        kv_out.free();
    }
    // Corruption should happened.
    CHK_GT(corrupted_idx, 0);
    CHK_SM(corrupted_idx, num);

    // Insert & recover.
    for (size_t ii=corrupted_idx; ii<num; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }

    // Get check.
    for (size_t ii=0; ii<num; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii+1, kv_out));
        CHK_EQ(kv[ii].key, kv_out.key);
        CHK_EQ(kv[ii].value, kv_out.value);
        kv_out.free();
        TestSuite::clearInfo();
    }

    // Close and reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get check.
    for (size_t ii=0; ii<num; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii+1, kv_out));
        CHK_EQ(kv[ii].key, kv_out.key);
        CHK_EQ(kv[ii].value, kv_out.value);
        kv_out.free();
        TestSuite::clearInfo();
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    CHK_Z(_free_kv_pairs(num, kv));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_file_corruption_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write multiple log files.
    size_t NUM = 50;
    std::vector<jungle::KV> kv(NUM);
    CHK_Z(_init_kv_pairs(NUM, kv, "key", "value"));

    for (size_t ii=0; ii<NUM; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));
    CHK_Z(jungle::DB::close(db));

    // Corrupt the second log file.
    CHK_Z(inject_crc_error(filename + "/log0000_00000001", 100));

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Close.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    CHK_Z(_free_kv_pairs(NUM, kv));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_manifest_corruption_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write something.
    size_t num = 6;
    std::vector<jungle::KV> kv(num);
    CHK_Z(_init_kv_pairs(num, kv, "key", "value"));

    for (size_t ii=0; ii<num/2; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));

    for (size_t ii=num/2; ii<num; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));
    jungle::FlushOptions f_opt;
    f_opt.purgeOnly = true;
    CHK_Z(db->flushLogs(f_opt, num/2 - 1));
    CHK_Z(jungle::DB::close(db));

    // Corrupt manifest file.
    CHK_Z(inject_crc_error(filename + "/log0000_manifest"));

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get check.
    for (size_t ii=num/2; ii<num; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii+1, kv_out));
        CHK_EQ(kv[ii].key, kv_out.key);
        CHK_EQ(kv[ii].value, kv_out.value);
        kv_out.free();
        TestSuite::clearInfo();
    }

    // Close.
    CHK_Z(jungle::DB::close(db));

    // Corrupt both manifest file & backup file.
    CHK_Z(inject_crc_error(filename + "/log0000_manifest"));
    CHK_Z(inject_crc_error(filename + "/log0000_manifest.bak"));

    // Should fail.
    s = jungle::DB::open(&db, filename, config);
    CHK_NOT(s);

    CHK_Z(jungle::shutdown());
    CHK_Z(_free_kv_pairs(num, kv));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int log_manifest_corruption_across_file_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write something.
    size_t num = 12;
    size_t kv_num = 100;
    std::vector<jungle::KV> kv(kv_num);
    CHK_Z(_init_kv_pairs(kv_num, kv, "key", "value"));

    for (size_t ii=0; ii<num/2; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));

    // Copy mani file somewhere
    TestSuite::copyfile(filename + "/log0000_manifest",
                        filename + "/log0000_manifest.keep");

    for (size_t ii=num/2; ii<num; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }

    // NOTE: `close` will internally call `sync`.
    CHK_Z(jungle::DB::close(db));

    // Restore as a backup file.
    TestSuite::copyfile(filename + "/log0000_manifest.keep",
                        filename + "/log0000_manifest.bak");

    // Corrupt manifest file.
    CHK_Z(inject_crc_error(filename + "/log0000_manifest"));

    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get last seq num.
    uint64_t last_seqnum;
    CHK_Z(db->getMaxSeqNum(last_seqnum));

    // Get check.
    for (size_t ii=1; ii<=last_seqnum; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii, kv_out));
        kv_out.free();
        TestSuite::clearInfo();
    }

    // Set more, it will overwrite previous log files.
    std::vector<jungle::KV> kv_after(kv_num);
    CHK_Z(_init_kv_pairs(kv_num, kv_after, "key", "value_after_crash"));
    for (size_t ii=last_seqnum+1; ii<=last_seqnum+5; ++ii) {
        CHK_Z(db->setSN(ii, kv_after[ii-1]));
    }

    // Close & reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get check.
    for (size_t ii=1; ii<=last_seqnum+5; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        jungle::KV kv_out;
        CHK_Z(db->getSN(ii, kv_out));
        if (ii <= last_seqnum) {
            CHK_EQ(kv[ii-1].key, kv_out.key);
            CHK_EQ(kv[ii-1].value, kv_out.value);
        } else {
            CHK_EQ(kv_after[ii-1].key, kv_out.key);
            CHK_EQ(kv_after[ii-1].value, kv_out.value);
        }
        kv_out.free();
        TestSuite::clearInfo();
    }
    CHK_Z(jungle::DB::close(db));

    CHK_Z(jungle::shutdown());
    CHK_Z(_free_kv_pairs(kv_num, kv));
    CHK_Z(_free_kv_pairs(kv_num, kv_after));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int incomplete_log_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t sync_point = 25;
    size_t more_insert = 100;

    // Write something.
    for (size_t ii=0; ii<sync_point; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }

    // Sync in the middle.
    CHK_Z( db->sync(false) );

    // Write more.
    for (size_t ii=sync_point; ii<more_insert; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }

    // Copy whole database to other place.
    std::string cmd;
    std::string filename_copy = filename + "_copy";
    cmd = "cp -R " + filename + " " + filename + "_copy";
    int r = ::system(cmd.c_str());
    (void)r;

    // Close original.
    CHK_Z(jungle::DB::close(db));

    // Open copy, should work.
    CHK_Z(jungle::DB::open(&db, filename_copy, config));

    // All synced KV should exist.
    for (size_t ii=0; ii<sync_point; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::SizedBuf value_out;
        CHK_Z( db->get(jungle::SizedBuf(key_str), value_out) );
        CHK_EQ(jungle::SizedBuf(val_str), value_out);
        value_out.free();
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int irrelevant_log_file_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Make an irrelevant log file.
    std::string log_file = filename + "/log0000_00000003";
    std::ofstream fs_out;
    fs_out.open(log_file);
    CHK_OK(fs_out.good());
    fs_out.write("garbage", 7);
    fs_out.close();

    size_t num = 100;

    // Write something beyond that irrelevant file.
    for (size_t ii=0; ii<num; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }

    // Sync.
    CHK_Z( db->sync(false) );

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // All synced KV should exist.
    for (size_t ii=0; ii<num; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::SizedBuf value_out;
        CHK_Z( db->get(jungle::SizedBuf(key_str), value_out) );
        CHK_EQ(jungle::SizedBuf(val_str), value_out);
        value_out.free();
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int not_existing_log_file_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num = 15;

    // Write something beyond that irrelevant file.
    for (size_t ii=0; ii<num; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }

    // Sync.
    CHK_Z( db->sync(false) );

    // Close.
    CHK_Z(jungle::DB::close(db));

    // Remove the 2nd log file.
    std::string log_file = filename + "/log0000_00000001";
    TestSuite::remove(log_file);

    // Re-open.
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Try to retrieve KV.
    size_t succ_count = 0;
    for (size_t ii=0; ii<num; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::SizedBuf value_out;
        s = db->get(jungle::SizedBuf(key_str), value_out);
        if (s) {
            CHK_EQ(jungle::SizedBuf(val_str), value_out);
            value_out.free();
            succ_count++;
        }
    }
    CHK_EQ(config.maxEntriesInLogFile, succ_count);

    // Write lost logs again.
    for (size_t ii=succ_count; ii<num; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }
    // Sync.
    CHK_Z( db->sync(false) );

    // Close and re-open.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Try to retrieve KV.
    for (size_t ii=0; ii<num; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::SizedBuf value_out;
        CHK_Z( db->get(jungle::SizedBuf(key_str), value_out) );
        CHK_EQ(jungle::SizedBuf(val_str), value_out);
        value_out.free();
    }
    CHK_Z(jungle::DB::close(db));

    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int wrong_manifest_test(bool log_section) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = log_section;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t NUM1 = 15;
    size_t NUM2 = 25;
    size_t NUM3 = 95;
    size_t NUM4 = 105;
    size_t NUM5 = 135;

    // Write something beyond that irrelevant file.
    for (size_t ii=0; ii<NUM2; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }

    // Sync.
    CHK_Z( db->sync(false) );
    // Flush.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), NUM1) );

    // Keep manifest file.
    jungle::FileMgr::copy(filename + "/log0000_manifest",
                          filename + "/log0000_manifest.copy");

    for (size_t ii=NUM2; ii<NUM4; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }
    // Sync.
    CHK_Z( db->sync(false) );
    // Flush.
    CHK_Z( db->flushLogs(jungle::FlushOptions(), NUM3) );

    // Wait until pending files are removed.
    TestSuite::sleep_sec(1, "waiting for file purge");

    // Close.
    CHK_Z(jungle::DB::close(db));

    // Restore old manifest file, to mimic crash without sync.
    jungle::FileMgr::copy(filename + "/log0000_manifest.copy",
                          filename + "/log0000_manifest");

    // Re-open and set more.
    CHK_Z(jungle::DB::open(&db, filename, config));
    for (size_t ii=NUM4; ii<NUM5; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }
    // Sync.
    CHK_Z( db->sync(false) );

    if (log_section) {
        for (size_t ii=NUM4; ii<NUM5; ++ii) {
            // Log mode:
            //   All keys after NUM4 should be retrieved.
            TestSuite::setInfo("ii=%zu", ii);

            std::string key_str = "k" + TestSuite::lzStr(6, ii);
            std::string val_str = "v" + TestSuite::lzStr(6, ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get( jungle::SizedBuf(key_str), value_out ) );
            CHK_EQ(val_str, value_out.toString());
        }

    } else {
        for (size_t ii=0; ii<NUM5; ++ii) {
            // Normal mode:
            //   All keys except for [NUM3, NUM4) should be retrieved.
            TestSuite::setInfo("ii=%zu", ii);
            if (NUM3 <= ii && ii < NUM4) continue;

            std::string key_str = "k" + TestSuite::lzStr(6, ii);
            std::string val_str = "v" + TestSuite::lzStr(6, ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get( jungle::SizedBuf(key_str), value_out ) );
            CHK_EQ(val_str, value_out.toString());
        }
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int removed_log_files_at_the_beginning_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.logFileTtl_sec = 60;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write something.
    size_t KV_NUM = 100;
    size_t FLUSH_NUM = 25;
    size_t EXPECTED_MIN_NUM = 41;

    std::vector<jungle::KV> kv(KV_NUM);
    CHK_Z(_init_kv_pairs(KV_NUM, kv, "key", "value"));

    for (size_t ii=0; ii<KV_NUM; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));

    jungle::FlushOptions f_opt;
    f_opt.purgeOnly = true;
    CHK_Z(db->flushLogs(f_opt, FLUSH_NUM));

    CHK_Z(jungle::DB::close(db));

    // Remove the first two log files.
    TestSuite::remove(filename + "/log0000_00000002");
    TestSuite::remove(filename + "/log0000_00000003");

    // Open.
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get first seq num, should not hang.
    uint64_t min_seqnum = 0;
    CHK_Z(db->getMinSeqNum(min_seqnum));
    CHK_EQ(EXPECTED_MIN_NUM, min_seqnum);

    // Get last seq num, should not hang.
    uint64_t last_seqnum = 0;
    CHK_Z(db->getMaxSeqNum(last_seqnum));
    CHK_EQ(KV_NUM, last_seqnum);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    CHK_Z(_free_kv_pairs(KV_NUM, kv));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int removed_log_files_in_the_middle_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DB* db;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.logFileTtl_sec = 60;
    config.maxEntriesInLogFile = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write something.
    size_t KV_NUM = 100;
    size_t FLUSH_NUM = 25;
    size_t EXPECTED_LAST_NUM = 70;

    std::vector<jungle::KV> kv(KV_NUM);
    CHK_Z(_init_kv_pairs(KV_NUM, kv, "key", "value"));

    for (size_t ii=0; ii<KV_NUM; ++ii) {
        CHK_Z(db->setSN(ii+1, kv[ii]));
    }
    CHK_Z(db->sync(false));

    jungle::FlushOptions f_opt;
    f_opt.purgeOnly = true;
    CHK_Z(db->flushLogs(f_opt, FLUSH_NUM));

    CHK_Z(jungle::DB::close(db));

    // Remove two log files in the middle.
    TestSuite::remove(filename + "/log0000_00000007");
    TestSuite::remove(filename + "/log0000_00000008");

    // Open.
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Get first seq num, should not hang.
    uint64_t min_seqnum = 0;
    CHK_Z(db->getMinSeqNum(min_seqnum));
    CHK_EQ(FLUSH_NUM + 1, min_seqnum);

    // Get last seq num, should not hang.
    uint64_t last_seqnum = 0;
    CHK_Z(db->getMaxSeqNum(last_seqnum));
    CHK_EQ(EXPECTED_LAST_NUM, last_seqnum);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());
    CHK_Z(_free_kv_pairs(KV_NUM, kv));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int incomplete_table_set_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);
    std::string filename_copy = filename + "_copy";

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numTableWriters = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    jungle::DebugParams d_params;
    d_params.tableSetBatchCb =
        [filename, filename_copy]
        (const jungle::DebugParams::GenericCbParams& p) {
            static size_t count = 0;
            if (count++ == 0) {
                // Sleep a second to flush log.
                TestSuite::sleep_sec(1);
                // Copy whole database to other place.
                std::string cmd;
                cmd = "cp -R " + filename + " " + filename_copy;
                int r = ::system(cmd.c_str());
                (void)r;
            }
            TestSuite::_msg("flush\n");
        };
    jungle::DB::setDebugParams(d_params);
    jungle::DB::enableDebugCallbacks(true);

    const size_t NUM = 1000;

    // Write something.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set(jungle::KV(key_str, val_str)) );
    }

    // Sync and flush.
    CHK_Z( db->sync(false) );
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

    // Close.
    CHK_Z(jungle::DB::close(db));

    // Open copied one, to mimic a crash in the middle of flush.
    CHK_Z(jungle::DB::open(&db, filename_copy, config));

    // Flush again.
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

    // Check if all records are there.
    for (size_t ii=0; ii<NUM; ++ii) {
        TestSuite::setInfo("ii=%zu", ii);
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h(value_out);
        CHK_Z( db->get(jungle::SizedBuf(key_str), value_out) );
        CHK_EQ(val_str, value_out.toString());
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int duplicate_seq_flush_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numTableWriters = 1;
    jungle::init(g_config);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM1 = 100;
    const size_t NUM2 = 200;

    // Write something.
    for (size_t ii=0; ii<NUM1; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::Record rec;
        rec.kv.key = key_str;
        rec.kv.value = val_str;
        rec.seqNum = ii + 1;
        CHK_Z( db->setRecord(rec) );
    }
    CHK_Z( db->sync(false) );

    // Write more.
    for (size_t ii=NUM1; ii<NUM2; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        jungle::Record rec;
        rec.kv.key = key_str;
        rec.kv.value = val_str;
        rec.seqNum = ii + 1;
        CHK_Z( db->setRecord(rec) );
    }
    CHK_Z( db->sync(false) );

    // Close & reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Write duplicate sequence number with different key.
    for (size_t ii=NUM1; ii<NUM2; ++ii) {
        std::string key_str = "kk" + TestSuite::lzStr(6, ii);
        std::string val_str = "vv" + TestSuite::lzStr(6, ii);
        jungle::Record rec;
        rec.kv.key = key_str;
        rec.kv.value = val_str;
        rec.seqNum = ii + 1;
        CHK_Z( db->setRecord(rec) );
    }
    CHK_Z( db->sync(false) );

    // Flush again.
    CHK_Z( db->flushLogs(jungle::FlushOptions()) );

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace corruption_test;

#include "db_internal.h"
#include "table_manifest.h"
#include "table_mgr.h"
#include <libjungle/jungle.h>

// To access the internal structure of DB.
namespace jungle {
namespace checker {
class Checker {
public:

static int compaction_empty_table_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::DB* db;
    jungle::Status s;

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.purgeDeletedDocImmediately = false;
    config.fastIndexScan = true;
    config.minFileSizeToCompact = 1024;
    config.bloomFilterBitsPerUnit = 10;
    CHK_Z(jungle::DB::open(&db, filename, config));

    // Inserting NUM_ENTRIES no of entries
    const size_t NUM_DIGITS = 6;
    const size_t NUM_ENTRIES = 10000;
    for (size_t ii = 0; ii < NUM_ENTRIES; ++ii) {
        jungle::Record rec;
        std::string key_str = "key" + TestSuite::lzStr(NUM_DIGITS, ii);
        std::string meta_str = "meta" + TestSuite::lzStr(NUM_DIGITS, ii);
        std::string value_str = "value" + TestSuite::lzStr(NUM_DIGITS, ii);
        rec.kv.key = jungle::SizedBuf(key_str);
        rec.kv.value = jungle::SizedBuf(value_str);
        rec.meta = jungle::SizedBuf(meta_str);
        CHK_Z(db->setRecordByKey(rec));
    }

    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < 4; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    // Getting the information on the last table in level 1
    std::list<jungle::TableInfo*> tables_out;
    db->p->tableMgr->mani->getTablesRange(
        1, jungle::SizedBuf(), jungle::SizedBuf(), tables_out);
    size_t table_size_before_deletion = tables_out.size();
    jungle::TableInfo* t_last = nullptr;
    auto itr = tables_out.begin();
    for (size_t ii = 0; ii < tables_out.size(); ++ii) {

        if (ii == tables_out.size() - 1) {
            t_last = *itr;
        }
        itr++;
    }
    TestSuite::Msg test_msg;
    test_msg << "No of tables: " << table_size_before_deletion
                << "\n Last table minKey " << t_last->minKey.toReadableString() << "\n";

    CHK_TRUE(!t_last->file->isEmpty());

    for (TableInfo* ii: tables_out) {
        ii->done();
    }

    size_t t_last_idx =
        (size_t)std::atoi(t_last->minKey.toString().substr(3).c_str());
    std::string key_t_last_idx = "key" + TestSuite::lzStr(NUM_DIGITS, t_last_idx);

    auto verify_func = [&](bool after_deletion) -> int {
        for (size_t ii = 0; ii < NUM_ENTRIES; ++ii) {
            TestSuite::setInfo("ii == %zu", ii);
            jungle::Record rec;
            std::string key_str = "key" + TestSuite::lzStr(NUM_DIGITS, ii);
            std::string meta_str = "meta" + TestSuite::lzStr(NUM_DIGITS, ii);
            std::string value_str = "value" + TestSuite::lzStr(NUM_DIGITS, ii);
            jungle::Record rec_out;
            jungle::Record::Holder h_rec_out(rec_out);
            s = db->getRecordByKey(jungle::SizedBuf(key_str), rec_out);
            if (after_deletion && ii >= t_last_idx) {
                CHK_GT(0, s);
            } else {
                CHK_Z(s);
            }
        }
        return 0;
    };
    CHK_Z(verify_func(false));

    // Delete all the entries in last table file of Level 1
    for (size_t ii = t_last_idx; ii < NUM_ENTRIES; ii++) {
        jungle::Record rec;
        std::string key_str = "key" + TestSuite::lzStr(NUM_DIGITS, ii);
        CHK_Z(db->del(jungle::SizedBuf(key_str)));
    }
    CHK_Z(verify_func(true));

    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());
    for (size_t ii = 0; ii < 4; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }
    CHK_Z(verify_func(true));

    for (size_t ii = 0; ii < 8; ++ii) {
        TestSuite::setInfo("ii == %zu", ii);
        CHK_Z(db->compactInplace(jungle::CompactOptions(), 1));
    }
    CHK_Z(verify_func(true));

    std::list<jungle::TableInfo*> tables_out_del;
    db->p->tableMgr->mani->getTablesRange(
        1, jungle::SizedBuf(), jungle::SizedBuf(), tables_out_del);
    size_t table_size_after_deletion = tables_out_del.size();
    jungle::TableInfo* t_last_del = nullptr;
    auto itr_del = tables_out_del.begin();
    for (size_t ii = 0; ii < tables_out_del.size(); ++ii) {

        if (ii == tables_out_del.size() - 1) {
            t_last_del = *itr_del;
        }
        itr_del++;
    }
    // After in place compaction the last table in level 1 should be empty

    CHK_EQ(table_size_after_deletion, table_size_before_deletion);
    CHK_TRUE(t_last_del->file->isEmpty());

    for (TableInfo* ii: tables_out_del) {
        ii->done();
    }
    // Now enter some entries where the key of them are greater than minKey of the
    // empty table
    for (size_t ii = NUM_ENTRIES; ii < NUM_ENTRIES + 10; ++ii) {
        jungle::Record rec;
        std::string key_str = "key" + TestSuite::lzStr(NUM_DIGITS, ii);
        std::string meta_str = "meta" + TestSuite::lzStr(NUM_DIGITS, ii);
        std::string value_str = "value" + TestSuite::lzStr(NUM_DIGITS, ii);
        rec.kv.key = jungle::SizedBuf(key_str);
        rec.kv.value = jungle::SizedBuf(value_str);
        rec.meta = jungle::SizedBuf(meta_str);
        CHK_Z(db->setRecordByKey(rec));
    }

    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs());

    // Now compact the L0 to L1 should try to compact the empty table
    for (size_t ii = 0; ii < 4; ++ii) {
        CHK_Z(db->compactL0(jungle::CompactOptions(), ii));
    }

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

static int corrupted_table_manifest_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;

    jungle::GlobalConfig g_conf;
    g_conf.compactorSleepDuration_ms = 500;
    jungle::init(g_conf);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.minFileSizeToCompact = 65536;
    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    const size_t NUM = 10000;

    // Write something.
    for (size_t ii=0; ii<NUM; ++ii) {
        std::string k_str = "k" + TestSuite::lzStr(6, ii * 10);
        std::string m_str = "m" + TestSuite::lzStr(6, ii * 10);
        std::string v_str = "v" + TestSuite::lzStr(6, ii * 10);
        Record rec;
        rec.kv.key = k_str;
        rec.kv.value = v_str;
        rec.meta = m_str;
        CHK_Z( db->setRecordByKey(rec) );
    }
    CHK_Z( db->sync(false) );
    CHK_Z( db->flushLogs() );

    for (size_t ii = 0; ii < 4; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }

    std::list<TableInfo*> tables_out;
    db->p->tableMgr->mani->getTablesRange(1, SizedBuf(), SizedBuf(), tables_out);

    TableInfo* t1 = nullptr;
    TableInfo* t2 = nullptr;
    auto itr = tables_out.begin();
    for (size_t ii = 0; ii < 3; ++ii) {
        if (ii == 1) {
            t1 = *itr;
        }
        if (ii == 2) {
            t2 = *itr;
        }
        itr++;
    }
    int t1_idx = std::atoi(t2->minKey.toString().substr(1).c_str()) + 1;
    int t2_idx = std::atoi(t2->minKey.toString().substr(1).c_str());
    int t3_idx = std::atoi(t2->minKey.toString().substr(1).c_str()) - 1;

    {    // Insert a record whose key is bigger than t2's min key.
        uint64_t offset_out;
        Record rec;
        std::string k_str = "k" + TestSuite::lzStr(6, t1_idx);
        std::string m_str = "m" + TestSuite::lzStr(6, t1_idx);
        std::string v_str = "v" + TestSuite::lzStr(6, t1_idx);
        rec.kv = KV(k_str, v_str);
        rec.meta = m_str;
        rec.seqNum = NUM * 2;
        uint32_t key_hash_val = get_murmur_hash_32(rec.kv.key);;
        CHK_Z( t1->file->setSingle(key_hash_val, rec, offset_out, false, true) );
    }

    {    // Insert a record with existing key but higher seq number.
        uint64_t offset_out;
        Record rec;
        std::string k_str = "k" + TestSuite::lzStr(6, t2_idx);
        std::string m_str = "M" + TestSuite::lzStr(6, t2_idx);
        std::string v_str = "V" + TestSuite::lzStr(6, t2_idx);
        rec.kv = KV(k_str, v_str);
        rec.meta = m_str;
        rec.seqNum = NUM * 2 + 1;
        uint32_t key_hash_val = get_murmur_hash_32(rec.kv.key);
        CHK_Z( t1->file->setSingle(key_hash_val, rec, offset_out, false, true) );
    }

    {    // Insert a record whose key is smaller than t2's min key.
        uint64_t offset_out;
        Record rec;
        std::string k_str = "k" + TestSuite::lzStr(6, t3_idx);
        std::string m_str = "m" + TestSuite::lzStr(6, t3_idx);
        std::string v_str = "v" + TestSuite::lzStr(6, t3_idx);
        rec.kv = KV(k_str, v_str);
        rec.meta = m_str;
        rec.seqNum = NUM * 2 + 2;
        uint32_t key_hash_val = get_murmur_hash_32(rec.kv.key);
        CHK_Z( t2->file->setSingle(key_hash_val, rec, offset_out, false, true) );
    }

    // Commit.
    std::list<Record*> rr;
    std::list<uint64_t> cc;
    SizedBuf empty_key;
    CHK_Z( t1->file->setBatch(rr, cc, empty_key, empty_key, -1, false) );
    CHK_Z( t2->file->setBatch(rr, cc, empty_key, empty_key, -1, false) );

    for (TableInfo* ii: tables_out) {
        ii->done();
    }

    // Close and reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));

    auto verify_func = [&]() -> int {
        // 3 keys should be found.
        {
            std::string k_str = "k" + TestSuite::lzStr(6, t1_idx);
            std::string m_str = "m" + TestSuite::lzStr(6, t1_idx);
            std::string v_str = "v" + TestSuite::lzStr(6, t1_idx);
            Record rec_out;
            Record::Holder h(rec_out);
            CHK_Z( db->getRecordByKey(SizedBuf(k_str), rec_out) );
            CHK_EQ(v_str, rec_out.kv.value.toString());
            CHK_EQ(m_str, rec_out.meta.toString());
        }
        {
            std::string k_str = "k" + TestSuite::lzStr(6, t2_idx);
            std::string m_str = "M" + TestSuite::lzStr(6, t2_idx);
            std::string v_str = "V" + TestSuite::lzStr(6, t2_idx);
            Record rec_out;
            Record::Holder h(rec_out);
            CHK_Z( db->getRecordByKey(SizedBuf(k_str), rec_out) );
            CHK_EQ(v_str, rec_out.kv.value.toString());
            CHK_EQ(m_str, rec_out.meta.toString());
        }
        {
            std::string k_str = "k" + TestSuite::lzStr(6, t3_idx);
            std::string m_str = "m" + TestSuite::lzStr(6, t3_idx);
            std::string v_str = "v" + TestSuite::lzStr(6, t3_idx);
            Record rec_out;
            Record::Holder h(rec_out);
            CHK_Z( db->getRecordByKey(SizedBuf(k_str), rec_out) );
            CHK_EQ(v_str, rec_out.kv.value.toString());
            CHK_EQ(m_str, rec_out.meta.toString());
        }

        // Should be found by iterator as well.
        {
            Iterator itr;
            std::string k_str = "k" + TestSuite::lzStr(6, t3_idx);
            itr.init(db, SizedBuf{k_str});

            Record rec_out;
            CHK_Z( itr.get(rec_out) );
            CHK_EQ( SizedBuf(k_str), rec_out.kv.key );
            std::string exp_v = "v" + TestSuite::lzStr(6, t3_idx);
            std::string exp_m = "m" + TestSuite::lzStr(6, t3_idx);
            CHK_EQ( exp_v, rec_out.kv.value.toString() );
            CHK_EQ( exp_m, rec_out.meta.toString() );
            rec_out.free();

            CHK_Z( itr.next() );
            CHK_Z( itr.get(rec_out) );
            std::string exp_k = "k" + TestSuite::lzStr(6, t2_idx);
            exp_v = "V" + TestSuite::lzStr(6, t2_idx);
            exp_m = "M" + TestSuite::lzStr(6, t2_idx);
            CHK_EQ( SizedBuf(exp_k), rec_out.kv.key );
            CHK_EQ( exp_v, rec_out.kv.value.toString() );
            CHK_EQ( exp_m, rec_out.meta.toString() );
            rec_out.free();

            CHK_Z( itr.next() );
            CHK_Z( itr.get(rec_out) );
            exp_k = "k" + TestSuite::lzStr(6, t1_idx);
            exp_v = "v" + TestSuite::lzStr(6, t1_idx);
            exp_m = "m" + TestSuite::lzStr(6, t1_idx);
            CHK_EQ( exp_k, rec_out.kv.key.toString() );
            CHK_EQ( exp_v, rec_out.kv.value.toString() );
            CHK_EQ( exp_m, rec_out.meta.toString() );
            rec_out.free();

            itr.close();
        }
        return 0;
    };
    CHK_Z(verify_func());

    TestSuite::sleep_sec(1);

    // Close and reopen.
    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::DB::open(&db, filename, config));
    CHK_Z(verify_func());

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}
};
} // namespace checker
} // namespace jungle

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("log file truncation test",
              log_file_truncation_test,
              TestRange<size_t>({100, 127, 60}));

    ts.doTest("log file corruption test",
              log_file_corruption_test);

    ts.doTest("log manifest corruption test",
              log_manifest_corruption_test);

    ts.doTest("log manifest corruption across multi log files test",
              log_manifest_corruption_across_file_test);

    ts.doTest("incomplete log test",
              incomplete_log_test);

    ts.doTest("irrelevant log file test",
              irrelevant_log_file_test);

    ts.doTest("not existing log file test",
              not_existing_log_file_test);

    ts.doTest("wrong manifest test",
              wrong_manifest_test,
              TestRange<bool>({true, false}));

    ts.doTest("removed log files at the beginning test",
              removed_log_files_at_the_beginning_test);

    ts.doTest("removed log files in the middle test",
              removed_log_files_in_the_middle_test);

    ts.doTest("incomplete table set test",
              incomplete_table_set_test);

    ts.doTest("duplicate seq number test",
              duplicate_seq_flush_test);

    ts.doTest("corrupted table manifest test",
              jungle::checker::Checker::corrupted_table_manifest_test);

    ts.doTest("compaction for empty table test",
              jungle::checker::Checker::compaction_empty_table_test);

    return 0;
}
