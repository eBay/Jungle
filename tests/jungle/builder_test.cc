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

#include "dummy_compression.h"
#include "fileops_posix.h"
#include "mutable_table_mgr.h"
#include "table_file.h"
#include "table_mgr.h"

#include <libjungle/jungle_builder.h>

#include <cstdio>

namespace builder_test {

static inline void dummy_fdb_log_cb(int level,
                                    int ec,
                                    const char* file,
                                    const char* func,
                                    size_t line,
                                    const char* err_msg,
                                    void* ctx)
{
    // Do nothing.
}

int build_from_table_files_test() {
    std::string path;
    TEST_SUITE_PREPARE_PATH(path);

    TestSuite::mkdir(path);

    fdb_set_log_callback_ex_global(dummy_fdb_log_cb, nullptr);

    // Manually create 4 table files and put key value pairs.
    jungle::FileOpsPosix f_ops;
    jungle::DBConfig d_conf;
    d_conf.maxEntriesInLogFile = 20;

    jungle::TableMgrOptions t_mgr_opt;
    t_mgr_opt.path = path;
    t_mgr_opt.fOps = &f_ops;
    t_mgr_opt.dbConfig = &d_conf;

    jungle::MutableTableMgr t_mgr(nullptr);
    t_mgr.setOpt(t_mgr_opt);

    uint64_t seqnum_cnt = 0;
    char key_buf[256];
    char val_buf[256];
    for (size_t ii = 0; ii < 4; ++ii) {
        jungle::TableFile t_file(&t_mgr);
        std::string t_filename = jungle::TableFile::getTableFileName(path, 0, ii);

        jungle::TableFileOptions t_opt;
        t_file.create(1, ii, t_filename, &f_ops, t_opt);

        std::list<jungle::Record*> batch;

        for (size_t jj = 0; jj < 100; ++jj) {
            size_t idx = ii * 100 + jj;
            sprintf(key_buf, "key%05zu", idx);
            sprintf(val_buf, "val%05zu", idx);

            jungle::Record* rec = new jungle::Record();;
            jungle::SizedBuf(key_buf).copyTo(rec->kv.key);
            jungle::SizedBuf(val_buf).copyTo(rec->kv.value);
            rec->seqNum = ++seqnum_cnt;
            batch.push_back(rec);
        }

        std::list<uint64_t> dummy_chk;
        jungle::SizedBuf empty_key;
        CHK_Z( t_file.setBatch( batch, dummy_chk, empty_key, empty_key,
                                _SCU32(-1), false ) );

        for (jungle::Record* rr: batch) {
            rr->free();
            delete rr;
        }
    }

    // Build from table files.
    jungle::builder::Builder::BuildParams params;
    params.path = path;
    params.dbConfig = d_conf;
    for (size_t ii = 0; ii < 4; ++ii) {
        jungle::builder::Builder::BuildParams::TableData td;
        td.tableNumber = ii;
        sprintf(key_buf, "key%05zu", ii * 100);
        td.minKey = std::string(key_buf);
        td.maxSeqnum = (ii + 1) * 100;
        params.tables.push_back(std::move(td));
    }
    CHK_Z( jungle::builder::Builder::buildFromTableFiles(params) );

    // Open it via Jungle API.
    jungle::DB* db;
    jungle::Status s;
    CHK_Z( jungle::DB::open(&db, path, d_conf) );

    // Get max seqnum should work.
    uint64_t max_seq_out = 0;
    CHK_Z( db->getMaxSeqNum(max_seq_out) );
    CHK_EQ( 400, max_seq_out );

    // Put a few key-value pairs.
    for (size_t ii = 400; ii < 500; ++ii) {
        sprintf(key_buf, "key%05zu", ii);
        sprintf(val_buf, "val%05zu", ii);
        CHK_Z( db->set(jungle::KV(key_buf, val_buf)) );
    }

    auto verify = [&]() -> int {
        // Search by key point.
        for (size_t ii = 0; ii < 500; ++ii) {
            TestSuite::setInfo("ii = %zu", ii);
            sprintf(key_buf, "key%05zu", ii);
            sprintf(val_buf, "val%05zu", ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get(jungle::SizedBuf(key_buf), value_out) );
            CHK_EQ( jungle::SizedBuf(val_buf), value_out );
        }

        // Search by key and seqnum by iterator
        for (auto by_key: {true, false}) {
            jungle::Iterator itr;
            if (by_key) {
                itr.init(db);
            } else {
                itr.initSN(db);
            }

            size_t ii = 0;
            do {
                jungle::Record rec_out;
                jungle::Record::Holder h(rec_out);
                s = itr.get(rec_out);
                if (!s.ok()) break;

                sprintf(key_buf, "key%05zu", ii);
                sprintf(val_buf, "val%05zu", ii);
                CHK_EQ( jungle::SizedBuf(key_buf), rec_out.kv.key );
                CHK_EQ( jungle::SizedBuf(val_buf), rec_out.kv.value );
                CHK_EQ( ii + 1, rec_out.seqNum );
                ii++;
            } while (itr.next().ok());
            itr.close();
            CHK_EQ(500, ii);
        }
        return 0;
    };
    // Verify at log level.
    db->sync(false);
    CHK_Z( verify() );

    // Flush into L0 and verify.
    db->flushLogs();
    CHK_Z( verify() );

    // Merge into L1 and verify.
    for (size_t ii = 0; ii < d_conf.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }
    CHK_Z( verify() );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int build_an_empty_db_test() {
    std::string path;
    TEST_SUITE_PREPARE_PATH(path);

    TestSuite::mkdir(path);

    fdb_set_log_callback_ex_global(dummy_fdb_log_cb, nullptr);

    jungle::DBConfig d_conf;

    // Build from table files.
    jungle::builder::Builder::BuildParams params;
    params.path = path;
    CHK_Z( jungle::builder::Builder::buildFromTableFiles(params) );

    // Open it via Jungle API.
    jungle::DB* db;
    jungle::Status s;

    CHK_Z( jungle::DB::open(&db, path, d_conf) );

    // Put a few key-value pairs.
    char key_buf[256];
    char val_buf[256];
    for (size_t ii = 0; ii < 100; ++ii) {
        sprintf(key_buf, "key%05zu", ii);
        sprintf(val_buf, "val%05zu", ii);
        CHK_Z( db->set(jungle::KV(key_buf, val_buf)) );
    }

    auto verify = [&]() -> int {
        // Search by key point.
        for (size_t ii = 0; ii < 100; ++ii) {
            TestSuite::setInfo("ii = %zu", ii);
            sprintf(key_buf, "key%05zu", ii);
            sprintf(val_buf, "val%05zu", ii);
            jungle::SizedBuf value_out;
            jungle::SizedBuf::Holder h(value_out);
            CHK_Z( db->get(jungle::SizedBuf(key_buf), value_out) );
            CHK_EQ( jungle::SizedBuf(val_buf), value_out );
        }

        // Search by key and seqnum by iterator
        for (auto by_key: {true, false}) {
            jungle::Iterator itr;
            if (by_key) {
                itr.init(db);
            } else {
                itr.initSN(db);
            }

            size_t ii = 0;
            do {
                jungle::Record rec_out;
                jungle::Record::Holder h(rec_out);
                s = itr.get(rec_out);
                if (!s.ok()) break;

                sprintf(key_buf, "key%05zu", ii);
                sprintf(val_buf, "val%05zu", ii);
                CHK_EQ( jungle::SizedBuf(key_buf), rec_out.kv.key );
                CHK_EQ( jungle::SizedBuf(val_buf), rec_out.kv.value );
                CHK_EQ( ii + 1, rec_out.seqNum );
                ii++;
            } while (itr.next().ok());
            itr.close();
            CHK_EQ(100, ii);
        }
        return 0;
    };
    // Verify at log level.
    db->sync(false);
    CHK_Z( verify() );

    // Flush into L0 and verify.
    db->flushLogs();
    CHK_Z( verify() );

    // Merge into L1 and verify.
    for (size_t ii = 0; ii < d_conf.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }
    CHK_Z( verify() );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int builder_api_test(bool compression) {
    std::string path;
    TEST_SUITE_PREPARE_PATH(path);

    TestSuite::mkdir(path);

    jungle::builder::Builder bb;
    jungle::DBConfig d_conf;
    d_conf.maxL1TableSize = 256 * 1024;

    if (compression) {
        d_conf.compOpt.cbGetMaxSize = dummy_get_max_size;
        d_conf.compOpt.cbCompress = dummy_compress;
        d_conf.compOpt.cbDecompress = dummy_decompress;
    }

    CHK_Z( bb.init(path, d_conf) );

    char key_buf[256];
    jungle::SizedBuf val_buf;
    jungle::SizedBuf::Holder h_val_buf(val_buf);
    val_buf.alloc(1024);
    memset(val_buf.data, 'x', 1023);
    for (size_t ii = 0; ii < 1023; ii += 3) {
        memset(val_buf.data + ii, 'a' + (ii % ('z' - 'a')), 3);
    }
    val_buf.data[1023] = 0;

    const size_t NUM = 2048;
    for (size_t ii = 0; ii < NUM; ++ii) {
        sprintf(key_buf, "key%05zu", ii);
        sprintf((char*)val_buf.data, "val%05zu", ii);

        jungle::Record rec;
        rec.kv.key = key_buf;
        rec.kv.value = val_buf;

        CHK_Z( bb.set(rec) );
    }

    CHK_Z( bb.commit() );
    CHK_Z( bb.close() );

    jungle::DB* db = nullptr;
    CHK_Z( jungle::DB::open(&db, path, d_conf) );

    for (size_t ii = 0; ii < NUM; ++ii) {
        sprintf(key_buf, "key%05zu", ii);
        sprintf((char*)val_buf.data, "val%05zu", ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get(jungle::SizedBuf(strlen(key_buf), key_buf), value_out) );

        CHK_EQ(val_buf.toString(), value_out.toString());
    }

    // Iteration from the middle of sequence number should work.
    jungle::Status s;
    jungle::Iterator itr;
    CHK_Z( itr.initSN(db, NUM / 2) );
    uint64_t seqnum_cnt = NUM / 2;
    do {
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        s = itr.get(rec_out);
        if (!s.ok()) break;

        CHK_EQ(seqnum_cnt, rec_out.seqNum);
        seqnum_cnt++;
    } while (itr.next().ok());
    CHK_Z( itr.close() );
    CHK_EQ(NUM + 1, seqnum_cnt);

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace builder_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    ts.doTest("build from table files test", build_from_table_files_test);
    ts.doTest("build an empty db test", build_an_empty_db_test);
    ts.doTest("builder api test", builder_api_test, TestRange<bool>({false, true}));

    return 0;
}