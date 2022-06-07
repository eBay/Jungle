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

#include "jungle_builder.h"
#include "libjungle/iterator.h"
#include "libjungle/jungle.h"
#include "libjungle/sized_buf.h"
#include "table_file.h"
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
    jungle::DBConfig db_config;

    jungle::TableMgrOptions t_mgr_opt;
    t_mgr_opt.path = path;
    t_mgr_opt.fOps = &f_ops;
    t_mgr_opt.dbConfig = &db_config;

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

    jungle::DBConfig d_conf;
    CHK_Z( jungle::DB::open(&db, path, d_conf) );

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
    for (size_t ii = 0; ii < db_config.numL0Partitions; ++ii) {
        CHK_Z( db->compactL0(jungle::CompactOptions(), ii) );
    }
    CHK_Z( verify() );

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} using namespace builder_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);
    ts.doTest("build from table files test", build_from_table_files_test);

    return 0;
}