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

#include "db_mgr.h"
#include "fileops_posix.h"
#include "table_file.h"
#include "tools/mutable_table_mgr.h"

#include "libjungle/jungle.h"

#include "test_common.h"

#include <cinttypes>
#include <memory>
#include <vector>

#include <stdio.h>
#include <string.h>

using namespace jungle;

int table_duplicate_seq_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    GlobalConfig g_config;
    DBMgr::init(g_config);

    FileOpsPosix f_ops;
    DBConfig db_config;

    TableMgrOptions t_mgr_opt;
    t_mgr_opt.path = filename;
    t_mgr_opt.fOps = &f_ops;
    t_mgr_opt.dbConfig = &db_config;

    MutableTableMgr t_mgr(nullptr);
    t_mgr.setOpt(t_mgr_opt);

    std::unique_ptr<TableFile> test_file(new TableFile(&t_mgr));
    TableFileOptions t_opt;
    test_file->create(1, 0, filename, &f_ops, t_opt);

    std::list<Record*> aa;
    std::list<uint64_t> bb;

    auto do_set = [&](size_t num, uint64_t seqnum_count) {
        for (size_t ii = 0; ii < num; ++ii) {
            Record rec;
            Record::Holder h_rec(rec);
            std::string key_str = "key" + TestSuite::lzStr(5, ii);
            std::string val_str = "val" + TestSuite::lzStr(5, ii);
            rec.kv.key.alloc(key_str);
            rec.kv.value.alloc(val_str);
            rec.seqNum = seqnum_count++;

            uint64_t offset_out = 0;
            test_file->setSingle(0, rec, offset_out);
        }
    };

    do_set(10, 1);
    test_file->setBatch(aa, bb, SizedBuf(), SizedBuf());

    do_set(10, 1);
    test_file->setBatch(aa, bb, SizedBuf(), SizedBuf());

    // Reopen.
    test_file.reset(new TableFile(&t_mgr));
    test_file->load(1, 0, filename, &f_ops, t_opt);

    // Seq iterator.
    TableFile::Iterator seq_itr;
    CHK_Z(seq_itr.initSN(nullptr, test_file.get(), NOT_INITIALIZED, NOT_INITIALIZED));
    size_t count = 0;
    do {
        Record rec_out;
        Record::Holder h_rec_out(rec_out);
        CHK_Z(seq_itr.get(rec_out));
        count++;
    } while (seq_itr.next().ok());
    CHK_EQ(10, count);
    seq_itr.close();

    test_file.reset();

    DBMgr::destroy();

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.doTest("table duplicate sequence number test", table_duplicate_seq_test);

    return 0;
}
