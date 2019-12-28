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

#include "config_test_common.h"
#include "libjungle/jungle.h"
#include "test_common.h"

#include <stdio.h>

int many_log_files_test(size_t num_logs) {
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    jungle::GlobalConfig g_config;
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.logSectionOnly = true;
    config.maxEntriesInLogFile = 1000;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t num_kv = config.maxEntriesInLogFile * num_logs;
    TestSuite::Progress pp(num_kv, "insert");
    for (size_t ii=0; ii<num_kv; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);
        std::string val = "v" + TestSuite::lzStr(7, ii);
        CHK_Z( db->set(jungle::KV(key, val)) );
        pp.update(ii);
    }
    pp.update(num_kv);

    TestSuite::Timer tt(5000);
    pp = TestSuite::Progress(num_kv, "retrieve");
    for (size_t ii=0; ii<num_kv; ++ii) {
        std::string key = "k" + TestSuite::lzStr(7, ii);
        jungle::SizedBuf value_out;
        CHK_Z( db->get(jungle::SizedBuf(key), value_out) );
        value_out.free();
        pp.update(ii);
        if (tt.timeover()) break;
    }
    pp.update(num_kv);

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    TestSuite::clearTestFile(prefix, TestSuite::END_OF_TEST);
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.doTest( "many log files test", many_log_files_test, TestRange<size_t>({2048}) );

    return 0;
}

