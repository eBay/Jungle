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
#include "test_common.h"

#include "libjungle/jungle.h"

#include <vector>

#include <stdio.h>

#define DURATION_MS 1000

int log_file_purge_test() {
    jungle::DB* db;
    jungle::Status s;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    // Open DB.
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.maxEntriesInLogFile = 1024;
    s = jungle::DB::open(&db, filename, config);
    CHK_OK(s);

    uint64_t idx = 0;
    TestSuite::Timer timer(DURATION_MS);
    do {
        jungle::KV kv;
        std::string key_str = "k" + TestSuite::lzStr(7, idx);
        std::string val_str = "v" + TestSuite::lzStr(7, idx);
        kv.alloc(key_str, val_str);
        db->setSN(idx, kv);
        kv.free();

        if (idx > 1000 && idx % 1000 == 0) {
            db->sync(false);

            jungle::FlushOptions f_opt;
            f_opt.purgeOnly = true;
            db->flushLogs(f_opt, idx - 1000);
        }
        idx++;
    } while (!timer.timeover() && idx < 1000000);

    // Close DB.
    s = jungle::DB::close(db);
    CHK_OK(s);

    // Free all resources for jungle.
    jungle::shutdown();

    TestSuite::clearTestFile(prefix);
    return 0;
}

/*
int basic_test_template() {
    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    jungle::DB* db;

    const std::string prefix = TEST_SUITE_AUTO_PREFIX;
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    jungle::shutdown();
    TestSuite::clearTestFile(prefix);
    return 0;
}
*/

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    //ts.options.printTestMessage = true;
    ts.doTest("log file purge test", log_file_purge_test);

    return 0;
}
