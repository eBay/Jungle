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

#include <atomic>
#include <vector>

#include <stdio.h>

namespace basic_robust_child {

int robust_child(size_t dur_sec) {
    std::string filename = "./robust_child";

    jungle::Status s;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    g_config.fdbCacheSize = (uint64_t)1024*1024*1024; // 1GB

    g_config.numCompactorThreads = 1;
    g_config.compactorSleepDuration_ms = 1000; // 1 second
    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    // NOTE: Make compaction happen very frequently.
    config.compactionFactor = 120;
    config.minFileSizeToCompact = 4*1024*1024;
    config.minBlockReuseCycleToCompact = 1;

    jungle::DB* db;
    CHK_Z(jungle::DB::open(&db, filename, config));

    size_t max_num = 100000;
    size_t val_size = 900;
    char val_buf[1024];
    memset(val_buf, 'x', 1024);

    {   // Initial verify
        TestSuite::UnknownProgress upp("verifying");
        upp.update(0);
        jungle::Iterator itr;
        itr.init(db);
        uint64_t ii = 0;
        do {
            jungle::Record rec;
            jungle::Record::Holder h(rec);
            s = itr.get(rec);
            if (!s) break;
            upp.update(ii++);
        } while (itr.next().ok());
        upp.done();
        itr.close();
    }

    TestSuite::Progress pp(dur_sec, "testing");
    TestSuite::Timer tt(dur_sec * 1000);

    TestSuite::WorkloadGenerator wg(30000.0);
    while (!tt.timeover()) {
        if (wg.getNumOpsToDo() == 0) {
            TestSuite::sleep_us(100);
            continue;
        }

        //size_t number = (idx * 7) % max_num;
        size_t number = rand() % max_num;

        std::string key_str = "k" + TestSuite::lzStr(7, number);
        jungle::SizedBuf key(key_str);

        sprintf(val_buf, "%s", TestSuite::getTimeString().c_str());
        jungle::SizedBuf val(val_size, val_buf);

        CHK_Z( db->set(jungle::KV(key, val)) );

        pp.update(tt.getTimeSec());
        wg.addNumOpsDone(1);
    }
    pp.done();

    CHK_Z(jungle::DB::close(db));
    CHK_Z(jungle::shutdown());

    return 0;
}

}; // namespace basic_robust_child
using namespace basic_robust_child;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;
    ts.doTest( "basic robust child", robust_child, TestRange<size_t>({90}) );

    return 0;
}


