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

#include <libjungle/jungle.h>

// For removing directory purpose, not needed in practice.
#include "test_common.h"

using namespace jungle;

int main() {
    const static std::string path = "./db_example_log_store_mode";

    // Remove existing DB first.
    TestSuite::clearTestFile(path);

    // Initialize global resources with default config.
    jungle::init(GlobalConfig());

    // Open an instance at the given path, with log store mode.
    DB* db = nullptr;
    DBConfig db_config;
    db_config.logSectionOnly = true;
    DB::open(&db, path, db_config);

    // Write logs with sequence numbers from 1 to 10.
    for (size_t ii=1; ii<=10; ++ii) {
        Record rec;
        rec.seqNum = ii;
        rec.kv.value = SizedBuf( "log" + std::to_string(ii) );
        db->setRecord(rec);
    }

    // Flush in-memory data to actual file
    // (`true`: calling `fsync`).
    db->sync(true);

    // Read logs from 3 to max.
    std::cout << "logs from 3 to max:" << std::endl;
    Iterator itr;
    itr.initSN(db, 3);
    do {
        Record rec_out;
        Record::Holder h(rec_out); // auto free.
        itr.get(rec_out);
        std::cout << rec_out.seqNum << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (itr.next().ok());
    itr.close();

    // Log compaction up to 5 (inclusive), logs from 1 to 5 will be discarded.
    db->flushLogs(FlushOptions(), 5);

    // Read logs from min to max, it will print logs from 6 to 10.
    std::cout << "after compaction up to 5:" << std::endl;
    Iterator itr2;
    itr2.initSN(db);
    do {
        Record rec_out;
        Record::Holder h(rec_out); // auto free.
        itr2.get(rec_out);
        std::cout << rec_out.seqNum << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (itr2.next().ok());
    itr2.close();

    // Close DB instance.
    DB::close(db);

    // Release global resources.
    jungle::shutdown();

    return 0;
}

