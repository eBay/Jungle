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
    const static std::string path = "./db_example_snapshot";

    // Remove existing DB first.
    TestSuite::clearTestFile(path);

    // Initialize global resources with default config.
    jungle::init(GlobalConfig());

    // Open an instance at the given path, with default config.
    DB* db = nullptr;
    DB::open(&db, path, DBConfig());

    // Set {"hello", "world"} pair.
    db->set( KV("hello", "world") );

    // Checkpoint - create a persistent snapshot.
    uint64_t checkpoint_marker1 = 0;
    db->checkpoint(checkpoint_marker1);

    // Update - set {"hello", "WORLD"} pair.
    db->set( KV("hello", "WORLD") );

    // Another checkpoint.
    uint64_t checkpoint_marker2 = 0;
    db->checkpoint(checkpoint_marker2);

    // Update again - set {"hello", "WoRlD"} pair.
    db->set( KV("hello", "WoRlD") );

    // Close and reopen DB.
    DB::close(db);
    DB::open(&db, path, DBConfig());

    // Search key "hello" in the latest DB instance,
    // should return the latest value "WoRlD".
    SizedBuf value_out;
    Status s;
    s = db->get( SizedBuf("hello"), value_out );
    std::cout << "latest: return code " << s << ", "
              << value_out.toString() << std::endl;
    // Should free the memory of value after use.
    value_out.free();

    // Get the current available checkpoint list.
    std::list<uint64_t> checkpoints;
    db->getCheckpoints(checkpoints);
    std::cout << "available checkpoint markers: ";
    for (auto& entry: checkpoints) std::cout << entry << " ";
    std::cout << std::endl;

    // Open a snapshot with the first checkpoint marker.
    DB* snapshot1 = nullptr;
    db->openSnapshot(&snapshot1, checkpoint_marker1);

    // Search key "hello" in the first snapshot, should return "world".
    s = snapshot1->get( SizedBuf("hello"), value_out );
    std::cout << "snapshot1: return code " << s << ", "
              << value_out.toString() << std::endl;
    // Should free the memory of value after use.
    value_out.free();

    // Open a snapshot with the second checkpoint marker.
    DB* snapshot2 = nullptr;
    db->openSnapshot(&snapshot2, checkpoint_marker2);

    // Search key "hello" in the second snapshot, should return "WORLD".
    s = snapshot2->get( SizedBuf("hello"), value_out );
    std::cout << "snapshot2: return code " << s << ", "
              << value_out.toString() << std::endl;
    // Should free the memory of value after use.
    value_out.free();

    // Should close snapshot handles first,
    // before closing the parent DB handle.
    DB::close(snapshot1);
    DB::close(snapshot2);
    DB::close(db);

    // Release global resources.
    jungle::shutdown();

    return 0;
}

