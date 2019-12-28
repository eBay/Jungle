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
    const static std::string path = "./db_example_iterator";

    // Remove existing DB first.
    TestSuite::clearTestFile(path);

    // Initialize global resources with default config.
    jungle::init(GlobalConfig());

    // Open an instance at the given path, with default config.
    DB* db = nullptr;
    DB::open(&db, path, DBConfig());

    // Set {"foo", "FOO"}
    //     {"bar", "VAR"}
    //     {"baz", "VAZ"} pairs.
    db->set( KV("foo", "FOO") );
    db->set( KV("bar", "BAR") );
    db->set( KV("baz", "BAZ") );

    // Initialize a key-order iterator.
    Iterator key_itr;
    key_itr.init(db);

    // Initialize a seq-order iterator.
    Iterator seq_itr;
    seq_itr.initSN(db);

    // Add one more pair: {"qux", "QUX"}.
    // Since each iterator is a snapshot,
    // this pair will not be visible to both key and seq iterators.
    db->set( KV("qux", "QUX") );

    // Iterate in key order, should be in bar, baz, and foo order.
    do {
        Record rec_out;
        Status s = key_itr.get(rec_out);
        if (!s) break;

        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
        rec_out.free();
    } while (key_itr.next().ok());

    // Close the key-iterator.
    key_itr.close();

    // Iterate in seq order, should be in foo, bar, and baz order.
    do {
        Record rec_out;
        Status s = seq_itr.get(rec_out);
        if (!s) break;

        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
        rec_out.free();
    } while (seq_itr.next().ok());

    // Close the seq-iterator.
    seq_itr.close();

    // Close and free DB instance.
    DB::close(db);

    // Release global resources.
    jungle::shutdown();

    return 0;
}

