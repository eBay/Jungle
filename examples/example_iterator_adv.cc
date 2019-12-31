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
    const static std::string path = "./db_example_iterator_adv";

    // Remove existing DB first.
    TestSuite::clearTestFile(path);

    // Initialize global resources with default config.
    jungle::init(GlobalConfig());

    // Open an instance at the given path, with default config.
    DB* db = nullptr;
    DB::open(&db, path, DBConfig());

    // Set {"k00", "v00"}
    //     {"k10", "v10"}
    //     ...
    //     {"k90", "v90"} pairs.
    for (size_t ii=0; ii<9; ++ii) {
        std::string key_str = "k" + std::to_string(ii) + "0";
        std::string val_str = "v" + std::to_string(ii) + "0";
        db->set( KV(key_str, val_str) );
    }

    // Initialize a key-order iterator for the range between
    // k15 and k85.
    Iterator key_itr;
    key_itr.init(db, SizedBuf("k15"), SizedBuf("k85"));

    // Iterate all: since both k15 and k85 do not exist,
    // it will traverse from k20 to k80.
    std::cout << "iterate all:" << std::endl;
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (key_itr.next().ok());

    // Seek k65: default option is GREATER,
    // cursor will point to k70.
    std::cout << "after seek k65:" << std::endl;
    key_itr.seek( SizedBuf("k65") );
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (key_itr.next().ok());

    // Seek k65 with SMALLER option:
    // cursor will point to k60.
    std::cout << "after seek k65 with SMALLER:" << std::endl;
    key_itr.seek( SizedBuf("k65"), Iterator::SMALLER );
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (key_itr.next().ok());

    // Seek k70 with SMALLER option:
    // cursor will point to k70, as k70 exists.
    std::cout << "after seek k70 with SMALLER:" << std::endl;
    key_itr.seek( SizedBuf("k70"), Iterator::SMALLER );
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (key_itr.next().ok());

    // Iterate all using prev: should print k20-k80 in reversed order.
    std::cout << "iterate all using prev:" << std::endl;
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (key_itr.prev().ok());

    // After goto end: cursor is located at the end.
    std::cout << "after gotoEnd:" << std::endl;
    key_itr.gotoEnd();
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (false); // Print the first record only.

    // After goto begin: cursor is located at the beginning.
    std::cout << "after gotoBegin:" << std::endl;
    key_itr.gotoBegin();
    do {
        Record rec_out;
        Record::Holder h(rec_out); // Auto free.
        if (!key_itr.get(rec_out).ok()) break;
        std::cout << rec_out.kv.key.toString() << ", "
                  << rec_out.kv.value.toString() << std::endl;
    } while (false); // Print the first record only.

    // Close the key-iterator.
    key_itr.close();

    // Close and free DB instance.
    // All iterator handles should be closed before closing DB instance.
    DB::close(db);

    // Release global resources.
    jungle::shutdown();

    return 0;
}

