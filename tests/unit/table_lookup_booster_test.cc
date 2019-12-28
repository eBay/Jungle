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

#include "test_common.h"

#include "libjungle/jungle.h"
#include "table_lookup_booster.h"

#include <cinttypes>
#include <vector>

#include <stdio.h>
#include <string.h>

using namespace jungle;

namespace booster_test {

using TElem = TableLookupBooster::Elem;

int basic_get_set_test() {
    size_t NUM = 1000;
    TableLookupBooster tlb(NUM, nullptr, nullptr);
    std::hash<size_t> h_func;

    for (size_t ii=1; ii<=NUM; ++ii) {
        CHK_Z( tlb.setIfNew( TElem(h_func(ii), ii*2, ii*10) ) );
    }
    CHK_EQ(NUM, tlb.size());

    for (size_t ii=1; ii<=NUM; ++ii) {
        uint64_t offset_out = 0;
        CHK_Z( tlb.get( h_func(ii), offset_out ) );
        CHK_EQ( ii*10, offset_out );
    }

    return 0;
}

int set_newer_test() {
    size_t NUM = 1000;
    TableLookupBooster tlb(NUM, nullptr, nullptr);
    std::hash<size_t> h_func;

    // Initial.
    for (size_t ii=1; ii<=NUM; ++ii) {
        CHK_Z( tlb.setIfNew( TElem(h_func(ii), ii*2, ii*10) ) );
    }
    CHK_EQ(NUM, tlb.size());

    // Set newer.
    for (size_t ii=1; ii<=NUM; ++ii) {
        CHK_Z( tlb.setIfNew( TElem(h_func(ii), ii*3, ii*20) ) );
    }
    CHK_EQ(NUM, tlb.size());

    // Set older.
    for (size_t ii=1; ii<=NUM; ++ii) {
        CHK_FALSE( tlb.setIfNew( TElem(h_func(ii), ii, ii*30) ) );
    }
    CHK_EQ(NUM, tlb.size());

    for (size_t ii=1; ii<=NUM; ++ii) {
        uint64_t offset_out = 0;
        CHK_Z( tlb.get( h_func(ii), offset_out ) );
        CHK_EQ( ii*20, offset_out );
    }

    return 0;
}

int eviction_test() {
    size_t NUM = 10;
    TableLookupBooster tlb(NUM, nullptr, nullptr);
    std::hash<size_t> h_func;

    // Initial.
    for (size_t ii = 1; ii <= NUM * 100; ++ii) {
        CHK_Z( tlb.setIfNew( TElem(h_func(ii), ii*2, ii*10) ) );
    }
    CHK_EQ(NUM, tlb.size());

    size_t count = 0;
    for (size_t ii = 1; ii <= NUM * 100; ++ii) {
        uint64_t offset_out = 0;
        Status s;
        s = tlb.get( h_func(ii), offset_out );
        if (s) {
            count++;
        }
    }
    CHK_EQ(NUM, count);

    for (size_t ii = NUM * 99 + 1; ii <= NUM * 99 + 3; ++ii) {
        uint64_t offset_out = 0;
        CHK_Z( tlb.get( h_func(ii), offset_out ) );
    }
    for (size_t ii = 1; ii <= 3; ++ii) {
        CHK_Z( tlb.setIfNew( TElem(h_func(ii), ii*3, ii*20) ) );
    }
    CHK_EQ(NUM, tlb.size());

    count = 0;
    for (size_t ii = 1; ii <= NUM * 100; ++ii) {
        uint64_t offset_out = 0;
        Status s;
        s = tlb.get( h_func(ii), offset_out );
        if (s) {
            count++;
        }
    }
    CHK_EQ(NUM, count);

    return 0;
}

}; // namespace booster_test;
using namespace booster_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.doTest("basic get set test", basic_get_set_test);
    ts.doTest("set newer test", set_newer_test);
    ts.doTest("eviction test", eviction_test);

    return 0;
}
