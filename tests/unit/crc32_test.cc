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

#include "crc32.h"

namespace crc32_test {

int helloworld_crc_test() {
    std::string tmp = "helloworld";
    uint32_t crc_val = crc32_8(tmp.data(), tmp.size(), 0);
    TestSuite::_msg("%zx\n", crc_val);

    // CRC32 of `helloworld` is known as follows:
    CHK_EQ( 0xf9eb20ad, crc_val );

    std::string tmp2 = "hello";
    std::string tmp3 = "world";
    crc_val = crc32_8(tmp2.data(), tmp2.size(), 0);
    crc_val = crc32_8(tmp3.data(), tmp3.size(), crc_val);
    TestSuite::_msg("%zx\n", crc_val);

    // Should be the same.
    CHK_EQ( 0xf9eb20ad, crc_val );

    std::vector<uint8_t> payload(1024, 0);
    for (size_t ii=1; ii<=1024; ++ii) {
        uint32_t crc_val = crc32_8(&payload[0], ii, 0);
        TestSuite::_msg("%zx\n", crc_val);
    }

    return 0;
}

}; // namespace crc32_test
using namespace crc32_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.doTest("helloworld crc test", helloworld_crc_test);

    return 0;
}


