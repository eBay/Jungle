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

#include "thread_name.h"

namespace thread_name_test {

int set_thread_name_test() {
#if defined(__linux__) || defined(__APPLE__)
    int get_name_result = 0;
    std::string actual_name;

    std::thread worker([&]() {
        jungle::setThreadName("j_name_test");

#if defined(__APPLE__)
        char name[MAXTHREADNAMESIZE] = {};
#else
        char name[16] = {};
#endif
        get_name_result = pthread_getname_np(pthread_self(), name, sizeof(name));
        actual_name = name;
    });
    worker.join();

    CHK_Z(get_name_result);
    CHK_EQ(std::string("j_name_test"), actual_name);
#else
    jungle::setThreadName("j_name_test");
#endif

    return 0;
}

}; // namespace thread_name_test
using namespace thread_name_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.doTest("set thread name test", set_thread_name_test);

    return 0;
}
