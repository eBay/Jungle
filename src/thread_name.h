/************************************************************************
Copyright 2017-2019 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <string>

#if defined(__APPLE__)
#include <mach/thread_info.h>
#include <pthread.h>
#elif defined(__linux__)
#include <pthread.h>
#endif

namespace jungle {

inline void setThreadName(const std::string& name) {
#if defined(__APPLE__)
    pthread_setname_np(name.substr(0, MAXTHREADNAMESIZE - 1).c_str());
#elif defined(__linux__)
    pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
#else
    (void)name;
#endif
}

} // namespace jungle
