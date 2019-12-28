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

#include "db_mgr.h"

#include <libjungle/jungle.h>

namespace jungle {

bool DBConfig::isValid() const {
    return true;
}

uint64_t DBConfig::getMaxTableSize(size_t level) const {
    if (level == 0) return maxL0TableSize;

    uint64_t ret = maxL1TableSize;
    size_t num_ratio_elems = tableSizeRatio.size();
    double last_ratio = num_ratio_elems
                        ? *tableSizeRatio.rbegin()
                        : 10;
    for (size_t ii = 1; ii < level; ++ii) {
        size_t vector_idx = ii - 1;
        if (num_ratio_elems > vector_idx) {
            ret *= tableSizeRatio[vector_idx];
        } else {
            ret *= last_ratio;
        }
    }
    return ret;
}

size_t DBConfig::getMaxParallelWriters() const {
    // If given, just return it.
    if (maxParallelWritesPerJob) return maxParallelWritesPerJob;
    if (readOnly) return 1;

    // If zero, calculate it.
    DBMgr* mgr = DBMgr::getWithoutInit();
    if (!mgr) return 1;

    GlobalConfig* g_conf = mgr->getGlobalConfig();
    if (!g_conf) return 1;

    size_t num_task_threads = g_conf->numFlusherThreads +
                              g_conf->numCompactorThreads;
    if (!num_task_threads) return 1;

    // Round-up.
    size_t ret = ( g_conf->numTableWriters + (num_task_threads * 2) - 1 ) /
                 num_task_threads;
    if (!ret) return 1;
    if (ret == 1 && g_conf->numTableWriters) return 2;
    return ret;
}


}; // namespace jungle;

