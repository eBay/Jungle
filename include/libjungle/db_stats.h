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

#pragma once

#include <stddef.h>
#include <stdint.h>

namespace jungle {

class DBStats {
public:
    DBStats()
        : numKvs(0)
        , workingSetSizeByte(0)
        , cacheSizeByte(0)
        , cacheUsedByte(0)
        {}

    /**
     * Approximate the number of key-value pairs in DB.
     */
    uint64_t numKvs;

    /**
     * Total working set (i.e., valid KV pairs) size.
     */
    uint64_t workingSetSizeByte;

    /**
     * Total block cache capacity (byte).
     */
    uint64_t cacheSizeByte;

    /**
     * Amount of cache used (byte).
     */
    uint64_t cacheUsedByte;
};

} // namespace jungle

