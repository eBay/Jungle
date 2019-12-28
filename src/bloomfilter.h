/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright 2016 Michael Schmatz
http://blog.michaelschmatz.com/2016/04/11/how-to-write-a-bloom-filter-cpp/

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

#include "generic_bitmap.h"

#include <vector>

#include <stdint.h>
#include <stdlib.h>

class BloomFilter {
public:
    BloomFilter(uint64_t bitmap_size, uint8_t hash_count);
    BloomFilter(void* data, size_t len, uint8_t hash_count);
    size_t size() const;

    /**
     * Set the filter with the given binary.
     */
    void set(void* data, size_t len);

    /**
     * Check if the filter is positive with the given binary.
     */
    bool check(void* data, size_t len);

    /**
     * Check if the filter is positive with the given
     * pre-calculated hash pair.
     */
    bool check(uint64_t* hash_pair);
    void moveBitmapFrom(void* data, size_t len);
    void exportBitmap(void* data, size_t len) const;
    void* getPtr() const;
    GenericBitmap& getBitmap();

private:
    uint8_t hashCount;
    GenericBitmap bitmap;
};

