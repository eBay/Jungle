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

#include "bloomfilter.h"

#include "assert.h"
#include "murmurhash3.h"
#include "string.h"

inline void get_hash_pair(void *data,
                          size_t len,
                          uint64_t* hash_pair_out)
{
    MurmurHash3_x64_128(data, len, 0, hash_pair_out);
}

inline uint64_t gen_hash(size_t idx,
                         uint64_t* hash_pair)
{
    return (hash_pair[0] + idx * hash_pair[1]);
}

BloomFilter::BloomFilter(uint64_t bitmap_size, uint8_t hash_count)
    : hashCount(hash_count)
    , bitmap( ((bitmap_size+7) / 8) * 8 )
    {}

BloomFilter::BloomFilter(void* data, size_t len, uint8_t hash_count)
    : hashCount(hash_count)
    , bitmap( data, len, len * 8 )
    {}

size_t BloomFilter::size() const {
    return bitmap.size();
}

void BloomFilter::set(void* data, size_t len) {
    uint64_t hash_pair[2] = {0, 0};
    get_hash_pair(data, len, hash_pair);

    for (size_t ii=0; ii<hashCount; ++ii) {
        bitmap.set( gen_hash(ii, hash_pair) % bitmap.size(), true );
    }
}

bool BloomFilter::check(void* data, size_t len) {
    uint64_t hash_pair[2] = {0, 0};
    get_hash_pair(data, len, hash_pair);

    for (size_t ii=0; ii<hashCount; ++ii) {
        if ( !bitmap.get( gen_hash(ii, hash_pair) % bitmap.size() ) ) {
            return false;
        }
    }
    return true;
}

bool BloomFilter::check(uint64_t* hash_pair) {
    for (size_t ii=0; ii<hashCount; ++ii) {
        if ( !bitmap.get( gen_hash(ii, hash_pair) % bitmap.size() ) ) {
            return false;
        }
    }
    return true;
}

void BloomFilter::moveBitmapFrom(void* data, size_t len) {
    bitmap.moveFrom(data, len, len * 8);
}

void BloomFilter::exportBitmap(void* data, size_t len) const {
    size_t num = bitmap.size();
    size_t byte_len = num / 8;
    // Always should be divisible by 8.
    assert( byte_len * 8 == num);
    // Given buffer should be equal to or bigger than bitmap.
    assert( len >= byte_len );

    uint8_t* ptr = (uint8_t*)bitmap.getPtr();
    memcpy(data, ptr, byte_len);
}

void* BloomFilter::getPtr() const {
    return bitmap.getPtr();
}

GenericBitmap& BloomFilter::getBitmap() {
    return bitmap;
}

