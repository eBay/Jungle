/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright:
See URL: https://github.com/greensky00/generic_bitmap
         (v0.1.2)

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

#include <cstdlib>
#include <mutex>
#include <thread>

#include <stdint.h>
#include <string.h>

class GenericBitmap {
public:
    /**
     * Default constructor: initialize to 0.
     */
    GenericBitmap(uint64_t num_bits,
                  size_t num_threads = 0)
        : numThreads(num_threads)
        , numBits(num_bits)
    {
        // Ceiling.
        memorySizeByte = (numBits + 7) / 8;
        if (memorySizeByte) {
            myBitmap = (uint8_t*)calloc(1, memorySizeByte);
        } else {
            myBitmap = nullptr;
        }

        init();
    }

    /**
     * Copy from given memory blob.
     */
    GenericBitmap(void* memory_ptr,
                  size_t memory_ptr_size,
                  size_t num_bits,
                  size_t num_threads = 0)
        : numThreads(num_threads)
        , numBits(num_bits)
        , memorySizeByte(memory_ptr_size)
    {
        myBitmap = (uint8_t*)calloc(1, memorySizeByte);
        memcpy(myBitmap, memory_ptr, memorySizeByte);

        init();
    }

    /**
     * Destructor.
     */
    ~GenericBitmap() {
        free(myBitmap);
        myBitmap = nullptr;

        delete[] locks;
        locks = nullptr;
    }

    /**
     * Replace internal bitmap with given memory region.
     * It will take ownership without memory copy.
     */
    void moveFrom(void* memory_ptr,
                  size_t memory_ptr_size,
                  size_t num_bits)
    {
        free(myBitmap);
        myBitmap = (uint8_t*)memory_ptr;
        memorySizeByte = memory_ptr_size;
        numBits = num_bits;
    }

    /**
     * Return the size of bitmap (number of bits).
     */
    size_t size() const { return numBits; }

    /**
     * Return the size of allocated memory region (in byte).
     */
    size_t getMemorySize() const { return memorySizeByte; }

    /**
     * Return the memory address of allocated memory region.
     */
    void* getPtr() const { return myBitmap; }

    /**
     * Get the bitmap value of given index.
     */
    inline bool get(uint64_t idx) {
        uint64_t lock_idx = 0, offset = 0, byte_idx = 0;
        parse(idx, lock_idx, offset, byte_idx);

        std::lock_guard<std::mutex> l(locks[lock_idx]);
        return getInternal(byte_idx, offset);
    }

    /**
     * Set the bitmap value of given index.
     */
    inline bool set(uint64_t idx, bool val) {
        uint64_t lock_idx = 0, offset = 0, byte_idx = 0;
        parse(idx, lock_idx, offset, byte_idx);

        std::lock_guard<std::mutex> l(locks[lock_idx]);
        // NOTE: bool -> int conversion is defined in C++ standard.
        return setInternal(offset, byte_idx, val);
    }

    inline size_t getNumThreads() const { return numThreads; }

    inline std::mutex* getLocks() const { return locks; }

private:
    void init() {
        masks8[0] = 0x80;
        masks8[1] = 0x40;
        masks8[2] = 0x20;
        masks8[3] = 0x10;
        masks8[4] = 0x08;
        masks8[5] = 0x04;
        masks8[6] = 0x02;
        masks8[7] = 0x01;

        // `numThreads` should be 2^n.
        if (!numThreads) {
            numThreads = 1;
            size_t num_cores = std::thread::hardware_concurrency();
            while (numThreads < num_cores) numThreads *= 2;
        }

        // TODO:
        //   To support partitioned lock, need to resolve
        //   aligned memory update issue.
        //   Until then, just use global latch.
        numThreads = 1;
        locks = new std::mutex[numThreads];

        for (size_t prev = 0; prev < 256; ++prev) {
            for (size_t offset = 0; offset < 8; ++offset) {
                uint8_t mask = masks8[offset];
                calcTable[prev][offset][0] = prev & (~mask);
                calcTable[prev][offset][1] = prev | mask;
            }
        }
    }

    inline void parse(uint64_t idx,
                      uint64_t& lock_idx_out,
                      uint64_t& offset_out,
                      uint64_t& byte_idx_out) const
    {
        lock_idx_out = idx & (numThreads - 1);
        offset_out = idx & 0x7;
        byte_idx_out = idx >> 3;
    }

    inline bool getInternal(uint64_t byte_idx,
                            uint64_t offset) const
    {
        uint8_t val = myBitmap[byte_idx];
        return val & masks8[offset];
    }

    inline bool setInternal(uint64_t offset,
                            uint64_t byte_idx,
                            uint8_t val)
    {
        uint8_t mask = masks8[offset];
        uint8_t prev = myBitmap[byte_idx];
        myBitmap[byte_idx] = calcTable[prev][offset][val];
        return prev & mask;
    }

    size_t numThreads;
    uint64_t numBits;
    uint64_t memorySizeByte;
    uint8_t* myBitmap;
    uint8_t calcTable[256][8][2];
    uint8_t masks8[8];
    std::mutex* locks;
};

