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

#include "sized_buf.h"

#include <string>
#include <iostream>

#include <string.h>

namespace jungle {

class KV {
public:
    /**
     * Automatically free the memory of given KV.
     */
    struct Holder {
        Holder(KV& _kv) : kv(_kv) {}
        ~Holder() { kv.free(); }
        KV& kv;
    };

    /**
     * Create an empty KV.
     */
    KV() {}

    /**
     * Create a KV referring to the given memory addresses.
     *
     * @param k_size Length of key.
     * @param k_data Start address of key.
     * @param v_size Length of value.
     * @param v_data Start address of value.
     */
    KV( size_t k_size, void *k_data,
        size_t v_size, void *v_data )
    {
        key.set(k_size, k_data);
        value.set(v_size, v_data);
    }

    /**
     * Create a KV referring to given SizedBufs for key and value.
     *
     * @param k SizedBuf for key.
     * @param v SizedBuf for value.
     */
    KV(const SizedBuf& k, const SizedBuf& v) : key(k), value(v) {}

    /**
     * Create a KV referring to the given KV.
     *
     * @param src Source KV.
     */
    KV(const KV& src) {
        set(src.key, src.value);
    }

    /**
     * Create a KV referring to the raw memory addresses of given strings.
     *
     * @param k String for key.
     * @param v String for value.
     */
    KV(const std::string& k, const std::string& v) {
        set(k, v);
    }

    /**
     * Create a KV referring to the raw memory addresses of given
     * null-terminated C-string.
     *
     * @param k C-string for key.
     * @param v C-string for value.
     */
    KV(const char* k, const char* v) {
        set(k, v);
    }

    /**
     * Calculate the sum of lengths of key and value.
     *
     * @return Length.
     */
    size_t size() const {
        return key.size + value.size;
    }

    /**
     * Move the contents of this KV to the given KV.
     * This KV will become empty as a result of this API call.
     *
     * @param dst Destination KV.
     */
    void moveTo(KV& dst) {
        key.moveTo(dst.key);
        value.moveTo(dst.value);
    }

    /**
     * Make a clone of this KV.
     * User is responsible for deallocating the memory of the destination
     * KV, by using `KV::Holder` or `KV::free()`.
     *
     * @param dst Destination KV.
     */
    void copyTo(KV& dst) const {
        key.copyTo(dst.key);
        value.copyTo(dst.value);
    }

    /**
     * Make this KV refer to the raw memory addresses of given
     * null-terminated C-strings.
     *
     * @param k C-string for key.
     * @param v C-string for value.
     */
    void set(const char* k, const char* v) {
        key.set(strlen(k), (void*)k);
        value.set(strlen(v), (void*)v);
    }

    /**
     * Make this KV refer to the raw memory addresses of given strings.
     *
     * @param k String for key.
     * @param v String for value.
     */
    void set(const std::string& k, const std::string& v) {
        key.set(k.size(), (void*)k.data());
        value.set(v.size(), (void*)v.data());
    }

    /**
     * Make this KV refer to given SizedBufs.
     *
     * @param k SizedBuf for key.
     * @param v SizedBuf for value.
     */
    void set(const SizedBuf& k, const SizedBuf& v) {
        key = k;
        value = v;
    }

    /**
     * Allocate memory for this KV and copy given C-strings into it.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `KV::Holder` or `KV::free()`.
     *
     * @param k Source C-string for key.
     * @param v Source C-string for value.
     */
    void alloc(const char* k, const char* v) {
        key.alloc(strlen(k), (void*)k);
        value.alloc(strlen(v), (void*)v);
    }

    /**
     * Allocate memory for this KV and copy the contents of given
     * strings into it.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `KV::Holder` or `KV::free()`.
     *
     * @param k Source string for key.
     * @param v Source string for value.
     */
    void alloc(const std::string& k, const std::string& v) {
        key.alloc(k.size(), (void*)k.data());
        value.alloc(v.size(), (void*)v.data());
    }

    /**
     * Clone the contents of given SizedBufs for key and value.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `KV::Holder` or `KV::free()`.
     *
     * @param k Source SizedBuf for key.
     * @param v Source SizedBuf for value.
     */
    void alloc(const SizedBuf& k, const SizedBuf& v) {
        key.alloc(k.size, k.data);
        value.alloc(v.size, v.data);
    }

    /**
     * Deallocate the memory of key and value.
     */
    void free() {
        key.free();
        value.free();
    }

    /**
     * Clear the memory of key and value, without deallocation.
     */
    void clear() {
        key.clear();
        value.clear();
    }

    /**
     * Buffer for key.
     */
    SizedBuf key;

    /**
     * Buffer for value.
     */
    SizedBuf value;
};

} // namespace jungle

