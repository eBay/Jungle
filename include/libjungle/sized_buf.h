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

#include <iostream>
#include <sstream>
#include <string>

#include <string.h>

namespace jungle {

struct SizedBufFlags {
    /**
     * If this flag is set, `free` will be used for deallocation.
     */
    static const uint8_t NEED_TO_FREE   = 0x1;

    /**
     * If this flag is set, `delete` will be used for deallocation.
     */
    static const uint8_t NEED_TO_DELETE = 0x2;
};

class SizedBuf {
public:
    /**
     * Automatically free the memory of given SizedBuf.
     */
    struct Holder {
        Holder(SizedBuf& s) : src(s) {}
        ~Holder() { src.free(); }
        SizedBuf& src;
    };

    /**
     * Create an empty SizedBuf.
     */
    SizedBuf() : flags(0x0), size(0), data(nullptr) { }

    /**
     * Create a SizedBuf with allocated memory of the given size.
     * User is responsible for deallocating the memory
     * by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param s Size to allocate.
     */
    SizedBuf(size_t s) : flags(0x0), size(0), data(nullptr) {
        alloc(s, nullptr);
    }

    /**
     * Create a SizedBuf referring to the given memory address.
     *
     * @param s Length of the memory region that this SizedBuf will refer to.
     * @param d Start address.
     */
    SizedBuf(size_t s, void* d) : flags(0x0) {
        set(s, d);
    }

    /**
     * Create a SizedBuf referring to the same memory address of the
     * given source SizedBuf.
     *
     * @param src Source SizedBuf.
     */
    SizedBuf(const SizedBuf& src) : flags(0x0) {
        set(src.size, src.data);
        flags = src.flags;
    }

    /**
     * Create a SizedBuf referring to the raw memory of the given string.
     *
     * @param str Source string.
     */
    SizedBuf(const std::string& str) : flags(0x0) {
        set(str.size(), (void*)str.data());
    }

    /**
     * Create a SizedBuf referring to the memory of the given
     * null-terminated C-string.
     *
     * @param str_char Source C-string.
     */
    SizedBuf(const char* str_char) : flags(0x0) {
        set(strlen(str_char), (void*)str_char);
    }

    /**
     * Assign the same memory address of the given SizedBuf.
     * Both this and the given SizedBufs will point to the same
     * memory as a result of this API call. Since both SizedBufs are
     * referring to the same memory, user should be careful about double-free.
     *
     * @param src Source SizedBuf.
     */
    SizedBuf& operator=(const SizedBuf& src) {
        flags = src.flags;
        size = src.size;
        data = src.data;
        return *this;
    }

    /**
     * Move the contents of this SizedBuf to the given SizedBuf.
     * This SizedBuf will become empty as a result of this API call.
     *
     * @param dst Destination SizedBuf.
     */
    void moveTo(SizedBuf& dst) {
        dst.flags = flags;
        dst.size = size;
        dst.data = data;
        flags = 0x0;
        size = 0;
        data = nullptr;
    }

    /**
     * Make a clone of this SizedBuf.
     * User is responsible for deallocating the memory of the destination
     * SizedBuf, by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param dst Destination SizedBuf.
     */
    void copyTo(SizedBuf& dst) const {
        dst.alloc(*this);
    }

    /**
     * Assign the same memory address of the given SizedBuf.
     * Unlike `operator=`, this API does not copy the original flag,
     * which means that calling `free()` or using `SizedBuf::Holder` on
     * this SizedBuf will not have any impact on the original SizedBuf.
     *
     * @param src Source SizedBuf.
     */
    void referTo(const SizedBuf& src) {
        size = src.size;
        data = src.data;
        flags = 0x0;
    }

    /**
     * Compare the given two SizedBufs in lexicographical order.
     *
     * @param l Left SizedBuf.
     * @param r Right SizedBuf.
     * @return Negative number if `l  < r`,
     *         Zero            if `l == r`, or
     *         Positive number if `l  > r`.
     */
    static inline int cmp(const SizedBuf& l, const SizedBuf& r) {
        if (l.size == r.size) {
            if (l.size == 0) return 0;
            return memcmp(l.data, r.data, l.size);
        } else {
            size_t len = std::min(l.size, r.size);
            int cmp = memcmp(l.data, r.data, len);
            if (cmp != 0) return cmp;
            else {
                return (int)((int)l.size - (int)r.size);
            }
        }
    }

    inline bool operator==(const SizedBuf &other) const {
        if (size != other.size) return false;
        if (size) {
            return memcmp(data, other.data, size) == 0;
        } else if (other.size == 0) {
            // Both are empty.
            return true;
        }
        return false;
    }

    inline bool operator!=(const SizedBuf &other) const {
        return !operator==(other);
    }

    friend inline bool operator<(const SizedBuf& l, const SizedBuf& r) {
        if (l.size == r.size) {
            if (l.size == 0) return false; // Both are empty.
            return (memcmp(l.data, r.data, l.size) < 0);
        } else if (l.size < r.size) {
            if (l.size == 0) return true;
            return (memcmp(l.data, r.data, l.size) <= 0);
        } else { // l.size > r.size
            if (r.size == 0) return false;
            return (memcmp(l.data, r.data, r.size) < 0);
        }

        return false;
    }

    friend inline bool operator<=(const SizedBuf& l, const SizedBuf& r) {
        if (l.size == r.size) {
            if (l.size == 0) return true; // Both are empty.
            return (memcmp(l.data, r.data, l.size) <= 0);
        } else if (l.size < r.size) {
            if (l.size == 0) return true;
            return (memcmp(l.data, r.data, l.size) <= 0);
        } else { // l.size > r.size
            if (r.size == 0) return false;
            return (memcmp(l.data, r.data, r.size) < 0);
        }

        return false;
    }

    friend inline bool operator>(const SizedBuf& l, const SizedBuf& r) {
        return !operator<=(l, r);
    }

    friend inline bool operator>=(const SizedBuf& l, const SizedBuf& r) {
        return !operator<(l, r);
    }

    #define MSG_MAX 24
    /**
     * Print the contents of the given SizedBuf with readable
     * (i.e., ASCII printable) characters.
     * Data after `MSG_MAX` will be emitted.
     */
    friend std::ostream &operator<<(std::ostream &output, const SizedBuf &sb) {
        if (sb.size == 0) {
            output << "(empty)";
            return output;
        }

        output << "(" << sb.size << ") ";
        size_t size_local = std::min(sb.size, (uint32_t)MSG_MAX);
        for (size_t ii=0; ii<size_local; ++ii) {
            char cc = ((char*)sb.data)[ii];
            if (0x20 <= cc && cc <= 0x7d) {
                output << cc;
            } else {
                output << '.';
            }
        }
        if (sb.size > MSG_MAX) output << "...";
        return output;
    }

    /**
     * Print the contents of the given SizedBuf with readable
     * (i.e., ASCII printable) characters.
     * Data after `MSG_MAX` will be emitted.
     */
    std::string toReadableString() const {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    /**
     * Assign the same memory address of the given SizedBuf.
     * Both this and the given SizedBufs will point to the same
     * memory as a result of this API call. Since both SizedBufs are
     * referring to the same memory, user should be careful about double-free.
     *
     * @param src Source SizedBuf.
     */
    void set(const SizedBuf& src) {
        set(src.size, src.data);
        flags = src.flags;
    }

    /**
     * Make this SizedBuf refer to the memory of the given
     * null-terminated C-string.
     *
     * @param str_char Source C-string.
     */
    void set(const char* str_char) {
        set(strlen(str_char), (void*)str_char);
    }

    /**
     * Make this SizedBuf refer to the raw memory of the given string.
     *
     * @param str Source string.
     */
    void set(const std::string& str) {
        set(str.size(), (void*)str.data());
    }

    /**
     * Make this SizedBuf refer to the given memory address.
     *
     * @param s Length of the memory region that this SizedBuf will refer to.
     * @param d Start address.
     */
    void set(size_t s, void* d) {
        clear();
        size = s;
        data = static_cast<uint8_t*>(d);
    }

    /**
     * Clone the contents of the given SizedBuf.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param src Source SizedBuf.
     */
    void alloc(const SizedBuf& src) {
        alloc(src.size, src.data);
    }

    /**
     * Allocate memory for this SizedBuf and copy the given C-string into it.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param str_char Source C-string.
     */
    void alloc(const char* str_char) {
        alloc(strlen(str_char), (void*)str_char);
    }

    /**
     * Allocate memory for this SizedBuf and copy the contents of the given
     * string into it.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param str Source string.
     */
    void alloc(const std::string& str) {
        alloc(str.size(), (void*)str.data());
    }

    /**
     * Allocate memory of the given size for this SizedBuf, and initialize it
     * with zero bytes.
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param s Length to allocate.
     */
    void alloc(size_t s) {
        alloc(s, nullptr);
    }

    /**
     * Allocate memory of the given size for this SizedBuf, and copy the data in
     * the given memory address into it
     * User is responsible for deallocating the memory of this
     * SizedBuf, by using `SizedBuf::Holder` or `SizedBuf::free()`.
     *
     * @param s Length to allocate and copy.
     * @param d Memory address of source data.
     */
    void alloc(size_t s, void* d) {
        clear();

        if (s == 0) {
            data = nullptr;
            flags = 0x0;
            return;
        }

        size = s;
        data = reinterpret_cast<uint8_t*>(malloc(size));
        if (d) {
            // Source data is given: copy.
            memcpy(data, d, size);
        } else {
            // NULL: just allocate space
            //       (set to 0 optionally).
            memset(data, 0x0, size);
        }
        flags |= SizedBufFlags::NEED_TO_FREE;
    }

    /**
     * Resize the buffer. Effective only when this SizedBuf owns the memory.
     *
     * @param s Length to allocate and copy.
     * @param d Memory address of source data.
     */
    void resize(size_t _size) {
        if ( !(flags & SizedBufFlags::NEED_TO_FREE) ) {
            // Not owning the memory, fail.
            return;
        }

        uint8_t* new_ptr = reinterpret_cast<uint8_t*>(::realloc(data, _size));
        if (new_ptr) {
            data = new_ptr;
            size = _size;
        }
    }

    /**
     * Export the contents of this SizedBuf as a string.
     *
     * @return String instance.
     */
    std::string toString() {
        return std::string((const char*)data, size);
    }

    /**
     * Deallocate the memory owned by this SizedBuf.
     * If this SizedBuf does not own the memory, will do nothing.
     *
     * @return `true` if the memory is deallocated.
     *         `false` otherwise.
     */
    bool free() {
        if (flags & SizedBufFlags::NEED_TO_FREE) {
            ::free(data);
            flags &= ~SizedBufFlags::NEED_TO_FREE;
            clear();
            return true;

        } else if (flags & SizedBufFlags::NEED_TO_DELETE) {
            delete[] data;
            flags &= ~SizedBufFlags::NEED_TO_DELETE;
            clear();
            return true;
        }
        return false;
    }

    /**
     * Force set the flag so as to make this SizedBuf own the memory,
     * with `malloc` and `free`.
     */
    void setNeedToFree() {
        flags |= SizedBufFlags::NEED_TO_FREE;
    }

    /**
     * Force set the flag so as to make this SizedBuf own the memory,
     * with `new` and `delete`.
     */
    void setNeedToDelete() {
        flags |= SizedBufFlags::NEED_TO_DELETE;
    }

    /**
     * Clear the contents without deallocation.
     */
    void clear() {
        flags = 0x0;
        size = 0;
        data = nullptr;
    }

    /**
     * Check if this SizedBuf is empty.
     *
     * @return `true` if empty.
     */
    bool empty() const {
        return (size == 0);
    }

    /**
     * Flags.
     */
    uint8_t flags;

    /**
     * Length of memory buffer.
     */
    uint32_t size;

    /**
     * Pointer to memory.
     */
    uint8_t* data;
};

} // namespace jungle

