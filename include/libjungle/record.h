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

#include "keyvalue.h"
#include "status.h"

#include <stdlib.h>
#include <string.h>

namespace jungle {

class Record {
public:
    static const uint64_t NIL_SEQNUM = (uint64_t)-1;

    enum Type : uint8_t {
        /**
         * Insertion marker.
         */
        INSERTION = 0,

        /**
         * Tombstone (deletion marker).
         */
        DELETION = 1,

        /**
         * Special type reserved for other commands.
         */
        COMMAND = 2
    };

    /**
     * Automatically free the memory of given Record.
     */
    struct Holder {
        Holder(Record& r) : rec(r) {}
        ~Holder() { rec.free(); }
        Record& rec;
    };

    /**
     * Create an empty Record.
     */
    Record()
        : seqNum(NIL_SEQNUM)
        , type(INSERTION) {}

    /**
     * Create an empty Record with the given type.
     *
     * @param t Type.
     */
    Record(Type t)
        : seqNum(NIL_SEQNUM)
        , type(t) {}

    /**
     * Create an empty Record with given sequence number and type.
     *
     * @param sn Sequence number.
     * @param t Type.
     */
    Record(const uint64_t sn,
           const Type t)
        : seqNum(sn)
        , type(t) {}

    /**
     * Calculate the sum of lengths of key, value, and meta.
     */
    size_t size() const {
        return kv.size() + meta.size;
    }

    /**
     * Move the contents of this Record to the given Record.
     * This Record will become empty as a result of this API call.
     *
     * @param dst Destination Record.
     */
    void moveTo(Record& dst) {
        kv.moveTo(dst.kv);
        meta.moveTo(dst.meta);
        dst.seqNum = seqNum;
        dst.type = type;
        clear();
    }

    /**
     * Make a clone of this Record.
     * User is responsible for deallocating the memory of the destination
     * KV, by using `KV::Holder` or `KV::free()`.
     *
     * @param dst Destination Record.
     */
    void copyTo(Record& dst) const {
        kv.copyTo(dst.kv);
        meta.copyTo(dst.meta);
        dst.seqNum = seqNum;
        dst.type = type;
    }

    /**
     * Make this Record as a clone of the given source Record.
     * This record should be empty before calling this API.
     * User is responsible for deallocating the memory of the destination
     * KV, by using `KV::Holder` or `KV::free()`.
     *
     * @param src Source Record.
     * @return OK on success.
     */
    Status clone(const Record& src) {
        if (kv.key.data || kv.value.data) {
            return Status(Status::ALREADY_INITIALIZED);
        }

        kv.key.alloc(src.kv.key.size, src.kv.key.data);
        kv.value.alloc(src.kv.value.size, src.kv.value.data);
        meta.alloc(src.meta);
        seqNum = src.seqNum;
        type = src.type;

        return Status();
    }

    /**
     * Deallocate the memory of key, value, and meta.
     */
    void free() {
        kv.key.free();
        kv.value.free();
        meta.free();
    }

    /**
     * Clear the memory of key, value, and meta, without deallocation.
     */
    void clear() {
        kv.clear();
        meta.clear();
        seqNum = NIL_SEQNUM;
        type = INSERTION;
    }

    /**
     * Check if this Record is empty, which means both key and value are empty.
     *
     * @return `true` if empty.
     */
    bool empty() const { return kv.key.empty() && kv.value.empty(); }

    /**
     * Check if this Record is an insertion marker.
     *
     * @return `true` if insertion marker.
     */
    bool isIns() const { return type == INSERTION; }

    /**
     * Check if this Record is a tombstone.
     *
     * @return `true` if tombstone.
     */
    bool isDel() const { return type == DELETION; }

    /**
     * Check if this Record is for a special command.
     *
     * @return `true` if special command.
     */
    bool isCmd() const { return type == COMMAND; }

    /**
     * Less functor.
     */
    struct Less {
        bool operator()(const Record* a, const Record* b) const {
            return a->kv.key < b->kv.key;
        }
    };

    /**
     * Key and value.
     */
    KV kv;

    /**
     * Custom metadata.
     */
    SizedBuf meta;

    /**
     * Sequence number.
     */
    uint64_t seqNum;

    /**
     * Type.
     */
    Type type;
};

} // namespace jungle
