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
#include "record.h"

namespace jungle {

class DB;
class Iterator {
public:
    Iterator();
    ~Iterator();

    enum SeekOption {
        /**
         * If exact match does not exist, find the smallest key
         * which is greater than the given key.
         */
        GREATER = 0,

        /**
         * If exact match does not exist, find the greatest key
         * which is smaller than the given key.
         */
        SMALLER = 1,
    };

    /**
     * Initialize a key iterator, based on the given DB (or snapshot) instance.
     * Note that even though the given instance is empty, this API will succeed.
     *
     * @param db DB (or snapshot) instance.
     * @param start_key
     *     Lower bound of the iterator (inclusive). If exact match does not
     *     exist, the iterator will start from the smallest key which is
     *     greater than the given `start_key`.
     *     If not given, there will be no lower bound for the iterator.
     * @param end_key
     *     Upper bound of the iterator (inclusive). If exact match does not
     *     exist, the iterator will start from the greatest key which is
     *     smaller than the given `end_key`.
     *     If not given, there will be no upper bound for the iterator.
     * @return OK on success.
     */
    Status init(DB* db,
                const SizedBuf& start_key = SizedBuf(),
                const SizedBuf& end_key = SizedBuf());

    /**
     * Initialize a sequence number iterator, based on the given DB
     * (or snapshot) instance.
     * Note that even though the given instance is empty, this API will succeed.
     *
     * @param db DB (or snapshot) instance.
     * @param min_seq
     *     Lower bound of the iterator (inclusive). If exact match does not
     *     exist, the iterator will start from the smallest sequence number
     *     which is greater than the given `min_seq`.
     *     If not given, there will be no lower bound for the iterator.
     * @param max_seq
     *     Upper bound of the iterator (inclusive). If exact match does not
     *     exist, the iterator will start from the greatest sequence number
     *     which is smaller than the given `max_seq`.
     *     If not given, there will be no upper bound for the iterator.
     * @return OK on success.
     */
    Status initSN(DB* db,
                  const uint64_t min_seq = -1,
                  const uint64_t max_seq = -1);

    /**
     * Get the record currently pointed to by the iterator.
     *
     * If this is a sequence number iterator, it will return
     * tombstones. Key iterator will not return tombstones.
     *
     * User is responsible for freeing the memory of `rec_out`,
     * by using `Record::free()` or `Record::Holder`.
     *
     * @param[out] rec_out Reference to the record
     *                     where the result will be stored.
     * @return OK on success.
     */
    Status get(Record& rec_out);

    /**
     * Move the cursor of the iterator one step backward.
     *
     * @return OK on success.
     */
    Status prev();

    /**
     * Move the cursor of the iterator one step forward.
     *
     * @return OK on success.
     */
    Status next();

    /**
     * Move the cursor of the iterator to the given key.
     * It is valid only for key iterator.
     * If exact match does not exist, the cursor will be located according
     * to the given seek option.
     *
     * @param key Key to find.
     * @param opt Seek option.
     * @return OK on success.
     */
    Status seek(const SizedBuf& key, SeekOption opt = GREATER);

    /**
     * Move the cursor of the iterator to the given sequence number.
     * It is valid only for sequence number iterator.
     * If exact match does not exist, the cursor will be located according
     * to the given seek option.
     *
     * @param seqnum Sequence number to find.
     * @param opt Seek option.
     * @return OK on success.
     */
    Status seekSN(const uint64_t seqnum, SeekOption opt = GREATER);

    /**
     * Move the cursor to the beginning of the iterator.
     *
     * @return OK on success.
     */
    Status gotoBegin();

    /**
     * Move the cursor to the end of the iterator.
     *
     * @return OK on success.
     */
    Status gotoEnd();

    /**
     * Close the iterator.
     *
     * @return OK on success.
     */
    Status close();

private:
    class IteratorInternal;
    IteratorInternal* const p;
    using ItrInt = Iterator::IteratorInternal;
};

} // namespace jungle
