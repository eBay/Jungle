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

#include "memtable.h"

namespace jungle {

MemTable::Iterator::Iterator()
    : mTable(nullptr)
    , cursor(nullptr)
    , minSeq(NOT_INITIALIZED)
    , maxSeq(NOT_INITIALIZED)
    , seqUpto(NOT_INITIALIZED)
    {}

MemTable::Iterator::~Iterator() {
    close();
}

Status MemTable::Iterator::init(const MemTable* m_table,
                                const SizedBuf& start_key,
                                const SizedBuf& end_key,
                                const uint64_t seq_upto)
{
    mTable = m_table;
    type = BY_KEY;

    RecNode query(&start_key);
    cursor = skiplist_find_greater_or_equal(mTable->idxByKey, &query.snode);
    if (!cursor) return Status::ITERATOR_INIT_FAIL;

    RecNode* rn = _get_entry(cursor, RecNode, snode);
    if (!end_key.empty() && SizedBuf::cmp(rn->key, end_key) > 0) {
        skiplist_release_node(cursor);
        cursor = nullptr;
        return Status::OUT_OF_RANGE;
    }

    startKey.alloc(start_key);
    endKey.alloc(end_key);
    seqUpto = seq_upto;

    // Skip invalid keys.
    for (; cursor ;) {
        RecNode* rn = _get_entry(cursor, RecNode, snode);
        if (!endKey.empty() && SizedBuf::cmp(rn->key, endKey) > 0) {
            // Greater than end key.
            skiplist_release_node(cursor);
            cursor = nullptr;
            return Status::OUT_OF_RANGE;
        }

        // Allow tombstone.
        if ( !rn->validKeyExist(seqUpto, true) ) {
            // No record belongs to the current snapshot (iterator),
            // move further.
            cursor = skiplist_next(mTable->idxByKey, cursor);
            skiplist_release_node(&rn->snode);
            continue;
        }
        break;
    }

    return Status();
}

Status MemTable::Iterator::initSN(const MemTable* m_table,
                                  const uint64_t min_seq,
                                  const uint64_t max_seq)
{
    mTable = m_table;
    type = BY_SEQ;

    if (mTable->maxSeqNum == NOT_INITIALIZED)
        return Status::NOT_INITIALIZED;

    // Doesn't allow to access logs already purged.
    uint64_t min_local = min_seq;
    if (mTable->minSeqNum != NOT_INITIALIZED &&
        min_local < mTable->minSeqNum)
        min_local = mTable->minSeqNum;

    if (mTable->flushedSeqNum != NOT_INITIALIZED &&
        min_local < mTable->flushedSeqNum)
        min_local = mTable->flushedSeqNum;

    if (min_local > mTable->maxSeqNum)
        min_local = mTable->maxSeqNum;

    minSeq = min_local;

    // Need to filter out iterator request beyond the range.
    if (minSeq > max_seq) return Status::OUT_OF_RANGE;

    Record query_rec;
    RecNodeSeq query(&query_rec);
    query.rec->seqNum = minSeq;
    cursor = skiplist_find_greater_or_equal(mTable->idxBySeq, &query.snode);
    if (!cursor) return Status::ITERATOR_INIT_FAIL;

    // Doesn't allow to access logs beyond max seqnum.
    uint64_t max_local = max_seq;
    if (mTable->minSeqNum != NOT_INITIALIZED &&
        max_local < mTable->minSeqNum)
        max_local = mTable->minSeqNum;

    if (max_local > mTable->maxSeqNum)
        max_local = mTable->maxSeqNum;

    maxSeq = max_local;

    return Status();
}

Status MemTable::Iterator::get(Record& rec_out) {
    if (!mTable) return Status::NOT_INITIALIZED;
    if (!cursor) return Status::KEY_NOT_FOUND;
    if (type == BY_KEY) {
        RecNode* node = _get_entry(cursor, RecNode, snode);
        Record* rec = node->getLatestRecord(seqUpto);
        assert(rec);
        rec_out = *rec;

    } else { // BY_SEQ
        RecNodeSeq* node = _get_entry(cursor, RecNodeSeq, snode);
        Record* rec = node->rec;
        assert(rec);
        rec_out = *rec;
    }

    return Status();
}

Status MemTable::Iterator::prev(bool allow_tombstone) {
    if (!mTable) return Status::NOT_INITIALIZED;
    if (!cursor) return Status::KEY_NOT_FOUND;
    if (type == BY_KEY) {
        skiplist_node* node_to_release = cursor;
        skiplist_node* prev_node = skiplist_prev(mTable->idxByKey, cursor);

        for (;;) {
            if (!prev_node) return Status::OUT_OF_RANGE;

            RecNode* rn = _get_entry(prev_node, RecNode, snode);
            if (!startKey.empty() && SizedBuf::cmp(rn->key, startKey) < 0) {
                // Smaller than start key.
                skiplist_release_node(prev_node);
                return Status::OUT_OF_RANGE;
            }

            if ( !rn->validKeyExist(seqUpto, allow_tombstone) ) {
                prev_node = skiplist_prev(mTable->idxByKey, prev_node);
                skiplist_release_node(&rn->snode);
                continue;
            }
            break;
        }

        cursor = prev_node;
        skiplist_release_node(node_to_release);

    } else { // BY_SEQ
        skiplist_node* node_to_release = cursor;
        skiplist_node* prev_node = skiplist_prev(mTable->idxBySeq, cursor);
        if (!prev_node) return Status::OUT_OF_RANGE;

        RecNodeSeq* rn = _get_entry(prev_node, RecNodeSeq, snode);
        if (rn->rec->seqNum < minSeq) {
            skiplist_release_node(prev_node);
            return Status::OUT_OF_RANGE;
        }

        cursor = prev_node;
        skiplist_release_node(node_to_release);
    }

    return Status();
}

Status MemTable::Iterator::next(bool allow_tombstone) {
    if (!mTable) return Status::NOT_INITIALIZED;
    if (!cursor) return Status::KEY_NOT_FOUND;
    if (type == BY_KEY) {
        skiplist_node* node_to_release = cursor;
        skiplist_node* next_node = skiplist_next(mTable->idxByKey, cursor);

        for(;;) {
            if (!next_node) return Status::OUT_OF_RANGE;

            RecNode* rn = _get_entry(next_node, RecNode, snode);
            if (!endKey.empty() && SizedBuf::cmp(rn->key, endKey) > 0) {
                // Greater than end key.
                skiplist_release_node(next_node);
                return Status::OUT_OF_RANGE;
            }

            if ( !rn->validKeyExist(seqUpto, allow_tombstone) ) {
                // No record belongs to the current snapshot (iterator),
                // move further.
                next_node = skiplist_next(mTable->idxByKey, next_node);
                skiplist_release_node(&rn->snode);
                continue;
            }
            break;
        }

        cursor = next_node;
        skiplist_release_node(node_to_release);

    } else { // BY_SEQ
        skiplist_node* node_to_release = cursor;
        skiplist_node* next_node = skiplist_next(mTable->idxBySeq, cursor);
        if (!next_node) return Status::OUT_OF_RANGE;

        RecNodeSeq* rn = _get_entry(next_node, RecNodeSeq, snode);
        if (rn->rec->seqNum > maxSeq) {
            skiplist_release_node(next_node);
            return Status::OUT_OF_RANGE;
        }

        cursor = next_node;
        skiplist_release_node(node_to_release);
    }

    return Status();
}

Status MemTable::Iterator::seek
       ( const SizedBuf& key, SeekOption opt )
{
    if (type != BY_KEY) return Status::INVALID_HANDLE_USAGE;
    if (!cursor) return Status::KEY_NOT_FOUND;

    skiplist_node* node_to_release = cursor;
    skiplist_node* seek_node = nullptr;

    const SizedBuf* query_key_ptr = &key;
    if (opt == GREATER && key.empty() && !startKey.empty()) {
        // gotoBegin, and start key is given.
        query_key_ptr = &startKey;
    }
    RecNode query(query_key_ptr);

    bool fwd_search = true;

    if (opt == GREATER) {
        fwd_search = true; // Foward search.
        seek_node = skiplist_find_greater_or_equal
                    ( mTable->idxByKey, &query.snode );
        if (!seek_node) {
            if (endKey.empty()) {
                // End key is not given.
                seek_node = skiplist_end(mTable->idxByKey);
            } else {
                // End key is given.
                RecNode end_query(&endKey);
                seek_node = skiplist_find_smaller_or_equal
                            ( mTable->idxByKey, &end_query.snode );
            }
            // If not found, backward search from the end.
            fwd_search = false;
        }

    } else if (opt == SMALLER) {
        fwd_search = false;
        seek_node = skiplist_find_smaller_or_equal
                    ( mTable->idxByKey, &query.snode );
        if (!seek_node) {
            if (startKey.empty()) {
                // Start key is not given.
                seek_node = skiplist_begin(mTable->idxByKey);
            } else {
                // End key is given.
                RecNode start_query(&startKey);
                seek_node = skiplist_find_greater_or_equal
                            ( mTable->idxByKey, &start_query.snode );
            }
            // If not found, forward search from the beginning.
            fwd_search = true;
        }

    } else {
        assert(0);
    }

    if (fwd_search) {
        // Go forward if no record belongs to the snapshot.
        for (; seek_node ;) {
            RecNode* rn = _get_entry(seek_node, RecNode, snode);
            if (!endKey.empty() && SizedBuf::cmp(rn->key, endKey) > 0) {
                // Greater than end key.
                skiplist_release_node(seek_node);
                return Status::OUT_OF_RANGE;
            }

            if ( !rn->validKeyExist(seqUpto, true) ) {
                // No record belongs to the current snapshot (iterator),
                // move further.
                seek_node = skiplist_next(mTable->idxByKey, seek_node);
                skiplist_release_node(&rn->snode);
                continue;
            }
            break;
        }

    } else {
        // Go backward if no record belongs to the snapshot.
        for (; seek_node ;) {
            RecNode* rn = _get_entry(seek_node, RecNode, snode);
            if (!startKey.empty() && SizedBuf::cmp(rn->key, startKey) < 0) {
                // Smaller than start key.
                skiplist_release_node(seek_node);
                return Status::OUT_OF_RANGE;
            }

            if ( !rn->validKeyExist(seqUpto, true) ) {
                // No record belongs to the current snapshot (iterator),
                // move further.
                seek_node = skiplist_prev(mTable->idxByKey, seek_node);
                skiplist_release_node(&rn->snode);
                continue;
            }
            break;
        }
    }

    if (!seek_node) return Status::OUT_OF_RANGE;

    cursor = seek_node;
    skiplist_release_node(node_to_release);

    return Status();
}

Status MemTable::Iterator::seekSN
       ( const uint64_t seqnum, MemTable::Iterator::SeekOption opt )
{
    if (type != BY_SEQ) return Status::INVALID_HANDLE_USAGE;
    if (!cursor) return Status::KEY_NOT_FOUND;

    skiplist_node* node_to_release = cursor;
    skiplist_node* seek_node = nullptr;

    Record rec;
    RecNodeSeq query(&rec);
    query.rec->seqNum = seqnum;

    if (opt == GREATER) {
        if (seqnum < minSeq) query.rec->seqNum = minSeq;

        seek_node = skiplist_find_greater_or_equal
                    ( mTable->idxBySeq, &query.snode );
        if (!seek_node) {
            query.rec->seqNum = maxSeq;
            seek_node = skiplist_find_smaller_or_equal
                        ( mTable->idxBySeq, &query.snode );
        }
    } else if (opt == SMALLER) {
        if (seqnum < maxSeq) query.rec->seqNum = maxSeq;

        seek_node = skiplist_find_smaller_or_equal
                    ( mTable->idxBySeq, &query.snode );
        if (!seek_node) {
            query.rec->seqNum = minSeq;
            seek_node = skiplist_find_greater_or_equal
                        ( mTable->idxBySeq, &query.snode );
        }
    } else {
        assert(0);
    }

    if (!seek_node) return Status::OUT_OF_RANGE;

    cursor = seek_node;
    skiplist_release_node(node_to_release);

    return Status();
}

Status MemTable::Iterator::gotoBegin() {
    if (!mTable) return Status::NOT_INITIALIZED;
    if (!cursor) return Status::KEY_NOT_FOUND;
    if (type == BY_KEY) {
        skiplist_node* node_to_release = cursor;
        skiplist_node* min_node = nullptr;
        if (startKey.empty()) {
            min_node = skiplist_begin(mTable->idxByKey);
        } else {
            RecNode query(&startKey);
            min_node = skiplist_find_greater_or_equal
                ( mTable->idxByKey, &query.snode );
        }
        if (!min_node) return Status::OUT_OF_RANGE;

        cursor = min_node;
        skiplist_release_node(node_to_release);

    } else { // BY_SEQ
        return seekSN(minSeq, GREATER);
    }
    return Status();
}

Status MemTable::Iterator::gotoEnd() {
    if (!mTable) return Status::NOT_INITIALIZED;
    if (type == BY_KEY) {
        skiplist_node* node_to_release = cursor;
        skiplist_node* max_node = nullptr;
        if (endKey.empty()) {
            max_node = skiplist_end(mTable->idxByKey);
        } else {
            RecNode query(&endKey);
            max_node = skiplist_find_smaller_or_equal
                ( mTable->idxByKey, &query.snode );
        }
        if (!max_node) return Status::OUT_OF_RANGE;

        cursor = max_node;
        skiplist_release_node(node_to_release);

    } else { // BY_SEQ
        return seekSN(maxSeq, SMALLER);
    }
    return Status();
}

Status MemTable::Iterator::close() {
    if (cursor) skiplist_release_node(cursor);
    // Nothing to free, just clear them.
    mTable = nullptr;
    cursor = nullptr;
    startKey.free();
    endKey.free();
    return Status();
}

}; // namespace jungle
