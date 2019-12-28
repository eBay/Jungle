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

#include "table_file.h"

#include "table_mgr.h"

namespace jungle {

// === iterator ===============================================================

TableFile::Iterator::Iterator()
    : tFile(nullptr)
    , tFileSnap(nullptr)
    , fdbSnap(nullptr)
    , fdbItr(nullptr)
    , minSeq(NOT_INITIALIZED)
    , maxSeq(NOT_INITIALIZED)
    {}

TableFile::Iterator::~Iterator() {
    close();
}

Status TableFile::Iterator::init(DB* snap_handle,
                                 TableFile* t_file,
                                 const SizedBuf& start_key,
                                 const SizedBuf& end_key)
{
    tFile = t_file;
    fdb_status fs;
    Status s;

    // Get last snap marker.
    fdb_kvs_handle* fdb_snap = nullptr;

    if (snap_handle) {
        // Snapshot.
        mGuard l(t_file->snapHandlesLock);
        auto entry = t_file->snapHandles.find(snap_handle);
        if (entry == t_file->snapHandles.end()) return Status::SNAPSHOT_NOT_FOUND;

        // WARNING:
        //   Currently one snapshot only allows one iterator.
        //
        // TODO:
        //   Should open a separate snapshot handle!
        //   Multiple iterators derived from the same snapshot handle will share
        //   `dhandle` and `bhandle` which will cause race condition.
        fdb_snap = entry->second;

    } else {
        // Normal, use latest snapshot.
        tFile->leaseSnapshot(tFileSnap);
        fs = fdb_snapshot_open( tFileSnap->fdbSnap,
                                &fdbSnap,
                                tFileSnap->fdbSeqnum );
        if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

        fdb_snap = fdbSnap;
    }

    // if (valid_number(chk) && snap_seqnum > chk) snap_seqnum = chk;

    fs = fdb_iterator_init(fdb_snap, &fdbItr,
                           start_key.data, start_key.size,
                           end_key.data, end_key.size,
                           FDB_ITR_NO_DELETES);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

    type = BY_KEY;

    return Status();
}

Status TableFile::Iterator::initSN(DB* snap_handle,
                                   TableFile* t_file,
                                   const uint64_t min_seq,
                                   const uint64_t max_seq)
{
    tFile = t_file;
    fdb_status fs;
    Status s;

    // Get last snap marker.
    fdb_seqnum_t snap_seqnum = 0;
    fdb_kvs_handle* fdb_snap = nullptr;

    if (snap_handle) {
        // Snapshot.
        mGuard l(t_file->snapHandlesLock);
        auto entry = t_file->snapHandles.find(snap_handle);
        if (entry == t_file->snapHandles.end()) return Status::SNAPSHOT_NOT_FOUND;

        // WARNING: See `init()` above.
        fdb_snap = entry->second;

        fs = fdb_get_kvs_seqnum(fdb_snap, &snap_seqnum);
        if (fs != FDB_RESULT_SUCCESS) return Status::INVALID_SNAPSHOT;

    } else {
        // Normal, use latest snapshot.
        tFile->leaseSnapshot(tFileSnap);
        fs = fdb_snapshot_open( tFileSnap->fdbSnap,
                                &fdbSnap,
                                tFileSnap->fdbSeqnum );
        if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

        fdb_snap = fdbSnap;
    }

    // if (valid_number(chk) && snap_seqnum > chk) snap_seqnum = chk;

    if (valid_number(min_seq)) {
        minSeq = min_seq;
    } else {
        minSeq = 0;
    }
    if (valid_number(max_seq)) {
        maxSeq = std::min(snap_seqnum, max_seq);
    } else {
        maxSeq = 0;
    }

    fs = fdb_iterator_sequence_init(fdb_snap, &fdbItr,
                                    minSeq, maxSeq,
                                    FDB_ITR_NO_DELETES);
    if (fs != FDB_RESULT_SUCCESS) return Status::FDB_OPEN_KVS_FAIL;

    type = BY_SEQ;

    return Status();
}

Status TableFile::Iterator::get(Record& rec_out) {
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;

    fdb_doc tmp_doc;
    memset(&tmp_doc, 0x0, sizeof(tmp_doc));

    fdb_doc *doc = &tmp_doc;
    fs = fdb_iterator_get(fdbItr, &doc);
    if (fs != FDB_RESULT_SUCCESS) {
        return Status::ERROR;
    }

    rec_out.kv.key.set(doc->keylen, doc->key);
    rec_out.kv.key.setNeedToFree();
    rec_out.kv.value.set(doc->bodylen, doc->body);
    rec_out.kv.value.setNeedToFree();

    // Decode meta.
    SizedBuf user_meta_out;
    SizedBuf raw_meta(doc->metalen, doc->meta);;
    SizedBuf::Holder h_raw_meta(raw_meta); // auto free raw meta.
    raw_meta.setNeedToFree();
    bool is_tombstone_out = false;
    TableFile::rawMetaToUserMeta(raw_meta, is_tombstone_out, user_meta_out);

    user_meta_out.moveTo( rec_out.meta );

    rec_out.seqNum = doc->seqnum;
    rec_out.type = (is_tombstone_out || doc->deleted)
                   ? Record::DELETION
                   : Record::INSERTION;

    return Status();
}

Status TableFile::Iterator::getMeta(Record& rec_out,
                                    size_t& valuelen_out,
                                    uint64_t& offset_out)
{
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;

    fdb_doc tmp_doc;
    memset(&tmp_doc, 0x0, sizeof(tmp_doc));

    fdb_doc *doc = &tmp_doc;
    fs = fdb_iterator_get_metaonly(fdbItr, &doc);
    if (fs != FDB_RESULT_SUCCESS) {
        return Status::ERROR;
    }

    rec_out.kv.key.set(doc->keylen, doc->key);
    rec_out.kv.key.setNeedToFree();
    valuelen_out = doc->bodylen;
    offset_out = doc->offset;

    // Decode meta.
    SizedBuf user_meta_out;
    SizedBuf raw_meta(doc->metalen, doc->meta);;
    SizedBuf::Holder h_raw_meta(raw_meta); // auto free raw meta.
    raw_meta.setNeedToFree();
    bool is_tombstone_out = false;
    TableFile::rawMetaToUserMeta(raw_meta, is_tombstone_out, user_meta_out);

    user_meta_out.moveTo( rec_out.meta );

    rec_out.seqNum = doc->seqnum;
    rec_out.type = (is_tombstone_out || doc->deleted)
                   ? Record::DELETION
                   : Record::INSERTION;

    return Status();
}

Status TableFile::Iterator::prev() {
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;
    fs = fdb_iterator_prev(fdbItr);
    if (fs != FDB_RESULT_SUCCESS) {
        fs = fdb_iterator_next(fdbItr);
        assert(fs == FDB_RESULT_SUCCESS);
        return Status::OUT_OF_RANGE;
    }
    return Status();
}

Status TableFile::Iterator::next() {
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;
    fs = fdb_iterator_next(fdbItr);
    if (fs != FDB_RESULT_SUCCESS) {
        fs = fdb_iterator_prev(fdbItr);
        assert(fs == FDB_RESULT_SUCCESS);
        return Status::OUT_OF_RANGE;
    }
    return Status();
}

Status TableFile::Iterator::seek(const SizedBuf& key, SeekOption opt) {
    if (key.empty()) return gotoBegin();

    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;
    fdb_iterator_seek_opt_t fdb_seek_opt =
        (opt == GREATER)
        ? FDB_ITR_SEEK_HIGHER
        : FDB_ITR_SEEK_LOWER;
    fs = fdb_iterator_seek(fdbItr, key.data, key.size, fdb_seek_opt);
    if (fs != FDB_RESULT_SUCCESS) {
        if (opt == GREATER) {
            fs = fdb_iterator_seek_to_max(fdbItr);
        } else {
            fs = fdb_iterator_seek_to_min(fdbItr);
        }
        if (fs != FDB_RESULT_SUCCESS) return Status::OUT_OF_RANGE;
    }
    return Status();
}

Status TableFile::Iterator::seekSN(const uint64_t seqnum, SeekOption opt) {
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;
    fdb_iterator_seek_opt_t fdb_seek_opt =
        (opt == GREATER)
        ? FDB_ITR_SEEK_HIGHER
        : FDB_ITR_SEEK_LOWER;
    fs = fdb_iterator_seek_byseq(fdbItr, seqnum, fdb_seek_opt);
    if (fs != FDB_RESULT_SUCCESS) {
        if (opt == GREATER) {
            fs = fdb_iterator_seek_to_max(fdbItr);
        } else {
            fs = fdb_iterator_seek_to_min(fdbItr);
        }
        assert(fs == FDB_RESULT_SUCCESS);
    }
    return Status();
}

Status TableFile::Iterator::gotoBegin() {
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;
    fs = fdb_iterator_seek_to_min(fdbItr);
    (void)fs;
    return Status();
}

Status TableFile::Iterator::gotoEnd() {
    if (!tFile || !fdbItr) return Status::NOT_INITIALIZED;

    fdb_status fs;
    fs = fdb_iterator_seek_to_max(fdbItr);
    (void)fs;
    return Status();
}

Status TableFile::Iterator::close() {
    if (fdbItr) {
        fdb_iterator_close(fdbItr);
        fdbItr = nullptr;
    }
    if (fdbSnap) {
        fdb_kvs_close(fdbSnap);
        fdbSnap = nullptr;
    }
    if (tFileSnap) {
        tFile->returnSnapshot(tFileSnap);
        tFileSnap = nullptr;
    }
    return Status();
}

}; // namespace jungle

