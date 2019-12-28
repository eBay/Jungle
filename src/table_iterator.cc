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

#include "table_mgr.h"

#include "db_internal.h"

namespace jungle {

// === Iterator

TableMgr::Iterator::Iterator()
    : tMgr(nullptr),
      snapTableList(nullptr),
      minSeqSnap(NOT_INITIALIZED),
      maxSeqSnap(NOT_INITIALIZED)
{
    avl_init(&curWindow, nullptr);
}

TableMgr::Iterator::~Iterator() {
    close();
}

void TableMgr::Iterator::addTableItr(DB* snap_handle, TableInfo* t_info) {
    // Open iterator.
    TableFile::Iterator* t_itr = new TableFile::Iterator();
    if (type == BY_SEQ) {
        t_itr->initSN(snap_handle, t_info->file, minSeqSnap, maxSeqSnap);
    } else if (type == BY_KEY) {
        t_itr->init(snap_handle, t_info->file, startKey, endKey);
    }

    ItrItem* ctx = new ItrItem();
    ctx->tInfo = t_info;
    ctx->tItr = t_itr;
    // If this iterator is out-of-range, `lastRec` will be empty.
    Status s = t_itr->get(ctx->lastRec);
    if (s) {
        // Insert available iterator only.
        avl_cmp_func* cmp_func = (type == BY_SEQ)
                                 ? (ItrItem::cmpSeq)
                                 : (ItrItem::cmpKey);
        avl_insert(&curWindow, &ctx->an, cmp_func);
    }
    tables[t_info->level].push_back(ctx);
}

Status TableMgr::Iterator::init(DB* snap_handle,
                                TableMgr* table_mgr,
                                const SizedBuf& start_key,
                                const SizedBuf& end_key)
{
    uint64_t empty_seq = NOT_INITIALIZED;
    return initInternal(snap_handle, table_mgr,
                        empty_seq, empty_seq,
                        start_key, end_key,
                        BY_KEY);
}

Status TableMgr::Iterator::initSN(DB* snap_handle,
                                  TableMgr* table_mgr,
                                  uint64_t min_seq,
                                  uint64_t max_seq)
{
    SizedBuf empty_key;
    return initInternal(snap_handle, table_mgr,
                        min_seq, max_seq,
                        empty_key, empty_key,
                        BY_SEQ);
}

Status TableMgr::Iterator::initInternal(DB* snap_handle,
                                        TableMgr* table_mgr,
                                        uint64_t min_seq,
                                        uint64_t max_seq,
                                        const SizedBuf& start_key,
                                        const SizedBuf& end_key,
                                        TableMgr::Iterator::Type _type)
{
    if (table_mgr->getDbConfig()->logSectionOnly)
        return Status::TABLES_ARE_DISABLED;

    tMgr = table_mgr;
    type = _type;
    if (tMgr->getDbConfig()->cmpFunc) {
        // Custom cmp mode.
        avl_set_aux(&curWindow, (void*)tMgr);
    }

    minSeqSnap = min_seq;
    maxSeqSnap = max_seq;
    if ( snap_handle &&
         maxSeqSnap > snap_handle->sn->chkNum ) {
        maxSeqSnap = snap_handle->sn->chkNum;
        if (!valid_number(min_seq)) minSeqSnap = 0;
    }
    startKey.alloc(start_key);
    endKey.alloc(end_key);

   try {
    size_t n_levels = tMgr->mani->getNumLevels();
    tables.resize(n_levels);

    if (snap_handle) {
        // Snapshot
        assert(snap_handle->sn->tableList);
        for (auto& entry: *snap_handle->sn->tableList) {
            TableInfo* t_info = entry;
            addTableItr(snap_handle, t_info);
        }
        snapTableList = snap_handle->sn->tableList;
    } else {
        // Normal
        for (size_t ii=0; ii<n_levels; ++ii) {
            std::list<TableInfo*> t_info_ret;
            SizedBuf empty_key;
            if (ii == 0) {
                tMgr->mani->getTablesRange(ii, empty_key, empty_key, t_info_ret);
            } else {
                tMgr->mani->getTablesRange(ii, start_key, end_key, t_info_ret);
            }

            for (auto& entry: t_info_ret) {
                TableInfo* t_info = entry;
                addTableItr(snap_handle, t_info);
            }
        }
    }

    windowCursor = avl_first(&curWindow);
    if (!windowCursor) throw Status(Status::OUT_OF_RANGE);
    return Status();

   } catch (Status s) {
    startKey.free();
    endKey.free();
    return s;
   }
}

Status TableMgr::Iterator::get(Record& rec_out) {
    if (!windowCursor) return Status::KEY_NOT_FOUND;

    ItrItem* item = _get_entry(windowCursor, ItrItem, an);
    rec_out = item->lastRec;
    return Status();
}

Status TableMgr::Iterator::prev() {
    Status s;

    ItrItem* cur_item = _get_entry(windowCursor, ItrItem, an);
    uint64_t cur_seq = cur_item->lastRec.seqNum;
    SizedBuf cur_key;
    cur_key.alloc(cur_item->lastRec.kv.key);

    // Do next() for all iterators GTEQ windowCursor.
    avl_node* cursor = avl_last(&curWindow);
    while (cursor) {
        ItrItem* item = _get_entry(cursor, ItrItem, an);
        if (item->flags & ItrItem::no_more_prev) {
            s = Status::ERROR;
        } else {
            if ( type == BY_SEQ &&
                 item->lastRec.seqNum < cur_seq) break;
            if ( type == BY_KEY &&
                 cmpSizedBuf(item->lastRec.kv.key, cur_key) < 0 ) break;
            s = item->tItr->prev();
        }

        if (s) {
            avl_remove(&curWindow, &item->an);
            item->flags = ItrItem::none;
            item->lastRec.free();
            s = item->tItr->get(item->lastRec);
            assert(s);

            avl_cmp_func* cmp_func = (type == BY_SEQ)
                                     ? (ItrItem::cmpSeq)
                                     : (ItrItem::cmpKey);
            avl_insert(&curWindow, &item->an, cmp_func);
            cursor = avl_last(&curWindow);
        } else {
            item->flags |= ItrItem::no_more_prev;
            cursor = avl_prev(&item->an);
        }
    }

    windowCursor = avl_last(&curWindow);
    ItrItem* last_valid_item = nullptr;
    while (windowCursor) {
        // Find *LAST* valid item (only for BY_KEY).
        ItrItem* item = _get_entry(windowCursor, ItrItem, an);

        bool valid = false;
        if (type == BY_SEQ) {
            valid = checkValidBySeq(item, cur_seq, true);
            if (!valid) windowCursor = avl_prev(windowCursor);
            else break;

        } else if (type == BY_KEY) {
            valid = checkValidByKey(item, cur_key, true);
            if (last_valid_item &&
                cmpSizedBuf(item->lastRec.kv.key,
                            last_valid_item->lastRec.kv.key) < 0) break;
            if (valid) last_valid_item = item;
            windowCursor = avl_prev(windowCursor);
        }
    }

    if (last_valid_item) windowCursor = &last_valid_item->an;

    cur_key.free();
    if (!windowCursor) {
        // Reached the end.
        windowCursor = avl_first(&curWindow);
        return Status::OUT_OF_RANGE;
    }
    return Status();
}

Status TableMgr::Iterator::next() {
    Status s;

    ItrItem* cur_item = _get_entry(windowCursor, ItrItem, an);
    uint64_t cur_seq = cur_item->lastRec.seqNum;
    SizedBuf cur_key;
    cur_key.alloc(cur_item->lastRec.kv.key);

    // Do next() for all iterators SMEQ windowCursor.
    avl_node* cursor = avl_first(&curWindow);
    while (cursor) {
        ItrItem* item = _get_entry(cursor, ItrItem, an);
        if (item->flags & ItrItem::no_more_next) {
            s = Status::ERROR;
        } else {
            if ( type == BY_SEQ &&
                 item->lastRec.seqNum > cur_seq) break;
            if ( type == BY_KEY &&
                 cmpSizedBuf(item->lastRec.kv.key, cur_key) > 0 ) break;
            s = item->tItr->next();
        }

        if (s) {
            avl_remove(&curWindow, &item->an);
            item->flags = ItrItem::none;
            item->lastRec.free();
            s = item->tItr->get(item->lastRec);
            assert(s);

            avl_cmp_func* cmp_func = (type == BY_SEQ)
                                     ? (ItrItem::cmpSeq)
                                     : (ItrItem::cmpKey);
            avl_insert(&curWindow, &item->an, cmp_func);
            cursor = avl_first(&curWindow);
        } else {
            item->flags |= ItrItem::no_more_next;
            cursor = avl_next(&item->an);
        }
    }

    windowCursor = avl_first(&curWindow);
    while (windowCursor) {
        // Find first valid item.
        ItrItem* item = _get_entry(windowCursor, ItrItem, an);

        bool valid = false;
        if (type == BY_SEQ) {
            valid = checkValidBySeq(item, cur_seq);
        } else if (type == BY_KEY) {
            valid = checkValidByKey(item, cur_key);
        }

        if (!valid) {
            windowCursor = avl_next(windowCursor);
        } else {
            break;
        }
    }

    cur_key.free();
    if (!windowCursor) {
        // Reached the end.
        windowCursor = avl_last(&curWindow);
        return Status::OUT_OF_RANGE;
    }
    return Status();
}

Status TableMgr::Iterator::seek(const SizedBuf& key, SeekOption opt) {
    return seekInternal(key, NOT_INITIALIZED, opt);
}

Status TableMgr::Iterator::seekSN(const uint64_t seqnum, SeekOption opt) {
    SizedBuf dummy_key;
    return seekInternal(dummy_key, seqnum, opt);
}

Status TableMgr::Iterator::gotoBegin() {
    SizedBuf empty_key;
    return seekInternal(empty_key, 0, GREATER);
}

Status TableMgr::Iterator::gotoEnd() {
    SizedBuf empty_key;
    return seekInternal(empty_key, 0, SMALLER, true);
}


Status TableMgr::Iterator::seekInternal
       ( const SizedBuf& key,
         const uint64_t seqnum,
         SeekOption opt,
         bool goto_end )
{
    Status s;

    // Remove current items from `curWindow`.
    std::vector<ItrItem*> items;
    avl_node* cursor = avl_first(&curWindow);
    while (cursor) {
        ItrItem* item = _get_entry(cursor, ItrItem, an);
        cursor = avl_next(&item->an);
        avl_remove(&curWindow, &item->an);
        items.push_back(item);
    }

    // Seek for all items.
    for (auto& entry: items) {
        ItrItem*& item = entry;

        if (goto_end) {
            // Goto end: special case.
            s = item->tItr->gotoEnd();

        } else {
            if (type == BY_SEQ) {
                s = item->tItr->seekSN(seqnum, (TableFile::Iterator::SeekOption)opt);
            } else {
                s = item->tItr->seek(key, (TableFile::Iterator::SeekOption)opt);
            }
        }

        if (s) {
            item->lastRec.free();
            s = item->tItr->get(item->lastRec);
        }
        if (s) {
            int cmp = 0;
            if (goto_end) {
                // Goto end: special case.
                cmp = -1;

            } else {
                if (type == BY_SEQ) {
                    if (item->lastRec.seqNum < seqnum) cmp = -1;
                    else if (item->lastRec.seqNum > seqnum) cmp = 1;
                    else cmp = 0;
                } else {
                    cmp = cmpSizedBuf(item->lastRec.kv.key, key);
                }
            }


            item->flags = ItrItem::none;
            if (opt == GREATER && cmp < 0) {
                item->flags |= ItrItem::no_more_next;
            } else if (opt == SMALLER && cmp > 0) {
                item->flags |= ItrItem::no_more_prev;
            }
        } else {
            item->flags = ItrItem::no_more_prev |
                          ItrItem::no_more_next;
        }

        avl_cmp_func* cmp_func = (type == BY_SEQ)
                                 ? (ItrItem::cmpSeq)
                                 : (ItrItem::cmpKey);
        avl_node* avl_ret = avl_insert(&curWindow, &item->an, cmp_func);
        assert(avl_ret == &item->an);
        (void)avl_ret;
    }

    if (opt == GREATER) {
        windowCursor = avl_first(&curWindow);
        while (windowCursor) {
            // Find first valid item.
            ItrItem* item = _get_entry(windowCursor, ItrItem, an);

            if (item->flags == ItrItem::none) break;
            else windowCursor = avl_next(windowCursor);
        }
    } else { // SMALLER
        windowCursor = avl_last(&curWindow);
        while (windowCursor) {
            // Find *LAST* valid item (only for BY_KEY).
            ItrItem* item = _get_entry(windowCursor, ItrItem, an);

            if (item->flags == ItrItem::none) break;
            else windowCursor = avl_prev(windowCursor);
        }
    }

    if (!windowCursor) {
        // Reached the end.
        if (opt == GREATER) windowCursor = avl_last(&curWindow);
        if (opt == SMALLER) windowCursor = avl_first(&curWindow);
    }
    return Status();
}


int TableMgr::Iterator::cmpSizedBuf(const SizedBuf& l, const SizedBuf& r) {
    CMP_NULL_CHK(l.data, r.data);
    if (tMgr->getDbConfig()->cmpFunc) {
        // Custom cmp mode.
        CustomCmpFunc func = tMgr->getDbConfig()->cmpFunc;
        void* param = tMgr->getDbConfig()->cmpFuncParam;
        return func(l.data, l.size, r.data, r.size, param);
    }
    return SizedBuf::cmp(l, r);
}

bool TableMgr::Iterator::checkValidBySeq(ItrItem* item,
                                         const uint64_t cur_seq,
                                         const bool is_prev)
{
    if ( ( !is_prev && (item->flags & ItrItem::no_more_next) ) ||
         (  is_prev && (item->flags & ItrItem::no_more_prev) ) ) {
        return false;
    } else if (item->lastRec.seqNum == cur_seq) {
        // Duplicate item, skip.
        return false;
    }
    return true;
}

bool TableMgr::Iterator::checkValidByKey(ItrItem* item,
                                         const SizedBuf& cur_key,
                                         const bool is_prev)
{
    if ( ( !is_prev && (item->flags & ItrItem::no_more_next) ) ||
         (  is_prev && (item->flags & ItrItem::no_more_prev) ) ) {
        return false;
    } else if (cmpSizedBuf(item->lastRec.kv.key, cur_key) == 0) {
        // Duplicate item, skip.
        return false;
    }
    return true;
}


Status TableMgr::Iterator::close() {
    if (!tMgr) return Status();

    avl_node* cursor = avl_first(&curWindow);
    while (cursor) {
        ItrItem* item = _get_entry(cursor, ItrItem, an);
        cursor = avl_next(&item->an);
        avl_remove(&curWindow, &item->an);
    }

    for (auto& _level: tables) {
        for (auto& _table: _level) {
            ItrItem* ctx = _table;
            ctx->tItr->close();
            ctx->lastRec.free();
            delete ctx->tItr;
            if (!snapTableList) {
                // Only when not a snapshot.
                ctx->tInfo->done();
            }
            delete ctx;
        }
    }

    tMgr = nullptr;
    windowCursor = nullptr;
    startKey.free();
    endKey.free();

    return Status();
}

}; // namespace jungle
