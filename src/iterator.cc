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

#include "db_internal.h"

#include <libjungle/jungle.h>

namespace jungle {

Iterator::Iterator() : p(new IteratorInternal(this)) {}
Iterator::~Iterator() {
    close();
    delete p;
}

Status Iterator::init(DB* dd,
                      const SizedBuf& start_key,
                      const SizedBuf& end_key)
{
    Status s;
    EP( dd->p->checkHandleValidity() );

    p->db = dd;
    p->type = ItrInt::BY_KEY;
    if (p->db->p->dbConfig.cmpFunc) {
        // Custom cmp mode.
        avl_set_aux(&p->curWindow, (void*)p->db);
    }

    // LogMgr iterator
    ItrInt::ItrItem* ctx_log = new ItrInt::ItrItem();
    ctx_log->logItr = new LogMgr::Iterator();
    s = ctx_log->logItr->init( (dd->sn) ? dd : nullptr,
                               p->db->p->logMgr,
                               start_key,
                               end_key );
    if (s) s = ctx_log->logItr->get(ctx_log->lastRec);
    if (s) {
        avl_node* avl_ret =
            avl_insert(&p->curWindow, &ctx_log->an, ItrInt::ItrItem::cmpKey);
        assert(avl_ret == &ctx_log->an);
        (void)avl_ret;
    }
    p->itrs.push_back(ctx_log);

    // TableMgr iterator
    ItrInt::ItrItem* ctx_table = new ItrInt::ItrItem();
    ctx_table->tableItr = new TableMgr::Iterator();
    s = ctx_table->tableItr->init( (dd->sn) ? dd : nullptr,
                                   p->db->p->tableMgr,
                                   start_key,
                                   end_key );
    if (s == Status::CHECKSUM_ERROR || s == Status::FILE_CORRUPTION) {
        // Intolerable error.
        DELETE(ctx_table->tableItr);
        DELETE(ctx_table);
        close();
        return s;
    }

    if (s) s = ctx_table->tableItr->get(ctx_table->lastRec);
    if (s) {
        avl_node* avl_ret =
            avl_insert(&p->curWindow, &ctx_table->an, ItrInt::ItrItem::cmpKey);
        assert(avl_ret == &ctx_table->an);
        (void)avl_ret;
    }
    p->itrs.push_back(ctx_table);

    p->windowCursor = avl_first(&p->curWindow);

    // NOTE:
    //  Even though this is an empty iterator (i.e., p->windowCursor == NULL),
    //  return OK for now, but all other API will not work.
    if (p->type == ItrInt::BY_KEY) {
        ItrInt::ItrItem* cur_item =
            _get_entry(p->windowCursor, ItrInt::ItrItem, an);
        while (cur_item && !cur_item->lastRec.isIns()) {
            s = next();
            if (!s) return Status();
            cur_item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);
        }
    }

    return Status();
}

Status Iterator::initSN(DB* db,
                        const uint64_t min_seq,
                        const uint64_t max_seq)
{
    Status s;
    EP( db->p->checkHandleValidity() );

    p->db = db;
    p->type = ItrInt::BY_SEQ;
    if (p->db->p->dbConfig.cmpFunc) {
        // Custom cmp mode.
        avl_set_aux(&p->curWindow, (void*)p->db);
    }

    // LogMgr iterator
    ItrInt::ItrItem* ctx_log = new ItrInt::ItrItem();
    ctx_log->logItr = new LogMgr::Iterator();
    s = ctx_log->logItr->initSN( (db->sn) ? db : nullptr,
                                 p->db->p->logMgr,
                                 min_seq,
                                 max_seq );
    if (s) s = ctx_log->logItr->get(ctx_log->lastRec);
    if (s) {
        avl_node* ret =
            avl_insert(&p->curWindow, &ctx_log->an, ItrInt::ItrItem::cmpSeq);
        assert(ret == &ctx_log->an);
        (void)ret;
    }
    p->itrs.push_back(ctx_log);

    // TableMgr iterator
    ItrInt::ItrItem* ctx_table = new ItrInt::ItrItem();
    ctx_table->tableItr = new TableMgr::Iterator();
    s = ctx_table->tableItr->initSN( (db->sn) ? db : nullptr,
                                     p->db->p->tableMgr,
                                     min_seq,
                                     max_seq );
    if (s) s = ctx_table->tableItr->get(ctx_table->lastRec);
    if (s) {
        avl_node* ret =
            avl_insert(&p->curWindow, &ctx_table->an, ItrInt::ItrItem::cmpSeq);
        assert(ret == &ctx_table->an);
        (void)ret;
    }
    p->itrs.push_back(ctx_table);

    p->windowCursor = avl_first(&p->curWindow);

    // NOTE:
    //  Same as in init(), allow empty iterator.
    return Status();
}

Status Iterator::get(Record& rec_out) {
    if (!p->fatalError.ok()) return p->fatalError;
    if (!p || !p->db) return Status::NOT_INITIALIZED;
    if (!p->windowCursor) return Status::KEY_NOT_FOUND;
    if (p && p->db) p->db->p->updateOpHistory();

    Status s;
    ItrInt::ItrItem* item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);
    if ( p->type == ItrInt::BY_KEY &&
         !item->lastRec.isIns() ) {
        // by-key: only allow insertion record.
        return Status::KEY_NOT_FOUND;
    }
    item->lastRec.copyTo(rec_out);
    return Status();
}

Status Iterator::prev() {
    if (!p->fatalError.ok()) return p->fatalError;
    if (!p || !p->db) return Status::NOT_INITIALIZED;
    if (!p->windowCursor) return Status::OUT_OF_RANGE;
    if (p && p->db) p->db->p->updateOpHistory();

   // Due to deleted key, it may include multiple steps.
   for (;;) {
    ItrInt::ItrItem* cur_item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);
    uint64_t cur_seq = cur_item->lastRec.seqNum;
    SizedBuf cur_key;
    cur_key.alloc(cur_item->lastRec.kv.key);

    Status s;
    avl_node* cursor = avl_last(&p->curWindow);
    while (cursor) {
        ItrInt::ItrItem* item = _get_entry(cursor, ItrInt::ItrItem, an);
        if (item->flags & ItrInt::ItrItem::no_more_prev) {
            s = Status::ERROR;
        } else {
            if ( p->type == ItrInt::BY_SEQ &&
                 item->lastRec.seqNum < cur_seq ) break;
            if ( p->type == ItrInt::BY_KEY &&
                 p->cmpSizedBuf(item->lastRec.kv.key, cur_key) < 0 ) break;

            if (item->logItr) s = item->logItr->prev();
            if (item->tableItr) s = item->tableItr->prev();
        }

        if (s) {
            avl_remove(&p->curWindow, &item->an);
            item->flags = ItrInt::ItrItem::none;
            if (item->logItr) {
                s = item->logItr->get(item->lastRec);
            }
            if (item->tableItr) {
                s = item->tableItr->get(item->lastRec);
            }

            avl_cmp_func* cmp_func = (p->type == ItrInt::BY_SEQ)
                                     ? (ItrInt::ItrItem::cmpSeq)
                                     : (ItrInt::ItrItem::cmpKey);
            avl_node* avl_ret = avl_insert(&p->curWindow, &item->an, cmp_func);

            if (s == Status::CHECKSUM_ERROR || s == Status::FILE_CORRUPTION) {
                // Intolerable error.
                p->fatalError = s;
                cur_key.free();

                // To make next `get()` call return error,
                // return this function without error.
                return Status();
            }

            assert(avl_ret == &item->an);
            (void)avl_ret;
            cursor = avl_last(&p->curWindow);
        } else {
            item->flags |= ItrInt::ItrItem::no_more_prev;
            cursor = avl_prev(&item->an);
        }
    }

    p->windowCursor = avl_last(&p->curWindow);
    ItrInt::ItrItem* last_valid_item = nullptr;
    while (p->windowCursor) {
        // Find *LAST* valid item (only for BY_KEY).
        ItrInt::ItrItem* item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);

        bool valid = false;
        if (p->type == ItrInt::BY_SEQ) {
            valid = p->checkValidBySeq(item, cur_seq, true);
            if (!valid) p->windowCursor = avl_prev(p->windowCursor);
            else break;

        } else if (p->type == ItrInt::BY_KEY) {
            valid = p->checkValidByKey(item, cur_key, true);
            if (last_valid_item &&
                p->cmpSizedBuf(item->lastRec.kv.key,
                               last_valid_item->lastRec.kv.key) < 0) break;
            if (valid) last_valid_item = item;
            p->windowCursor = avl_prev(p->windowCursor);
        }
    }

    if (last_valid_item) p->windowCursor = &last_valid_item->an;

    cur_key.free();

    if (!p->windowCursor) {
        // Reached the end.
        p->windowCursor = avl_first(&p->curWindow);
        return Status::OUT_OF_RANGE;
    }

    if (p->type == ItrInt::BY_KEY) {
        cur_item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);
        if ( !cur_item->lastRec.isIns() ) {
            // Deleted key, move further.
            continue;
        }
    }
    break;
   }

    return Status();
}

Status Iterator::next() {
    if (!p->fatalError.ok()) return p->fatalError;
    if (!p || !p->db) return Status::NOT_INITIALIZED;
    if (!p->windowCursor) return Status::OUT_OF_RANGE;
    if (p && p->db) p->db->p->updateOpHistory();

   // Due to deleted key, it may include multiple steps.
   for (;;) {
    ItrInt::ItrItem* cur_item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);
    uint64_t cur_seq = cur_item->lastRec.seqNum;
    SizedBuf cur_key;
    cur_key.alloc(cur_item->lastRec.kv.key);

    Status s;
    avl_node* cursor = avl_first(&p->curWindow);
    while (cursor) {
        ItrInt::ItrItem* item = _get_entry(cursor, ItrInt::ItrItem, an);
        if (item->flags & ItrInt::ItrItem::no_more_next) {
            s = Status::ERROR;
        } else {
            if ( p->type == ItrInt::BY_SEQ &&
                 item->lastRec.seqNum > cur_seq ) break;
            if ( p->type == ItrInt::BY_KEY &&
                 p->cmpSizedBuf(item->lastRec.kv.key, cur_key) > 0 ) break;

            if (item->logItr) s = item->logItr->next();
            if (item->tableItr) s = item->tableItr->next();
        }

        if (s) {
            avl_remove(&p->curWindow, &item->an);
            item->flags = ItrInt::ItrItem::none;
            if (item->logItr) {
                s = item->logItr->get(item->lastRec);
            }
            if (item->tableItr) {
                s = item->tableItr->get(item->lastRec);
            }

            avl_cmp_func* cmp_func = (p->type == ItrInt::BY_SEQ)
                                     ? (ItrInt::ItrItem::cmpSeq)
                                     : (ItrInt::ItrItem::cmpKey);
            avl_node* avl_ret = avl_insert(&p->curWindow, &item->an, cmp_func);

            if (s == Status::CHECKSUM_ERROR || s == Status::FILE_CORRUPTION) {
                // Intolerable error.
                p->fatalError = s;
                cur_key.free();

                // To make next `get()` call return error,
                // return this function without error.
                return Status();
            }

            assert(avl_ret == &item->an);
            (void)avl_ret;
            cursor = avl_first(&p->curWindow);
        } else {
            item->flags |= ItrInt::ItrItem::no_more_next;
            cursor = avl_next(&item->an);
        }
    }

    p->windowCursor = avl_first(&p->curWindow);
    while (p->windowCursor) {
        // Find first valid item.
        ItrInt::ItrItem* item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);

        bool valid = false;
        if (p->type == ItrInt::BY_SEQ) {
            valid = p->checkValidBySeq(item, cur_seq);
        } else if (p->type == ItrInt::BY_KEY) {
            valid = p->checkValidByKey(item, cur_key);
        }

        if (!valid) {
            p->windowCursor = avl_next(p->windowCursor);
        } else {
            break;
        }
    }
    cur_key.free();

    if (!p->windowCursor) {
        // Reached the end.
        p->moveToLastValid();
        return Status::OUT_OF_RANGE;
    }

    if (p->type == ItrInt::BY_KEY) {
        cur_item = _get_entry(p->windowCursor, ItrInt::ItrItem, an);
        if ( !cur_item->lastRec.isIns() ) {
            // Deleted key, move further.
            continue;
        }
    }
    break;
   }

    return Status();
}

Status Iterator::seek(const SizedBuf& key, SeekOption opt) {
    return p->seekInternal(key, NOT_INITIALIZED, opt);
}

Status Iterator::seekSN(const uint64_t seqnum, SeekOption opt) {
    SizedBuf dummy_key;
    return p->seekInternal(dummy_key, seqnum, opt);
}

Status Iterator::gotoBegin() {
    SizedBuf empty_key;
    return p->seekInternal(empty_key, 0, GREATER);
}

Status Iterator::gotoEnd() {
    SizedBuf empty_key;
    return p->seekInternal(empty_key, 0, SMALLER, true);
}

Status Iterator::IteratorInternal::moveToLastValid() {
    windowCursor = avl_last(&curWindow);
    while (windowCursor) {
        // Find *LAST* valid item (only for BY_KEY).
        //
        // e.g.)
        //  ... Del K9 (seq 100), Ins K9 (seq 99)
        //  We should pick up `Del K9`.
        ItrInt::ItrItem* item = _get_entry(windowCursor, ItrInt::ItrItem, an);

        if (type == ItrInt::BY_KEY) {
            ItrInt::ItrItem* prev_item = nullptr;
            avl_node* prev_cursor = avl_prev(windowCursor);
            if (prev_cursor) prev_item = _get_entry(prev_cursor, ItrItem, an);

            if (prev_item) {
                int cmp = cmpSizedBuf( item->lastRec.kv.key,
                                       prev_item->lastRec.kv.key );
                if (cmp == 0) {
                    // Same key, should take previous one.
                    windowCursor = prev_cursor;
                    continue;
                }
            }
        }
        break;
#if 0
        if (item->flags == ItrItem::none) break;
        else windowCursor = avl_prev(windowCursor);
#endif
    }
    return Status();
}

Status Iterator::IteratorInternal::seekInternal
       ( const SizedBuf& key,
         const uint64_t seqnum,
         SeekOption opt,
         bool goto_end )
{
    // Empty iterator: do nothing.
    if (!windowCursor) return Status();

    Status s;

    // Remove current items from `curWindow`.
    std::vector<ItrInt::ItrItem*> items;
    avl_node* cursor = avl_first(&curWindow);
    while (cursor) {
        ItrInt::ItrItem* item = _get_entry(cursor, ItrInt::ItrItem, an);
        cursor = avl_next(&item->an);
        avl_remove(&curWindow, &item->an);
        items.push_back(item);
    }

    // Seek for all items.
    for (auto& entry: items) {
        ItrInt::ItrItem*& item = entry;
        if (item->logItr) {
            if (goto_end) {
                s = item->logItr->gotoEnd();
            } else {
                if (type == ItrInt::BY_SEQ) {
                    s = item->logItr->seekSN(seqnum, (LogMgr::Iterator::SeekOption)opt);
                } else {
                    s = item->logItr->seek(key, (LogMgr::Iterator::SeekOption)opt);
                }
            }

        } else {
            if (goto_end) {
                s = item->tableItr->gotoEnd();
            } else {
                if (type == ItrInt::BY_SEQ) {
                    s = item->tableItr->seekSN(seqnum, (TableMgr::Iterator::SeekOption)opt);
                } else {
                    s = item->tableItr->seek(key, (TableMgr::Iterator::SeekOption)opt);
                }
            }
        }

        if (s) {
            if (item->logItr) {
                s = item->logItr->get(item->lastRec);
            }
            if (item->tableItr) {
                s = item->tableItr->get(item->lastRec);
            }
            assert(s);

            int cmp = 0;
            if (goto_end) {
                // Goto end: special case.
                cmp = -1;
            } else {
                if (type == ItrInt::BY_SEQ) {
                    if (item->lastRec.seqNum < seqnum) cmp = -1;
                    else if (item->lastRec.seqNum > seqnum) cmp = 1;
                    else cmp = 0;
                } else {
                    cmp = ItrInt::cmpSizedBuf(item->lastRec.kv.key, key);
                }
            }

            item->flags = ItrInt::ItrItem::none;
            if (opt == GREATER && cmp < 0) {
                item->flags |= ItrInt::ItrItem::no_more_next;
            } else if (opt == SMALLER && cmp > 0) {
                item->flags |= ItrInt::ItrItem::no_more_prev;
            }
        } else {
            item->flags = ItrInt::ItrItem::no_more_prev |
                          ItrInt::ItrItem::no_more_next;
        }

        avl_cmp_func* cmp_func = (type == ItrInt::BY_SEQ)
                                 ? (ItrInt::ItrItem::cmpSeq)
                                 : (ItrInt::ItrItem::cmpKey);
        avl_node* avl_ret = avl_insert(&curWindow, &item->an, cmp_func);
        assert(avl_ret == &item->an);
        (void)avl_ret;
    }

    if (opt == GREATER) {
        windowCursor = avl_first(&curWindow);
        while (windowCursor) {
            // Find first valid item.
            ItrInt::ItrItem* item = _get_entry(windowCursor, ItrInt::ItrItem, an);

            if (item->flags == ItrItem::none) break;
            else windowCursor = avl_next(windowCursor);
        }
    } else { // SMALLER
        moveToLastValid();
    }

    if (!windowCursor) {
        // Reached the end.
        if (opt == GREATER) windowCursor = avl_last(&curWindow);
        if (opt == SMALLER) windowCursor = avl_first(&curWindow);
    }

    if (type == BY_KEY) {
        ItrItem* item = _get_entry(windowCursor, ItrItem, an);
        while ( !item->lastRec.isIns() ) {
            // Deleted key, move the cursor.
            if (opt == GREATER) s = parent->next();
            if (opt == SMALLER) s = parent->prev();

            // NOTE: no key exists, should we return OK?
            // if (!s) return s;
            if (!s) break;;
            item = _get_entry(windowCursor, ItrItem, an);
        }
    }

    return Status();
}


int Iterator::IteratorInternal::cmpSizedBuf(const SizedBuf& l, const SizedBuf& r) {
    CMP_NULL_CHK(l.data, r.data);
    if (db->p->dbConfig.cmpFunc) {
        // Custom cmp mode.
        CustomCmpFunc func = db->p->dbConfig.cmpFunc;
        void* param = db->p->dbConfig.cmpFuncParam;
        return func(l.data, l.size, r.data, r.size, param);
    }
    return SizedBuf::cmp(l, r);
}

bool Iterator::IteratorInternal::checkValidBySeq(ItrInt::ItrItem* item,
                                                 const uint64_t cur_seq,
                                                 const bool is_prev)
{
    if ( ( !is_prev && (item->flags & ItrInt::ItrItem::no_more_next) ) ||
         (  is_prev && (item->flags & ItrInt::ItrItem::no_more_prev) ) ) {
        return false;
    } else if (item->lastRec.seqNum == cur_seq) {
        // Duplicate item, skip.
        return false;
    }
    return true;
}

bool Iterator::IteratorInternal::checkValidByKey(ItrInt::ItrItem* item,
                                                 const SizedBuf& cur_key,
                                                 const bool is_prev)
{
    if ( ( !is_prev && (item->flags & ItrInt::ItrItem::no_more_next) ) ||
         (  is_prev && (item->flags & ItrInt::ItrItem::no_more_prev) ) ) {
        return false;
    } else if (cmpSizedBuf(item->lastRec.kv.key, cur_key) == 0) {
        // Duplicate item, skip.
        return false;
    }
    return true;
}

Status Iterator::close() {
    if (p) {
        avl_node* cursor = avl_first(&p->curWindow);
        while (cursor) {
            ItrInt::ItrItem* item = _get_entry(cursor, ItrInt::ItrItem, an);
            cursor = avl_next(&item->an);
            avl_remove(&p->curWindow, &item->an);
        }

        for (auto& entry: p->itrs) {
            ItrInt::ItrItem* item = entry;
            if (item->logItr) {
                item->logItr->close();
                delete item->logItr;
            }
            if (item->tableItr) {
                item->tableItr->close();
                delete item->tableItr;
            }
            delete item;
        }

        p->itrs.clear();
        p->db = nullptr;
        p->windowCursor = nullptr;
    }
    return Status();
}

}; // namespace jungle

