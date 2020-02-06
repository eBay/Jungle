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

#include "log_mgr.h"

#include "db_internal.h"

namespace jungle {

LogMgr::Iterator::Iterator()
    : lMgr(nullptr)
    , snapLogList(nullptr)
    , minSeqSnap(NOT_INITIALIZED)
    , maxSeqSnap(NOT_INITIALIZED)
    , windowCursor(nullptr)
{
    avl_init(&curWindow, nullptr);
}

LogMgr::Iterator::~Iterator() {
    close();
}

void LogMgr::Iterator::addLogFileItr(LogFileInfo* l_info) {
    LogFile::Iterator* l_itr = new LogFile::Iterator();
    if (type == BY_SEQ) {
        l_itr->initSN(l_info->file, minSeqSnap, maxSeqSnap);
    } else if (type == BY_KEY) {
        l_itr->init(l_info->file, startKey, endKey, maxSeqSnap);
    }

    ItrItem* ctx = new ItrItem();
    ctx->lInfo = l_info;
    ctx->lItr = l_itr;
    Status s = ctx->lItr->get(ctx->lastRec);
    if (s) {
        avl_cmp_func* cmp_func = (type == BY_SEQ)
                                 ? (ItrItem::cmpSeq)
                                 : (ItrItem::cmpKey);
        avl_node* avl_ret = avl_insert(&curWindow, &ctx->an, cmp_func);
        assert(avl_ret == &ctx->an); (void)avl_ret;
    }
    itrs.push_back(ctx);
}


Status LogMgr::Iterator::init(DB* snap_handle,
                              LogMgr* log_mgr,
                              const SizedBuf& start_key,
                              const SizedBuf& end_key)
{
    uint64_t empty_seq = NOT_INITIALIZED;
    return initInternal(snap_handle, log_mgr,
                        empty_seq, empty_seq,
                        start_key, end_key, BY_KEY);
}

Status LogMgr::Iterator::initSN(DB* snap_handle,
                                LogMgr* log_mgr,
                                uint64_t min_seq,
                                uint64_t max_seq)
{
    SizedBuf empty_key;
    return initInternal(snap_handle, log_mgr,
                        min_seq, max_seq,
                        empty_key, empty_key, BY_SEQ);
}

Status LogMgr::Iterator::initInternal(DB* snap_handle,
                                      LogMgr* log_mgr,
                                      uint64_t min_seq,
                                      uint64_t max_seq,
                                      const SizedBuf& start_key,
                                      const SizedBuf& end_key,
                                      LogMgr::Iterator::Type _type)
{
    // Save current seq number status.
    Status s;
    s = log_mgr->getAvailSeqRange(minSeqSnap, maxSeqSnap);
    // No log yet.
    if (!s) return s;

    if (valid_number(min_seq) && minSeqSnap < min_seq) minSeqSnap = min_seq;
    if (valid_number(max_seq) && max_seq < maxSeqSnap) maxSeqSnap = max_seq;
    if (snap_handle) {
        assert(snap_handle->sn);
        if (maxSeqSnap > snap_handle->sn->chkNum) {
            maxSeqSnap = snap_handle->sn->chkNum;
        }
        if (minSeqSnap > snap_handle->sn->lastFlush) {
            minSeqSnap = snap_handle->sn->lastFlush;
        }
    }

    // WARNING:
    //   The same as the comment in `LogMgr::get()`,
    //   fetching `visibleSeqBarrier` should be done AFTER
    //   fetching max seq number (`getAvailSeqRange` above).
    if ( log_mgr->visibleSeqBarrier &&
         maxSeqSnap > log_mgr->visibleSeqBarrier ) {
        maxSeqSnap = log_mgr->visibleSeqBarrier;
    }

    // No available records in log section.
    if (minSeqSnap > maxSeqSnap) return Status::OUT_OF_RANGE;

    lMgr = log_mgr;
    type = _type;
    startKey.alloc(start_key);
    endKey.alloc(end_key);
    if (lMgr->getDbConfig()->cmpFunc) {
        // Custom cmp function exists.
        avl_set_aux(&curWindow, (void*)lMgr);
    }

   try {
    uint64_t l_num_min;
    uint64_t l_num_max;

    if (snap_handle) {
        // Snapshot
        assert(snap_handle->sn->logList);
        for (auto& entry: *snap_handle->sn->logList) {
            LogFileInfo* l_info = entry;
            addLogFileItr(l_info);
        }
        snapLogList = snap_handle->sn->logList;
    }

    // Only when not a snapshot.
    for (; !snap_handle; ) {
        // Normal
        TC( lMgr->mani->getLastFlushedLog(l_num_min) );
        TC( lMgr->mani->getMaxLogFileNum(l_num_max) );

        // If seq-range is given, load that files only.
        if (valid_number(minSeqSnap)) {
            uint64_t min_seq_log;
            s = lMgr->mani->getLogFileNumBySeq(minSeqSnap, min_seq_log);
            if (s) {
                l_num_min = min_seq_log;
            }
        }
        if (valid_number(maxSeqSnap)) {
            uint64_t max_seq_log;
            s = lMgr->mani->getLogFileNumBySeq(maxSeqSnap, max_seq_log);
            if (s) {
                l_num_max=  max_seq_log;
            }
        }

        bool retry_init = false;
        for (uint64_t ii = l_num_min; ii <= l_num_max; ++ii) {
            LogFileInfo* l_info = nullptr;
            s = lMgr->mani->getLogFileInfo(ii, l_info);
            if (!s || l_info->isRemoved()) {
                _log_info(log_mgr->myLog, "log file %lu: %d %s, retry",
                          ii, (int)s,
                          (l_info && l_info->isRemoved())?"removed":"not-exist");
                // `l_num_min` is invalid. Retry.
                // Free all resources.
                for (auto& entry: itrs) {
                    ItrItem* ctx = entry;
                    ctx->lItr->close();
                    delete ctx->lItr;
                    ctx->lInfo->done();
                    delete ctx;
                }
                if (s) l_info->done();
                retry_init = true;

                // Reset item list.
                itrs.clear();

                // Reset AVL-tree as well.
                avl_init(&curWindow, curWindow.aux);
                break;
            }
            addLogFileItr(l_info);
        }

        if (retry_init) continue;
        break;
    }

    windowCursor = avl_first(&curWindow);
    if (!windowCursor) throw Status(Status::OUT_OF_RANGE);

#if 0
    // Now this feature is done by higher level iterator (i.e., Jungle iterator).
    if (type == BY_KEY) {
        ItrItem* cur_item = _get_entry(windowCursor, ItrItem, an);
        while (cur_item && !cur_item->lastRec.isIns()) {
            s = next();
            if (!s) return s;
            cur_item = _get_entry(windowCursor, ItrItem, an);
        }
    }
#endif

    return Status();

   } catch (Status s) {
    startKey.free();
    endKey.free();
    return s;
   }
}


Status LogMgr::Iterator::get(Record& rec_out) {
    if (!windowCursor) return Status::KEY_NOT_FOUND;

    ItrItem* item = _get_entry(windowCursor, ItrItem, an);
    rec_out = item->lastRec;
    return Status();
}

Status LogMgr::Iterator::prev() {
    Status s;

    ItrItem* cur_item = _get_entry(windowCursor, ItrItem, an);
    uint64_t cur_seq = cur_item->lastRec.seqNum;
    SizedBuf cur_key;
    cur_key.alloc(cur_item->lastRec.kv.key);

    // Do prev() for all iterators GTEQ windowCursor.
    // Note: opposite direction.
    avl_node* cursor = avl_last(&curWindow);
    while (cursor) {
        ItrItem* item = _get_entry(cursor, ItrItem, an);
        if (item->flags & ItrItem::no_more_prev) {
            s = Status::ERROR;
        } else {
            if ( type == BY_SEQ &&
                 item->lastRec.seqNum < cur_seq ) break;
            if ( type == BY_KEY &&
                 cmpSizedBuf(item->lastRec.kv.key, cur_key) < 0 ) break;
            // Include tombstone.
            s = item->lItr->prev(true);
        }

        if (s) {
            avl_remove(&curWindow, &item->an);
            item->flags = ItrItem::none;
            s = item->lItr->get(item->lastRec);
            assert(s);

            avl_cmp_func* cmp_func = (type == BY_SEQ)
                                     ? (ItrItem::cmpSeq)
                                     : (ItrItem::cmpKey);
            avl_node* avl_ret = avl_insert(&curWindow, &item->an, cmp_func);
            assert(avl_ret == &item->an);  (void)avl_ret;
            cursor = avl_last(&curWindow);
        } else {
            item->flags |= ItrItem::no_more_prev;
            cursor = avl_prev(&item->an);
        }
    }

    // Opposite direction.
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

Status LogMgr::Iterator::next() {
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
                 item->lastRec.seqNum > cur_seq ) break;
            if ( type == BY_KEY &&
                 cmpSizedBuf(item->lastRec.kv.key, cur_key) > 0 ) break;
            // Include tombstone.
            s = item->lItr->next(true);
        }

        if (s) {
            avl_remove(&curWindow, &item->an);
            item->flags = ItrItem::none;
            s = item->lItr->get(item->lastRec);
            assert(s);

            avl_cmp_func* cmp_func = (type == BY_SEQ)
                                     ? (ItrItem::cmpSeq)
                                     : (ItrItem::cmpKey);
            avl_node* avl_ret = avl_insert(&curWindow, &item->an, cmp_func);
            assert(avl_ret == &item->an);  (void)avl_ret;
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
        moveToLastValid();
        return Status::OUT_OF_RANGE;
    }
    return Status();
}

Status LogMgr::Iterator::seek(const SizedBuf& key, SeekOption opt)
{
    return seekInternal(key, NOT_INITIALIZED, opt);
}

Status LogMgr::Iterator::seekSN(const uint64_t seqnum, SeekOption opt)
{
    SizedBuf dummy_key;
    return seekInternal(dummy_key, seqnum, opt);
}

Status LogMgr::Iterator::gotoBegin() {
    SizedBuf empty_key;
    return seekInternal(empty_key, 0, GREATER);
}

Status LogMgr::Iterator::gotoEnd() {
    SizedBuf empty_key;
    return seekInternal(empty_key, 0, SMALLER, true);
}

Status LogMgr::Iterator::moveToLastValid() {
    windowCursor = avl_last(&curWindow);
    while (windowCursor) {
        // Find *LAST* valid item (only for BY_KEY).
        //
        // e.g.)
        //  ... Del K9 (seq 100), Ins K9 (seq 99)
        //  We should pick up `Del K9`.
        ItrItem* item = _get_entry(windowCursor, ItrItem, an);

        if (type == BY_KEY) {
            ItrItem* prev_item = nullptr;
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

Status LogMgr::Iterator::seekInternal
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
            s = item->lItr->gotoEnd();

        } else {
            if (type == BY_SEQ) {
                s = item->lItr->seekSN(seqnum, (LogFile::Iterator::SeekOption)opt);
            } else {
                s = item->lItr->seek(key, (LogFile::Iterator::SeekOption)opt);
            }
        }

        if (s) {
            s = item->lItr->get(item->lastRec);
            assert(s);

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
        assert(avl_ret == &item->an); (void)avl_ret;
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
        moveToLastValid();
    }

    if (!windowCursor) {
        // Reached the end.
        if (opt == GREATER) windowCursor = avl_last(&curWindow);
        if (opt == SMALLER) windowCursor = avl_first(&curWindow);
    }

#if 0
    // Now this feature is done by higher level iterator (i.e., Jungle iterator).
    if (type == BY_KEY) {
        ItrItem* item = _get_entry(windowCursor, ItrItem, an);
        while ( !item->lastRec.isIns() ) {
            // Deleted key, move the cursor.
            if (opt == GREATER) s = next();
            if (opt == SMALLER) s = prev();
            if (!s) return s;
            item = _get_entry(windowCursor, ItrItem, an);
        }
    }
#endif

    return Status();
}


int LogMgr::Iterator::cmpSizedBuf(const SizedBuf& l, const SizedBuf& r) {
    CMP_NULL_CHK(l.data, r.data);
    if (lMgr->getDbConfig()->cmpFunc) {
        // Custom cmp mode.
        CustomCmpFunc func = lMgr->getDbConfig()->cmpFunc;
        void* param = lMgr->getDbConfig()->cmpFuncParam;
        return func(l.data, l.size, r.data, r.size, param);
    }
    return SizedBuf::cmp(l, r);
}

bool LogMgr::Iterator::checkValidBySeq(ItrItem* item,
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

bool LogMgr::Iterator::checkValidByKey(ItrItem* item,
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

Status LogMgr::Iterator::close() {
    if (!lMgr) return Status();

    avl_node* cursor = avl_first(&curWindow);
    while (cursor) {
        ItrItem* item = _get_entry(cursor, ItrItem, an);
        cursor = avl_next(&item->an);
        avl_remove(&curWindow, &item->an);
    }

    for (auto& entry: itrs) {
        ItrItem* ctx = entry;
        ctx->lItr->close();
        delete ctx->lItr;
        if (!snapLogList) {
            // Only when not a snapshot.
            ctx->lInfo->done();
        }
        delete ctx;
    }

    lMgr = nullptr;
    windowCursor = nullptr;
    startKey.free();
    endKey.free();
    return Status();
}

}; // namespace jungle

