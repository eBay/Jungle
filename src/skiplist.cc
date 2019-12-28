/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright 2017 Jung-Sang Ahn
See URL: https://github.com/greensky00/skiplist
         (v0.2.9)

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

#include "skiplist.h"

#include <stdlib.h>

#define __SLD_RT_INS(e, n, t, c)
#define __SLD_NC_INS(n, nn, t, c)
#define __SLD_RT_RMV(e, n, t, c)
#define __SLD_NC_RMV(n, nn, t, c)
#define __SLD_BM(n)
#define __SLD_ASSERT(cond)
#define __SLD_P(args...)
#define __SLD_(b)

//#define __SL_DEBUG (1)
#ifdef __SL_DEBUG
    #ifndef __cplusplus
        #error "Debug mode is available with C++ compiler only."
    #endif
    #include "skiplist_debug.h"
#endif

#define __SL_YIELD (1)
#ifdef __SL_YIELD
    #ifdef __cplusplus
        #include <thread>
        #define YIELD() std::this_thread::yield()
    #else
        #include <sched.h>
        #define YIELD() sched_yield()
    #endif
#else
    #define YIELD()
#endif

#if defined(_STL_ATOMIC) && defined(__cplusplus)
    // C++ (STL) atomic operations
    #define MOR                         std::memory_order_relaxed
    #define ATM_GET(var)                (var).load(MOR)
    #define ATM_LOAD(var, val)          (val) = (var).load(MOR)
    #define ATM_STORE(var, val)         (var).store((val), MOR)
    #define ATM_CAS(var, exp, val)      (var).compare_exchange_weak((exp), (val))
    #define ATM_FETCH_ADD(var, val)     (var).fetch_add(val, MOR)
    #define ATM_FETCH_SUB(var, val)     (var).fetch_sub(val, MOR)
    #define ALLOC_(type, var, count)    (var) = new type[count]
    #define FREE_(var)                  delete[] (var)
#else
    // C-style atomic operations
    #ifndef __cplusplus
        typedef uint8_t bool;
        #ifndef true
            #define true 1
        #endif
        #ifndef false
            #define false 0
        #endif
    #endif

    #ifndef __cplusplus
        #define thread_local /*_Thread_local*/
    #endif

    #define MOR                         __ATOMIC_RELAXED
    #define ATM_GET(var)                (var)
    #define ATM_LOAD(var, val)          __atomic_load(&(var), &(val), MOR)
    #define ATM_STORE(var, val)         __atomic_store(&(var), &(val), MOR)
    #define ATM_CAS(var, exp, val)      \
            __atomic_compare_exchange(&(var), &(exp), &(val), 1, MOR, MOR)
    #define ATM_FETCH_ADD(var, val)     __atomic_fetch_add(&(var), (val), MOR)
    #define ATM_FETCH_SUB(var, val)     __atomic_fetch_sub(&(var), (val), MOR)
    #define ALLOC_(type, var, count)    \
            (var) = (type*)calloc(count, sizeof(type))
    #define FREE_(var)                  free(var)
#endif

static inline void _sl_node_init(skiplist_node *node,
                                 size_t top_layer)
{
    if (top_layer > UINT8_MAX) top_layer = UINT8_MAX;

    __SLD_ASSERT(node->is_fully_linked == false);
    __SLD_ASSERT(node->being_modified == false);

    bool bool_val = false;
    ATM_STORE(node->is_fully_linked, bool_val);
    ATM_STORE(node->being_modified, bool_val);
    ATM_STORE(node->removed, bool_val);

    if (node->top_layer != top_layer ||
        node->next == NULL) {

        node->top_layer = top_layer;

        if (node->next) FREE_(node->next);
        ALLOC_(atm_node_ptr, node->next, top_layer+1);
    }
}

void skiplist_init(skiplist_raw *slist,
                   skiplist_cmp_t *cmp_func) {

    slist->cmp_func = NULL;
    slist->aux = NULL;

    // fanout 4 + layer 12: 4^12 ~= upto 17M items under O(lg n) complexity.
    // for +17M items, complexity will grow linearly: O(k lg n).
    slist->fanout = 4;
    slist->max_layer = 12;
    slist->num_entries = 0;

    ALLOC_(atm_uint32_t, slist->layer_entries, slist->max_layer);
    slist->top_layer = 0;

    skiplist_init_node(&slist->head);
    skiplist_init_node(&slist->tail);

    _sl_node_init(&slist->head, slist->max_layer);
    _sl_node_init(&slist->tail, slist->max_layer);

    size_t layer;
    for (layer = 0; layer < slist->max_layer; ++layer) {
        slist->head.next[layer] = &slist->tail;
        slist->tail.next[layer] = NULL;
    }

    bool bool_val = true;
    ATM_STORE(slist->head.is_fully_linked, bool_val);
    ATM_STORE(slist->tail.is_fully_linked, bool_val);
    slist->cmp_func = cmp_func;
}

void skiplist_free(skiplist_raw *slist)
{
    skiplist_free_node(&slist->head);
    skiplist_free_node(&slist->tail);

    FREE_(slist->layer_entries);
    slist->layer_entries = NULL;

    slist->aux = NULL;
    slist->cmp_func = NULL;
}

void skiplist_init_node(skiplist_node *node)
{
    node->next = NULL;

    bool bool_false = false;
    ATM_STORE(node->is_fully_linked, bool_false);
    ATM_STORE(node->being_modified, bool_false);
    ATM_STORE(node->removed, bool_false);

    node->accessing_next = 0;
    node->top_layer = 0;
    node->ref_count = 0;
}

void skiplist_free_node(skiplist_node *node)
{
    FREE_(node->next);
    node->next = NULL;
}

size_t skiplist_get_size(skiplist_raw* slist) {
    uint32_t val;
    ATM_LOAD(slist->num_entries, val);
    return val;
}

skiplist_raw_config skiplist_get_default_config()
{
    skiplist_raw_config ret;
    ret.fanout = 4;
    ret.maxLayer = 12;
    ret.aux = NULL;
    return ret;
}

skiplist_raw_config skiplist_get_config(skiplist_raw *slist)
{
    skiplist_raw_config ret;
    ret.fanout = slist->fanout;
    ret.maxLayer = slist->max_layer;
    ret.aux = slist->aux;
    return ret;
}

void skiplist_set_config(skiplist_raw *slist,
                         skiplist_raw_config config)
{
    slist->fanout = config.fanout;

    slist->max_layer = config.maxLayer;
    if (slist->layer_entries) FREE_(slist->layer_entries);
    ALLOC_(atm_uint32_t, slist->layer_entries, slist->max_layer);

    slist->aux = config.aux;
}

static inline int _sl_cmp(skiplist_raw *slist,
                          skiplist_node *a,
                          skiplist_node *b)
{
    if (a == b) return 0;
    if (a == &slist->head || b == &slist->tail) return -1;
    if (a == &slist->tail || b == &slist->head) return 1;
    return slist->cmp_func(a, b, slist->aux);
}

static inline bool _sl_valid_node(skiplist_node *node) {
    bool is_fully_linked = false;
    ATM_LOAD(node->is_fully_linked, is_fully_linked);
    return is_fully_linked;
}

static inline void _sl_read_lock_an(skiplist_node* node) {
    for(;;) {
        // Wait for active writer to release the lock
        uint32_t accessing_next = 0;
        ATM_LOAD(node->accessing_next, accessing_next);
        while (accessing_next & 0xfff00000) {
            YIELD();
            ATM_LOAD(node->accessing_next, accessing_next);
        }

        ATM_FETCH_ADD(node->accessing_next, 0x1);
        ATM_LOAD(node->accessing_next, accessing_next);
        if ((accessing_next & 0xfff00000) == 0) {
            return;
        }

        ATM_FETCH_SUB(node->accessing_next, 0x1);
    }
}

static inline void _sl_read_unlock_an(skiplist_node* node) {
    ATM_FETCH_SUB(node->accessing_next, 0x1);
}

static inline void _sl_write_lock_an(skiplist_node* node) {
    for(;;) {
        // Wait for active writer to release the lock
        uint32_t accessing_next = 0;
        ATM_LOAD(node->accessing_next, accessing_next);
        while (accessing_next & 0xfff00000) {
            YIELD();
            ATM_LOAD(node->accessing_next, accessing_next);
        }

        ATM_FETCH_ADD(node->accessing_next, 0x100000);
        ATM_LOAD(node->accessing_next, accessing_next);
        if((accessing_next & 0xfff00000) == 0x100000) {
            // Wait until there's no more readers
            while (accessing_next & 0x000fffff) {
                YIELD();
                ATM_LOAD(node->accessing_next, accessing_next);
            }
            return;
        }

        ATM_FETCH_SUB(node->accessing_next, 0x100000);
    }
}

static inline void _sl_write_unlock_an(skiplist_node* node) {
    ATM_FETCH_SUB(node->accessing_next, 0x100000);
}

// Note: it increases the `ref_count` of returned node.
//       Caller is responsible to decrease it.
static inline skiplist_node* _sl_next(skiplist_raw* slist,
                                      skiplist_node* cur_node,
                                      int layer,
                                      skiplist_node* node_to_find,
                                      bool* found)
{
    skiplist_node *next_node = NULL;

    // Turn on `accessing_next`:
    // now `cur_node` is not removable from skiplist,
    // which means that `cur_node->next` will be consistent
    // until clearing `accessing_next`.
    _sl_read_lock_an(cur_node); {
        if (!_sl_valid_node(cur_node)) {
            _sl_read_unlock_an(cur_node);
            return NULL;
        }
        ATM_LOAD(cur_node->next[layer], next_node);
        // Increase ref count of `next_node`:
        // now `next_node` is not destroyable.

        //   << Remaining issue >>
        // 1) initially: A -> B
        // 2) T1: call _sl_next(A):
        //        A.accessing_next := true;
        //        next_node := B;
        // ----- context switch happens here -----
        // 3) T2: insert C:
        //        A -> C -> B
        // 4) T2: and then erase B, and free B.
        //        A -> C    B(freed)
        // ----- context switch back again -----
        // 5) T1: try to do something with B,
        //        but crash happens.
        //
        // ... maybe resolved using RW spinlock (Aug 21, 2017).
        __SLD_ASSERT(next_node);
        ATM_FETCH_ADD(next_node->ref_count, 1);
        __SLD_ASSERT(next_node->top_layer >= layer);
    } _sl_read_unlock_an(cur_node);

    size_t num_nodes = 0;
    skiplist_node* nodes[256];

    while ( (next_node && !_sl_valid_node(next_node)) ||
             next_node == node_to_find ) {
        if (found && node_to_find == next_node) *found = true;

        skiplist_node* temp = next_node;
        _sl_read_lock_an(temp); {
            __SLD_ASSERT(next_node);
            if (!_sl_valid_node(temp)) {
                _sl_read_unlock_an(temp);
                ATM_FETCH_SUB(temp->ref_count, 1);
                next_node = NULL;
                break;
            }
            ATM_LOAD(temp->next[layer], next_node);
            ATM_FETCH_ADD(next_node->ref_count, 1);
            nodes[num_nodes++] = temp;
            __SLD_ASSERT(next_node->top_layer >= layer);
        } _sl_read_unlock_an(temp);
    }

    for (size_t ii=0; ii<num_nodes; ++ii) {
        ATM_FETCH_SUB(nodes[ii]->ref_count, 1);
    }

    return next_node;
}

static inline size_t _sl_decide_top_layer(skiplist_raw *slist)
{
    size_t layer = 0;
    while (layer+1 < slist->max_layer) {
        // coin filp
        if (rand() % slist->fanout == 0) {
            // grow: 1/fanout probability
            layer++;
        } else {
            // stop: 1 - 1/fanout probability
            break;
        }
    }
    return layer;
}

static inline void _sl_clr_flags(skiplist_node** node_arr,
                                 int start_layer,
                                 int top_layer)
{
    int layer;
    for (layer = start_layer; layer <= top_layer; ++layer) {
        if ( layer == top_layer ||
             node_arr[layer] != node_arr[layer+1] ) {

            bool exp = true;
            bool bool_false = false;
            if (!ATM_CAS(node_arr[layer]->being_modified, exp, bool_false)) {
                __SLD_ASSERT(0);
            }
        }
    }
}

static inline bool _sl_valid_prev_next(skiplist_node *prev,
                                       skiplist_node *next) {
    return _sl_valid_node(prev) && _sl_valid_node(next);
}

static inline int _skiplist_insert(skiplist_raw *slist,
                                   skiplist_node *node,
                                   bool no_dup)
{
    __SLD_(
        thread_local std::thread::id tid = std::this_thread::get_id();
        thread_local size_t tid_hash = std::hash<std::thread::id>{}(tid) % 256;
        (void)tid_hash;
    )

    int top_layer = _sl_decide_top_layer(slist);
    bool bool_true = true;

    // init node before insertion
    _sl_node_init(node, top_layer);
    _sl_write_lock_an(node);

    skiplist_node* prevs[SKIPLIST_MAX_LAYER];
    skiplist_node* nexts[SKIPLIST_MAX_LAYER];

    __SLD_P("%02x ins %p begin\n", (int)tid_hash, node);

insert_retry:
    // in pure C, a label can only be part of a stmt.
    (void)top_layer;

    int cmp = 0, cur_layer = 0, layer;
    skiplist_node *cur_node = &slist->head;
    ATM_FETCH_ADD(cur_node->ref_count, 1);

    __SLD_(size_t nh = 0);
    __SLD_(thread_local skiplist_node* history[1024]; (void)history);

    int sl_top_layer = slist->top_layer;
    if (top_layer > sl_top_layer) sl_top_layer = top_layer;
    for (cur_layer = sl_top_layer; cur_layer >= 0; --cur_layer) {
        do {
            __SLD_( history[nh++] = cur_node );

            skiplist_node *next_node = _sl_next(slist, cur_node, cur_layer,
                                                NULL, NULL);
            if (!next_node) {
                _sl_clr_flags(prevs, cur_layer+1, top_layer);
                ATM_FETCH_SUB(cur_node->ref_count, 1);
                YIELD();
                goto insert_retry;
            }
            cmp = _sl_cmp(slist, node, next_node);
            if (cmp > 0) {
                // cur_node < next_node < node
                // => move to next node
                skiplist_node* temp = cur_node;
                cur_node = next_node;
                ATM_FETCH_SUB(temp->ref_count, 1);
                continue;
            } else {
                // otherwise: cur_node < node <= next_node
                ATM_FETCH_SUB(next_node->ref_count, 1);
            }

            if (no_dup && cmp == 0) {
                // Duplicate key is not allowed.
                _sl_clr_flags(prevs, cur_layer+1, top_layer);
                ATM_FETCH_SUB(cur_node->ref_count, 1);
                return -1;
            }

            if (cur_layer <= top_layer) {
                prevs[cur_layer] = cur_node;
                nexts[cur_layer] = next_node;
                // both 'prev' and 'next' should be fully linked before
                // insertion, and no other thread should not modify 'prev'
                // at the same time.

                int error_code = 0;
                int locked_layer = cur_layer + 1;

                // check if prev node is duplicated with upper layer
                if (cur_layer < top_layer &&
                    prevs[cur_layer] == prevs[cur_layer+1]) {
                    // duplicate
                    // => which means that 'being_modified' flag is already true
                    // => do nothing
                } else {
                    bool expected = false;
                    if (ATM_CAS(prevs[cur_layer]->being_modified,
                                expected, bool_true)) {
                        locked_layer = cur_layer;
                    } else {
                        error_code = -1;
                    }
                }

                if (error_code == 0 &&
                    !_sl_valid_prev_next(prevs[cur_layer], nexts[cur_layer])) {
                    error_code = -2;
                }

                if (error_code != 0) {
                    __SLD_RT_INS(error_code, node, top_layer, cur_layer);
                    _sl_clr_flags(prevs, locked_layer, top_layer);
                    ATM_FETCH_SUB(cur_node->ref_count, 1);
                    YIELD();
                    goto insert_retry;
                }

                // set current node's pointers
                ATM_STORE(node->next[cur_layer], nexts[cur_layer]);

                // check if `cur_node->next` has been changed from `next_node`.
                skiplist_node* next_node_again =
                    _sl_next(slist, cur_node, cur_layer, NULL, NULL);
                ATM_FETCH_SUB(next_node_again->ref_count, 1);
                if (next_node_again != next_node) {
                    __SLD_NC_INS(cur_node, next_node, top_layer, cur_layer);
                    // clear including the current layer
                    // as we already set modification flag above.
                    _sl_clr_flags(prevs, cur_layer, top_layer);
                    ATM_FETCH_SUB(cur_node->ref_count, 1);
                    YIELD();
                    goto insert_retry;
                }
            }

            if (cur_layer) {
                // non-bottom layer => go down
                break;
            }

            // bottom layer => insertion succeeded
            // change prev/next nodes' prev/next pointers from 0 ~ top_layer
            for (layer = 0; layer <= top_layer; ++layer) {
                // `accessing_next` works as a spin-lock.
                _sl_write_lock_an(prevs[layer]);
                skiplist_node* exp = nexts[layer];
                if ( !ATM_CAS(prevs[layer]->next[layer], exp, node) ) {
                    __SLD_P("%02x ASSERT ins %p[%d] -> %p (expected %p)\n",
                            (int)tid_hash, prevs[layer], cur_layer,
                            ATM_GET(prevs[layer]->next[layer]), nexts[layer] );
                    __SLD_ASSERT(0);
                }
                __SLD_P("%02x ins %p[%d] -> %p -> %p\n",
                        (int)tid_hash, prevs[layer], layer,
                        node, ATM_GET(node->next[layer]) );
                _sl_write_unlock_an(prevs[layer]);
            }

            // now this node is fully linked
            ATM_STORE(node->is_fully_linked, bool_true);

            // allow removing next nodes
            _sl_write_unlock_an(node);

            __SLD_P("%02x ins %p done\n", (int)tid_hash, node);

            ATM_FETCH_ADD(slist->num_entries, 1);
            ATM_FETCH_ADD(slist->layer_entries[node->top_layer], 1);
            for (int ii=slist->max_layer-1; ii>=0; --ii) {
                if (slist->layer_entries[ii] > 0) {
                    slist->top_layer = ii;
                    break;
                }
            }

            // modification is done for all layers
            _sl_clr_flags(prevs, 0, top_layer);
            ATM_FETCH_SUB(cur_node->ref_count, 1);

            return 0;
        } while (cur_node != &slist->tail);
    }
    return 0;
}

int skiplist_insert(skiplist_raw *slist,
                    skiplist_node *node)
{
    return _skiplist_insert(slist, node, false);
}

int skiplist_insert_nodup(skiplist_raw *slist,
                          skiplist_node *node)
{
    return _skiplist_insert(slist, node, true);
}

typedef enum {
    SM   = -2,
    SMEQ = -1,
    EQ   =  0,
    GTEQ =  1,
    GT   =  2
} _sl_find_mode;

// Note: it increases the `ref_count` of returned node.
//       Caller is responsible to decrease it.
static inline skiplist_node* _sl_find(skiplist_raw *slist,
                                      skiplist_node *query,
                                      _sl_find_mode mode)
{
    // mode:
    //  SM   -2: smaller
    //  SMEQ -1: smaller or equal
    //  EQ    0: equal
    //  GTEQ  1: greater or equal
    //  GT    2: greater
find_retry:
    (void)mode;
    int cmp = 0;
    int cur_layer = 0;
    skiplist_node *cur_node = &slist->head;
    ATM_FETCH_ADD(cur_node->ref_count, 1);

    __SLD_(size_t nh = 0);
    __SLD_(thread_local skiplist_node* history[1024]; (void)history);

    uint8_t sl_top_layer = slist->top_layer;
    for (cur_layer = sl_top_layer; cur_layer >= 0; --cur_layer) {
        do {
            __SLD_(history[nh++] = cur_node);

            skiplist_node *next_node = _sl_next(slist, cur_node, cur_layer,
                                                NULL, NULL);
            if (!next_node) {
                ATM_FETCH_SUB(cur_node->ref_count, 1);
                YIELD();
                goto find_retry;
            }
            cmp = _sl_cmp(slist, query, next_node);
            if (cmp > 0) {
                // cur_node < next_node < query
                // => move to next node
                skiplist_node* temp = cur_node;
                cur_node = next_node;
                ATM_FETCH_SUB(temp->ref_count, 1);
                continue;
            } else if (-1 <= mode && mode <= 1 && cmp == 0) {
                // cur_node < query == next_node .. return
                ATM_FETCH_SUB(cur_node->ref_count, 1);
                return next_node;
            }

            // otherwise: cur_node < query < next_node
            if (cur_layer) {
                // non-bottom layer => go down
                ATM_FETCH_SUB(next_node->ref_count, 1);
                break;
            }

            // bottom layer
            if (mode < 0 && cur_node != &slist->head) {
                // smaller mode
                ATM_FETCH_SUB(next_node->ref_count, 1);
                return cur_node;
            } else if (mode > 0 && next_node != &slist->tail) {
                // greater mode
                ATM_FETCH_SUB(cur_node->ref_count, 1);
                return next_node;
            }
            // otherwise: exact match mode OR not found
            ATM_FETCH_SUB(cur_node->ref_count, 1);
            ATM_FETCH_SUB(next_node->ref_count, 1);
            return NULL;
        } while (cur_node != &slist->tail);
    }

    return NULL;
}

skiplist_node* skiplist_find(skiplist_raw *slist,
                             skiplist_node *query)
{
    return _sl_find(slist, query, EQ);
}

skiplist_node* skiplist_find_smaller_or_equal(skiplist_raw *slist,
                                              skiplist_node *query)
{
    return _sl_find(slist, query, SMEQ);
}

skiplist_node* skiplist_find_greater_or_equal(skiplist_raw *slist,
                                              skiplist_node *query)
{
    return _sl_find(slist, query, GTEQ);
}

int skiplist_erase_node_passive(skiplist_raw *slist,
                                skiplist_node *node)
{
    __SLD_(
        thread_local std::thread::id tid = std::this_thread::get_id();
        thread_local size_t tid_hash = std::hash<std::thread::id>{}(tid) % 256;
        (void)tid_hash;
    )

    int top_layer = node->top_layer;
    bool bool_true = true, bool_false = false;
    bool removed = false;
    bool is_fully_linked = false;

    ATM_LOAD(node->removed, removed);
    if (removed) {
        // already removed
        return -1;
    }

    skiplist_node* prevs[SKIPLIST_MAX_LAYER];
    skiplist_node* nexts[SKIPLIST_MAX_LAYER];

    bool expected = false;
    if (!ATM_CAS(node->being_modified, expected, bool_true)) {
        // already being modified .. cannot work on this node for now.
        __SLD_BM(node);
        return -2;
    }

    // set removed flag first, so that reader cannot read this node.
    ATM_STORE(node->removed, bool_true);

    __SLD_P("%02x rmv %p begin\n", (int)tid_hash, node);

erase_node_retry:
    ATM_LOAD(node->is_fully_linked, is_fully_linked);
    if (!is_fully_linked) {
        // already unlinked .. remove is done by other thread
        ATM_STORE(node->removed, bool_false);
        ATM_STORE(node->being_modified, bool_false);
        return -3;
    }

    int cmp = 0;
    bool found_node_to_erase = false;
    (void)found_node_to_erase;
    skiplist_node *cur_node = &slist->head;
    ATM_FETCH_ADD(cur_node->ref_count, 1);

    __SLD_(size_t nh = 0);
    __SLD_(thread_local skiplist_node* history[1024]; (void)history);

    int cur_layer = slist->top_layer;
    for (; cur_layer >= 0; --cur_layer) {
        do {
            __SLD_( history[nh++] = cur_node );

            bool node_found = false;
            skiplist_node *next_node = _sl_next(slist, cur_node, cur_layer,
                                                node, &node_found);
            if (!next_node) {
                _sl_clr_flags(prevs, cur_layer+1, top_layer);
                ATM_FETCH_SUB(cur_node->ref_count, 1);
                YIELD();
                goto erase_node_retry;
            }

            // Note: unlike insert(), we should find exact position of `node`.
            cmp = _sl_cmp(slist, node, next_node);
            if (cmp > 0 || (cur_layer <= top_layer && !node_found) ) {
                // cur_node <= next_node < node
                // => move to next node
                skiplist_node* temp = cur_node;
                cur_node = next_node;
                __SLD_( if (cmp > 0) {
                    int cmp2 = _sl_cmp(slist, cur_node, node);
                    if (cmp2 > 0) {
                        // node < cur_node <= next_node: not found.
                        _sl_clr_flags(prevs, cur_layer+1, top_layer);
                        ATM_FETCH_SUB(temp->ref_count, 1);
                        ATM_FETCH_SUB(next_node->ref_count, 1);
                        __SLD_ASSERT(0);
                    }
                } )
                ATM_FETCH_SUB(temp->ref_count, 1);
                continue;
            } else {
                // otherwise: cur_node <= node <= next_node
                ATM_FETCH_SUB(next_node->ref_count, 1);
            }

            if (cur_layer <= top_layer) {
                prevs[cur_layer] = cur_node;
                // note: 'next_node' and 'node' should not be the same,
                //       as 'removed' flag is already set.
                __SLD_ASSERT(next_node != node);
                nexts[cur_layer] = next_node;

                // check if prev node duplicates with upper layer
                int error_code = 0;
                int locked_layer = cur_layer + 1;
                if (cur_layer < top_layer &&
                    prevs[cur_layer] == prevs[cur_layer+1]) {
                    // duplicate with upper layer
                    // => which means that 'being_modified' flag is already true
                    // => do nothing.
                } else {
                    expected = false;
                    if (ATM_CAS(prevs[cur_layer]->being_modified,
                                expected, bool_true)) {
                        locked_layer = cur_layer;
                    } else {
                        error_code = -1;
                    }
                }

                if (error_code == 0 &&
                    !_sl_valid_prev_next(prevs[cur_layer], nexts[cur_layer])) {
                    error_code = -2;
                }

                if (error_code != 0) {
                    __SLD_RT_RMV(error_code, node, top_layer, cur_layer);
                    _sl_clr_flags(prevs, locked_layer, top_layer);
                    ATM_FETCH_SUB(cur_node->ref_count, 1);
                    YIELD();
                    goto erase_node_retry;
                }

                skiplist_node* next_node_again =
                    _sl_next(slist, cur_node, cur_layer, node, NULL);
                ATM_FETCH_SUB(next_node_again->ref_count, 1);
                if (next_node_again != nexts[cur_layer]) {
                    // `next` pointer has been changed, retry.
                    __SLD_NC_RMV(cur_node, nexts[cur_layer], top_layer, cur_layer);
                    _sl_clr_flags(prevs, cur_layer, top_layer);
                    ATM_FETCH_SUB(cur_node->ref_count, 1);
                    YIELD();
                    goto erase_node_retry;
                }
            }
            if (cur_layer == 0) found_node_to_erase = true;
            // go down
            break;
        } while (cur_node != &slist->tail);
    }
    // Not exist in the skiplist, should not happen.
    __SLD_ASSERT(found_node_to_erase);
    // bottom layer => removal succeeded.
    // mark this node unlinked
    _sl_write_lock_an(node); {
        ATM_STORE(node->is_fully_linked, bool_false);
    } _sl_write_unlock_an(node);

    // change prev nodes' next pointer from 0 ~ top_layer
    for (cur_layer = 0; cur_layer <= top_layer; ++cur_layer) {
        _sl_write_lock_an(prevs[cur_layer]);
        skiplist_node* exp = node;
        __SLD_ASSERT(exp != nexts[cur_layer]);
        __SLD_ASSERT(nexts[cur_layer]->is_fully_linked);
        if ( !ATM_CAS(prevs[cur_layer]->next[cur_layer],
                      exp, nexts[cur_layer]) ) {
            __SLD_P("%02x ASSERT rmv %p[%d] -> %p (node %p)\n",
                    (int)tid_hash, prevs[cur_layer], cur_layer,
                    ATM_GET(prevs[cur_layer]->next[cur_layer]), node );
            __SLD_ASSERT(0);
        }
        __SLD_ASSERT(nexts[cur_layer]->top_layer >= cur_layer);
        __SLD_P("%02x rmv %p[%d] -> %p (node %p)\n",
                (int)tid_hash, prevs[cur_layer], cur_layer,
                nexts[cur_layer], node);
        _sl_write_unlock_an(prevs[cur_layer]);
    }

    __SLD_P("%02x rmv %p done\n", (int)tid_hash, node);

    ATM_FETCH_SUB(slist->num_entries, 1);
    ATM_FETCH_SUB(slist->layer_entries[node->top_layer], 1);
    for (int ii=slist->max_layer-1; ii>=0; --ii) {
        if (slist->layer_entries[ii] > 0) {
            slist->top_layer = ii;
            break;
        }
    }

    // modification is done for all layers
    _sl_clr_flags(prevs, 0, top_layer);
    ATM_FETCH_SUB(cur_node->ref_count, 1);

    ATM_STORE(node->being_modified, bool_false);

    return 0;
}

int skiplist_erase_node(skiplist_raw *slist,
                        skiplist_node *node)
{
    int ret = 0;
    do {
        ret = skiplist_erase_node_passive(slist, node);
        // if ret == -2, other thread is accessing the same node
        // at the same time. try again.
    } while (ret == -2);
    return ret;
}

int skiplist_erase(skiplist_raw *slist,
                   skiplist_node *query)
{
    skiplist_node *found = skiplist_find(slist, query);
    if (!found) {
        // key not found
        return -4;
    }

    int ret = 0;
    do {
        ret = skiplist_erase_node_passive(slist, found);
        // if ret == -2, other thread is accessing the same node
        // at the same time. try again.
    } while (ret == -2);

    ATM_FETCH_SUB(found->ref_count, 1);
    return ret;
}

int skiplist_is_valid_node(skiplist_node* node) {
    return _sl_valid_node(node);
}

int skiplist_is_safe_to_free(skiplist_node* node) {
    if (node->accessing_next) return 0;
    if (node->being_modified) return 0;
    if (!node->removed) return 0;

    uint16_t ref_count = 0;
    ATM_LOAD(node->ref_count, ref_count);
    if (ref_count) return 0;
    return 1;
}

void skiplist_wait_for_free(skiplist_node* node) {
    while (!skiplist_is_safe_to_free(node)) {
        YIELD();
    }
}

void skiplist_grab_node(skiplist_node* node) {
    ATM_FETCH_ADD(node->ref_count, 1);
}

void skiplist_release_node(skiplist_node* node) {
    __SLD_ASSERT(node->ref_count);
    ATM_FETCH_SUB(node->ref_count, 1);
}

skiplist_node* skiplist_next(skiplist_raw *slist,
                             skiplist_node *node) {
    //   << Issue >>
    // If `node` is already removed and its next node is also removed
    // and then released, the link update will not be applied to `node`
    // as it is already unrechable from skiplist. `node` still points to
    // the released node so that `_sl_next(node)` may return corrupted
    // memory region.
    //
    // 0) initial:
    //    A -> B -> C -> D
    //
    // 1) B is `node`, which is removed but not yet released:
    //    B --+-> C -> D
    //        |
    //    A --+
    //
    // 2) remove C, and then release:
    //    B -> !C!  +-> D
    //              |
    //    A --------+
    //
    // 3) skiplist_next(B):
    //    will fetch C, which is already released so that
    //    may contain garbage data.
    //
    // In this case, start over from the top layer,
    // to find valid link (same as in prev()).

    skiplist_node *next = _sl_next(slist, node, 0, NULL, NULL);
    if (!next) next = _sl_find(slist, node, GT);

    if (next == &slist->tail) return NULL;
    return next;
}

skiplist_node* skiplist_prev(skiplist_raw *slist,
                             skiplist_node *node) {
    skiplist_node *prev = _sl_find(slist, node, SM);
    if (prev == &slist->head) return NULL;
    return prev;
}

skiplist_node* skiplist_begin(skiplist_raw *slist) {
    skiplist_node *next = NULL;
    while (!next) {
        next = _sl_next(slist, &slist->head, 0, NULL, NULL);
    }
    if (next == &slist->tail) return NULL;
    return next;
}

skiplist_node* skiplist_end(skiplist_raw *slist) {
    return skiplist_prev(slist, &slist->tail);
}

