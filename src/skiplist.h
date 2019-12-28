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

#ifndef _JSAHN_SKIPLIST_H
#define _JSAHN_SKIPLIST_H (1)

#include <stddef.h>
#include <stdint.h>

#define SKIPLIST_MAX_LAYER (64)

struct _skiplist_node;

//#define _STL_ATOMIC (1)
#ifdef __APPLE__
    #define _STL_ATOMIC (1)
#endif
#if defined(_STL_ATOMIC) && defined(__cplusplus)
    #include <atomic>
    typedef std::atomic<_skiplist_node*>   atm_node_ptr;
    typedef std::atomic<bool>              atm_bool;
    typedef std::atomic<uint8_t>           atm_uint8_t;
    typedef std::atomic<uint16_t>          atm_uint16_t;
    typedef std::atomic<uint32_t>          atm_uint32_t;
#else
    typedef struct _skiplist_node*         atm_node_ptr;
    typedef uint8_t                        atm_bool;
    typedef uint8_t                        atm_uint8_t;
    typedef uint16_t                       atm_uint16_t;
    typedef uint32_t                       atm_uint32_t;
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _skiplist_node {
    atm_node_ptr *next;
    atm_bool is_fully_linked;
    atm_bool being_modified;
    atm_bool removed;
    uint8_t top_layer; // 0: bottom
    atm_uint16_t ref_count;
    atm_uint32_t accessing_next;
} skiplist_node;

// *a  < *b : return neg
// *a == *b : return 0
// *a  > *b : return pos
typedef int skiplist_cmp_t(skiplist_node *a, skiplist_node *b, void *aux);

typedef struct {
    size_t fanout;
    size_t maxLayer;
    void *aux;
} skiplist_raw_config;

typedef struct {
    skiplist_node head;
    skiplist_node tail;
    skiplist_cmp_t *cmp_func;
    void *aux;
    atm_uint32_t num_entries;
    atm_uint32_t* layer_entries;
    atm_uint8_t top_layer;
    uint8_t fanout;
    uint8_t max_layer;
} skiplist_raw;

#ifndef _get_entry
#define _get_entry(ELEM, STRUCT, MEMBER)                              \
        ((STRUCT *) ((uint8_t *) (ELEM) - offsetof (STRUCT, MEMBER)))
#endif

void skiplist_init(skiplist_raw* slist,
                   skiplist_cmp_t* cmp_func);
void skiplist_free(skiplist_raw* slist);

void skiplist_init_node(skiplist_node* node);
void skiplist_free_node(skiplist_node* node);

size_t skiplist_get_size(skiplist_raw* slist);

skiplist_raw_config skiplist_get_default_config();
skiplist_raw_config skiplist_get_config(skiplist_raw* slist);

void skiplist_set_config(skiplist_raw* slist,
                         skiplist_raw_config config);

int skiplist_insert(skiplist_raw* slist,
                    skiplist_node* node);
int skiplist_insert_nodup(skiplist_raw *slist,
                          skiplist_node *node);

skiplist_node* skiplist_find(skiplist_raw* slist,
                             skiplist_node* query);
skiplist_node* skiplist_find_smaller_or_equal(skiplist_raw* slist,
                                              skiplist_node* query);
skiplist_node* skiplist_find_greater_or_equal(skiplist_raw* slist,
                                              skiplist_node* query);

int skiplist_erase_node_passive(skiplist_raw* slist,
                                skiplist_node* node);
int skiplist_erase_node(skiplist_raw *slist,
                        skiplist_node *node);
int skiplist_erase(skiplist_raw* slist,
                   skiplist_node* query);

int skiplist_is_valid_node(skiplist_node* node);
int skiplist_is_safe_to_free(skiplist_node* node);
void skiplist_wait_for_free(skiplist_node* node);

void skiplist_grab_node(skiplist_node* node);
void skiplist_release_node(skiplist_node* node);

skiplist_node* skiplist_next(skiplist_raw* slist,
                             skiplist_node* node);
skiplist_node* skiplist_prev(skiplist_raw* slist,
                             skiplist_node* node);
skiplist_node* skiplist_begin(skiplist_raw* slist);
skiplist_node* skiplist_end(skiplist_raw* slist);

#ifdef __cplusplus
}
#endif

#endif  // _JSAHN_SKIPLIST_H
