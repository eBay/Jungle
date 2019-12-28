/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright 2018 Jung-Sang Ahn.
See URL: https://github.com/greensky00/linkedlist
         (v0.1.2)

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

#include <stddef.h>
#include <stdint.h>

namespace c_list {

struct list_elem {
    struct list_elem* prev;
    struct list_elem* next;
};

struct list {
    struct list_elem* head;
    struct list_elem* tail;
    uint32_t num_nodes;
};

#ifndef _get_entry
#define _get_entry(ELEM, STRUCT, MEMBER) \
    ((STRUCT *) ((uint8_t *) (ELEM) - offsetof (STRUCT, MEMBER)))
#endif

static inline void list_init(struct list* list)
{
    list->head = NULL;
    list->tail = NULL;
    list->num_nodes = 0;
}

static inline void list_elem_init(struct list_elem* le)
{
    le->prev = NULL;
    le->next = NULL;
}

static inline size_t list_size(struct list* list) {
    return list->num_nodes;
}

// Insert `e` at the head of `list`.
void list_push_front(struct list* list, struct list_elem* e);

// Insert `e` at the tail of `list`.
void list_push_back(struct list* list, struct list_elem* e);

// Insert `e` before `pivot`.
void list_insert_before(struct list* list,
                        struct list_elem* pivot,
                        struct list_elem* e);

// Insert `e` after `pivot`.
void list_insert_after(struct list* list,
                       struct list_elem* pivot,
                       struct list_elem* e);

// Remove `e`, and return its next.
struct list_elem* list_remove(struct list* list, struct list_elem* e);

// Remove `e`, and return its prev.
struct list_elem* list_remove_reverse(struct list* list, struct list_elem* e);

// Remove the head of `list`, and then return it.
struct list_elem* list_pop_front(struct list* list);

// Remove the tail of `list`, and then return it.
struct list_elem* list_pop_back(struct list* list);

static inline struct list_elem* list_begin(struct list* list)
{
    return list->head;
}

static inline struct list_elem* list_end(struct list* list)
{
    return list->tail;
}

static inline struct list_elem* list_next(struct list_elem* e)
{
    return e->next;
}

static inline struct list_elem* list_prev(struct list_elem* e)
{
    return e->prev;
}

static inline int list_is_empty(struct list* list)
{
    return list->head == NULL;
}

}

