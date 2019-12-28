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

#include "list.h"

// LCOV_EXCL_START

namespace c_list {

void list_push_front(struct list* list, struct list_elem* e)
{
    if (list->head == NULL) {
        list->head = e;
        list->tail = e;
        e->next = e->prev = NULL;
    } else {
        list->head->prev = e;
        e->prev = NULL;
        e->next = list->head;
        list->head = e;
    }
    list->num_nodes++;
}

void list_push_back(struct list* list, struct list_elem* e)
{
    if (list->tail == NULL) {
        list->head = e;
        list->tail = e;
        e->next = e->prev = NULL;
    } else {
        list->tail->next = e;
        e->prev = list->tail;
        e->next = NULL;
        list->tail = e;
    }
    list->num_nodes++;
}

void list_insert_before(struct list* list,
                        struct list_elem* pivot,
                        struct list_elem* e)
{
    e->prev = pivot->prev;
    e->next = pivot;
    if (pivot->prev) {
        pivot->prev->next = e;
    } else {
        list->head = e;
    }
    pivot->prev = e;

    list->num_nodes++;
}

void list_insert_after(struct list* list,
                       struct list_elem* pivot,
                       struct list_elem* e)
{
    e->next = pivot->next;
    e->prev = pivot;
    if (pivot->next) {
        pivot->next->prev = e;
    } else {
        list->tail = e;
    }
    pivot->next = e;

    list->num_nodes++;
}

struct list_elem* list_remove(struct list* list,
                              struct list_elem* e)
{
    if (e) {
        if (e->next) e->next->prev = e->prev;
        if (e->prev) e->prev->next = e->next;

        if (list->head == e) list->head = e->next;
        if (list->tail == e) list->tail = e->prev;

        list->num_nodes--;
        return e->next;
    }
    return NULL;
}

struct list_elem* list_remove_reverse(struct list* list,
                                      struct list_elem* e)
{
    if (e) {
        if (e->next) e->next->prev = e->prev;
        if (e->prev) e->prev->next = e->next;

        if (list->head == e) list->head = e->next;
        if (list->tail == e) list->tail = e->prev;

        list->num_nodes--;
        return e->prev;
    }
    return NULL;
}

struct list_elem* list_pop_front(struct list* list)
{
    struct list_elem *e = list->head;
    if (e) {
        if (e->next) e->next->prev = e->prev;
        if (e->prev) e->prev->next = e->next;

        if (list->head == e) list->head = e->next;
        if (list->tail == e) list->tail = e->prev;

        list->num_nodes--;
        return e;
    }
    return NULL;
}

struct list_elem* list_pop_back(struct list* list)
{
    struct list_elem* e = list->tail;
    if (e) {
        if (e->next) e->next->prev = e->prev;
        if (e->prev) e->prev->next = e->next;

        if (list->head == e) list->head = e->next;
        if (list->tail == e) list->tail = e->prev;

        list->num_nodes--;
        return e;
    }
    return NULL;
}

}

// LCOV_EXCL_STOP

