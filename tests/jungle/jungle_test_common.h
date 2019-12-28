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

#include "config_test_common.h"
#include "event_awaiter.h"
#include "test_common.h"

#include <libjungle/jungle.h>

#define _INT_UNUSED_ int __attribute__((unused))

#define MAX_TEST_LEN (1024)

static _INT_UNUSED_
_init_kv_pairs(int n,
               std::vector<jungle::KV>& kv,
               std::string key_string,
               std::string value_string) {
    for (int i=0; i<n; ++i) {
        kv[i].alloc( key_string   + TestSuite::lzStr(3, i),
                     value_string + TestSuite::lzStr(3, i) );
    }
    return 0;
}

static _INT_UNUSED_
_free_kv_pairs(int n,
               std::vector<jungle::KV>& kv) {
    for (int i=0; i<n; ++i) {
        kv[i].free();
    }
    return 0;
}

// e.g.) from = 0, to = 10, seq_offset = 5
// setSN(6, kv[0])
// setSN(7, kv[1])
// ...
// setSN(15, kv[9])
static _INT_UNUSED_
_set_byseq_kv_pairs(int from,
                    int to,
                    int seq_offset,
                    jungle::DB* db,
                    std::vector<jungle::KV>& kv) {
    for (int i=from; i<to; ++i) {
        jungle::Status s = db->setSN(i + seq_offset + 1, kv[i]);
        CHK_OK(s);
    }
    return 0;
}

// e.g.) from = 5, to = 10
// set(kv[5])
// set(kv[6])
// ...
// set(kv[9])
static _INT_UNUSED_
_set_bykey_kv_pairs(int from,
                    int to,
                    jungle::DB* db,
                    std::vector<jungle::KV>& kv) {
    for (int i=from; i<to; ++i) {
        jungle::Status s = db->set(kv[i]);
        CHK_OK(s);
    }
    return 0;
}

// e.g.) from = 0, to = 10, seq_offset = 5
// getSN(6, kv[0])
// getSN(7, kv[1])
// ...
// getSN(15, kv[9])
static _INT_UNUSED_
_get_byseq_check(int from,
                 int to,
                 int seq_offset,
                 jungle::DB* db,
                 std::vector<jungle::KV>& kv,
                 bool exist = true) {
    for (int i=from; i<to; ++i) {
        jungle::KV kv_ret;
        jungle::Status s = db->getSN(i + seq_offset + 1, kv_ret);
        if (!exist) {
            CHK_NOT(s);
        } else {
            CHK_Z(s);
            CHK_EQ(kv[i].key, kv_ret.key);
            CHK_EQ(kv[i].value, kv_ret.value);
            kv_ret.free();
        }
    }
    return 0;
}

// e.g.) from = 5, to = 10
// get(kv[5])
// get(kv[6])
// ...
// get(kv[9])
static _INT_UNUSED_
_get_bykey_check(int from,
                 int to,
                 jungle::DB* db,
                 std::vector<jungle::KV>& kv,
                 bool exist = true) {
    for (int i=from; i<to; ++i) {
        TestSuite::setInfo("i=%d", i);
        jungle::SizedBuf value;
        jungle::Status s = db->get(kv[i].key, value);
        if (!exist) {
            CHK_NOT(s);
        } else {
            CHK_Z(s);
            CHK_EQ(kv[i].value, value);
            value.free();
        }
    }
    return 0;
}

// e.g.) from = 5, to = 10
// itr.get(kv[5])
// itr.get(kv[6])
// ...
// itr.get(kv[9])
static _INT_UNUSED_
_itr_check_step(int from,
                int to,
                int step,
                jungle::Iterator& itr,
                std::vector<jungle::KV>& kv) {
    jungle::Record rec;
    size_t idx = from;
    size_t count = 0;
    do {
        if (idx >= (size_t)to) break;

        CHK_OK(itr.get(rec));
        CHK_EQ(kv[idx].key, rec.kv.key);
        CHK_EQ(kv[idx].value, rec.kv.value);
        rec.free();

        idx += step;
        count++;
    } while (itr.next());
    CHK_EQ( (to - from + (step - 1)) / step, (int)count);

    return 0;
}

static _INT_UNUSED_
_itr_check(int from,
           int to,
           jungle::Iterator& itr,
           std::vector<jungle::KV>& kv) {
    return _itr_check_step(from, to, 1, itr, kv);
}

// e.g.) from = 5, to = 10
// itr.get(kv[9])
// itr.get(kv[8])
// ...
// itr.get(kv[5])
static _INT_UNUSED_
_itr_check_bwd_step(int from,
                    int to,
                    int step,
                    jungle::Iterator& itr,
                    std::vector<jungle::KV>& kv) {
    jungle::Record rec;
    int idx = to - 1;
    size_t count = 0;
    do {
        if (idx < from) break;

        CHK_OK(itr.get(rec));
        CHK_EQ(kv[idx].key, rec.kv.key);
        CHK_EQ(kv[idx].value, rec.kv.value);
        rec.free();

        idx -= step;
        count++;
    } while (itr.prev());
    CHK_EQ( (to - from + (step - 1)) / step, (int)count);

    return 0;
}

static _INT_UNUSED_
_itr_check_bwd(int from,
               int to,
               jungle::Iterator& itr,
               std::vector<jungle::KV>& kv) {
    return _itr_check_bwd_step(from, to, 1, itr, kv);
}


template<typename T>
static _INT_UNUSED_
_cmp_lists(std::list<T>& a, std::list<T>& b) {
    CHK_EQ(a.size(), b.size());
    for (auto& e_a: a) {
        bool found = false;
        for (auto& e_b: b) {
            if (e_a == e_b) {
                found = true;
                break;
            }
        }
        CHK_OK(found);
    }
    return 0;
}

int _set_keys(jungle::DB* db,
              size_t start,
              size_t end,
              size_t step,
              const std::string& key_fmt,
              const std::string& val_fmt)
{
    char key_str[MAX_TEST_LEN];
    char val_str[MAX_TEST_LEN];
    for (size_t ii=start; ii<end; ii+=step) {
        sprintf(key_str, key_fmt.c_str(), ii);
        sprintf(val_str, val_fmt.c_str(), ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val(val_str);
        CHK_Z(db->set(jungle::KV(key, val)));
    }
    return 0;
}

int _del_keys(jungle::DB* db,
              size_t start,
              size_t end,
              size_t step,
              const std::string& key_fmt)
{
    char key_str[MAX_TEST_LEN];
    for (size_t ii=start; ii<end; ii+=step) {
        sprintf(key_str, key_fmt.c_str(), ii);
        jungle::SizedBuf key(key_str);
        CHK_Z(db->del(jungle::SizedBuf(key)));
    }
    return 0;
}

int _get_keys(jungle::DB* db,
              size_t start,
              size_t end,
              size_t step,
              const std::string& key_fmt,
              const std::string& val_fmt)
{
    char key_str[MAX_TEST_LEN];
    char val_str[MAX_TEST_LEN];
    for (size_t ii=start; ii<end; ii+=step) {
        TestSuite::setInfo("ii=%zu", ii);
        sprintf(key_str, key_fmt.c_str(), ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val_out;
        CHK_Z(db->get(key, val_out));

        if (!val_fmt.empty()) {
            sprintf(val_str, val_fmt.c_str(), ii);
            jungle::SizedBuf val(val_str);
            CHK_EQ(val, val_out);
        }
        val_out.free();
    }
    return 0;
}

int _non_existing_keys(jungle::DB* db,
                       size_t start,
                       size_t end,
                       size_t step,
                       const std::string& key_fmt)
{
    char key_str[MAX_TEST_LEN];
    for (size_t ii=start; ii<end; ii+=step) {
        TestSuite::setInfo("ii=%zu", ii);
        sprintf(key_str, key_fmt.c_str(), ii);
        jungle::SizedBuf key(key_str);
        jungle::SizedBuf val_out;
        jungle::SizedBuf::Holder h(val_out);
        jungle::Status s = db->get(key, val_out);
        CHK_FALSE(s);
    }
    return 0;
}

int _iterate_keys(jungle::DB* db,
                  size_t start,
                  size_t end,
                  size_t step,
                  const std::string& key_fmt,
                  const std::string& val_fmt)
{
    // NOTE: `end` is inclusive: [start, end]

    jungle::Status s;
    jungle::Iterator itr;

    char s_str[MAX_TEST_LEN];
    char e_str[MAX_TEST_LEN];
    sprintf(s_str, key_fmt.c_str(), start);
    sprintf(e_str, key_fmt.c_str(), end);
    jungle::SizedBuf s_key(s_str);
    jungle::SizedBuf e_key(e_str);
    CHK_Z(itr.init(db, s_key, e_key));

    char key_str[MAX_TEST_LEN];
    char val_str[MAX_TEST_LEN];
    size_t count = 0;
    size_t idx = start;
    do {
        jungle::Record rec_out;
        s = itr.get(rec_out);
        if (!s) break;

        sprintf(key_str, key_fmt.c_str(), idx);
        jungle::SizedBuf key(key_str);

        CHK_EQ(key, rec_out.kv.key);

        if (!val_fmt.empty()) {
            sprintf(val_str, val_fmt.c_str(), idx);
            jungle::SizedBuf val(val_str);
            CHK_EQ(val, rec_out.kv.value);
        }
        rec_out.free();

        count++;
        idx += step;
    } while (itr.next().ok());
    CHK_EQ( ((end - start) / step) + 1, count );
    itr.close();

    return 0;
}

