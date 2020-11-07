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

#include <cinttypes>
#include <vector>

#include <stdio.h>
#include <string.h>

#include "libjungle/jungle.h"
#include "log_file.h"
#include "log_mgr.h"
#include "memtable.h"

#include "test_common.h"

using namespace jungle;

int memtable_key_itr_test() {
    Status s;
    DBConfig config;
    LogMgrOptions l_opt;
    l_opt.dbConfig = &config;
    LogMgr l_mgr(nullptr, l_opt);
    LogFile l_file(&l_mgr);
    MemTable mt(&l_file);
    mt.init();

    size_t num = 10;
    size_t modulo = 3;
    std::vector<Record> rec(num);
    size_t idx[modulo];
    char keybuf[32];
    char valbuf[32];
    for (size_t ii=0; ii<num; ++ii) {
        rec[ii].seqNum = ii;
        sprintf(keybuf, "key%zu", ii % modulo);
        sprintf(valbuf, "value%zu_%zu", ii % modulo, ii);
        idx[ii % 3] = ii;
        rec[ii].kv.key.alloc(keybuf);
        rec[ii].kv.value.alloc(valbuf);
        CHK_Z(mt.putNewRecord(rec[ii]));
    }

    // Point query.
    for (size_t ii=0; ii<modulo; ++ii) {
        sprintf(keybuf, "key%zu", ii);
        sprintf(valbuf, "value%zu_%zu", ii, idx[ii]);
        SizedBuf key(keybuf);
        Record rec_out;
        CHK_Z(mt.getRecordByKey(NOT_INITIALIZED, key, nullptr, rec_out, false, false));
        CHK_EQ(SizedBuf(valbuf), rec_out.kv.value);
    }

    MemTable::Iterator m_itr;
    CHK_Z(m_itr.init(&mt, SizedBuf(), SizedBuf(), NOT_INITIALIZED));

    size_t count = 0;

    // Forward
    do {
        Record rec_out;
        s = m_itr.get(rec_out);
        if (!s) break;

        sprintf(keybuf, "key%zu", count);
        sprintf(valbuf, "value%zu_%zu", count, idx[count]);
        CHK_EQ(SizedBuf(keybuf), rec_out.kv.key);
        CHK_EQ(SizedBuf(valbuf), rec_out.kv.value);
        count++;
    } while (m_itr.next());
    CHK_EQ(modulo, count);

    // Backward
    do {
        count--;

        Record rec_out;
        s = m_itr.get(rec_out);
        if (!s) break;

        sprintf(keybuf, "key%zu", count);
        sprintf(valbuf, "value%zu_%zu", count, idx[count]);
        CHK_EQ(SizedBuf(keybuf), rec_out.kv.key);
        CHK_EQ(SizedBuf(valbuf), rec_out.kv.value);
    } while (m_itr.prev());

    for (size_t ii=0; ii<num; ++ii) {
        rec[ii].free();
    }

    return 0;
}

int memtable_key_itr_chk_test() {
    Status s;
    DBConfig config;
    LogMgrOptions l_opt;
    l_opt.dbConfig = &config;
    LogMgr l_mgr(nullptr, l_opt);
    LogFile l_file(&l_mgr);
    MemTable mt(&l_file);
    mt.init();

    size_t num = 10;
    size_t modulo = 3;
    std::vector<Record> rec(num);

    char keybuf[32];
    char valbuf[32];
    for (size_t ii=0; ii<num; ++ii) {
        rec[ii].seqNum = ii;
        sprintf(keybuf, "key%zu", ii % modulo);
        sprintf(valbuf, "value%zu_%zu", ii % modulo, ii);
        rec[ii].kv.key.alloc(keybuf);
        rec[ii].kv.value.alloc(valbuf);
        CHK_Z(mt.putNewRecord(rec[ii]));
    }

    MemTable::Iterator m_itr;
    // Iterator on snapshot upto 2.
    CHK_Z(m_itr.init(&mt, SizedBuf(), SizedBuf(), 2));

    size_t count = 0;

    // Forward
    do {
        Record rec_out;
        s = m_itr.get(rec_out);
        if (!s) break;

        sprintf(keybuf, "key%zu", count);
        sprintf(valbuf, "value%zu_%zu", count, count);
        CHK_EQ(SizedBuf(keybuf), rec_out.kv.key);
        CHK_EQ(SizedBuf(valbuf), rec_out.kv.value);
        count++;
    } while (m_itr.next());
    CHK_EQ(modulo, count);

    // Backward
    do {
        count--;

        Record rec_out;
        s = m_itr.get(rec_out);
        if (!s) break;

        sprintf(keybuf, "key%zu", count);
        sprintf(valbuf, "value%zu_%zu", count, count);
        CHK_EQ(SizedBuf(keybuf), rec_out.kv.key);
        CHK_EQ(SizedBuf(valbuf), rec_out.kv.value);
    } while (m_itr.prev());

    for (size_t ii=0; ii<num; ++ii) {
        rec[ii].free();
    }

    return 0;
}

int memtable_nearest_search_test() {
    Status s;
    DBConfig config;
    LogMgrOptions l_opt;
    l_opt.dbConfig = &config;
    LogMgr l_mgr(nullptr, l_opt);
    LogFile l_file(&l_mgr);
    MemTable mt(&l_file);
    mt.init();

    const size_t NUM = 1000;
    std::vector<Record> rec{NUM};
    char keybuf[64];
    char valbuf[64];
    for (size_t ii = 0; ii < NUM; ++ii) {
        size_t idx = ii * 10;
        rec[ii].seqNum = ii;
        sprintf(keybuf, "key%05zu", idx);
        sprintf(valbuf, "value%05zu", idx);
        rec[ii].kv.key.alloc(keybuf);
        rec[ii].kv.value.alloc(valbuf);
        CHK_Z(mt.putNewRecord(rec[ii]));
    }

    auto verify_func = [&](size_t ii, SearchOptions s_opt, bool exact_query) -> int {
        int64_t idx = exact_query ? ii * 10 : ii * 10 + (s_opt.isGreater() ? 1 : -1);
        int64_t exp_idx = 0;

        if (exact_query) {
            if (s_opt.isExactMatchAllowed()) {
                exp_idx = idx;
            } else {
                exp_idx = (ii + (s_opt.isGreater() ? 1 : -1)) * 10;
            }
        } else {
            exp_idx = (ii + (s_opt.isGreater() ? 1 : -1)) * 10;
        }

        if (idx >= 0) {
            sprintf(keybuf, "key%05zd", idx);
        } else {
            sprintf(keybuf, "000");
        }
        sprintf(valbuf, "value%05zd", exp_idx);
        Record rec_out;
        s = mt.getNearestRecordByKey( NOT_INITIALIZED,
                                      SizedBuf(strlen(keybuf), keybuf),
                                      rec_out,
                                      false,
                                      false,
                                      s_opt );
        bool exp_succ = false;
        if (exact_query) {
            if ( s_opt.isExactMatchAllowed() ||
                 (s_opt.isGreater() && ii < NUM - 1) ||
                 (s_opt.isSmaller() && ii > 0) ) {
                exp_succ = true;
            }
        } else {
            if ( (s_opt.isGreater() && ii < NUM - 1) ||
                 (s_opt.isSmaller() && ii > 0) ) {
                exp_succ = true;
            }
        }

        if (exp_succ) {
            CHK_Z(s);
            CHK_EQ( SizedBuf(strlen(valbuf), valbuf), rec_out.kv.value );
        } else  {
            CHK_NOT(s);
        }
        return 0;
    };

    // Greater or equal
    for (size_t ii = 0; ii < NUM; ++ii) {
        for (bool exact_query: {true, false}) {
            for (SearchOptions s_opt: { SearchOptions::GREATER_OR_EQUAL,
                                        SearchOptions::GREATER,
                                        SearchOptions::SMALLER_OR_EQUAL,
                                        SearchOptions::SMALLER }) {
                CHK_Z( verify_func(ii, s_opt, exact_query) );
            }
        }
    }

    for (size_t ii=0; ii<NUM; ++ii) {
        rec[ii].free();
    }

    return 0;
}


int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.doTest("memtable key itr test", memtable_key_itr_test);
    ts.doTest("memtable key itr chk test", memtable_key_itr_chk_test);
    ts.doTest("memtable nearest search test", memtable_nearest_search_test);

    return 0;
}
