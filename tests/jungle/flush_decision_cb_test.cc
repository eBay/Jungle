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

/**
 * Tests for FlushDecisionCb and rollback in normal (non-logSectionOnly) mode.
 * These tests verify the fixes for the snapshot isolation violation bug
 * (NG-7651) where:
 *   1. rollbackSafeSeqnum=0 was treated as "no limit" instead of "flush nothing"
 *   2. Rollback in normal mode should work for data still in the log section
 *   3. FlushDecisionCb correctly clamps flush boundaries
 */

#include "jungle_test_common.h"

#include "internal_helper.h"

#include <atomic>
#include <cstdlib>
#include <vector>

#include <stdio.h>

namespace flush_decision_cb_test {

using namespace jungle;

// ==========================================================================
// Test 1: FlushDecisionCb blocks flush when allowFlush=false
// ==========================================================================
int flush_blocked_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 10;

    // Callback always blocks flush.
    config.flushDecisionCbFunc = []() -> FlushDecisionCbResult {
        return FlushDecisionCbResult(false, 0);
    };

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Write and sync entries.
    const size_t N = 20;
    _set_keys(db, 0, N, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Attempt flush — should be blocked.
    FlushOptions f_opt;
    s = db->flushLogs(f_opt);
    // OPERATION_IN_PROGRESS means flush was blocked by callback.
    CHK_EQ( (int)Status::OPERATION_IN_PROGRESS, (int)s );

    // Data should still be readable (in log section).
    _get_keys(db, 0, N, 1, "k%06zu", "v%06zu");

    // Nothing should be flushed.
    uint64_t flushed_seq = 0;
    s = db->getLastFlushedSeqNum(flushed_seq);
    // Either error (never flushed) or 0.
    if (s.ok()) {
        CHK_EQ( (uint64_t)0, flushed_seq );
    }

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 2: FlushDecisionCb clamps flush to rollbackSafeSeqnum
// ==========================================================================
int flush_clamped_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    const size_t SAFE_SEQ = 10;

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 5;

    // Callback allows flush but clamps to SAFE_SEQ.
    config.flushDecisionCbFunc = [SAFE_SEQ]() -> FlushDecisionCbResult {
        return FlushDecisionCbResult(true, SAFE_SEQ);
    };

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Write 20 entries (seqnums 1..20).
    const size_t N = 20;
    _set_keys(db, 0, N, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Flush — should be clamped to SAFE_SEQ.
    FlushOptions f_opt;
    s = db->flushLogs(f_opt);
    // Should succeed (flush was allowed, just clamped).
    CHK_Z(s);

    // Verify: last flushed seqnum should be <= SAFE_SEQ.
    uint64_t flushed_seq = 0;
    s = db->getLastFlushedSeqNum(flushed_seq);
    if (s.ok()) {
        CHK_SMEQ( flushed_seq, SAFE_SEQ );
    }

    // All data should still be readable.
    _get_keys(db, 0, N, 1, "k%06zu", "v%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 3: FlushDecisionCb with rollbackSafeSeqnum=0 blocks all flushes
//         (This is the NG-7651 bug fix — 0 must NOT mean "no limit")
// ==========================================================================
int flush_safe_seqnum_zero_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 5;

    // Callback: allowFlush=true, rollbackSafeSeqnum=0.
    // With the bugfix, seqnum=0 means "flush nothing".
    config.flushDecisionCbFunc = []() -> FlushDecisionCbResult {
        return FlushDecisionCbResult(true, 0);
    };

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Write entries.
    const size_t N = 15;
    _set_keys(db, 0, N, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Attempt flush — rollbackSafeSeqnum=0 should block the flush.
    FlushOptions f_opt;
    s = db->flushLogs(f_opt);
    CHK_EQ( (int)Status::OPERATION_IN_PROGRESS, (int)s );

    // Verify nothing was flushed.
    uint64_t flushed_seq = 0;
    s = db->getLastFlushedSeqNum(flushed_seq);
    if (s.ok()) {
        CHK_EQ( (uint64_t)0, flushed_seq );
    }

    // All data should still be in log section and readable.
    _get_keys(db, 0, N, 1, "k%06zu", "v%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 4: Rollback in normal mode (non-logSectionOnly) for data in log section
// ==========================================================================
int rollback_normal_mode_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 10;
    // NOT logSectionOnly — this is normal (log+table) mode.

    // Block all flushes so data stays in log section.
    config.flushDecisionCbFunc = []() -> FlushDecisionCbResult {
        return FlushDecisionCbResult(false, 0);
    };

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Write 15 entries.
    const size_t N = 15, ROLLBACK = 8;
    _set_keys(db, 0, N, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Rollback to seqnum ROLLBACK.
    CHK_Z( db->rollback(ROLLBACK) );

    // Verify max seqnum.
    uint64_t max_seq = 0;
    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( ROLLBACK, max_seq );

    // Close and reopen to verify persistence and safe re-read.
    // (In production, rollback happens during init, followed by reopen.)
    CHK_Z( DB::close(db) );
    CHK_Z( DB::open(&db, filename, config) );

    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( ROLLBACK, max_seq );

    // Entries up to ROLLBACK should exist.
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");

    // Entries beyond ROLLBACK should NOT exist.
    KV kv_out;
    for (size_t ii = ROLLBACK + 1; ii <= N; ++ii) {
        CHK_FALSE( db->getSN(ii, kv_out) );
    }

    // Write new entries after rollback (simulating Raft replay).
    _set_keys(db, ROLLBACK, ROLLBACK + 5, 1, "k%06zu", "v2_%06zu");
    _get_keys(db, ROLLBACK, ROLLBACK + 5, 1, "k%06zu", "v2_%06zu");

    // Close and reopen again.
    CHK_Z( DB::close(db) );
    CHK_Z( DB::open(&db, filename, config) );

    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( (uint64_t)(ROLLBACK + 5), max_seq );
    _get_keys(db, 0, ROLLBACK, 1, "k%06zu", "v%06zu");
    _get_keys(db, ROLLBACK, ROLLBACK + 5, 1, "k%06zu", "v2_%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 5: Rollback fails with ALREADY_FLUSHED for data in LSM tables
// ==========================================================================
int rollback_already_flushed_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 10;

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Write entries and flush to LSM.
    const size_t N = 20;
    _set_keys(db, 0, N, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    FlushOptions f_opt;
    CHK_Z( db->flushLogs(f_opt) );

    // Verify data is flushed.
    uint64_t flushed_seq = 0;
    CHK_Z( db->getLastFlushedSeqNum(flushed_seq) );
    CHK_GT( flushed_seq, (uint64_t)0 );

    // Try to rollback to a point within the flushed range — should fail.
    s = db->rollback(5);
    CHK_EQ( (int)Status::ALREADY_FLUSHED, (int)s );

    // Data should remain intact.
    _get_keys(db, 0, N, 1, "k%06zu", "v%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 6: Rollback on empty DB is a no-op (should not crash)
// ==========================================================================
int rollback_empty_db_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Rollback on empty DB — should succeed (no-op).
    CHK_Z( db->rollback(0) );
    CHK_Z( db->rollback(100) );

    // DB should still work fine.
    _set_keys(db, 0, 5, 1, "k%06zu", "v%06zu");
    _get_keys(db, 0, 5, 1, "k%06zu", "v%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 7: Rollback where target >= max seqnum is a no-op
// ==========================================================================
int rollback_noop_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.logSectionOnly = true;

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    const size_t N = 10;
    _set_keys(db, 0, N, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Rollback to exactly max seqnum — no-op.
    CHK_Z( db->rollback(N) );
    uint64_t max_seq = 0;
    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( N, max_seq );

    // Rollback beyond max seqnum — no-op.
    CHK_Z( db->rollback(N + 100) );
    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( N, max_seq );

    // Data intact.
    _get_keys(db, 0, N, 1, "k%06zu", "v%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 8: FlushDecisionCb + rollback integration
//         Simulates the NuKvCore production scenario:
//         1. Write data, flush clamped by checkpoint seqnum
//         2. Write more data (beyond checkpoint), close DB
//         3. Reopen DB, rollback to checkpoint seqnum
//         4. Verify data integrity
// ==========================================================================
int flush_clamp_then_rollback_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    std::atomic<uint64_t> safe_seqnum{0};

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 10;

    // Simulate NuKvCore's FlushDecisionCb: clamp to safe_seqnum.
    config.flushDecisionCbFunc = [&safe_seqnum]() -> FlushDecisionCbResult {
        uint64_t ss = safe_seqnum.load();
        if (ss == 0) {
            return FlushDecisionCbResult(false, 0);
        }
        return FlushDecisionCbResult(true, ss);
    };

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Phase 1: Write 10 entries. No checkpoint yet (safe_seqnum=0).
    _set_keys(db, 0, 10, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Flush should be blocked (safe_seqnum=0 → allowFlush=false).
    FlushOptions f_opt;
    s = db->flushLogs(f_opt);
    CHK_EQ( (int)Status::OPERATION_IN_PROGRESS, (int)s );

    // Phase 2: Simulate "first checkpoint" at seqnum 10.
    safe_seqnum.store(10);

    // Write more entries (11..20) — these are beyond the checkpoint.
    _set_keys(db, 10, 20, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // Flush — should clamp to seqnum 10.
    CHK_Z( db->flushLogs(f_opt) );

    uint64_t flushed_seq = 0;
    CHK_Z( db->getLastFlushedSeqNum(flushed_seq) );
    CHK_SMEQ( flushed_seq, (uint64_t)10 );

    // All data readable before close.
    _get_keys(db, 0, 20, 1, "k%06zu", "v%06zu");

    // Phase 3: Close and reopen (simulating process crash + restart).
    CHK_Z( DB::close(db) );
    CHK_Z( DB::open(&db, filename, config) );

    // Rollback to checkpoint seqnum (simulating rollback-on-init).
    // Data 1-10 is in LSM tables, data 11-20 is in log section.
    CHK_Z( db->rollback(10) );

    // Verify max seqnum after rollback (without close/reopen).
    uint64_t max_seq = 0;
    s = db->getMaxSeqNum(max_seq);
    // max_seq should be 10 (entries 11-20 truncated from log).
    if (s.ok()) {
        CHK_EQ( (uint64_t)10, max_seq );
    }

    // Entries 11-20 should NOT exist by seqnum.
    KV kv_out;
    for (size_t ii = 11; ii <= 20; ++ii) {
        CHK_FALSE( db->getSN(ii, kv_out) );
    }

    // Phase 4: Close and reopen to verify persistence.
    CHK_Z( DB::close(db) );
    CHK_Z( DB::open(&db, filename, config) );

    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( (uint64_t)10, max_seq );

    // Entries 1-10 should exist (from LSM tables).
    _get_keys(db, 0, 10, 1, "k%06zu", "v%06zu");

    // Phase 5: Write new entries after rollback (simulating Raft replay).
    _set_keys(db, 10, 15, 1, "k%06zu", "v2_%06zu");
    _get_keys(db, 10, 15, 1, "k%06zu", "v2_%06zu");

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

// ==========================================================================
// Test 9: Dynamic FlushDecisionCb — safe seqnum advances over time
// ==========================================================================
int flush_dynamic_safe_seqnum_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    Status s;
    jungle::init(GlobalConfig());

    std::atomic<uint64_t> safe_seqnum{5};

    DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.maxEntriesInLogFile = 10;

    config.flushDecisionCbFunc = [&safe_seqnum]() -> FlushDecisionCbResult {
        return FlushDecisionCbResult(true, safe_seqnum.load());
    };

    DB* db;
    CHK_Z( DB::open(&db, filename, config) );

    // Write 20 entries.
    _set_keys(db, 0, 20, 1, "k%06zu", "v%06zu");
    CHK_Z( db->sync(false) );

    // First flush: clamped to 5.
    FlushOptions f_opt;
    CHK_Z( db->flushLogs(f_opt) );
    uint64_t flushed_seq = 0;
    CHK_Z( db->getLastFlushedSeqNum(flushed_seq) );
    CHK_SMEQ( flushed_seq, (uint64_t)5 );

    // Advance safe seqnum to 15.
    safe_seqnum.store(15);

    // Second flush: should flush up to 15.
    CHK_Z( db->flushLogs(f_opt) );
    CHK_Z( db->getLastFlushedSeqNum(flushed_seq) );
    CHK_SMEQ( flushed_seq, (uint64_t)15 );

    // All data readable.
    _get_keys(db, 0, 20, 1, "k%06zu", "v%06zu");

    // Rollback to 15 should succeed (data 16-20 in log).
    CHK_Z( db->rollback(15) );
    uint64_t max_seq = 0;
    CHK_Z( db->getMaxSeqNum(max_seq) );
    CHK_EQ( (uint64_t)15, max_seq );

    // Rollback to 5 should fail (data 1-15 is in LSM).
    s = db->rollback(5);
    CHK_EQ( (int)Status::ALREADY_FLUSHED, (int)s );

    CHK_Z( DB::close(db) );
    CHK_Z( jungle::shutdown() );
    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

} // namespace flush_decision_cb_test

using namespace flush_decision_cb_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    ts.doTest("flush blocked test",
              flush_blocked_test);

    ts.doTest("flush clamped test",
              flush_clamped_test);

    ts.doTest("flush safe seqnum zero test",
              flush_safe_seqnum_zero_test);

    ts.doTest("rollback normal mode test",
              rollback_normal_mode_test);

    ts.doTest("rollback already flushed test",
              rollback_already_flushed_test);

    ts.doTest("rollback empty db test",
              rollback_empty_db_test);

    ts.doTest("rollback noop test",
              rollback_noop_test);

    ts.doTest("flush clamp then rollback test",
              flush_clamp_then_rollback_test);

    ts.doTest("flush dynamic safe seqnum test",
              flush_dynamic_safe_seqnum_test);

    return 0;
}
