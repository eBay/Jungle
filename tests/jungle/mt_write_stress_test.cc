/**
 * Copyright (C) 2024-present Jung-Sang Ahn <jungsang.ahn@gmail.com>
 * All rights reserved.
 *
 * Multi-threaded write stress test for fast path optimization.
 * Tests concurrent writes without sequence number override.
 */

#include "libjungle/jungle.h"

#include "test_common.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace jungle;

int mt_write_stress_test(size_t num_threads, size_t ops_per_thread) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    DBConfig config;
    config.maxEntriesInLogFile = 1000000;  // Large enough to avoid file rotation during test

    DB* db = nullptr;
    CHK_Z(DB::open(&db, filename, config));

    std::atomic<size_t> total_writes{0};
    std::atomic<bool> has_error{false};
    std::vector<std::thread> threads;

    // Spawn writer threads
    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < ops_per_thread && !has_error; i++) {
                char key[64], val[128];
                snprintf(key, sizeof(key), "thread%zu_key%08zu", t, i);
                snprintf(val, sizeof(val), "value_%zu_%zu", t, i);

                Status s = db->set(KV(key, val));
                if (!s) {
                    has_error = true;
                    TestSuite::_msg("Thread %zu write failed at %zu: %d\n", t, i, (int)s);
                    break;
                }
                total_writes.fetch_add(1);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    CHK_FALSE(has_error);
    CHK_EQ(num_threads * ops_per_thread, total_writes.load());

    // Verify data integrity - read back all keys
    TestSuite::_msg("Verifying %zu writes...\n", total_writes.load());
    
    for (size_t t = 0; t < num_threads; t++) {
        for (size_t i = 0; i < ops_per_thread; i++) {
            char key[64], expected_val[128];
            snprintf(key, sizeof(key), "thread%zu_key%08zu", t, i);
            snprintf(expected_val, sizeof(expected_val), "value_%zu_%zu", t, i);

            SizedBuf value_out;
            SizedBuf::Holder h(value_out);
            Status s = db->get(SizedBuf(key), value_out);
            CHK_Z(s);
            CHK_EQ(SizedBuf(expected_val), value_out);
        }
    }

    CHK_Z(DB::close(db));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int mt_write_read_interleaved_test(size_t num_threads, size_t ops_per_thread) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    DBConfig config;
    config.maxEntriesInLogFile = 1000000;

    DB* db = nullptr;
    CHK_Z(DB::open(&db, filename, config));

    std::atomic<size_t> total_ops{0};
    std::atomic<bool> has_error{false};
    std::vector<std::thread> threads;

    // Half writers, half readers
    size_t num_writers = num_threads / 2;
    size_t num_readers = num_threads - num_writers;

    // Start writers
    for (size_t t = 0; t < num_writers; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < ops_per_thread && !has_error; i++) {
                char key[64], val[128];
                snprintf(key, sizeof(key), "key%08zu", (t * ops_per_thread + i) % (ops_per_thread * 2));
                snprintf(val, sizeof(val), "value_%zu_%zu", t, i);

                Status s = db->set(KV(key, val));
                if (!s) {
                    has_error = true;
                    break;
                }
                total_ops.fetch_add(1);
            }
        });
    }

    // Start readers (may read partially written data)
    for (size_t t = 0; t < num_readers; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < ops_per_thread && !has_error; i++) {
                char key[64];
                snprintf(key, sizeof(key), "key%08zu", i % (ops_per_thread * 2));

                SizedBuf value_out;
                SizedBuf::Holder h(value_out);
                Status s = db->get(SizedBuf(key), value_out);
                // Key may not exist yet, that's OK
                if (!s && s != Status::KEY_NOT_FOUND) {
                    has_error = true;
                    break;
                }
                total_ops.fetch_add(1);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    CHK_FALSE(has_error);

    CHK_Z(DB::close(db));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int mt_write_with_flush_test(size_t num_threads, size_t ops_per_thread) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    DBConfig config;
    config.maxEntriesInLogFile = 100000;  // Larger to reduce file rotation during test

    DB* db = nullptr;
    CHK_Z(DB::open(&db, filename, config));

    std::atomic<size_t> total_writes{0};
    std::atomic<bool> has_error{false};
    std::atomic<bool> stop_flusher{false};
    std::vector<std::thread> threads;

    // Background flusher thread
    std::thread flusher([&]() {
        while (!stop_flusher) {
            db->sync(false);
            db->flushLogs(FlushOptions());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    // Writer threads
    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (size_t i = 0; i < ops_per_thread && !has_error; i++) {
                char key[64], val[128];
                snprintf(key, sizeof(key), "thread%zu_key%08zu", t, i);
                snprintf(val, sizeof(val), "value_%zu_%zu", t, i);

                Status s = db->set(KV(key, val));
                if (!s) {
                    has_error = true;
                    TestSuite::_msg("Thread %zu write failed at %zu: %d\n", t, i, (int)s);
                    break;
                }
                total_writes.fetch_add(1);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    stop_flusher = true;
    flusher.join();

    CHK_FALSE(has_error);
    CHK_EQ(num_threads * ops_per_thread, total_writes.load());

    // Sync and flush remaining data
    CHK_Z(db->sync(false));
    CHK_Z(db->flushLogs(FlushOptions()));

    // Verify
    TestSuite::_msg("Verifying %zu writes after flush...\n", total_writes.load());
    
    for (size_t t = 0; t < num_threads; t++) {
        for (size_t i = 0; i < ops_per_thread; i++) {
            char key[64], expected_val[128];
            snprintf(key, sizeof(key), "thread%zu_key%08zu", t, i);
            snprintf(expected_val, sizeof(expected_val), "value_%zu_%zu", t, i);

            SizedBuf value_out;
            SizedBuf::Holder h(value_out);
            Status s = db->get(SizedBuf(key), value_out);
            CHK_Z(s);
            CHK_EQ(SizedBuf(expected_val), value_out);
        }
    }

    CHK_Z(DB::close(db));

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int mt_write_reopen_test(size_t num_threads, size_t ops_per_thread) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    DBConfig config;
    config.maxEntriesInLogFile = 100000;

    // Phase 1: Multi-threaded writes
    {
        DB* db = nullptr;
        CHK_Z(DB::open(&db, filename, config));

        std::atomic<bool> has_error{false};
        std::vector<std::thread> threads;

        for (size_t t = 0; t < num_threads; t++) {
            threads.emplace_back([&, t]() {
                for (size_t i = 0; i < ops_per_thread && !has_error; i++) {
                    char key[64], val[128];
                    snprintf(key, sizeof(key), "thread%zu_key%08zu", t, i);
                    snprintf(val, sizeof(val), "value_%zu_%zu", t, i);

                    Status s = db->set(KV(key, val));
                    if (!s) {
                        has_error = true;
                        break;
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        CHK_FALSE(has_error);
        CHK_Z(db->sync(false));
        CHK_Z(DB::close(db));
    }

    // Phase 2: Reopen and verify
    {
        DB* db = nullptr;
        CHK_Z(DB::open(&db, filename, config));

        TestSuite::_msg("Verifying after reopen...\n");
        
        for (size_t t = 0; t < num_threads; t++) {
            for (size_t i = 0; i < ops_per_thread; i++) {
                char key[64], expected_val[128];
                snprintf(key, sizeof(key), "thread%zu_key%08zu", t, i);
                snprintf(expected_val, sizeof(expected_val), "value_%zu_%zu", t, i);

                SizedBuf value_out;
                SizedBuf::Holder h(value_out);
                Status s = db->get(SizedBuf(key), value_out);
                CHK_Z(s);
                CHK_EQ(SizedBuf(expected_val), value_out);
            }
        }

        CHK_Z(DB::close(db));
    }

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    // Basic multi-threaded write tests
    ts.doTest("mt write stress 2 threads", mt_write_stress_test, 2, (size_t)10000);
    ts.doTest("mt write stress 4 threads", mt_write_stress_test, 4, (size_t)10000);
    ts.doTest("mt write stress 8 threads", mt_write_stress_test, 8, (size_t)10000);
    ts.doTest("mt write stress 16 threads", mt_write_stress_test, 16, (size_t)5000);

    // Interleaved read/write
    ts.doTest("mt write read interleaved 4 threads", mt_write_read_interleaved_test, 4, (size_t)10000);
    ts.doTest("mt write read interleaved 8 threads", mt_write_read_interleaved_test, 8, (size_t)10000);

    // Multi-threaded writes with concurrent flush (less aggressive)
    ts.doTest("mt write with flush 4 threads", mt_write_with_flush_test, 4, (size_t)5000);

    // Reopen and verify durability
    ts.doTest("mt write reopen 4 threads", mt_write_reopen_test, 4, (size_t)10000);
    ts.doTest("mt write reopen 8 threads", mt_write_reopen_test, 8, (size_t)5000);

    return 0;
}
