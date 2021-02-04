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

#include "bench_config.h"
#include "internal_helper.h"
#include "latency_collector.h"
#include "test_common.h"

// TODO: Other DBs
#include "adapter_selector.h"
#include "db_adapter_jungle.h"

#include "murmurhash3.h"

#include <algorithm>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include <signal.h>
#include <stdio.h>
#include <unistd.h>

namespace jungle_bench {

#define MAX_KEYLEN (2048)

static LatencyCollector global_lat;
static bool force_use_existing = false;

static char* ABT_ARRAY = (char*)"abcdefghijklmnopqrstuvwxyz";
static size_t ABT_NUM = 26;

void generate_snippt(uint64_t index, char* buf, size_t len) {
    uint64_t vv = index;
#if 1
    uint32_t seed = 0;
    for (size_t jj = 0; jj < len; ++jj) {
        MurmurHash3_x86_32(&vv, sizeof(vv), seed, &seed);
        buf[jj] = ABT_ARRAY[seed % ABT_NUM];
    }

#else
    for (size_t jj = 0; jj < len; ++jj) {
        vv = vv % ABT_NUM;
        buf[jj] = ABT_ARRAY[vv];
        vv = (vv * 48271) % 199;
    }
#endif
}

void generate_key(const BenchConfig& conf,
                  uint64_t index,
                  char* buf,
                  size_t& buflen_inout)
{

    // TODO: conf should ensure the total length of key <= MAX_KEYLEN
    // or in there?
    size_t buf_len = 0;
    char* key = buf;
    for (size_t i = 0; i < conf.prefixLens.size(); ++i) {
        const auto& prefix_len = conf.prefixLens[i];
        auto fanout = conf.prefixFanout[i];

        size_t hash = 0;
        MurmurHash3_x86_32(&i, sizeof(i), hash, &hash);
        size_t prefix_index = index % fanout + hash;
        auto len = prefix_len.get(prefix_index);
        generate_snippt(prefix_index, key, len);
        key[len++] = '|';
        key += len;
        buf_len += len;
    }

    size_t len = conf.keyLen.get(index);
    // Minimum length: 8 bytes.
    if (len < 8) len = 8;
    buf_len += len;

    uint64_t vv = index;
    int ii = 7;
    while (vv >= ABT_NUM) {
        key[ii--] = ABT_ARRAY[vv % ABT_NUM];
        vv /= ABT_NUM;
    }
    key[ii--] = ABT_ARRAY[vv];
    for (int jj=ii; jj>=0; --jj) {
        key[jj] = ABT_ARRAY[0];
    }

    vv = index;
    key += 8;
    generate_snippt(vv, key, len - 8);

    buflen_inout = buf_len;
}

std::string generate_value(const BenchConfig& conf,
                           uint64_t index)
{
    size_t len = conf.valueLen.get(index);
    std::string ret = TestSuite::getTimeStringPlain();
    size_t remain = (len > ret.size()) ? len - ret.size() : 0;
    ret += "_" + std::string(remain, 'x');
    return ret;
}

uint64_t get_write_bytes() {
#if defined(__linux) && !defined(__ANDROID__)
    FILE *fp = fopen("/proc/self/io", "r");
    while(!feof(fp)) {
        char str[64];
        unsigned long temp;
        int ret = fscanf(fp, "%s %lu", str, &temp);
        (void)ret;
        if (!strcmp(str, "write_bytes:")) {
            fclose(fp);
            return temp;
        }
    }
    fclose(fp);
#endif
    // TODO: Other platforms?
    return 0;
}

uint64_t get_cpu_usage_ms() {
#if defined(__linux) && !defined(__ANDROID__)
    std::ifstream fs;
    std::string path = "/proc/self/stat";

    fs.open(path.c_str());
    if (!fs.good()) return 0;

    std::string dummy_str;
    uint64_t dummy_int;
    uint64_t user_time_ms = 0;
    uint64_t kernel_time_ms = 0;

    // 1) pid
    // 2) executable name (str)
    // 3) state (str)
    // 4) ppid
    // 5) pgrp
    // 6) session
    // 7) tty
    // 8) tpgid
    // 9) flags
    // 10) # minor page faults
    // 11) # minor page faults including children
    // 12) # major page faults
    // 13) # major page faults including children
    // 14) user time
    // 15) kernel time
    // ...

    fs >> dummy_int >> dummy_str >> dummy_str;
    fs >> dummy_int >> dummy_int >> dummy_int >> dummy_int;
    fs >> dummy_int >> dummy_int >> dummy_int >> dummy_int;
    fs >> dummy_int >> dummy_int >> user_time_ms >> kernel_time_ms;

    fs.close();

    // TODO: currently assuming 100Hz (10ms) jiffy.
    //       It should support all kinds of platforms.
    user_time_ms *= 10;
    kernel_time_ms *= 10;
    return user_time_ms + kernel_time_ms;
#endif
    // TODO: Other platforms?
    return 0;
}

uint64_t get_mem_usage() {
#if defined(__linux) && !defined(__ANDROID__)
    std::ifstream fs;
    std::string path = "/proc/self/statm";

    fs.open(path.c_str());
    if (!fs.good()) return 0;

    uint64_t dummy_int;
    uint64_t rss = 0;
    fs >> dummy_int >> rss >> dummy_int;
    fs.close();

    uint32_t pgsize = sysconf(_SC_PAGESIZE);
    rss *= pgsize;
    return rss;
#endif
    // TODO: Other platforms?
    return 0;
}

void print_latencies( const std::string& log_file,
                      const std::vector<std::string>& items )
{
    char buffer[1024];
    std::stringstream ss;

    sprintf(buffer,
            "%12s%12s%12s%12s%12s%12s\n",
            "type", "p50", "p99", "p99.9", "p99.99", "p99.999" );
    ss << buffer;

    for (const std::string& item: items) {
        sprintf(
            buffer,
            "%12s%12s%12s%12s%12s%12s\n",
            item.c_str(),
            TestSuite::usToString(global_lat.getPercentile(item, 50)).c_str(),
            TestSuite::usToString(global_lat.getPercentile(item, 99)).c_str(),
            TestSuite::usToString(global_lat.getPercentile(item, 99.9)).c_str(),
            TestSuite::usToString(global_lat.getPercentile(item, 99.99)).c_str(),
            TestSuite::usToString(global_lat.getPercentile(item, 99.999)).c_str() );
        ss << buffer;
    }
    ss << "-----\n";
    TestSuite::_msg("%s", ss.str().c_str());

    std::ofstream fs;
    fs.open(log_file, std::ofstream::out | std::ofstream::app);
    if (!fs.good()) return;

    fs << ss.str();
    fs.close();
}

int initial_load(const BenchConfig& conf,
                 const std::string& log_file_name,
                 DbAdapter* db_inst,
                 std::atomic<bool>& stop_signal)
{
    if (!conf.initialLoad) {
        TestSuite::_msg("skipping initial load, use existing DB\n");
        TestSuite::_msg("-----\n");
        return 0;
    }

    CHK_Z( db_inst->startInitialLoad() );

    // NOTE:
    //   Initial loading in a random order will be
    //   the worst case scenario for LSM-based approaches,
    //   as the key range every merge operation will be
    //   as wide as the entire key space, so that will touch
    //   almost all next level tables.
    std::vector<uint64_t> key_arr(conf.numKvPairs);
    std::iota(key_arr.begin(), key_arr.end(), 0);

    if (conf.initialLoadOrder == BenchConfig::RANDOM) {
        // Randomly ordered initial load, shuffle.
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(key_arr.begin(), key_arr.end(), g);
    }

    uint64_t wamp_base = get_write_bytes();
    uint64_t cpu_base = get_cpu_usage_ms();

    std::ofstream log_fs;
    log_fs.open( log_file_name, std::ofstream::out | std::ofstream::app );

    TestSuite::Timer tt;
    TestSuite::WorkloadGenerator wg(conf.initialLoadRate);

    //         time     xx s
    // initial load     x / y   speed   xx.x ops/s
    //        w.amp             s.amp
    //  average cpu             rss
    TestSuite::Displayer dd(4, 4);
    TestSuite::Timer dd_timer(80);
    TestSuite::Timer wamp_timer(1500);
    TestSuite::Timer samp_timer(5000);
    TestSuite::Timer log_timer(5000);
    dd.init();
    dd.setWidth( {20, 15, 10, 18} );
    dd.set(0, 0, "time");
    dd.set(1, 0, "initial load");
    dd.set(2, 0, "w.amp");
    dd.set(2, 2, "s.amp");
    dd.set(3, 0, "average cpu");
    dd.set(3, 2, "rss");

    uint64_t w_amount = 0;
    uint64_t s_amount = 0;
    uint64_t cpu_ms = 0;
    uint64_t rss_amount = 0;

    auto prefix_median_sum = std::accumulate(
        conf.prefixLens.begin(),
        conf.prefixLens.end(),
        static_cast<uint64_t>(0),
        [](uint64_t sum, const DistDef& dist_def) { return sum + dist_def.median; });
    char key_buf[MAX_KEYLEN];
    size_t key_len = 0;
    for (size_t ii = 0; ii < conf.numKvPairs; ++ii) {
        while (!wg.getNumOpsToDo()) {
            TestSuite::sleep_ms(1);
        }

        DbAdapter::KvElem elem;
        // TODO: Should be replaced with random generation.
        generate_key(conf, key_arr[ii], key_buf, key_len);
        elem.key = DbAdapter::Buffer(key_buf, key_len);

        std::string value_str = generate_value(conf, key_arr[ii]);
        elem.value = DbAdapter::Buffer(value_str);

        {   TestSuite::Timer set_timer;
            db_inst->set(elem);
            global_lat.addLatency("init.load", set_timer.getTimeUs());
        }

        uint64_t num_sets = ii + 1;

        if ( dd_timer.timeout() ||
             num_sets == conf.numKvPairs ) {
            dd_timer.reset();

            dd.set(0, 1, "%zu s", tt.getTimeSec());
            dd.set(1, 1, "%zu", num_sets);
            dd.set( 1, 3, "%.1f ops/s",
                    (double)num_sets * 1000000 / tt.getTimeUs() );

            if (wamp_timer.timeout()) {
                wamp_timer.reset();

                // Write amplification.
                w_amount = get_write_bytes() - wamp_base;
                dd.set(
                    2,
                    1,
                    "%.1f",
                    (double)w_amount / num_sets /
                        (conf.keyLen.median + conf.valueLen.median + prefix_median_sum));

                // CPU.
                cpu_ms = get_cpu_usage_ms() - cpu_base;
                dd.set( 3, 1, "%.1f %%", (double)cpu_ms / tt.getTimeMs() * 100 );

                // RSS.
                rss_amount = get_mem_usage();
                dd.set( 3, 3, "%s", TestSuite::sizeToString(rss_amount).c_str() );
            }

            if (samp_timer.timeout()) {
                samp_timer.reset();

                // Space amplification.
                s_amount = jungle::FileMgr::dirSize(conf.dbPath, true);
                dd.set(
                    2,
                    3,
                    "%.1f",
                    (double)s_amount / num_sets /
                        (conf.keyLen.median + conf.valueLen.median + prefix_median_sum));
            }

            // Write log file.
            if (log_timer.timeout()) {
                log_timer.reset();

                log_fs << TestSuite::getTimeStringPlain() << "\t"
                       << num_sets << "\t"
                       << 0 << "\t"
                       << 0 << "\t"
                       << w_amount << "\t"
                       << s_amount << "\t"
                       << cpu_ms << "\t"
                       << rss_amount
                       << std::endl;
            }

            dd.print();
        }

        if (stop_signal) break;
        wg.addNumOpsDone(1);
    }
    dd.print();

    CHK_Z( db_inst->endInitialLoad() );

    TestSuite::_msg("-----\n");
    log_fs << "-----" << std::endl;
    log_fs.close();

    print_latencies(log_file_name, {"init.load"});

    return 0;
}

struct BenchStatus {
    BenchStatus()
        : numSet(0)
        , numPoint(0)
        , numRange(0)
        , amountWriteByte(0)
        , amountSpaceByte(0)
        , cpuMs(0)
        , rssByte(0)
        {}
    std::atomic<uint64_t> numSet;
    std::atomic<uint64_t> numPoint;
    std::atomic<uint64_t> numRange;
    std::atomic<uint64_t> amountWriteByte;
    std::atomic<uint64_t> amountSpaceByte;
    std::atomic<uint64_t> cpuMs;
    std::atomic<uint64_t> rssByte;
};

int do_write(WorkerHandle* args,
             const WorkerDef& my_def,
             TestSuite::WorkloadGenerator& wg)
{
    thread_local TestSuite::Timer tt;

    if (my_def.rate >= 0) {
        size_t ops_todo = wg.getNumOpsToDo();
        if (!ops_todo) {
            TestSuite::sleep_us(100);
            return 0;
        }
    }

    thread_local char key_buf[MAX_KEYLEN];
    size_t key_len = 0;
    uint64_t idx = std::rand() % args->conf.numKvPairs;
    DbAdapter::KvElem elem;
    generate_key(args->conf, idx, key_buf, key_len);
    elem.key = DbAdapter::Buffer(key_buf, key_len);
    std::string value_str = generate_value(args->conf, idx);
    elem.value = DbAdapter::Buffer(value_str);
    {
        tt.reset();
        CHK_Z( args->dbInst->set(elem) );
        global_lat.addLatency("set", tt.getTimeUs());
    }

    wg.addNumOpsDone(1);
    args->stats.numSet.fetch_add(1);
    return 0;
}

int do_point_read(WorkerHandle* args,
                  const WorkerDef& my_def,
                  TestSuite::WorkloadGenerator& wg)
{
    thread_local TestSuite::Timer tt;

    if (my_def.rate >= 0) {
        size_t ops_todo = wg.getNumOpsToDo();
        if (!ops_todo) {
            TestSuite::sleep_us(100);
            return 0;
        }
    }

    char key_buf[MAX_KEYLEN];
    size_t key_len = 0;
    uint64_t idx = std::rand() % args->conf.numKvPairs;
    generate_key(args->conf, idx, key_buf, key_len);
    bool succ = true;
    {
        tt.reset();
        DbAdapter::Buffer value_out;
        int ret = args->dbInst->get( DbAdapter::Buffer(key_buf, key_len),
                                     value_out );
        if (ret != 0) {
            abort();
            succ = false;
        }
        global_lat.addLatency("get", tt.getTimeUs());
        value_out.free();
    }

    if (succ) {
        wg.addNumOpsDone(1);
        args->stats.numPoint.fetch_add(1);
    }
    return 0;
}

int do_range_read(WorkerHandle* args,
                  const WorkerDef& my_def,
                  TestSuite::WorkloadGenerator& wg)
{
    thread_local TestSuite::Timer tt;

    if (my_def.rate >= 0) {
        size_t ops_todo = wg.getNumOpsToDo();
        if (!ops_todo) {
            TestSuite::sleep_us(100);
            return 0;
        }
    }

    thread_local char skey_buf[MAX_KEYLEN], ekey_buf[MAX_KEYLEN];
    size_t skey_len = 0, ekey_len = 0;

    uint64_t s_idx = std::rand() % args->conf.numKvPairs;
    generate_key(args->conf, s_idx, skey_buf, skey_len);

    size_t batch_size = my_def.batchSize.get();
    if (s_idx + batch_size >= args->conf.numKvPairs) {
        batch_size = args->conf.numKvPairs - s_idx - 1;
    }

    // NOTE: Both start/end keys are inclusive,
    //       so that `batchsize == 0` means that `start_key == end_key`.
    generate_key(args->conf, s_idx + batch_size, ekey_buf, ekey_len);

    bool succ = true;
    {
        tt.reset();
        std::list<DbAdapter::KvElem> kvs_out;
        int ret = args->dbInst->getRange
                  ( DbAdapter::Buffer(skey_buf, skey_len),
                    DbAdapter::Buffer(ekey_buf, ekey_len),
                    kvs_out );
        if (ret != 0) {
            abort();
            succ = false;
        }
        // The number of returned records should metach.
        assert( kvs_out.size() == batch_size + 1 );

        global_lat.addLatency("iterate", tt.getTimeUs());

        for (auto& entry: kvs_out) {
            DbAdapter::KvElem& kk = entry;
            kk.key.free();
            kk.value.free();
        }
    }

    if (succ) {
        wg.addNumOpsDone(1);
        args->stats.numRange.fetch_add(1);
    }
    return 0;
}

int bench_worker(TestSuite::ThreadArgs* base_args) {
    WorkerHandle* args = static_cast<WorkerHandle*>(base_args);
    const WorkerDef& my_def = args->conf.workerDefs[args->wId];

#ifdef __linux__
    std::string t_name = my_def.getThreadName() + "_" +
                         std::to_string(args->wId);
    pthread_setname_np(pthread_self(), t_name.c_str());
#endif

    TestSuite::WorkloadGenerator wg(my_def.rate);
    while (!args->stopSignal.load()) {
        switch (my_def.type) {
        case WorkerDef::WRITER:
            CHK_Z( do_write(args, my_def, wg) );
            break;

        case WorkerDef::POINT_READER:
            CHK_Z( do_point_read(args, my_def, wg) );
            break;

        case WorkerDef::RANGE_READER:
            CHK_Z( do_range_read(args, my_def, wg) );
            break;

        default:
            TestSuite::sleep_us(1000);
            break;
        }
    }

    return 0;
}

struct DisplayArgs : public TestSuite::ThreadArgs {
    DisplayArgs(const BenchConfig& _conf,
                BenchStatus& _stat,
                std::atomic<bool>& _stop_signal)
        : conf(_conf)
        , stat(_stat)
        , stopSignal(_stop_signal)
        {}
    std::string dbFile;
    std::string logFile;
    const BenchConfig& conf;
    BenchStatus& stat;
    std::atomic<bool>& stopSignal;
};

int displayer(TestSuite::ThreadArgs* base_args) {
#ifdef __linux__
    pthread_setname_np(pthread_self(), "displayer");
#endif

    DisplayArgs* args = static_cast<DisplayArgs*>(base_args);

    //       x/x    total   IOPS    p50     p99
    //       set
    // point get
    // range get
    //     w.amp
    //     s.amp
    //       cpu                    rss

    // 6 rows, 5 columns

    TestSuite::Displayer dd(7, 5);
    dd.init();
    dd.set(0, 1, "total");
    dd.set(0, 2, "IOPS");
    dd.set(0, 3, "p50");
    dd.set(0, 4, "p99");
    dd.set(1, 0, "set");
    dd.set(2, 0, "point get");
    dd.set(3, 0, "range get");
    dd.set(4, 0, "w.amp");
    dd.set(5, 0, "s.amp");
    dd.set(6, 0, "cpu");
    dd.set(6, 3, "rss");

    std::vector<size_t> col_width( {15, 15, 15, 15, 15} );
    dd.setWidth(col_width);

    uint64_t wamp_base = get_write_bytes();
    uint64_t cpu_base = get_cpu_usage_ms();
    uint64_t cpu_inst_base = get_cpu_usage_ms();

    std::ofstream log_fs;
    log_fs.open( args->logFile, std::ofstream::out | std::ofstream::app );

    TestSuite::Timer tt;
    TestSuite::Timer wamp_timer(1500);
    TestSuite::Timer samp_timer(5000);
    TestSuite::Timer log_timer(5000);
    tt.resetSec(args->conf.durationSec);

    auto prefix_median_sum = std::accumulate(
        args->conf.prefixLens.begin(),
        args->conf.prefixLens.end(),
        static_cast<uint64_t>(0),
        [](uint64_t sum, const DistDef& dist_def) { return sum + dist_def.median; });
    while (!tt.timeout() && !args->stopSignal.load()) {
        TestSuite::sleep_ms(80);
        uint64_t cur_us = tt.getTimeUs();
        if (!cur_us) continue;

        dd.set( 0, 0, "%zu/%zu s", cur_us / 1000000, args->conf.durationSec );

        // writer row
        dd.set( 1, 1, "%s",
                TestSuite::countToString( args->stat.numSet ).c_str() );
        dd.set( 1, 2, "%s ops/s",
                TestSuite::countToString
                ( args->stat.numSet * 1000000 / cur_us ).c_str() );
        dd.set( 1, 3, "%s",
                TestSuite::usToString
                ( global_lat.getPercentile("set", 50) ).c_str() );
        dd.set( 1, 4, "%s",
                TestSuite::usToString
                ( global_lat.getPercentile("set", 99) ).c_str() );

        // p reader row
        dd.set( 2, 1, "%s",
                TestSuite::countToString( args->stat.numPoint ).c_str() );
        dd.set( 2, 2, "%s ops/s",
                TestSuite::countToString
                ( args->stat.numPoint * 1000000 / cur_us ).c_str() );
        dd.set( 2, 3, "%s",
                TestSuite::usToString
                ( global_lat.getPercentile("get", 50) ).c_str() );
        dd.set( 2, 4, "%s",
                TestSuite::usToString
                ( global_lat.getPercentile("get", 99) ).c_str() );

        // r reader row
        dd.set( 3, 1, "%s",
                TestSuite::countToString( args->stat.numRange ).c_str() );
        dd.set( 3, 2, "%s ops/s",
                TestSuite::countToString
                ( args->stat.numRange * 1000000 / cur_us ).c_str() );
        dd.set( 3, 3, "%s",
                TestSuite::usToString
                ( global_lat.getPercentile("iterate", 50) ).c_str() );
        dd.set( 3, 4, "%s",
                TestSuite::usToString
                ( global_lat.getPercentile("iterate", 99) ).c_str() );

        // w.amp row
        if (wamp_timer.timeout()) {
            uint64_t inst_time_us = wamp_timer.getTimeUs();
            uint64_t inst_time_ms = inst_time_us / 1000;
            wamp_timer.reset();

            uint64_t w_amt = get_write_bytes() - wamp_base;

            // Amount of writes.
            dd.set( 4, 1, "%s",
                    TestSuite::sizeToString(w_amt).c_str() );
            if (args->stat.numSet.load()) {
                // Write amplification.
                dd.set(4,
                       2,
                       "%.1fx",
                       (double)w_amt / args->stat.numSet /
                           (args->conf.keyLen.median + args->conf.valueLen.median +
                            prefix_median_sum));
            } else {
                dd.set( 4, 2, "--" );
            }
            // Instant write throughput.
            dd.set( 4, 3, "I %s/s",
                    TestSuite::sizeThroughputStr
                               ( w_amt - args->stat.amountWriteByte,
                                 inst_time_us ).c_str() );
            // Average write throughput.
            dd.set( 4, 4, "A %s/s",
                    TestSuite::sizeThroughputStr
                               ( w_amt, tt.getTimeUs() ).c_str() );
            args->stat.amountWriteByte = w_amt;

            // CPU.
            uint64_t cur_cpu_ms = get_cpu_usage_ms();
            uint64_t cpu_ms = cur_cpu_ms - cpu_base;
            uint64_t cpu_ms_inst = cur_cpu_ms - cpu_inst_base;
            cpu_inst_base = cur_cpu_ms;
            args->stat.cpuMs = cpu_ms;
            // Instant usage (since last display).
            dd.set( 6, 1, "I %.1f %%", (double)cpu_ms_inst / inst_time_ms * 100 );
            // Overall average.
            dd.set( 6, 2, "A %.1f %%", (double)cpu_ms / tt.getTimeMs() * 100 );

            // RSS.
            uint64_t rss_amount = get_mem_usage();
            args->stat.rssByte = rss_amount;
            dd.set( 6, 4, "%s", TestSuite::sizeToString(rss_amount).c_str() );
        }

        // s.amp row
        if (samp_timer.timeout()) {
            samp_timer.reset();

            uint64_t samp = jungle::FileMgr::dirSize(args->dbFile, true);
            args->stat.amountSpaceByte = samp;

            dd.set( 5, 1, "%s",
                    TestSuite::sizeToString(samp).c_str() );
            dd.set(5,
                   2,
                   "%.1fx",
                   (double)samp / args->conf.numKvPairs /
                       (args->conf.keyLen.median + args->conf.valueLen.median +
                        prefix_median_sum));
        }

        // Write log file.
        if (log_timer.timeout()) {
            log_timer.reset();

            log_fs
                << TestSuite::getTimeStringPlain() << "\t"
                << args->stat.numSet.load() << "\t"
                << args->stat.numPoint.load() << "\t"
                << args->stat.numRange.load() << "\t"
                << args->stat.amountWriteByte.load() << "\t"
                << args->stat.amountSpaceByte.load() << "\t"
                << args->stat.cpuMs.load() << "\t"
                << args->stat.rssByte.load()
                << std::endl;
        }
        dd.print();

        // Check the number of ops if set.
        if (args->conf.durationOps) {
            uint64_t ops = args->stat.numSet +
                           args->stat.numPoint +
                           args->stat.numRange;
            if ( args->conf.durationOps &&
                 ops >= args->conf.durationOps ) break;
        }
    }

    TestSuite::_msg("-----\n");
    log_fs << "-----\n";
    log_fs.close();
    args->stopSignal.store(true);

    return 0;
}

static void (*old_sigint_handler)(int) = nullptr;
static std::atomic<bool>* global_ref_of_stop_signal = nullptr;
static std::atomic<bool>* global_ref_of_skip_cooling_down = nullptr;

void custom_handler_2nd(int signum)
{
    std::cout << std::endl
              << "Force termination (DB corruption may happen)"
              << std::endl;
    exit(-1);
}

void custom_handler(int signum)
{
    std::cout << std::endl
              << "Got termination signal, will wait for safe DB close.."
              << std::endl;
    signal(SIGINT, custom_handler_2nd);
    global_ref_of_stop_signal->store(true);
    global_ref_of_skip_cooling_down->store(true);
}

int bench_main(const std::string& config_file) {
    BenchConfig conf = BenchConfig::load(config_file);
    if (force_use_existing) conf.initialLoad = false;
    std::cout << conf.toString() << std::endl;

    std::string db_file = conf.dbPath;
    if (conf.initialLoad && jungle::FileMgr::exist(db_file)) {
        // Previous DB already exists, ask user before delete it.
        char answer[64],
             *ret = nullptr;
        memset(answer, 0x0, sizeof(answer));

        std::cout << "-----" << std::endl
                  << "Previous DB file "
                  << db_file
                  << " already exists. "
                  << "Are you sure to remove it (y/N)? ";
        ret = fgets(answer, sizeof(answer), stdin);

        if (ret && !(answer[0] == 'Y' || answer[0] == 'y')) {
            return 0;
        }
        if (!db_file.empty() && db_file != "/") {
            std::string cmd = "rm -rf " + db_file + "/*";
            int r = ::system(cmd.c_str());
            (void)r;
            assert(r == 0);
        }
    }

    std::atomic<bool> stop_signal(false);
    std::atomic<bool> skip_cooling_down(false);
    global_ref_of_stop_signal = &stop_signal;
    global_ref_of_skip_cooling_down = &skip_cooling_down;
    old_sigint_handler = signal(SIGINT, custom_handler);

    TestSuite::_msg("-----\n");
    DbAdapter* db_inst = getAdapter();
    CHK_Z( db_inst->open(db_file, conf, conf.dbConf) );
    TestSuite::_msg("DB settings: %s\n", conf.dbConf.dump().c_str());

    std::ofstream log_fs;
    std::string log_file_name = conf.metricsPath + "/" +
                                db_inst->getName() + "_" + "bench_log_" +
                                TestSuite::getTimeStringPlain();;
    log_fs.open( log_file_name, std::ofstream::out | std::ofstream::app );
    log_fs << conf.confJson.dump() << std::endl;
    log_fs << "-----" << std::endl;
    log_fs.close();

    TestSuite::_msg("-----\n");
    CHK_Z( initial_load(conf, log_file_name, db_inst, stop_signal) );

    // Warming up phase.
    {   TestSuite::Progress pp(conf.warmingUpSec, "warming up", "s");
        TestSuite::Timer tt;
        tt.resetSec(conf.warmingUpSec);
        while (!tt.timeout() && !stop_signal) {
            pp.update(tt.getTimeSec());
            TestSuite::sleep_ms(100);
        }
        pp.done();
        TestSuite::_msg("-----\n");
    }

    BenchStatus cur_status;

    // Spawn workers.
    size_t num_workers = conf.workerDefs.size();
    std::vector<WorkerHandle*> w_handles(num_workers);
    std::vector<TestSuite::ThreadHolder*> w_holders(num_workers);
    for (size_t ii=0; ii<num_workers; ++ii) {
        w_handles[ii] =
            new WorkerHandle(ii, db_inst, conf, cur_status, stop_signal);
        w_holders[ii] =
            new TestSuite::ThreadHolder(w_handles[ii], bench_worker, nullptr);
    }

    // Spawn displayer.
    DisplayArgs d_args(conf, cur_status, stop_signal);
    d_args.dbFile = db_file;
    d_args.logFile = log_file_name;

    TestSuite::ThreadHolder d_holder(&d_args, displayer, nullptr);

    // Wait for thread termination.
    d_holder.join();
    CHK_Z( d_holder.getResult() );
    for (size_t ii=0; ii<num_workers; ++ii) {
        w_holders[ii]->join();
        CHK_Z( w_holders[ii]->getResult() );
        delete w_holders[ii];
        delete w_handles[ii];
    }

    // Print out latencies.
    print_latencies(d_args.logFile, {"set", "get", "iterate"});

    // Cooling down phase.
    {   TestSuite::Progress pp(conf.coolingDownSec, "cooling down", "s");
        TestSuite::Timer tt;
        tt.resetSec(conf.coolingDownSec);
        while (!tt.timeout() && !skip_cooling_down) {
            pp.update(tt.getTimeSec());
            TestSuite::sleep_ms(100);
        }
        pp.done();
        TestSuite::_msg("-----\n");
    }

    CHK_Z( db_inst->close() );
    db_inst->shutdown();
    delete db_inst;

    return 0;
}

void usage(int argc, char** argv) {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "    " << argv[0] << " [config file name] <options>\n"
       << std::endl
       << "Options:\n"
       << "    -e, --use-existing:\n"
       << "        Force use the existing DB. It will overwrites the config "
       << "in the given file."
       << std::endl;

    std::cout << ss.str();
    exit(0);
}

int check_args(int argc, char** argv) {
    int input_file_idx = 0;
    for (int ii=1; ii<argc; ++ii) {
        std::string cur_str = argv[ii];
        if ( cur_str == "--use-existing" ||
             cur_str == "-e" ) {
            force_use_existing = true;

        } else {
            input_file_idx = ii;
        }
    }
    return input_file_idx;
}

}  // namespace jungle_bench;
using namespace jungle_bench;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    if (argc < 2) usage(argc, argv);

    int file_idx = check_args(argc, argv);
    if (file_idx == 0) usage(argc, argv);

    std::string config_file = argv[file_idx];

    ts.options.printTestMessage = true;
    ts.doTest("bench main", bench_main, config_file);

    return 0;
}

