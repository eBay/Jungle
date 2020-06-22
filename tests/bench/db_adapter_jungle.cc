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

#include "db_adapter_jungle.h"

#include "json_common.h"
#include "libjungle/jungle.h"

#ifdef SNAPPY_AVAILABLE
#include "snappy-c.h"
#endif

namespace jungle_bench {

jungle::SizedBuf conv_buf(const DbAdapter::Buffer& buf) {
    return jungle::SizedBuf(buf.size, buf.data);
}

DbAdapter::Buffer conv_buf(const jungle::SizedBuf& buf) {
    DbAdapter::Buffer b;
    b.data = buf.data;
    b.size = buf.size;
    return b;
}

jungle::KV conv_kv(const DbAdapter::KvElem& elem) {
    return jungle::KV( conv_buf(elem.key), conv_buf(elem.value) );
}

ssize_t get_max_comp_size(jungle::DB* db,
                          const jungle::Record& rec)
{
#if SNAPPY_AVAILABLE
    return snappy_max_compressed_length(rec.kv.value.size) + 2;

#else
    // Compression is not available.
    return 0;
#endif
}

ssize_t compress(jungle::DB* db,
                 const jungle::Record& src,
                 jungle::SizedBuf& dst)
{
#if SNAPPY_AVAILABLE
    dst.data[0] = 1;
    dst.data[1] = 1;

    size_t len = dst.size - 2;
    int ret = snappy_compress( (char*)src.kv.value.data,
                               src.kv.value.size,
                               (char*)dst.data + 2,
                               &len );
    if (ret < 0) return ret;
    return len;

#else
    // Compression is not available.
    return 0;
#endif
}

ssize_t decompress(jungle::DB* db,
                   const jungle::SizedBuf& src,
                   jungle::SizedBuf& dst)
{
#if SNAPPY_AVAILABLE
    size_t uncomp_len = dst.size;
    int ret = snappy_uncompress( (char*)src.data + 2,
                                 src.size - 2,
                                 (char*)dst.data,
                                 &uncomp_len );
    if (ret < 0) return ret;
    return uncomp_len;

#else
    // It should not happen.
    assert(0);
    return -1;
#endif
}


int JungleAdapter::open(const std::string& db_file,
                        const BenchConfig& bench_config,
                        json::JSON db_config)
{
    dbPath = db_file;
    configObj = db_config;

    jungle::GlobalConfig g_config;
    g_config.numFlusherThreads = 1;
    //g_config.flusherSleepDuration_ms = 5000;
    uint64_t wal_size_mb = 256;
    _jint(wal_size_mb, configObj, "wal_size_mb");
    g_config.flusherMinRecordsToTrigger =
        wal_size_mb * 1024 * 1024 / bench_config.valueLen.median;

    uint64_t cache_size_mb = 4096;
    _jint(cache_size_mb, configObj, "cache_size_mb");
    g_config.fdbCacheSize = (uint64_t)cache_size_mb*1024*1024;

    _jint(g_config.numCompactorThreads, configObj, "num_compactor_threads");
    if (!g_config.numCompactorThreads) g_config.numCompactorThreads = 1;

    g_config.compactorSleepDuration_ms = 1000; // 1 second

    _jint(g_config.numTableWriters, configObj, "num_table_writers");
    if (!g_config.numTableWriters) g_config.numTableWriters = 8;

    g_config.flusherAutoSync = false;

    //g_config.itcOpt.timeWindow_sec = 10;
    g_config.itcOpt.startHour = 0;
    g_config.itcOpt.endHour = 0;

    g_config.ctOpt.throttlingFactor = 0;

    jungle::init(g_config);

    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config)
    config.compactionFactor = 300;
    _jint(config.compactionFactor, configObj, "compaction_factor");

    config.minFileSizeToCompact = 16*1024*1024;

    _jint(config.blockReuseFactor, configObj, "block_reuse_factor");
    config.minBlockReuseCycleToCompact = 0;
    config.maxBlockReuseCycle = 100;

    config.nextLevelExtension = true;

    uint64_t table_size_mb = 1024;
    _jint(table_size_mb, configObj, "l0_table_size_mb");
    config.maxL0TableSize = table_size_mb * 1024 * 1024;

    table_size_mb = 1024;
    _jint(table_size_mb, configObj, "l1_table_size_mb");
    config.maxL1TableSize = table_size_mb * 1024 * 1024;

    /*
    config.lookupBoosterLimit_mb = { (uint32_t)cache_size_mb / 12,
                                     (uint32_t)cache_size_mb * 2 / 12 };*/
    config.lookupBoosterLimit_mb = {100, 200};

    uint64_t max_l1_size_mb = 10240;
    _jint(max_l1_size_mb, configObj, "l1_size_mb");
    config.maxL1Size = max_l1_size_mb * 1024 * 1024;

    config.bloomFilterBitsPerUnit = 10;
    _jfloat(config.bloomFilterBitsPerUnit, configObj, "bloom_filter_bits");

    //config.numWritesToCompact = 100000;
    //config.useBloomFilter = false;

#if SNAPPY_AVAILABLE
    bool compression_enabled = false;
    _jbool(compression_enabled, configObj, "compression");
    if (compression_enabled) {
        config.compOpt.cbGetMaxSize = get_max_comp_size;
        config.compOpt.cbCompress = compress;
        config.compOpt.cbDecompress = decompress;
    }
#endif

    jungle::Status s = jungle::DB::open(&myDb, db_file, config);
    if (!s) return s.getValue();

    // Flush all logs on initial open.
    myDb->sync(false);
    myDb->flushLogs( jungle::FlushOptions() );

    jungle::DebugParams jungle_d_params;
    jungle_d_params.urgentCompactionRatio = 120;
    jungle_d_params.urgentCompactionNumWrites = 10000;
    //jungle::setDebugParams(jungle_d_params);

    return 0;
}

int JungleAdapter::close() {
    // Flush all logs before close.
    myDb->sync(false);
    myDb->flushLogs( jungle::FlushOptions() );

    jungle::Status s = jungle::DB::close(myDb);
    if (!s) return s.getValue();

    return 0;
}

int JungleAdapter::shutdown() {
    jungle::shutdown();
    return 0;
}

int JungleAdapter::startInitialLoad() {
    return 0;
}

int JungleAdapter::endInitialLoad() {
    myDb->sync(false);
    myDb->flushLogs( jungle::FlushOptions() );
    return 0;
}

int JungleAdapter::set(const KvElem& elem) {
    jungle::Status s;
    s = myDb->set( conv_kv(elem) );
    if (!s) return (int)s;

    return 0;
}

int JungleAdapter::setBatch(const std::list<KvElem>& batch,
                            const BatchOptions& opt)
{
    jungle::Status s;
    for (auto& entry: batch) {
        const KvElem& elem = entry;
        s = myDb->set( conv_kv(elem) );
        if (!s) return s.getValue();
    }

    if (opt.sync) {
        s = myDb->sync(opt.sync);
        if (!s) return s.getValue();
    }

    return 0;
}

int JungleAdapter::get(const Buffer& key,
                       Buffer& value_out)
{
    jungle::Status s;
    jungle::SizedBuf local_value_out;
    s = myDb->get( conv_buf(key), local_value_out );
    if (!s) return (int)s;

    value_out = conv_buf(local_value_out);
    return 0;
}

int JungleAdapter::getRange(const Buffer& start,
                            const Buffer& end,
                            std::list<KvElem>& kvs_out)
{
    jungle::Status s;
    jungle::Iterator itr;
    s = itr.init(myDb, conv_buf(start), conv_buf(end));
    if (!s) return (int)s;

    do {
        jungle::Record rec_out;
        jungle::Record::Holder h_rec_out(rec_out);
        s = itr.get(rec_out);
        if (!s) break;
        if (rec_out.kv.key > conv_buf(end)) break;

        jungle::SizedBuf key_out;
        jungle::SizedBuf val_out;
        rec_out.kv.key.moveTo(key_out);
        rec_out.kv.value.moveTo(val_out);

        KvElem elem_to_add;
        elem_to_add.key = conv_buf(key_out);
        elem_to_add.value = conv_buf(val_out);
        kvs_out.push_back(elem_to_add);

    } while (itr.next());

    s = itr.close();
    if (!s) return (int)s;

    return 0;
}

} // namespace jungle_bench;

