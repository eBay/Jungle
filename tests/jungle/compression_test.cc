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

#include "jungle_test_common.h"

#include "internal_helper.h"

#ifdef SNAPPY_AVAILABLE
#include "snappy-c.h"
#endif

#include <vector>

#include <stdio.h>

static size_t NUM_RECORDS = 100;

#ifdef SNAPPY_AVAILABLE // ===== USING SNAPPY =====

ssize_t dummy_get_max_size(jungle::DB* db,
                           const jungle::Record& rec)
{
    return snappy_max_compressed_length(rec.kv.value.size);
}

ssize_t dummy_compress(jungle::DB* db,
                       const jungle::Record& src,
                       jungle::SizedBuf& dst)
{
    size_t len = dst.size;
    int ret = snappy_compress((char*)src.kv.value.data,
                              src.kv.value.size,
                              (char*)dst.data,
                              &len);
    if (ret < 0) return ret;
    return len;
}

ssize_t dummy_decompress(jungle::DB* db,
                         const jungle::SizedBuf& src,
                         jungle::SizedBuf& dst)
{
    size_t uncomp_len = dst.size;
    int ret = snappy_uncompress((char*)src.data,
                                src.size,
                                (char*)dst.data,
                                &uncomp_len);
    if (ret < 0) return ret;
    return uncomp_len;
}

#else  // ===== USING DUMMY NAIVE COMPRESSION =====
// This compression is based on the counting the number of
// consecutive characters, and then we store
// the character (1-byte) + length (1-byte).

ssize_t dummy_get_max_size(jungle::DB* db,
                           const jungle::Record& rec)
{
    return rec.kv.value.size * 2;
}

ssize_t dummy_compress(jungle::DB* db,
                       const jungle::Record& src,
                       jungle::SizedBuf& dst)
{
    if (!src.kv.value.size) return 0;

    size_t n_consecutive = 0;
    uint8_t cur_char = 0x0;

    jungle::RwSerializer rws(src.kv.value);
    jungle::RwSerializer rws_out(dst);

    while (rws.available(1)) {
        if (!n_consecutive) {
            cur_char = rws.getU8();
            n_consecutive = 1;
            continue;
        }

        uint8_t new_char = rws.getU8();
        bool flush = false;
        if (cur_char == new_char) {
            n_consecutive++;
            if (n_consecutive > 255) {
                n_consecutive--;
                flush = true;
            }
        } else {
            flush = true;
        }

        if (flush) {
            rws_out.putU8(cur_char);
            rws_out.putU8(n_consecutive);

            cur_char = new_char;
            n_consecutive = 1;
        }
    }

    if (n_consecutive) {
        rws_out.putU8(cur_char);
        rws_out.putU8(n_consecutive);
    }
    return rws_out.pos();
}

ssize_t dummy_decompress(jungle::DB* db,
                         const jungle::SizedBuf& src,
                         jungle::SizedBuf& dst)
{
    if (!src.size) return 0;

    jungle::RwSerializer rws(src);
    jungle::RwSerializer rws_out(dst);

    while (rws.available(1)) {
        uint8_t cur_char = rws.getU8();
        size_t n_consecutive = rws.getU8();

        for (size_t ii = 0; ii < n_consecutive; ++ii) {
            rws_out.putU8(cur_char);
        }
    }
    return rws_out.pos();
}
#endif

int compression_correctness_test() {
    jungle::SizedBuf src(1024);
    jungle::SizedBuf::Holder h_src(src);
    jungle::RwSerializer rws_src(src);
    for (size_t ii=0; ii<1024/8; ++ii) {
        rws_src.putU64(ii);
    }

    jungle::Record rec;
    rec.kv.value = src;
    jungle::SizedBuf dst( dummy_get_max_size(nullptr, rec) );
    jungle::SizedBuf::Holder h_dst(dst);

    ssize_t comp_len = dummy_compress(nullptr, rec, dst);
    TestSuite::_msg("compressed len: %zd\n", comp_len);

    jungle::SizedBuf decomp(1024);
    jungle::SizedBuf::Holder h_decomp(decomp);
    dummy_decompress(nullptr, dst, decomp);

    CHK_Z( memcmp(src.data, decomp.data, src.size) );
    return 0;
}

int compression_small_value_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        std::string val_str = "v" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );

        CHK_EQ( val_str, value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_mid_size_value_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );

        CHK_EQ( original_value_arr[ii], value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_large_value_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(60, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );

        CHK_EQ( original_value_arr[ii], value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

ssize_t selective_get_max_size(jungle::DB* db,
                               const jungle::Record& rec)
{
    // Compress only when the last byte of key is an odd number.
    if (rec.kv.key.data[rec.kv.key.size - 1] % 2 == 1) {
        return rec.kv.value.size * 2;
    }
    return 0;
}

int selective_compression_by_max_size_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = selective_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );

        CHK_EQ( original_value_arr[ii], value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

ssize_t selective_compress
        ( std::function< ssize_t( jungle::DB*,
                                  const jungle::Record&,
                                  jungle::SizedBuf& ) > orig_compress,
          jungle::DB* db,
          const jungle::Record& rec,
          jungle::SizedBuf& dst )
{
    // Compress only when the last byte of key is an odd number.
    if (rec.kv.key.data[rec.kv.key.size - 1] % 2 == 1) {
        return orig_compress(db, rec, dst);
    }
    return 0;
}

int selective_compression_by_compress_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = std::bind( selective_compress,
                                           dummy_compress,
                                           std::placeholders::_1,
                                           std::placeholders::_2,
                                           std::placeholders::_3 );
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);
        CHK_Z( db->get( jungle::SizedBuf(key_str), value_out) );

        CHK_EQ( original_value_arr[ii], value_out.toString() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_with_tombstones_test(bool flush_to_table) {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(6, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->set( jungle::KV(key_str, val_str) ) );
    }

    // Remove odd number records.
    for (size_t ii=1; ii<NUM_RECORDS; ii+=2) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        CHK_Z( db->del( jungle::SizedBuf(key_str) ) );
    }

    CHK_Z( db->sync(false) );

    if (flush_to_table) {
        CHK_Z( db->flushLogs() );
    }

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::SizedBuf value_out;
        jungle::SizedBuf::Holder h_value_out(value_out);

        s = db->get( jungle::SizedBuf(key_str), value_out);
        if (ii % 2 == 0) {
            CHK_Z(s);
            CHK_EQ( original_value_arr[ii], value_out.toString() );
        } else {
            CHK_SM(s, 0);
        }
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int compression_log_store_test() {
    std::string filename;
    TEST_SUITE_PREPARE_PATH(filename);

    jungle::Status s;
    jungle::DBConfig config;
    TEST_CUSTOM_DB_CONFIG(config);
    config.compOpt.cbGetMaxSize = dummy_get_max_size;
    config.compOpt.cbCompress = dummy_compress;
    config.compOpt.cbDecompress = dummy_decompress;
    config.logSectionOnly = true;

    jungle::DB* db;
    CHK_Z( jungle::DB::open(&db, filename, config) );

    std::vector<std::string> original_value_arr(NUM_RECORDS);
    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);
        size_t value_len = (std::rand() % 100) + 100;
        std::string val_str;
        for (size_t jj=0; jj<value_len; ++jj) {
            size_t rr = std::rand() % 10;
            val_str += TestSuite::lzStr(60, rr);
        }
        original_value_arr[ii] = val_str;
        CHK_Z( db->setSN( ii+1, jungle::KV(key_str, val_str) ) );
    }
    CHK_Z( db->sync(false) );

    // Close and reopen.
    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::DB::open(&db, filename, config) );

    for (size_t ii=0; ii<NUM_RECORDS; ++ii) {
        std::string key_str = "k" + TestSuite::lzStr(6, ii);

        jungle::KV kv_out;
        jungle::KV::Holder h_kv_out(kv_out);
        CHK_Z( db->getSN( ii+1, kv_out) );

        CHK_EQ( key_str, kv_out.key.toString() );
        CHK_EQ( original_value_arr[ii], kv_out.value.toString() );
    }

    CHK_Z( jungle::DB::close(db) );
    CHK_Z( jungle::shutdown() );

    TEST_SUITE_CLEANUP_PATH();
    return 0;
}

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

#ifdef SNAPPY_AVAILABLE
    std::cout << "TEST WITH SNAPPY" << std::endl;
#endif

    for (int ii=1; ii<argc; ++ii) {
        if ( ii < argc-1 &&
             !strcmp(argv[ii], "--num-records") ) {
            NUM_RECORDS = atoi(argv[++ii]);
            std::cout << "NUM_RECORDS = " << NUM_RECORDS << std::endl;
        }
    }

    //ts.options.printTestMessage = true;
    ts.doTest( "compression correctness test",
               compression_correctness_test );

    ts.doTest( "compression small value test",
               compression_small_value_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression mid size value test",
               compression_mid_size_value_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression large value test",
               compression_large_value_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "selective compression by cbGetMaxSize test",
               selective_compression_by_max_size_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "selective compression by cbCompress test",
               selective_compression_by_compress_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression with tombstones test",
               compression_with_tombstones_test,
               TestRange<bool>( {false, true} ) );

    ts.doTest( "compression log store test",
               compression_log_store_test );

    return 0;
}

