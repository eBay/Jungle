/************************************************************************
Copyright 2017-present eBay Inc.

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
#include "test_common.h"

#include <fstream>
#include <vector>

#include <stdio.h>

int main(int argc , char** argv) {
    // format:
    //   [0]                  [1]    [2]             [3]         [4]
    //   custom_mani_verifier <path> <manifest path> <start idx> <end idx>

    if (argc < 4) {
        printf("too few arguments\n");
        return -1;
    }

    std::string db_path = argv[1];
    uint64_t start_idx = std::atol(argv[3]);
    uint64_t end_idx = std::atol(argv[4]);

    jungle::Status s;
    jungle::DBConfig config;
    config.logSectionOnly = true;
    config.customManifestPath = argv[2];
    config.readOnly = true;

    jungle::DB* db;

    CHK_Z( jungle::DB::open(&db, db_path, config) );

    for (size_t ii = start_idx; ii <= end_idx; ++ii) {
        jungle::KV kv_out;
        jungle::KV::Holder h(kv_out);
        CHK_Z( db->getSN(ii, kv_out) );

        std::string key_str = "k" + TestSuite::lzStr(8, ii);
        std::string val_str = "v" + TestSuite::lzStr(16, ii);
        CHK_EQ(key_str, kv_out.key.toString());
        CHK_EQ(val_str, kv_out.value.toString());
    }

    // Reading end_idx + 1 should fail.
    jungle::KV kv_out;
    CHK_NOT( db->getSN(end_idx + 1, kv_out) );

    CHK_Z( jungle::DB::close(db) );

    return 0;
}
