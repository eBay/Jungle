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

#include "db_adapter.h"

#include <list>

namespace jungle {
    class DB;
};

namespace jungle_bench {

class JungleAdapter : public DbAdapter {
public:
    JungleAdapter() : myDb(nullptr) {}

    ~JungleAdapter() {}

    std::string getName() { return "jungle"; }

    int open(const std::string& db_file,
             const BenchConfig& bench_config,
             json::JSON db_confi);

    int close();

    int shutdown();

    int startInitialLoad();

    int endInitialLoad();

    int set(const KvElem& elem);

    int setBatch(const std::list<KvElem>& batch,
                 const BatchOptions& opt);

    int get(const Buffer& key,
            Buffer& value_out);

    int getRange(const Buffer& start,
                 const Buffer& end,
                 std::list<KvElem>& kvs_out);

    std::string dbPath;
    json::JSON configObj;
    jungle::DB* myDb;
};

} // namespace jungle_bench;

