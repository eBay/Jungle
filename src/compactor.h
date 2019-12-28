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

#include "table_mgr.h"
#include "worker_mgr.h"

#include <libjungle/jungle.h>

#include <list>

namespace jungle {

class DB;
struct DBWrap;
struct TableInfo;
class Compactor : public WorkerBase {
public:
    struct CompactorOptions : public WorkerOptions {
    };

    Compactor(const std::string& _w_name,
              const GlobalConfig& _config);
    ~Compactor();
    void work(WorkerOptions* opt_base);

    GlobalConfig gConfig;

private:
    bool chkLevel(size_t level,
                  DBWrap* dbwrap,
                  DB*& target_db_out,
                  size_t& target_level_out,
                  size_t& target_hash_num_out,
                  TableInfo*& target_table_out,
                  TableMgr::MergeStrategy& target_strategy_out);

    size_t lastCheckedFileIndex;
    size_t lastCompactedHashNum;
};


} // namespace jungle
