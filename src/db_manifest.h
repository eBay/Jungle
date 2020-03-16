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

#include "fileops_base.h"
#include "skiplist.h"
#include "table_file.h"

#include <libjungle/jungle.h>

#include <atomic>
#include <map>

class SimpleLogger;

namespace jungle {

class DBManifest {
public:
    DBManifest(FileOps* _f_ops);
    ~DBManifest();

    Status create(const std::string& path,
                  const std::string& filename);
    Status load(const std::string& path,
                const std::string& filename);
    Status store(bool call_fsync);
    Status addNewKVS(const std::string& kvs_name);
    Status getKVSID(const std::string& kvs_name,
                    uint64_t& kvs_id_out);
    Status getKVSName(const uint64_t kvs_id,
                      std::string& kvs_name_out);

    void setLogger(SimpleLogger* logger) { myLog = logger; }

private:
    FileOps* fOps;
    FileHandle* mFile;
    std::string dirPath;
    std::string mFileName;
    std::atomic<uint64_t> maxKVSID;
    std::map<std::string, uint64_t> NameToID;
    std::map<uint64_t, std::string> IDToName;
    SimpleLogger* myLog;
};

} // namespace jungle
