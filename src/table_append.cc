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

#include "table_mgr.h"

#include "db_internal.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

void TableMgr::putLsmFlushResultWithKey( TableFile* cur_file,
                                         const SizedBuf& key,
                                         std::list<TableMgr::LsmFlushResult>& res_out )
{
    LsmFlushResult res(cur_file, key);
    res_out.push_back(res);
}

}

