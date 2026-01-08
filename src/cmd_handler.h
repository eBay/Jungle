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

#include "worker_mgr.h"

#include <libjungle/jungle.h>

namespace jungle {

struct DBWrap;
class CmdHandler : public WorkerBase {
public:
    struct CmdHandlerOptions : public WorkerOptions {
    };

    CmdHandler(const std::string& wn,
               const GlobalConfig& c);

    ~CmdHandler();

    void applyNewGlobalConfig(const GlobalConfig& g_config);

    void work();

private:
    void handleCmd(DBWrap* target_dbw);

    std::string hGetStats(DBWrap* target_dbw,
                          const std::vector<std::string>& tokens);

    std::string hLogLevel(DBWrap* target_dbw,
                          const std::vector<std::string>& tokens);

    std::string hLogCacheStats(DBWrap* target_dbw,
                               const std::vector<std::string>& tokens);

    std::string hDumpKv(DBWrap* target_dbw,
                        const std::vector<std::string>& tokens);

    std::string hCompactUpto(DBWrap* target_dbw,
                             const std::vector<std::string>& tokens);

    std::string hTableInfo(DBWrap* target_dbw,
                           const std::vector<std::string>& tokens);

};


} // namespace jungle
