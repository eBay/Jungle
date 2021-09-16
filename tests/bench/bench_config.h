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

#include "dist_def.h"
#include "bench_worker.h"
#include "json_common.h"
#include "json_to_dist_def.h"

#include "test_common.h"

#include <sstream>
#include <string>
#include <vector>

namespace jungle_bench {

struct BenchConfig {
    enum InitialLoadOrder {
        SEQ = 0x0,
        RANDOM = 0x1,
    };

    BenchConfig()
        : dbPath()
        , metricsPath("./")
        , durationSec(10)
        , durationOps(0)
        , warmingUpSec(3)
        , coolingDownSec(5)
        , numKvPairs(100000)
        , initialLoad(true)
        , initialLoadRate(20000)
        , initialLoadOrder(RANDOM)
        , keyLen(DistDef::RANDOM, 8, 0)
        , valueLen(DistDef::RANDOM, 512, 0)
        {}

    static BenchConfig load(const std::string& filename) {
        BenchConfig conf;
        json::JSON obj;
        read_json_object(filename, obj);
        conf.confJson = obj;

        _jstr(conf.dbPath, obj, "db_path");
        _jstr(conf.metricsPath, obj, "log_path");
        _jint(conf.durationSec, obj, "duration_sec");
        _jint(conf.durationOps, obj, "duration_ops");
        _jint(conf.warmingUpSec, obj, "warming_up_sec");
        _jint(conf.coolingDownSec, obj, "cooling_down_sec");
        _jint(conf.numKvPairs, obj, "num_kv_pairs");
        _jbool(conf.initialLoad, obj, "initial_load");
        _jint(conf.initialLoadRate, obj, "initial_load_rate");

        std::string initial_load_order;
        _jstr(initial_load_order, obj, "initial_load_order");
        conf.initialLoadOrder =
            (initial_load_order.find("seq") == std::string::npos)
            ? RANDOM : SEQ;

        const auto& prefix = obj["prefix"];
        if (prefix.NotNull()) {
            conf.prefixLengths.reserve(prefix.size());
            conf.prefixFanouts.reserve(prefix.size());
            for (auto& element: obj["prefix"].ArrayRange()) {
                conf.prefixLengths.push_back(load_dist_def_from_json(element["length"]));
                conf.prefixFanouts.push_back(element["fanout"].ToInt());
            }
        }
        if (obj["key"].NotNull()) {
            conf.keyLen = load_dist_def_from_json( obj["key"] );
        }
        if (obj["value"].NotNull()) {
            conf.valueLen = load_dist_def_from_json( obj["value"] );
        }
        conf.dbConf = obj["db_configs"];

        // Workers
        json::JSON w_obj = obj["workers"];
        if (w_obj.NotNull()) {
            size_t num = w_obj.size();

            for (size_t ii = 0; ii < num; ++ii) {
                WorkerDef ww = WorkerDef::load(w_obj[ii]);
                if (ww.type != WorkerDef::UNKNOWN) {
                    size_t num_clones = 1;
                    _jint(num_clones, w_obj[ii], "num_clones");

                    for (size_t jj = 0; jj < num_clones; ++jj) {
                        conf.workerDefs.push_back(ww);
                    }
                }
            }
        }

        if (conf.dbPath.empty()) conf.dbPath = "./bench_db";
        if (conf.metricsPath.empty()) conf.metricsPath = "./";

        return conf;
    }

    std::string toString() {
        char msg[1024];
        std::stringstream ss;
        sprintf( msg, "total %zu kv, duration %s",
                 numKvPairs,
                 TestSuite::usToString(durationSec * 1000000).c_str() );
        ss << msg;
        if (durationOps) {
            sprintf(msg, " OR %s ops\n",
                    TestSuite::countToString(durationOps).c_str());
            ss << msg;
        } else {
            ss << std::endl;
        }

        ss << "key: " << keyLen.toString() << std::endl;
        ss << "value: " << valueLen.toString() << std::endl;
        ss << "approx working set size: "
           << TestSuite::sizeToString
                  ( (keyLen.median + valueLen.median) * numKvPairs )
           << std::endl;
        ss << "DB path: " << dbPath << std::endl;
        ss << "log path: " << metricsPath << std::endl;
        if (initialLoad) {
            ss << " - will do initial load: "
               << initialLoadRate << ", "
               << ((initialLoadOrder == SEQ) ? "SEQ" : "RANDOM")
               << std::endl;
        } else {
            ss << " - will use existing DB" << std::endl;
        }

        ss << " -- " << workerDefs.size() << " workers --" << std::endl;
        for (size_t ii = 0; ii < workerDefs.size(); ++ii) {
            WorkerDef& wd = workerDefs[ii];
            ss << "  [" << ii << "] ";
            ss << wd.toString();
            if (ii < workerDefs.size() - 1) ss << std::endl;
        }

        return ss.str();
    }

    std::string dbPath;
    std::string metricsPath;
    size_t durationSec;
    size_t durationOps;
    size_t warmingUpSec;
    size_t coolingDownSec;
    size_t numKvPairs;
    bool initialLoad;
    size_t initialLoadRate;
    InitialLoadOrder initialLoadOrder;
    std::vector<DistDef> prefixLengths;
    std::vector<size_t> prefixFanouts;
    DistDef keyLen;
    DistDef valueLen;
    std::vector<WorkerDef> workerDefs;
    json::JSON confJson;
    // It will be passed to DB adapter.
    json::JSON dbConf;
};

};

