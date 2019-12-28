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

#include "json_common.h"
#include "json_to_dist_def.h"

#include "test_common.h"

#include <atomic>
#include <string>
#include <unordered_map>

namespace jungle_bench {

struct BenchConfig;
struct BenchStatus;
class DbAdapter;
struct WorkerDef {
    enum Type {
        WRITER = 0x0,
        POINT_READER = 0x1,
        RANGE_READER = 0x2,
        UNKNOWN = 0x3,
    };

    WorkerDef(Type _type = WRITER,
              uint64_t _rate = 1,
              DistDef _dist = DistDef())
        : type(_type)
        , rate(_rate)
        , batchSize(_dist)
        {}

    static WorkerDef load(json::JSON& obj) {
        std::string type_str;
        uint64_t rate;
        _jstr(type_str, obj, "type");
        _jint(rate, obj, "rate");

        Type type = UNKNOWN;
        if (!type_str.empty()) {
            if (type_str[0] == 'p' || type_str[0] == 'P') {
                type = POINT_READER;
            } else if (type_str[0] == 'r' || type_str[0] == 'R') {
                type = RANGE_READER;
            } else if (type_str[0] == 'w' || type_str[0] == 'W') {
                type = WRITER;
            }
        }

        DistDef batch = load_dist_def_from_json( obj["batch"] );
        return WorkerDef(type, rate, batch);
    }

    std::string toString() {
        char msg[128];
        static std::unordered_map<int, std::string>
            w_type_name
            ( { {WRITER,        "writer"},
                {POINT_READER,  "P reader"},
                {RANGE_READER,  "R reader"} } );

        std::stringstream ss;
        if (rate >= 0) {
            sprintf( msg, "%-10s %8zu.0 ops/sec",
                     w_type_name[type].c_str(),
                     (size_t)rate );
        } else {
            sprintf( msg, "%-10s MAX SPEED",
                     w_type_name[type].c_str() );
        }
        ss << msg;
        if (type == RANGE_READER) {
            ss << ", batch " << batchSize.toString();
        }
        return ss.str();
    }

    Type type;
    int64_t rate;
    DistDef batchSize;
};

struct WorkerHandle : public TestSuite::ThreadArgs {
    WorkerHandle(size_t _id,
                 DbAdapter* _db_inst,
                 const BenchConfig& _conf,
                 BenchStatus& _stat,
                 std::atomic<bool>& _stop_signal)
        : wId(_id)
        , dbInst(_db_inst)
        , conf(_conf)
        , stats(_stat)
        , stopSignal(_stop_signal)
        {}
    size_t wId;
    DbAdapter* dbInst;
    const BenchConfig& conf;
    BenchStatus& stats;
    std::atomic<bool>& stopSignal;
};

}

