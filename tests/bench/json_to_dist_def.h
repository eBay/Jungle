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

static DistDef load_dist_def_from_json(json::JSON& obj) {
    std::string type_str;
    uint64_t median = 100;
    uint64_t sigma = 50;

    if (obj["type"].NotNull()) type_str = obj["type"].ToString();
    if (obj["median"].NotNull()) median = obj["median"].ToInt();
    if (obj["sigma"].NotNull()) sigma = obj["sigma"].ToInt();

    DistDef::Type type = DistDef::RANDOM;
    if (!type_str.empty()) {
        if (type_str[0] == 'n' || type_str[0] == 'N') type = DistDef::NORMAL;
        else if (type_str[0] == 'z' || type_str[0] == 'Z') type = DistDef::ZIPF;
    }

    switch (type) {
    case DistDef::RANDOM:
    case DistDef::NORMAL:
    default:
        return DistDef(type, median, sigma);

    case DistDef::ZIPF:
        uint64_t n = 10;
        double alpha = 1.0;
        if (obj["n"].NotNull()) n = obj["n"].ToInt();
        if (obj["alpha"].NotNull()) alpha = obj["alpha"].ToFloat();
        return DistDef(type, median, sigma, alpha, n);
    }

    return DistDef(type, median, sigma);
}

