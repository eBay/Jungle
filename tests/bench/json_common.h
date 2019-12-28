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

#include "config_test_common.h"
#include "json.hpp"

#include <fstream>
#include <sstream>

namespace jungle_bench {

#define VOID_UNUSED     void __attribute__((unused))

template<typename T>
static VOID_UNUSED _jint(T& t, json::JSON& obj, const std::string field) {
    if (obj[field].IsNull()) return;
    t = obj[field].ToInt();
}

template<typename T>
static VOID_UNUSED _jfloat(T& t, json::JSON& obj, const std::string field) {
    if (obj[field].IsNull()) return;
    t = obj[field].ToFloat();
}

template<typename T>
static VOID_UNUSED _jstr(T& t, json::JSON& obj, const std::string field) {
    if (obj[field].IsNull()) return;
    t = obj[field].ToString();
}

template<typename T>
static VOID_UNUSED _jbool(T& t, json::JSON& obj, const std::string field) {
    if (obj[field].IsNull()) return;
    t = obj[field].ToBool();
}

// Write given json object to the file.
static VOID_UNUSED write_json_object(const std::string& filename,
                                     const json::JSON& obj)
{
    std::ofstream fs;
    fs.open(filename.c_str());
    fs << obj;
    fs.close();
}

// Read json object from the file.
static VOID_UNUSED read_json_object(const std::string& filename,
                                    json::JSON& obj_out)
{
    std::ifstream fs;
    std::stringstream ss;
    fs.open(filename.c_str());
    if (!fs.good()) return;
    ss << fs.rdbuf();
    fs.close();
    obj_out = json::JSON::Load(ss.str());
}

} // namespace jungle_bench;

