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

#include <cstring>

#define TEST_CUSTOM_DB_CONFIG(config)                                   \
    const char* direct_env = std::getenv("TEST_ENABLE_DIRECTIO");       \
    if (direct_env != nullptr && !std::strcmp(direct_env, "true")) {    \
        config.directIoOpt.enabled = true;                                         \
    }                                                                   \

