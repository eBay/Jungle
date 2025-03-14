
/************************************************************************
Copyright 2017-present eBay Inc.

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

#include <libjungle/jungle.h>

#include <list>

typedef struct _fdb_file_handle fdb_file_handle;

typedef struct _fdb_kvs_handle fdb_kvs_handle;

class SimpleLogger;

namespace jungle {

class FileOps;

namespace builder {

class Builder {
public:
    /**
     * Parameters for build operation.
     */
    struct BuildParams {
        /**
         * DB path where table files are located.
         */
        std::string path;

        /**
         * DBConfig for the DB instance to be created.
         */
        DBConfig dbConfig;

        /**
         * Detailed table info.
         */
        struct TableData {
            TableData() : tableNumber(0), maxSeqnum(0) {}

            /**
             * Table number.
             */
            uint64_t tableNumber;

            /**
             * The smallest key that the table has. Empty if the table is empty.
             */
            std::string minKey;

            /**
             * The biggest sequence number that the table has.
             */
            uint64_t maxSeqnum;
        };

        /**
         * List of tables.
         * If this list is empty, an empty DB instance will be created.
         */
        std::list<TableData> tables;
    };

    static Status buildFromTableFiles(const BuildParams& params);

    Builder()
        : fOps(nullptr)
        , curFhandle(nullptr)
        , curKvsHandle(nullptr)
        , curTableIdx(0)
        , seqnumCounter(1)
        , curMaxSeqnum(0)
        , curEstSize(0)
        , numDocs(0)
        , tableIdxCounter(0)
        , myLog(nullptr)
        {}

    ~Builder() {
        close();
    }

    Status init(const std::string& path,
                const DBConfig& db_config);

    Status set(const Record& rec);

    Status commit();

    Status close();

private:
    Status finalizeFile();

    std::string dstPath;
    DBConfig dbConfig;
    FileOps* fOps;
    BuildParams buildParams;
    fdb_file_handle* curFhandle;
    fdb_kvs_handle* curKvsHandle;
    std::string curFileName;
    uint64_t curTableIdx;
    uint64_t seqnumCounter;
    uint64_t curMaxSeqnum;
    SizedBuf curMinKey;
    uint64_t curEstSize;
    uint64_t numDocs;
    uint64_t tableIdxCounter;
    SimpleLogger* myLog;
};

}

}
