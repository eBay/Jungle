
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

#include "db_internal.h"
#include "fileops_posix.h"
#include "internal_helper.h"
#include "libjungle/sized_buf.h"
#include "log_mgr.h"
#include "mutable_table_mgr.h"
#include "table_manifest.h"

#include <libjungle/jungle.h>

#include <limits>
#include <list>

namespace jungle {

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
     */
    std::list<TableData> tables;
};

static Status buildFromTableFiles(const BuildParams& params) {
    Status s;

    FileOpsPosix f_ops;
    DBConfig db_config;

    TableMgrOptions t_mgr_opt;
    t_mgr_opt.path = params.path;
    t_mgr_opt.fOps = &f_ops;
    t_mgr_opt.dbConfig = &db_config;

    MutableTableMgr t_mgr(nullptr);
    t_mgr.setOpt(t_mgr_opt);

    jungle::TableFileOptions t_opt;

    TableManifest t_mani(&t_mgr, &f_ops);
    std::string mani_filename = params.path + "/table0000_manifest";
    EP( t_mani.create(params.path, mani_filename) );

    t_mgr.mani = &t_mani;

    // Make level 1.
    EP( t_mani.extendLevel() );

    // 1) Find the table with the smallest key.
    // 2) Find the biggest seq number.
    uint64_t min_table_number = 0;
    uint64_t max_seqnum = 0;
    std::string cur_min_key;
    for (auto& td: params.tables) {
        if (td.minKey < cur_min_key) {
            cur_min_key = td.minKey;
            min_table_number = td.tableNumber;
        }
        max_seqnum = std::max(td.maxSeqnum, max_seqnum);
    }

    // Add given table files to manifest.
    uint64_t max_table_num = 0;
    for (auto& td: params.tables) {
        TableFile* t_file = new TableFile(&t_mgr);
        std::string t_filename =
            TableFile::getTableFileName(params.path, 0, td.tableNumber);
        EP( t_file->load(1, td.tableNumber, t_filename, &f_ops, t_opt) );

        // WARNING: If smallest table, put empty minkey.
        SizedBuf table_min_key;
        if (min_table_number != td.tableNumber) {
            table_min_key = SizedBuf(td.minKey);
        }
        EP( t_mani.addTableFile(1, 0, table_min_key, t_file) );

        max_table_num = std::max(td.tableNumber, max_table_num);
    }

    // Create and add empty L0 tables.
    for (size_t ii = 0; ii < db_config.numL0Partitions; ++ii) {
        TableFile* t_file = new TableFile(&t_mgr);
        uint64_t table_number = ++max_table_num;
        std::string t_filename =
            TableFile::getTableFileName(params.path, 0, table_number);

        EP( t_file->create(0, table_number, t_filename, &f_ops, t_opt) );
        EP( t_mani.addTableFile(0, ii, SizedBuf(), t_file) );
    }

    // Flush manifest file.
    t_mani.store(true);

    // Create the initial log file with the biggest sequence number + 1.
    LogMgr l_mgr(nullptr);
    LogMgrOptions l_opt;
    l_opt.path = params.path;
    l_opt.fOps = &f_ops;
    l_opt.dbConfig = &db_config;
    l_opt.startSeqnum = max_seqnum + 1;
    l_mgr.init(l_opt);
    l_mgr.sync(false);
    l_mgr.close();

    t_mgr.mani = nullptr;
    return s;
}

};

}

}
