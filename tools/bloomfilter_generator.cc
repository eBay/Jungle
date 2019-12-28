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

#include "bloomfilter.h"
#include "db_mgr.h"
#include "fileops_posix.h"
#include "table_file.h"
#include "table_mgr.h"
#include "test_common.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <libjungle/jungle.h>

#include <sstream>
#include <vector>

#include <stdio.h>

using namespace jungle;

namespace bf_generator {

class MutableTableMgr : public TableMgr {
public:
    MutableTableMgr(DB* parent_db) : TableMgr(parent_db) {}

    void setOpt(const TableMgrOptions& to) { opt = to; }
};

enum BfMode {
    ORIGINAL = 0x0,
    APPEND = 0x1,
};

int bf_gen_file(TableMgr* t_mgr,
                FileOps* f_ops,
                const std::string& file_path)
{
    size_t pos = file_path.rfind("_");
    if (pos == std::string::npos) return -10000;

    const DBConfig* db_config = t_mgr->getDbConfig();

    uint64_t t_num = atoi(file_path.substr(pos + 1).c_str());

    TableFile t_file(t_mgr);
    Status s;
    TableFileOptions tf_opt;
    s = t_file.load(0, t_num, file_path, f_ops, tf_opt);
    if (!s) return (int)s;

    std::list<Record*> recs;
    TableFile::Iterator t_itr;
    s = t_itr.init(nullptr, &t_file, SizedBuf(), SizedBuf());
    if (!s) return (int)s;
    do {
        Record rec_out;
        Record::Holder h_rec_out(rec_out);
        s = t_itr.get(rec_out);
        if (!s) break;

        Record* rec_obj = new Record();
        rec_out.moveTo(*rec_obj);
        recs.push_back(rec_obj);
    } while (t_itr.next().ok());
    t_itr.close();

    uint64_t bf_size = recs.size() * db_config->bloomFilterBitsPerUnit;

    BloomFilter bf(bf_size, 3);
    for (Record* rr: recs) {
        bf.set(rr->kv.key.data, rr->kv.key.size);
        rr->free();
        delete rr;
    }

    // Remove previous file if exists.
    std::string bf_file_name = file_path + ".bf";
    if (FileMgr::exist(bf_file_name)) {
        FileMgr::remove(bf_file_name);
    }
    t_file.saveBloomFilter(bf_file_name, &bf, true);

    return 0;
}

int bf_gen(const std::string& db_path,
           BfMode bf_mode,
           size_t bpk,
           size_t max_table_size_mb)
{
    GlobalConfig g_config;
    DBMgr::init(g_config);

    FileOpsPosix f_ops;
    DBConfig db_config;

    db_config.bloomFilterBitsPerUnit = bpk;

    TableMgrOptions t_mgr_opt;
    t_mgr_opt.path = db_path;
    t_mgr_opt.fOps = &f_ops;
    t_mgr_opt.dbConfig = &db_config;

    MutableTableMgr t_mgr(nullptr);
    t_mgr.setOpt(t_mgr_opt);

    // Scan & find table files.
    std::vector<std::string> files;
    int rc = FileMgr::scan(db_path, files);
    if (rc != 0) return rc;

    std::vector<std::string> actual_files;

    for (auto& entry: files) {
        std::string& cur_file = entry;
        std::string file_part = FileMgr::filePart(cur_file);

        if ( file_part.find("table") == 0 &&
             file_part.rfind("manifest") == std::string::npos &&
             file_part.rfind("bf") == std::string::npos ) {
            actual_files.push_back(cur_file);
        }
    }

    size_t cnt = 0;
    TestSuite::Progress pp(actual_files.size());
    for (auto& entry: actual_files) {
        std::string& cur_file = entry;
        std::string file_part = FileMgr::filePart(cur_file);

        rc = bf_gen_file(&t_mgr, &f_ops, db_path + "/" + cur_file);
        if (rc != 0) return rc;

        pp.update(++cnt);
    }
    pp.done();

    DBMgr::destroy();

    return 0;
}

void usage(int argc, char** argv) {
    std::stringstream ss;
    ss << "Usage: \n";
    ss << "    " << argv[0] << " [DB path] "
       << "[max table size in MB] "
       << "[bits per key]\n";
    ss << std::endl;

    std::cout << ss.str();

    exit(0);
}

}; // namespace bf_generator;
using namespace bf_generator;

int main(int argc, char** argv) {
    if (argc < 4) {
        usage(argc, argv);
    }

    TestSuite ts(argc, argv);
    ts.options.printTestMessage = true;

    std::string db_path = argv[1];

    size_t max_table_size_mb = atoi(argv[2]);
    BfMode bf_mode = ORIGINAL;
    if (max_table_size_mb) bf_mode = APPEND;

    size_t bpk = atoi(argv[3]);
    if (bpk == 0 || bpk > 100000) usage(argc, argv);

    ts.doTest( "bloomfilter generator", bf_gen,
               db_path, bf_mode, bpk, max_table_size_mb );

    return 0;
}

