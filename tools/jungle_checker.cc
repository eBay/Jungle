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

#include "db_internal.h"
#include "internal_helper.h"

#include <libjungle/jungle.h>

#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

#include <stdio.h>

namespace jungle {

namespace checker {

static std::string exec_filename;

class Checker {
public:

static int load_db(const std::string& db_path,
                   jungle::DB*& db_out,
                   bool& log_mode_out) {
    if (!FileMgr::exist(db_path)) {
        std::cout << "DB does not exist: " << db_path << std::endl;
        return -1;
    }

    GlobalConfig g_conf;
    g_conf.fdbCacheSize = 0;
    g_conf.numCompactorThreads = 0;
    g_conf.numFlusherThreads = 0;
    g_conf.numTableWriters = 0;
    jungle::init(g_conf);

    // Check the mode of the given DB.
    log_mode_out = DB::isLogSectionMode(db_path);

    Status s;
    db_out = nullptr;
    DBConfig d_conf;
    d_conf.readOnly = true;
    d_conf.logSectionOnly = log_mode_out;
    if (log_mode_out) {
        d_conf.logFileTtl_sec = 3;
    }
    s = DB::open(&db_out, db_path, d_conf);
    if (!s) {
        std::cout << "DB open failed: " << (int)s << std::endl;
        return -1;
    }

    // Number of log files.
    std::cout << "path: " << db_path << std::endl;

    // Log mode.
    if (log_mode_out) {
        std::cout << "mode: log store" << std::endl;
    } else {
        std::cout << "mode: database" << std::endl;
    }
    return 0;
}

static int db_overview(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout
            << "too few arguments:" << std::endl
            << "    " << exec_filename
            << " " << args[0] << " <DB path>"
            << std::endl;
        return -1;
    }

    Status s;
    const std::string& db_path = args[1];
    jungle::DB* db = nullptr;
    bool log_mode = false;
    int rc = load_db(db_path, db, log_mode);
    if (rc != 0) return rc;

    size_t num_log_files = db->p->logMgr->getNumLogFiles();

    // Log file number range.
    uint64_t min_log_file_idx = 0;
    uint64_t max_log_file_idx = 0;
    db->p->logMgr->mani->getMinLogFileNum(min_log_file_idx);
    db->p->logMgr->mani->getMaxLogFileNum(max_log_file_idx);

    uint64_t last_flushed_idx = 0;
    uint64_t last_synced_idx = 0;
    db->p->logMgr->mani->getLastFlushedLog(last_flushed_idx);
    db->p->logMgr->mani->getLastSyncedLog(last_synced_idx);

    printf( "number of log files: %zu (%zu - %zu)\n",
            (size_t)num_log_files,
            (size_t)min_log_file_idx,
            (size_t)max_log_file_idx );
    if (valid_number(last_flushed_idx)) {
        uint64_t last_flushed_seqnum = 0;
        db->p->logMgr->getLastFlushedSeqNum(last_flushed_seqnum);
        printf( "  last flushed log file index: %zu (seq %zu)\n",
                (size_t)last_flushed_idx,
                (size_t)last_flushed_seqnum );
    }
    if (valid_number(last_synced_idx)) {
        uint64_t last_synced_seqnum = 0;
        db->p->logMgr->getLastSyncedSeqNum(last_synced_seqnum);
        printf( "  last synced log file index:  %zu (seq %zu)\n",
                (size_t)last_synced_idx,
                (size_t)last_synced_seqnum );
    }
    uint64_t min_seq = 0;
    uint64_t max_seq = 0;
    db->p->logMgr->getMinSeqNum(min_seq);
    db->p->logMgr->getMaxSeqNum(max_seq);
    if ( min_seq && max_seq && valid_number(min_seq) && valid_number(max_seq) ) {
        printf("  sequence number range: %zu - %zu\n",
               (size_t)min_seq, (size_t)max_seq);
    } else {
        printf("  no active log (all logs have been flushed)\n");
    }

    if (!log_mode) {
        // Number of levels.
        size_t num_levels = db->p->tableMgr->getNumLevels();
        printf("number of levels: %zu (bottommost level %zu)\n",
               num_levels, num_levels - 1);

        size_t total_num_tables = 0;
        size_t total_num_records = 0;
        size_t total_size = 0;
        size_t total_active_size = 0;
        for (size_t ii=0; ii<num_levels; ++ii) {
            size_t num_tables = 0;
            size_t num_records = 0;
            size_t level_size_total = 0;
            size_t level_size_active = 0;
            std::list<TableInfo*> tables;

            db->p->tableMgr->mani->getTablesRange
                                   ( ii, SizedBuf(), SizedBuf(), tables );
            num_tables = tables.size();
            TableStats t_stats;
            for (auto& entry: tables) {
                TableInfo*& t_info = entry;
                t_info->file->getStats(t_stats);
                level_size_total += t_stats.totalSizeByte;
                level_size_active += t_stats.workingSetSizeByte;
                num_records += t_stats.numKvs;
                t_info->done();
            }
            printf("  level %2zu: %4zu tables, %zu records, "
                   "%zu / %zu, %s / %s\n",
                   ii, num_tables, num_records,
                   level_size_active, level_size_total,
                   Formatter::sizeToString(level_size_active).c_str(),
                   Formatter::sizeToString(level_size_total).c_str());

            total_num_records += num_records;
            total_num_tables += num_tables;
            total_size += level_size_total;
            total_active_size += level_size_active;
        }
        printf("  ---\n");
        printf("  total   : %4zu tables, %zu records, "
               "%zu / %zu, %s / %s\n",
               total_num_tables, total_num_records,
               total_active_size, total_size,
               Formatter::sizeToString(total_active_size).c_str(),
               Formatter::sizeToString(total_size).c_str());
    }

    s = DB::close(db);
    if (!s) {
        std::cout << "DB close failed: " << db_path << std::endl;
        return -1;
    }

    return 0;
}

static int dump_logs(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cout
            << "too few arguments:" << std::endl
            << "    " << exec_filename
            << " " << args[0] << " <DB path> <start log index> [<end log index>]"
            << std::endl;
        return -1;
    }

    Status s;
    const std::string& db_path = args[1];
    uint64_t start_idx = std::atoll(args[2].c_str());
    uint64_t end_idx = start_idx;
    if (args.size() >= 4) end_idx = std::atoll(args[3].c_str());

    jungle::DB* db = nullptr;
    bool log_mode = false;
    int rc = load_db(db_path, db, log_mode);
    if (rc != 0) return rc;

    // 0       1    2   (3)
    // logmeta path 100
    //   => display meta of log 100 on terminal.
    //
    // logmeta path 100 110
    //   => display meta of logs [100, 110] on terminal.
    //
    // dumplog path 100 110
    //   => display meta and value (hex) of logs [100, 110] on terminal.
    //
    // dumplog2file path 100 110
    //   => display meta of logs [100, 110] on terminal,
    //      and dump values to file.

    for (uint64_t ii=start_idx; ii<=end_idx; ++ii) {
        Record rec_out;
        Record::Holder h(rec_out);
        s = db->p->logMgr->getSN(ii, rec_out);
        printf( "  seq: %zu\n", (size_t)ii );
        if (!s) {
            printf("  READ FAILED: %d\n", (int)s);
            continue;
        }
        printf( "  key: %s\n",
                HexDump::toString(rec_out.kv.key).c_str() );
        printf( "  meta: %s\n",
                HexDump::toString(rec_out.meta).c_str() );
        if (args[0] == "dumplog") {
            printf( "  value: %s\n",
                    HexDump::toString(rec_out.kv.value).c_str() );

        } else if (args[0] == "dumplog2file") {
            std::ofstream fs;
            std::string filename = "log_dump_" + std::to_string(ii);
            fs.open(filename);
            fs << rec_out.kv.value.toString();
            fs.close();
            printf("  value: %u bytes, dumped to %s\n",
                   rec_out.kv.value.size,
                   filename.c_str());

        } else {
            printf("  value: %u bytes\n", rec_out.kv.value.size);
        }
        printf( "\n" );
    }

    return 0;
}

static int table_info(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout
            << "too few arguments:" << std::endl
            << "    " << exec_filename
            << " " << args[0] << " <DB path> [<level>]"
            << std::endl;
        return -1;
    }

    Status s;
    const std::string& db_path = args[1];
    jungle::DB* db = nullptr;
    bool log_mode = false;
    int rc = load_db(db_path, db, log_mode);
    if (rc != 0) return rc;

    // Number of levels.
    size_t num_levels = db->p->tableMgr->getNumLevels();
    printf("number of levels: %zu (bottommost level %zu)\n",
           num_levels, num_levels - 1);

    int target_level = -1;
    if (args.size() >= 3) {
        target_level = std::atoi(args[2].c_str());
    }

    for (size_t ii=0; ii<num_levels; ++ii) {
        if (target_level >= 0 && target_level != (int)ii) continue;

        printf("  level %zu:\n", ii);
        std::list<TableInfo*> tables;
        db->p->tableMgr->mani->getTablesRange
                               ( ii, SizedBuf(), SizedBuf(), tables );
        for (auto& entry: tables) {
            TableInfo*& t_info = entry;
            TableStats t_stats;
            t_info->file->getStats(t_stats);

            printf("    table %zu:\n", (size_t)t_info->number);
            if (ii == 0) {
                // L0: hash partition.
                printf("      hash: %u\n", t_info->hashNum);
            } else {
                printf("      min key: %s",
                       HexDump::toString(t_info->minKey).c_str());

                SizedBuf max_key_out;
                SizedBuf::Holder h(max_key_out);
                t_info->file->getMaxKey(max_key_out);
                printf("      max key: %s",
                       HexDump::toString(max_key_out).c_str());
            }
            printf("      number of records: %zu\n", (size_t)t_stats.numKvs);
            printf("      last sequence number: %zu\n", (size_t)t_stats.lastSeqnum);
            printf("      space: %zu / %zu, %s / %s\n",
                   (size_t)t_stats.workingSetSizeByte,
                   (size_t)t_stats.totalSizeByte,
                   Formatter::sizeToString(t_stats.workingSetSizeByte).c_str(),
                   Formatter::sizeToString(t_stats.totalSizeByte).c_str());
            printf("      block reuse cycle: %zu\n", (size_t)t_stats.blockReuseCycle);
            printf("      status: %d\n", t_info->status.load());

            t_info->done();
            printf("\n");
        }
        printf("\n");
    }

    s = DB::close(db);
    if (!s) {
        std::cout << "DB close failed: " << db_path << std::endl;
        return -1;
    }

    return 0;
}

static int dump_kv(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cout
        << "too few arguments:" << std::endl
        << "    " << exec_filename
        << " " << args[0] << " <DB path> <key> [<options>]\n"
        << "  Options:\n"
        << "   -h, --hex\n"
        << "       treat given key as a hex value.\n"
        << "       If not set, key will be treated as a plain text.\n"
        << "   -p, --prefix\n"
        << "       instead of exact match, find all keys matching\n"
        << "       the given prefix.\n"
        << "   -i, --iterate <NUMBER>\n"
        << "       print out NUMBER records equal to or greater than the given key.\n";
        return -1;
    }

    Status s;
    const std::string& db_path = args[1];
    const std::string& given_key = args[2];
    bool hex_key = false;
    bool prefix_match = false;
    bool iterate = false;
    size_t iterate_number = 1;
    if (args.size() >= 4) {
        for (size_t ii=3; ii<args.size(); ++ii) {
            if (args[ii] == "-h" || args[ii] == "--hex") {
                hex_key = true;
            }
            if (args[ii] == "-p" || args[ii] == "--prefix") {
                prefix_match = true;
            }
            if ( ii + 1 < args.size() &&
                 ( args[ii] == "-i" || args[ii] == "--iterate") ) {
                iterate = true;
                iterate_number = std::atoll(args[++ii].c_str());
            }
        }
    }

    SizedBuf key_buf;
    SizedBuf::Holder h_key_buf(key_buf);
    if (hex_key) {
        // If hex key, check validity of given string.
        key_buf = HexDump::hexStr2bin(given_key);
        if (key_buf.empty()) {
            std::cout << "incorrect hex value: " << given_key << std::endl;
            return -1;
        }

    } else {
        key_buf.set(given_key);
    }

    jungle::DB* db = nullptr;
    bool log_mode = false;
    int rc = load_db(db_path, db, log_mode);
    if (rc != 0) return rc;

    // 0       1    2   (3)
    // kvmeta path foo
    //   => display meta of record with key `foo` on terminal.
    //
    // kvmeta path 00ff -h
    //   => display meta of record with key {0x0, 0xff} on terminal.
    //
    // kvmeta path 00ff -h -p
    //   => display meta of record with key starting with
    //      {0x0, 0xff} on terminal.

    auto print_rec = [&](Record& rec_out, size_t& idx) {
        printf( "  key: %s\n",
                HexDump::toString(rec_out.kv.key).c_str() );
        printf( "  meta: %s\n",
                HexDump::toString(rec_out.meta).c_str() );

        if (args[0] == "dumpkv") {
            printf( "  value: %s\n",
                    HexDump::toString(rec_out.kv.value).c_str() );

        } else if (args[0] == "dumpkv2file") {
            std::ofstream fs;
            std::string filename = "kv_dump_" + std::to_string(idx);
            fs.open(filename);
            fs << rec_out.kv.value.toString();
            fs.close();
            printf("  value: %u bytes, dumped to %s\n",
                   rec_out.kv.value.size,
                   filename.c_str());

        } else {
            printf("  value: %u bytes\n", rec_out.kv.value.size);
        }
        printf( "\n" );
        idx++;
    };

    size_t count = 0;
    if (prefix_match || iterate) {
        jungle::Iterator itr;
        s = itr.init(db, key_buf);
        if (!s) {
            printf("  ITERATOR INIT FAILED: %d\n", (int)s);
            return -1;
        }

        do {
            Record rec_out;
            Record::Holder h_rec_out(rec_out);
            s = itr.get(rec_out);
            if (!s) break;

            if (prefix_match) {
                if ( rec_out.kv.key.size >= key_buf.size &&
                     SizedBuf::cmp( key_buf,
                                    SizedBuf( key_buf.size,
                                              rec_out.kv.key.data ) ) != 0 ) {
                    break;
                }
            }
            print_rec(rec_out, count);

            if (iterate && count >= iterate_number) {
                break;
            }
            if (count >= 100) {
                // To be safe, we limit the count by 100.
                printf("more records may exist ...\n");
                break;
            }
        } while (itr.next().ok());
        itr.close();

    } else {
        Record rec_out;
        Record::Holder h_rec_out(rec_out);
        s = db->getRecordByKey(key_buf, rec_out);
        if (!s) {
            printf("  READ FAILED: %d\n", (int)s);
            return -1;
        }
        print_rec(rec_out, count);
    }
    return 0;
}

};

void usage(int argc, char** argv) {
    std::stringstream ss;
    ss << "Usage:" << std::endl;
    ss << "    " << argv[0] << " <command> <DB path> [<parameters>]"
       << std::endl << std::endl;

    ss << "Commands:" << std::endl;
    ss << "    overview         "
       << "Print log and table file info." << std::endl;

    ss << "    logmeta          "
       << "Print key, seq number, and meta of logs "
          "in the given range of sequence number." << std::endl;

    ss << "    dumplog          "
       << "In addition to logmeta, print value as well." << std::endl;

    ss << "    dumplog2file     "
       << "In addition to logmeta, dump value to a file (per log)." << std::endl;

    ss << "    tableinfo        "
       << "Print table info in each level." << std::endl;

    ss << "    kvmeta           "
       << "Print seq number and meta of record(s) "
          "corresponding to the given key." << std::endl;

    ss << "    dumpkv           "
       << "In addition to kvmeta, print value as well." << std::endl;

    ss << "    dumpkv2file           "
       << "In addition to kvmeta, dump value to a file (per record)." << std::endl;

    std::cout << ss.str();

    exit(0);
}

int process_cmd(int argc, char** argv) {
    std::vector<std::string> args;
    for (int ii=1; ii<argc; ++ii) {
        args.push_back(argv[ii]);
    }

    if (args[0] == "overview") {
        return Checker::db_overview(args);

    } else if ( args[0] == "logmeta" ||
                args[0] == "dumplog" ||
                args[0] == "dumplog2file" ) {
        return Checker::dump_logs(args);

    } else if ( args[0] == "tableinfo" ) {
        return Checker::table_info(args);

    } else if ( args[0] == "kvmeta" ||
                args[0] == "dumpkv" ||
                args[0] == "dumpkv2file" ) {
        return Checker::dump_kv(args);

    } else {
        std::cout << "unknown command: " << args[0] << std::endl;
        usage(argc, argv);
        return -1;
    }
    return 0;
}

}; // namespace jungle_checker;

}; // namespace jungle;
using namespace jungle::checker;

int main(int argc, char** argv) {
    exec_filename = argv[0];
    if (argc < 2) {
        usage(argc, argv);
    }

    return process_cmd(argc, argv);
}

