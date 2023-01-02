#include "jungle_builder.h"

#include "db_internal.h"
#include "internal_helper.h"
#include "libjungle/sized_buf.h"
#include "log_mgr.h"
#include "mutable_table_mgr.h"

namespace jungle {

namespace builder {

Status Builder::buildFromTableFiles(const BuildParams& params) {
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

    // Make level 1 (only when `tables` is not empty).
    if (!params.tables.empty()) {
        EP( t_mani.extendLevel() );
    }

    // 1) Find the table with the smallest key.
    // 2) Find the biggest seq number.
    uint64_t minkey_table_number = 0;
    uint64_t max_seqnum = 0;
    std::string cur_min_key;
    for (auto& td: params.tables) {
        if (td.minKey < cur_min_key) {
            cur_min_key = td.minKey;
            minkey_table_number = td.tableNumber;
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
        if (minkey_table_number != td.tableNumber) {
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

    // Adjust max table number.
    t_mani.adjustMaxTableNumber(max_table_num);

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

Status Builder::init(const std::string& path,
                     const DBConfig& db_config)
{
    Status s;

    dstPath = path;
    dbConfig = db_config;
    fOps = new FileOpsPosix();
    buildParams.path = path;

    if (!fOps->exist(dstPath)) {
        // Create the directory.
        s = fOps->mkdir(dstPath.c_str());
        if (!s.ok()) return s;
    }

    DBMgr* db_mgr = DBMgr::getWithoutInit();
    if (db_mgr) {
        myLog = db_mgr->getLogger();
    }

    return Status::OK;
}

#define handle_fdb_error(fdb_code, jungle_code) \
    if (fdb_code != FDB_RESULT_SUCCESS) { \
        _log_err(myLog, "forestdb error %d", (int)fdb_code); \
        return jungle_code; \
    }

Status Builder::set(const Record& rec) {
    fdb_status s;

    if (!curFhandle || curEstSize > dbConfig.maxL1TableSize) {
        // Close the current handle and create a new file.
        if (curFhandle) {
            Status ret = finalizeFile();
            if (!ret.ok()) {
                return ret;
            }
        }

        fdb_config f_conf = getConfig();
        curTableIdx = tableIdxCounter++;
        curFileName = TableFile::getTableFileName(dstPath, 0, curTableIdx);

        s = fdb_open(&curFhandle, curFileName.c_str(), &f_conf);
        handle_fdb_error(s, Status::FDB_OPEN_FILE_FAIL);

        fdb_kvs_config k_conf = fdb_get_default_kvs_config();
        s = fdb_kvs_open(curFhandle, &curKvsHandle, NULL, &k_conf);
        handle_fdb_error(s, Status::FDB_OPEN_KVS_FAIL);

        _log_info(myLog, "opened %s", curFileName.c_str());
    }

    fdb_doc doc;
    memset(&doc, 0x0, sizeof(doc));
    doc.key = rec.kv.key.data;
    doc.keylen = rec.kv.key.size;
    doc.body = rec.kv.value.data;
    doc.bodylen = rec.kv.value.size;

    SizedBuf raw_meta;
    SizedBuf::Holder h_raw_meta(raw_meta);
    TableFile::InternalMeta i_meta;

    size_t original_value_size = rec.kv.value.size;
    ssize_t comp_buf_size = 0; // Output buffer size.
    ssize_t comp_size = 0; // Actual compressed size.

    // Local compression buffer to avoid frequent memory allocation.
    const ssize_t LOCAL_COMP_BUF_SIZE = 4096;
    char local_comp_buf[LOCAL_COMP_BUF_SIZE];
    SizedBuf comp_buf;
    SizedBuf::Holder h_comp_buf(comp_buf);
    // Refer to the local buffer by default.
    comp_buf.referTo( SizedBuf(LOCAL_COMP_BUF_SIZE, local_comp_buf) );

    if ( dbConfig.compOpt.cbCompress &&
         dbConfig.compOpt.cbDecompress &&
         dbConfig.compOpt.cbGetMaxSize ) {
        // If compression is enabled, ask if we compress this record.
        comp_buf_size = dbConfig.compOpt.cbGetMaxSize(nullptr, rec);
        if (comp_buf_size > 0) {
            if (comp_buf_size > LOCAL_COMP_BUF_SIZE) {
                // Bigger than the local buffer size, allocate a new.
                comp_buf.alloc(comp_buf_size);
            }
            // Do compression.
            comp_size = dbConfig.compOpt.cbCompress(nullptr, rec, comp_buf);
            if (comp_size > 0) {
                // Compression succeeded, set the flag.
                i_meta.isCompressed = true;
                i_meta.originalValueLen = original_value_size;
            } else if (comp_size < 0) {
                _log_err( myLog, "compression failed: %zd, db %s, key %s",
                          comp_size,
                          dstPath.c_str(),
                          rec.kv.key.toReadableString().c_str() );
            }
            // Otherwise: if `comp_size == 0`,
            //            that implies cancelling compression.
        }
    }

    if (i_meta.isCompressed) {
        doc.body = comp_buf.data;
        doc.bodylen = comp_size;
    } else {
        doc.body = rec.kv.value.data;
        doc.bodylen = rec.kv.value.size;
    }
    TableFile::userMetaToRawMeta(rec.meta, i_meta, raw_meta);
    doc.meta = raw_meta.data;
    doc.metalen = raw_meta.size;
    doc.seqnum = (rec.seqNum == Record::NIL_SEQNUM) ? seqnumCounter++ : rec.seqNum;
    curMaxSeqnum = std::max(curMaxSeqnum, doc.seqnum);

    if (curMinKey.empty()) {
        rec.kv.key.copyTo(curMinKey);
    }

    s = fdb_set(curKvsHandle, &doc);
    handle_fdb_error(s, Status::FDB_SET_FAIL);

    numDocs++;
    curEstSize += rec.size() + TableMgr::APPROX_META_SIZE;

    return Status::OK;
}

Status Builder::finalizeFile() {
    fdb_status s;
    Timer tt;
    s = fdb_commit(curFhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
    handle_fdb_error(s, Status::FDB_COMMIT_FAIL);
    _log_info(myLog, "built index of %s, %lu records, %lu bytes, "
              "max seqnum %lu, %lu us",
              curFileName.c_str(),
              numDocs,
              curEstSize,
              curMaxSeqnum,
              tt.getUs());

    fdb_kvs_close(curKvsHandle);
    curKvsHandle = nullptr;

    fdb_close(curFhandle);
    curFhandle = nullptr;

    _log_info(myLog, "closed %s, %lu us", curFileName.c_str(), tt.getUs());

    BuildParams::TableData td;
    td.tableNumber = curTableIdx;
    td.minKey = curMinKey.toString();
    td.maxSeqnum = curMaxSeqnum;
    buildParams.tables.push_back(std::move(td));

    numDocs = curEstSize = 0;
    curMinKey.free();

    return Status::OK;
}

Status Builder::commit() {
    if (numDocs) {
        finalizeFile();
    }

    return buildFromTableFiles(buildParams);
}

Status Builder::close() {
    DELETE(fOps);
    curMinKey.free();
    if (curKvsHandle) {
        fdb_kvs_close(curKvsHandle);
        curKvsHandle = nullptr;
    }
    if (curFhandle) {
        fdb_close(curFhandle);
        curFhandle = nullptr;
    }
    return Status::OK;
}

fdb_config Builder::getConfig() {
    fdb_config config = fdb_get_default_config();
    config.seqtree_opt = FDB_SEQTREE_USE;
    config.wal_flush_before_commit = false;
    config.bulk_load_mode = true;
    config.bottom_up_index_build = true;
    config.do_not_cache_doc_blocks = true;
    return config;
}

}

}
