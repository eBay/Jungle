#include "db_internal.h"
#include "db_mgr.h"
#include "fileops_directio.h"
#include "fileops_posix.h"
#include "internal_helper.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <libjungle/jungle.h>

namespace jungle {

DBGroup::DBGroup() : p(new DBGroupInternal()) {}
DBGroup::~DBGroup() {
    delete p;
}

// Static function.
Status DBGroup::open(DBGroup** ptr_out,
                     std::string path,
                     const DBConfig& db_config)
{
    Status s;
    DBMgr* mgr = DBMgr::get();
    std::string empty_kvs_name;
    DB* default_db = mgr->openExisting(path, empty_kvs_name);
    if (default_db) {
        if (default_db->p->dbGroup == nullptr) {
            // User already directly opened DB using DB::open().
            // Cannot use DBGroup at the same time. Error.
            mgr->close(default_db);
            return Status::INVALID_HANDLE_USAGE;
        } else {
            // Return existing one.
            *ptr_out = default_db->p->dbGroup;
            return Status();
        }
    }

    // Otherwise: create a new one.
    s = DB::open(&default_db, path, db_config);
    if (!s) return s;

    DBGroup* db_group;
    db_group = new DBGroup();

    DBGroup::DBGroupInternal* p = db_group->p;
    p->path = path;
    p->config = db_config;
    p->defaultDB = default_db;

    p->defaultDB->p->dbGroup = db_group;
    *ptr_out = db_group;
    return Status();
}

// Static function.
Status DBGroup::close(DBGroup* db_group) {
    Status s;

    if (db_group->p->defaultDB) {
        DBMgr* mgr = DBMgr::getWithoutInit();
        if (!mgr) return Status::ALREADY_SHUTDOWN;

        s = mgr->close(db_group->p->defaultDB);
    }
    return Status();
}

// Static function.
Status DBGroup::openDefaultDB(DB** ptr_out)
{
    if (!p->defaultDB) return Status::NOT_INITIALIZED;

    *ptr_out = p->defaultDB;
    return Status();
}

Status DBGroup::openDB(DB** ptr_out,
                      std::string db_name)
{
    return openDB(ptr_out, db_name, p->config);
}

Status DBGroup::openDB(DB** ptr_out,
                      std::string db_name,
                      const DBConfig& db_config)
{
    if (!ptr_out || db_name.empty() || !db_config.isValid()) {
        return Status::INVALID_PARAMETERS;
    }

    DBMgr* db_mgr = DBMgr::get();
    DB* src = p->defaultDB;
    DB* db = db_mgr->openExisting(src->p->path, db_name);
    if (db) {
        *ptr_out = db;
        return Status();
    }

    Status s;

    db = new DB();
    db->p->path = src->p->path;
    db->p->fOps = new FileOpsPosix;
    db->p->fDirectOps =
        new FileOpsDirectIO(src->p->myLog,
                            db_config.directIoOpt.bufferSize,
                            db_config.directIoOpt.alignSize);
    db->p->dbConfig = db_config;
    db->p->myLog = src->p->myLog;

    // Shared objects: mani.
    db->p->mani = src->p->mani;

    // Own objects: logMgr, tableMgr.
    s = db->p->mani->getKVSID(db_name, db->p->kvsID);
    if (!s) {
        // Create a new KVS
        s = db->p->mani->addNewKVS(db_name);
        if (!s) return s;

        s = db->p->mani->getKVSID(db_name, db->p->kvsID);
        if (!s) return s;

        db->p->mani->store();
        db->p->mani->sync();
    }

    db->p->kvsName = db_name;

    LogMgrOptions log_mgr_opt;
    log_mgr_opt.fOps = db->p->fOps;
    log_mgr_opt.fDirectOps = db->p->fDirectOps;
    log_mgr_opt.path = db->p->path;
    log_mgr_opt.prefixNum = db->p->kvsID;
    log_mgr_opt.kvsName = db_name;
    log_mgr_opt.dbConfig = &db->p->dbConfig;
    db->p->logMgr = new LogMgr(db);
    db->p->logMgr->setLogger(db->p->myLog);
    db->p->logMgr->init(log_mgr_opt);

    TableMgrOptions table_mgr_opt;
    table_mgr_opt.fOps = db->p->fOps;
    table_mgr_opt.path = db->p->path;
    table_mgr_opt.prefixNum = db->p->kvsID;
    table_mgr_opt.dbConfig = &db->p->dbConfig;
    db->p->tableMgr = new TableMgr(db);
    db->p->tableMgr->setLogger(db->p->myLog);
    db->p->tableMgr->init(table_mgr_opt);

    // In case of previous crash,
    // sync table's last seqnum if log is lagging behind.
    db->p->logMgr->syncSeqnum(db->p->tableMgr);

    s = db_mgr->assignNew(db);
    if (!s) {
        // Other thread already creates the handle.
        db->p->destroy();
        delete db;
        db = db_mgr->openExisting(src->p->path, db_name);
    }
    *ptr_out = db;

    return Status();
}

}

