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

#include "log_file.h"

#include "db_mgr.h"
#include "internal_helper.h"
#include "log_mgr.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

static uint8_t LOGFILE_FOOTER[8] = {0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xab, 0xcc};
static uint32_t LOGFILE_VERSION = 0x1;

LogFile::LogFile(LogMgr* log_mgr)
    : logFileNum(0)
    , fHandle(nullptr)
    , mTable(nullptr)
    , immutable(false)
    , coldChk(false)
    , integrity(IntegrityTypes::UNKNOWN)
    , memtablePurged(false)
    , logMgr(log_mgr)
    , fileSize(0)
    , myLog(nullptr)
{}

LogFile::~LogFile() {
    if (fHandle) {
        if (fHandle->isOpened()) {
            fOps->close(fHandle);
        }
        DELETE(fHandle);
    }

    if (mTable) {
        DELETE(mTable);
    }
}

bool LogFile::isSynced() {
    if (coldChk) return false;
    if (mTable) {
        uint64_t max = mTable->maxSeqNum;
        uint64_t synced = mTable->syncedSeqNum;
        if ( valid_number(max) &&
             ( !valid_number(synced) ||
               synced < max ) ) {
            return false;
        }
    }
    return true;
}

bool LogFile::okToCloseFHandle() {
    return immutable && isSynced();
}

std::string LogFile::getLogFileName(const std::string& path,
                                    uint64_t prefix_num,
                                    uint64_t log_file_num)
{
    // Log file name example: log0001_00000001
    //                        log0001_00000002
    //                        ...
    char p_num[16];
    char l_num[16];
    sprintf(p_num, "%04" PRIu64, prefix_num);
    sprintf(l_num, "%08" PRIu64, log_file_num);
    std::string l_filename = path + "/log" + p_num + "_" + l_num;
    return l_filename;
}

Status LogFile::openFHandle() {
    Status s;
    EP( fOps->open(&fHandle, filename.c_str()) );
    _log_debug(myLog, "open file %s handle %p",
               filename.c_str(), fHandle);
    return s;
}

Status LogFile::closeFHandle() {
    if (!fHandle) return Status::ALREADY_CLOSED;

    if (fHandle->isOpened()) {
        // Once file is closed, file integrity is unkown
        integrity = IntegrityTypes::UNKNOWN;
        fOps->close(fHandle);
    }
    _log_debug(myLog, "close file %s handle %p",
               filename.c_str(), fHandle);
    delete fHandle;
    fHandle = nullptr;

    return Status();
}

void LogFile::touch() {
    lastAcc.reset();
}

bool LogFile::isExpired() {
    if ( lastAcc.getUs() >
             (uint64_t)logMgr->getDbConfig()->logFileTtl_sec * 1000000 ) {
        return true;
    }
    return false;
}

uint64_t LogFile::getLastAcc() {
    return lastAcc.getUs();
}

Status LogFile::create(const std::string& _filename,
                       FileOps* _f_ops,
                       const uint64_t log_file_num,
                       const uint64_t start_seq_num)
{
    if (mTable) return Status::ALREADY_INITIALIZED;

    filename = _filename;
    fOps = _f_ops;
    logFileNum = log_file_num;

    if (fOps->exist(filename)) {
        // Previous file exists, which means that there is a legacy log file.
        // We should overwrite it.
        _log_warn(myLog, "file %s already exists, remove it", filename.c_str());
        fOps->remove(filename);
    }

    // New created file is valid
    integrity = IntegrityTypes::VALID;
    Status s;

    // Create a memtable
    mTable = new MemTable(this);
    mTable->setLogger(myLog);
    EP( mTable->init(start_seq_num) );

    // Prepend footer and version
    if (!fHandle) {
        EP( openFHandle() );
    }

    size_t fsize = fOps->eof(fHandle);
    SizedBuf fbuf(fsize);
    SizedBuf::Holder h_fbuf(fbuf);
    EP( fOps->pread(fHandle, fbuf.data, fbuf.size, 0) );

    RwSerializer ss(&fbuf);
    ss.put(LOGFILE_FOOTER, 8);
    ss.putU32(LOGFILE_VERSION);
    EP( fOps->append(fHandle, fbuf.data, ss.pos()) );
    EP( fOps->flush(fHandle) );

    if (okToCloseFHandle()) {
        EP( closeFHandle() );
    }
    touch();

    return Status();
}

Status LogFile::load(const std::string& _filename,
                     FileOps* _f_ops,
                     uint64_t log_file_num,
                     uint64_t min_seq,
                     uint64_t flushed_seq,
                     uint64_t synced_seq)
{
    touch();

    if (mTable) return Status::ALREADY_INITIALIZED;

    filename = _filename;
    fOps = _f_ops;
    logFileNum = log_file_num;
    // We will not append existing file.
    immutable = true;

    if (!fOps->exist(filename.c_str())) return Status::FILE_NOT_EXIST;

    memTableOnFileMeta.minSeq = min_seq;
    memTableOnFileMeta.flushedSeq = flushed_seq;
    memTableOnFileMeta.syncedSeq = synced_seq;
    memTableOnFileMeta.maxSeq = synced_seq;
    if (logMgr->isLogStoreMode()) {
        // If reclaim mode, load mem-table lazily.
        memtablePurged = true;
        return Status();
    }
    return loadMemTable();
}

Status LogFile::loadMemTable() {
    touch();

    if (mTable) return Status::ALREADY_INITIALIZED;

    Status s;
    // Open log file
    if (!fHandle) {
        EP( openFHandle() );
    }

    // Check footer and version.
    fileSize = fOps->eof(fHandle);
    if (!fileSize) {
        // If file is empty, treat it as a non-exist file.
        closeFHandle();
        return Status::FILE_NOT_EXIST;
    }

    SizedBuf read_buf(fileSize);
    SizedBuf::Holder h_read_buf(read_buf);

    fOps->pread(fHandle, read_buf.data, read_buf.size, 0);

    uint8_t footer_file[8];
    RwSerializer ss(read_buf);
    ss.get(footer_file, 8);
    uint32_t ver_file = ss.getU32(s);
    (void)ver_file;

    // Load to MemTable.
    mTable = new MemTable(this);
    mTable->setLogger(myLog);
    EP( mTable->load( ss,
                      memTableOnFileMeta.minSeq,
                      memTableOnFileMeta.flushedSeq,
                      memTableOnFileMeta.syncedSeq ) );
    if (okToCloseFHandle()) {
        EP( closeFHandle() );
    }

    memtablePurged = false;
    _log_info(myLog, "loaded memtable of file %zu", logFileNum);

    logMgr->increaseOpenMemtable();
    logMgr->doBackgroundLogReclaimIfNecessary();

    return Status();
}

Status LogFile::truncate(uint64_t seq_upto) {
    Status s;
    // Open log file
    if (!fHandle) {
        EP( openFHandle() );
    }

    // Check footer and version.
    fileSize = fOps->eof(fHandle);
    if (!fileSize) {
        // If file is empty, treat it as a non-exist file.
        closeFHandle();
        return Status::FILE_NOT_EXIST;
    }

    try {
        SizedBuf read_buf(fileSize);
        SizedBuf::Holder h_read_buf(read_buf);

        TC( fOps->pread(fHandle, read_buf.data, read_buf.size, 0) );

        RwSerializer ss(read_buf);
        if (!ss.available(8 + 4)) {
            throw Status(Status::INCOMPLETE_LOG);
        }

        uint8_t footer_file[8];
        ss.get(footer_file, 8);
        uint32_t ver_file = ss.getU32(s);
        (void)ver_file;

        uint64_t offset_to_truncate = 0;
        MemTable::findOffsetOfSeq(myLog, ss, seq_upto, offset_to_truncate);

        if (mTable) {
            mTable->maxSeqNum = seq_upto;
            mTable->syncedSeqNum = seq_upto;
            mTable->seqNumAlloc = seq_upto;
        }
        memTableOnFileMeta.maxSeq = seq_upto;
        memTableOnFileMeta.syncedSeq = seq_upto;

        if (!offset_to_truncate) {
            if (okToCloseFHandle()) {
                EP( closeFHandle() );
            }
            return Status::SEQNUM_NOT_FOUND;
        }

        // Truncate.
        fOps->ftruncate(fHandle, offset_to_truncate);

        if (okToCloseFHandle()) {
            EP( closeFHandle() );
        }

        return Status();
    } catch (Status s) {
        if (okToCloseFHandle()) {
            closeFHandle();
        }
        return s;
    }
}

Status LogFile::assignSeqNum(Record& rec_local) {
    touch();
    return mTable->assignSeqNum(rec_local);
}

Status LogFile::setSN(const Record& rec) {
    touch();

    // Put into memtable
    Status s;
    bool overwrite = false;
    if ( valid_number(rec.seqNum) &&
         logMgr->getDbConfig()->allowOverwriteSeqNum &&
         rec.seqNum <= mTable->maxSeqNum ) {
        overwrite = true;
    }

    s = mTable->putNewRecord(rec);
    if (overwrite) {
        if ( valid_number(mTable->flushedSeqNum) &&
             rec.seqNum <= mTable->flushedSeqNum ) {
            mTable->flushedSeqNum = rec.seqNum - 1;
        }

        if ( valid_number(mTable->syncedSeqNum) &&
             rec.seqNum <= mTable->syncedSeqNum ) {
            mTable->syncedSeqNum = rec.seqNum - 1;
        }
    }
    return s;
}

Status LogFile::getSN(const uint64_t seq_num, Record& rec_out) {
    touch();
    return mTable->findRecordBySeq(seq_num, rec_out);
}

Status LogFile::get(const uint64_t chk,
                    const SizedBuf& key,
                    uint64_t* key_hash,
                    Record& rec_out,
                    bool allow_flushed_log,
                    bool allow_tombstone)
{
    touch();
    Status s;
    EP( mTable->getRecordByKey(chk, key, key_hash, rec_out,
                               allow_flushed_log, allow_tombstone) );
    return Status();
}

Status LogFile::flushMemTable(uint64_t upto) {
   touch();
   // Skip unnecessary flushing
   if (immutable && !fHandle && isSynced()) {
       _log_debug(myLog,
                  "skip unnecessary flushing to file %s %ld",
                  filename.c_str(), logFileNum);
       return Status();
   }

  try {
    Status s;
    if (!fHandle) {
        _log_warn(myLog,
                  "try to flush into a file that already closed %s %ld",
                  filename.c_str(), logFileNum);
        integrity = IntegrityTypes::UNKNOWN;
        TC( openFHandle() );
    }
    assert(fHandle);

#if 0
    // Memory buffer based approach.
    SizedBuf a_buf(4096);
    SizedBuf::Holder h_a_buf(a_buf);

    RwSerializer rws(&a_buf);

    TC( mTable->flush(rws) );
    TC( mTable->appendFlushMarker(rws) );

    // Append at the end.
    size_t last_pos = fOps->eof(fHandle);
    TC( fOps->pwrite(fHandle, a_buf.data, rws.pos(), last_pos) );

    fileSize = fOps->eof(fHandle);
    if (immutable) {
        TC( closeFHandle() );
    }
    return Status();

#else
    // Writing into pre-exist file needs to check integrity
    if (IntegrityTypes::UNKNOWN == integrity) {
        std::string message;
        fileSize = fOps->eof(fHandle);
        _log_warn(myLog,
                  "flush into pre-exist file %s %ld, size %ld, "
                  "checking file integrity... ",
                  filename.c_str(), logFileNum, fileSize);
        if (!fileSize) {
            // If file is empty, treat it as new created file.
            integrity = IntegrityTypes::VALID;
            message = "it's empty";
        } else {
            SizedBuf read_buf(fileSize);
            SizedBuf::Holder h_read_buf(read_buf);

            TC( fOps->pread(fHandle, read_buf.data, read_buf.size, 0) );

            RwSerializer ss(read_buf);
            if (!ss.available(8 + 4)) {
                integrity = IntegrityTypes::CORRUPTED;
                message = "file footer is incomplete";
            } else {
                uint8_t footer_file[8];
                ss.get(footer_file, 8);
                uint32_t ver_file = ss.getU32(s);
                (void) ver_file;

                uint64_t offset_to_truncate = 0;
                uint64_t padding_start_pos = NOT_INITIALIZED;
                s = MemTable::findOffsetOfSeq(myLog,
                                              ss,
                                              NOT_INITIALIZED,
                                              offset_to_truncate,
                                              &padding_start_pos);
                integrity = s ? IntegrityTypes::VALID : IntegrityTypes::CORRUPTED;
                if (s && NOT_INITIALIZED != padding_start_pos) {
                    // Get rid of padding bytes for new writes
                    Status ts = fOps->ftruncate(fHandle, padding_start_pos);
                    if (!ts) {
                        // Treat it corrupted if failed to remove padding bytes
                        integrity = IntegrityTypes::CORRUPTED;
                        message = "failed to remove padding bytes";
                    }
                }
            }
        }
        if (IntegrityTypes::CORRUPTED == integrity) {
            _log_err(myLog,
                     "integrity check of file %s %ld, size %ld, "
                     "result CORRUPTED, msg %s",
                     filename.c_str(), logFileNum, fileSize,
                     message.c_str());
        }
    }

    if (IntegrityTypes::CORRUPTED == integrity) {
        _log_err(myLog,
                 "failed to flush into file %s %ld as file is CORRUPTED",
                 filename.c_str(), logFileNum);
        // Don't break other files' flushing, so return OK
        return Status();
    }

    RwSerializer rws(fOps, fHandle, true);

    TC( mTable->flush(rws, upto) );
    TC( mTable->appendFlushMarker(rws) );

    TC( fOps->flush(fHandle) );

    fileSize = fOps->eof(fHandle);
    if (okToCloseFHandle()) {
        TC( closeFHandle() );
    }
    return Status();
#endif

  } catch (Status s) {
    if (okToCloseFHandle()) {
        closeFHandle();
    }
    return s;
  }
}

Status LogFile::purgeMemTable() {
    Timer timer;

    // WARNING:
    //   The caller of this function should coordinate
    //   race condition between load(), get(), and so on.

    // Can purge memtable only when the file is immutable.
    if (!immutable) return Status::FILE_IS_NOT_IMMUTABLE;

    memTableOnFileMeta.minSeq = mTable->minSeqNum;
    memTableOnFileMeta.maxSeq = mTable->maxSeqNum;
    memTableOnFileMeta.flushedSeq = mTable->flushedSeqNum;
    memTableOnFileMeta.syncedSeq = mTable->syncedSeqNum;

    // Now all incoming request should not go to `mTable`.
    memtablePurged = true;
    uint32_t remaining_memtables = logMgr->decreaseOpenMemtable();

    if (mTable) {
        DELETE(mTable);
    }
    _log_info( myLog,
               "purged memtable of file %s %zu, %zu us, min seq %s, "
               "max seq %s, flush seq %s, sync seq %s, %zu memtables "
               "in memory",
               filename.c_str(),
               logFileNum,
               timer.getUs(),
               _seq_str( memTableOnFileMeta.minSeq ).c_str(),
               _seq_str( memTableOnFileMeta.maxSeq ).c_str(),
               _seq_str( memTableOnFileMeta.flushedSeq ).c_str(),
               _seq_str( memTableOnFileMeta.syncedSeq ).c_str(),
               remaining_memtables );

    return Status();
}

uint64_t LogFile::getMemTableSize() const {
    return mTable ? mTable->size() : 0;
}

Status LogFile::sync() {
    touch();

    Status s;
    if (!fHandle) {
        EP( openFHandle() );
    }
    s = mTable->sync(fOps, fHandle);
    if (okToCloseFHandle()) {
        EP( closeFHandle() );
    }
    return s;
}

Status LogFile::checkpoint(uint64_t& seq_num_out) {
    touch();

    // A new checkpoint is added into an immutable file
    // Needs to flush memtable again
    if (immutable) coldChk = true;

    Status s;
    s = mTable->checkpoint(seq_num_out);
    return s;
}

Status LogFile::getLogsToFlush(const uint64_t seq_num,
                               std::list<Record*>& list_out,
                               bool ignore_sync_seqnum)
{
    touch();

    Status s;
    s = mTable->getLogsToFlush(seq_num, list_out, ignore_sync_seqnum);
    return s;
}

Status LogFile::getCheckpoints(const uint64_t seq_num,
                               std::list<uint64_t>& list_out)
{
    touch();

    Status s;
    s = mTable->getCheckpoints(seq_num, list_out);
    return s;
}

Status LogFile::setSyncedSeqNum(const uint64_t seq_num) {
    touch();
    if (!mTable) {
        memTableOnFileMeta.syncedSeq = seq_num;
        return Status();
    }

    if (!valid_number(mTable->maxSeqNum)) {
        // Nothing was appended.
        return Status();
    }

    if (seq_num > mTable->maxSeqNum) {
        mTable->syncedSeqNum.store(mTable->maxSeqNum.load());
    } else {
        mTable->syncedSeqNum = seq_num;
    }
    _log_debug(myLog, "Set log file %ld synced seqnum to %ld.",
               logFileNum, mTable->syncedSeqNum.load());
    return Status();
}

Status LogFile::setFlushedSeqNum(const uint64_t seq_num) {
    touch();
    if (!mTable) {
        memTableOnFileMeta.flushedSeq = seq_num;
        return Status();
    }

    if (!valid_number(mTable->syncedSeqNum)) {
        // Nothing was flushed.
        return Status();
    }

    if (seq_num > mTable->syncedSeqNum) {
        mTable->flushedSeqNum.store(mTable->syncedSeqNum.load());
    } else {
        mTable->flushedSeqNum = seq_num;
    }
    _log_debug(myLog, "Set log file %ld flushed seqnum to %ld.",
               logFileNum, mTable->flushedSeqNum.load());
    return Status();
}

Status LogFile::updateSeqNumByBulkLoader(const uint64_t seq_num) {
    touch();

    mTable->flushedSeqNum = seq_num;
    mTable->syncedSeqNum = seq_num;
    mTable->minSeqNum = seq_num;
    mTable->maxSeqNum = seq_num;
    return Status();
}

Status LogFile::destroySelf() {
    if (fHandle) {
        if (fHandle->isOpened()) {
            fOps->close(fHandle);
        }
        delete fHandle;
        fHandle = nullptr;
    }

    if (fOps->exist(filename.c_str())) {
        // Instead removing it immediately,
        // put it into remove list.
        DBMgr* dbm = DBMgr::getWithoutInit();
        if (!dbm) {
            fOps->remove(filename.c_str());
        } else {
            dbm->addFileToRemove(filename);
        }
    }

    if (mTable) {
        delete mTable;
        mTable = nullptr;
        if (immutable) {
            logMgr->decreaseOpenMemtable();
        }
    }

    return Status();
}

uint64_t LogFile::getMinSeqNum() const {
    if (mTable) {
        return mTable->minSeqNum;
    } else {
        return memTableOnFileMeta.minSeq;
    }
}

uint64_t LogFile::getFlushedSeqNum() const {
    if (mTable) {
        return mTable->flushedSeqNum;
    } else {
        return memTableOnFileMeta.flushedSeq;
    }
}

uint64_t LogFile::getSyncedSeqNum() const {
    if (mTable) {
        return mTable->syncedSeqNum;
    } else {
        return memTableOnFileMeta.syncedSeq;
    }
}

uint64_t LogFile::getMaxSeqNum() const {
    if (mTable) {
        return mTable->maxSeqNum.load(MOR);
    } else {
        return memTableOnFileMeta.maxSeq;
    }
}

void LogFile::setImmutable() {
    immutable = true;
    if (mTable) {
        _log_info( myLog,
                   "log file %s %zu becomes immutable, min seq %s, "
                   "max seq %s, flushed seq %s, synced seq %s, size %zu",
                   filename.c_str(),
                   logFileNum,
                   _seq_str(mTable->minSeqNum).c_str(),
                   _seq_str(mTable->maxSeqNum).c_str(),
                   _seq_str(mTable->flushedSeqNum).c_str(),
                   _seq_str(mTable->syncedSeqNum).c_str(),
                   mTable->bytesSize.load() );
        // Now memtable of this log file is able to be purged.
        logMgr->increaseOpenMemtable();
        logMgr->doBackgroundLogReclaimIfNecessary();

    } else {
        _log_info(myLog, "log file %s %zu becomes immutable",
                  filename.c_str(), logFileNum);
    }
}

bool LogFile::isValidToWrite() {
    // If 1) already set immutable OR
    //    2) file size exceeds OR
    //    3) # entries exceeds
    //    then not writable.
    uint32_t max_log_file_size = logMgr->getDbConfig()->maxLogFileSize;
    uint32_t max_log_entries = logMgr->getDbConfig()->maxEntriesInLogFile;
    if ( immutable ||
         fileSize > max_log_file_size ||
         getMemTableSize() > max_log_file_size ||
         mTable->getNumLogs() >= max_log_entries )
        return false;
    return true;
}

bool LogFile::isIncreasingOrder() const {
    if (mTable) return mTable->isIncreasingOrder();
    return false;
}


LogFile::Iterator::Iterator() : lFile(nullptr) {}
LogFile::Iterator::~Iterator() {}

Status LogFile::Iterator::init(LogFile* l_file,
                               const SizedBuf& start_key,
                               const SizedBuf& end_key,
                               const uint64_t seq_upto)
{
    lFile = l_file;
    lFile->touch();
    return mItr.init(lFile->mTable, start_key, end_key, seq_upto);
}

Status LogFile::Iterator::initSN(LogFile* l_file,
                                 const uint64_t min_seq,
                                 const uint64_t max_seq)
{
    lFile = l_file;
    lFile->touch();
    return mItr.initSN(lFile->mTable, min_seq, max_seq);
}


Status LogFile::Iterator::get(Record& rec_out) {
    lFile->touch();
    return mItr.get(rec_out);
}

Status LogFile::Iterator::prev(bool allow_tombstone) {
    lFile->touch();
    return mItr.prev(allow_tombstone);
}

Status LogFile::Iterator::next(bool allow_tombstone) {
    lFile->touch();
    return mItr.next(allow_tombstone);
}

Status LogFile::Iterator::seek(const SizedBuf& key, SeekOption opt) {
    lFile->touch();
    return mItr.seek(key, (MemTable::Iterator::SeekOption)opt);
}

Status LogFile::Iterator::seekSN(const uint64_t seqnum, SeekOption opt) {
    lFile->touch();
    return mItr.seekSN(seqnum, (MemTable::Iterator::SeekOption)opt);
}

Status LogFile::Iterator::gotoBegin() {
    lFile->touch();
    return mItr.gotoBegin();
}

Status LogFile::Iterator::gotoEnd() {
    lFile->touch();
    return mItr.gotoEnd();
}

Status LogFile::Iterator::close() {
    lFile->touch();
    Status s;
    EP( mItr.close() );
    lFile = nullptr;
    return Status();
}

} // namespace jungle


