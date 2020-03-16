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

#include "db_manifest.h"

#include "crc32.h"
#include "internal_helper.h"

#include _MACRO_TO_STR(LOGGER_H)

#include <atomic>

namespace jungle {

static uint8_t DBMANI_FOOTER[8] = {0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xab, 0xab};
static uint32_t DBMANI_VERSION = 0x1;

DBManifest::DBManifest(FileOps* _f_ops)
    : fOps(_f_ops)
    , mFile(nullptr)
    , maxKVSID(1) // 0 is reserved for default.
    , myLog(nullptr)
{}

DBManifest::~DBManifest() {
    if (mFile) {
        delete mFile;
    }
}

Status DBManifest::create(const std::string& path,
                          const std::string& filename)
{
    if (!fOps)
        return Status::NOT_INITIALIZED;
    if (fOps->exist(filename.c_str()))
        return Status::ALREADY_EXIST;
    if (filename.empty()) {
        return Status::INVALID_PARAMETERS;
    }

    dirPath = path;
    mFileName = filename;

    // Create a new file.
    Status s;
    _log_info(myLog, "Create new DB manifest %s", mFileName.c_str());
    fOps->open(&mFile, mFileName.c_str());

    store(false);

    return Status();
}

Status DBManifest::load(const std::string& path,
                        const std::string& filename)
{
    if (!fOps) return Status::NOT_INITIALIZED;
    if (mFile) return Status::ALREADY_LOADED;
    if (filename.empty()) return Status::INVALID_PARAMETERS;

    dirPath = path;
    mFileName = filename;

    Status s;

    _log_debug(myLog, "Load DB manifest %s", mFileName.c_str());
    EP( fOps->open(&mFile, mFileName.c_str()) );

    // File should be bigger than 12 bytes (footer + version + CRC32).
    size_t file_size = fOps->eof(mFile);
    if (file_size < 16) return Status::FILE_CORRUPTION;

    SizedBuf mani_buf(file_size);
    SizedBuf::Holder h_mani_buf(mani_buf);
    EP( fOps->pread(mFile, mani_buf.data, mani_buf.size, 0) );

    RwSerializer ss(mani_buf);

    // Magic check
    uint8_t footer_file[8];
    ss.pos(file_size - 16);
    ss.get(footer_file, 8);
    if (memcmp(DBMANI_FOOTER, footer_file, 8) != 0) return Status::FILE_CORRUPTION;

    // Version check
    uint32_t ver_file = ss.getU32(s);
    (void)ver_file;

    // CRC check
    uint32_t crc_file = ss.getU32(s);

    SizedBuf chk_buf(file_size - 4);
    SizedBuf::Holder h_chk_buf(chk_buf);
    ss.pos(0);
    ss.get(chk_buf.data, chk_buf.size);
    uint32_t crc_local = crc32_8(chk_buf.data, chk_buf.size, 0);
    if (crc_local != crc_file) return Status::CHECKSUM_ERROR;

    ss.pos(0);
    maxKVSID.store(ss.getU64(s), MOR);
    uint32_t num_kvs = ss.getU32(s);

    _log_debug(myLog, "Max KVS ID %ld, # KVS %d", maxKVSID.load(), num_kvs);

    char kvs_name_buf[256];
    for (uint32_t ii=0; ii<num_kvs; ++ii) {
        uint64_t kvs_id = ss.getU64(s);
        uint32_t name_len = ss.getU32(s);
        kvs_name_buf[name_len] = 0;
        s = ss.get(kvs_name_buf, name_len);

        _log_debug(myLog, "ID %d, name %s", kvs_id, kvs_name_buf);
        std::string kvs_name_str(kvs_name_buf);
        NameToID.insert( std::make_pair(kvs_name_str, kvs_id) );
        IDToName.insert( std::make_pair(kvs_id, kvs_name_str) );
    }

    return Status();
}

Status DBManifest::store(bool call_fsync)
{
    if (mFileName.empty() || !fOps) return Status::NOT_INITIALIZED;

    _log_debug(myLog, "Store DB manifest %s", mFileName.c_str());

    Status s;

    SizedBuf mani_buf(512);
    SizedBuf::Holder h_mani_buf(mani_buf);

    RwSerializer ss(&mani_buf);

    //   << DB manifest file format >>
    // Latest KVS ID, 8 bytes
    // Number of KVSes, 4 bytes
    ss.putU64(maxKVSID.load(MOR));
    ss.putU32(IDToName.size());
    _log_debug(myLog, "Max KVS ID %ld, # KVS %d", maxKVSID.load(), IDToName.size());

    for (auto& entry: IDToName) {
        //   << KVS entry format >>
        // KVS ID, 8 bytes
        // Length of name, 4 bytes
        // Name, xx bytes
        ss.putU64(entry.first);
        std::string kvs_name = entry.second;
        ss.putU32(kvs_name.size());
        ss.put(kvs_name.c_str(), kvs_name.size());
        _log_debug(myLog, "ID %d, name %s", entry.first, kvs_name.c_str());
    }

    // Footer.
    ss.put(DBMANI_FOOTER, 8);

    // Version.
    ss.putU32(DBMANI_VERSION);

    // CRC32.
    uint32_t crc_val = crc32_8(mani_buf.data, ss.pos(), 0);

    ss.putU32(crc_val);

    EP( fOps->pwrite(mFile, mani_buf.data, ss.pos(), 0) );

    // Should truncate tail.
    fOps->ftruncate(mFile, ss.pos());

    if (call_fsync) {
        fOps->fsync(mFile);
    }

    // After success, make a backup file one more time,
    // using the latest data.
    EP( BackupRestore::backup(fOps, mFileName, call_fsync) );

    return Status();
}

Status DBManifest::addNewKVS(const std::string& kvs_name)
{
    auto entry = NameToID.find(kvs_name);
    if (entry != NameToID.end()) {
        _log_debug(myLog, "Add new KVS %s failed, already exists.",
            kvs_name.c_str());
        return Status::ALREADY_EXIST;
    }

    uint64_t new_id = maxKVSID.fetch_add(1, MOR);
    NameToID.insert( std::make_pair(kvs_name, new_id) );
    IDToName.insert( std::make_pair(new_id, kvs_name) );
    _log_debug(myLog, "Added new KVS %ld %s.", new_id, kvs_name.c_str());

    return Status();
}

Status DBManifest::getKVSID(const std::string& kvs_name,
                            uint64_t& kvs_id_out)
{
    auto entry = NameToID.find(kvs_name);
    if (entry == NameToID.end())
        return Status::KVS_NOT_FOUND;

    kvs_id_out = entry->second;

    return Status();
}

Status DBManifest::getKVSName(const uint64_t kvs_id,
                              std::string& kvs_name_out)
{
    auto entry = IDToName.find(kvs_id);
    if (entry == IDToName.end())
        return Status::KVS_NOT_FOUND;

    kvs_name_out = entry->second;

    return Status();
}


} // namespace jungle

