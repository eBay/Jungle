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

#include <string>
#include <vector>

namespace jungle {

class Status {
public:
    enum Value {
        OK                              =  0,
        INVALID_PARAMETERS              = -1,
        ALREADY_EXIST                   = -2,
        NOT_INITIALIZED                 = -3,
        ALLOCATION_FAILURE              = -4,
        ALREADY_INITIALIZED             = -5,
        LOG_FILE_NOT_FOUND              = -6,
        INVALID_SEQNUM                  = -7,
        SEQNUM_NOT_FOUND                = -8,
        INVALID_LEVEL                   = -9,
        FDB_OPEN_FILE_FAIL              = -10,
        FDB_OPEN_KVS_FAIL               = -11,
        FDB_SET_FAIL                    = -12,
        FDB_COMMIT_FAIL                 = -13,
        LOG_NOT_SYNCED                  = -14,
        ALREADY_PURGED                  = -15,
        KEY_NOT_FOUND                   = -16,
        TABLE_NOT_FOUND                 = -17,
        ITERATOR_INIT_FAIL              = -18,
        OUT_OF_RANGE                    = -19,
        ALREADY_LOADED                  = -20,
        FILE_NOT_EXIST                  = -21,
        KVS_NOT_FOUND                   = -22,
        NOT_KV_PAIR                     = -23,
        ALREADY_CLOSED                  = -24,
        ALREADY_SHUTDOWN                = -25,
        INVALID_HANDLE_USAGE            = -26,
        LOG_NOT_EXIST                   = -27,
        OPERATION_IN_PROGRESS           = -28,
        ALREADY_REMOVED                 = -29,
        WRITE_VIOLATION                 = -30,
        TABLES_ARE_DISABLED             = -31,
        INVALID_CHECKPOINT              = -32,
        INVALID_SNAPSHOT                = -33,
        FDB_CLOSE_FAIL                  = -34,
        ALREADY_FLUSHED                 = -35,
        DB_HANDLE_NOT_FOUND             = -36,
        NULL_FILEOPS_HANDLE             = -37,
        INVALID_FILE_DESCRIPTOR         = -38,
        FILE_WRITE_SIZE_MISMATCH        = -39,
        FILE_READ_SIZE_MISMATCH         = -40,
        CHECKSUM_ERROR                  = -41,
        FILE_CORRUPTION                 = -42,
        INVALID_RECORD                  = -43,
        INVALID_MODE                    = -44,
        COMPACTION_IS_NOT_ALLOWED       = -45,
        SNAPSHOT_NOT_FOUND              = -46,
        FILE_IS_NOT_IMMUTABLE           = -47,
        MANUAL_COMPACTION_OPEN_FAILED   = -48,
        FDB_KVS_CLOSE_FAIL              = -49,
        COMPACTION_CANCELLED            = -50,
        READ_VIOLATION                  = -51,
        TIMEOUT                         = -52,
        EVICTION_FAILED                 = -53,
        INVALID_OFFSET                  = -54,
        HANDLE_IS_BEING_CLOSED          = -55,
        ROLLBACK_IN_PROGRESS            = -56,
        DIRECT_IO_NOT_SUPPORTED         = -57,
        NOT_IMPLEMENTED                 = -58,
        FILE_SIZE_MISMATCH              = -59,
        INCOMPLETE_LOG                  = -60,
        UNKNOWN_LOG_FLAG                = -61,
        EMPTY_BATCH                     = -62,
        INVALID_CONFIG                  = -63,
        DECOMPRESSION_FAILED            = -64,
        OPERATION_STOPPED               = -65,
        MANIFEST_NOT_EXIST              = -66,
        SEQINDEX_UNAVAILABLE            = -67,

        ERROR                           = -32768
    };

    Status() : val(OK) {}
    Status(int _val) : val((Value)_val) {}
    Status(Value _val) : val(_val) {}

    explicit operator bool() { return ok(); }
    inline bool operator==(const Status::Value _val) const {
        return val == _val;
    }
    operator int() const { return (int)val; }
    Value getValue() const { return val; }
    bool ok() const { return val == OK; }
    std::string toString() {
        static std::vector<std::string> names
            ( { "OK",
                "INVALID_PARAMETERS",
                "ALREADY_EXIST",
                "NOT_INITIALIZED",
                "ALLOCATION_FAILURE",
                "ALREADY_INITIALIZED",
                "LOG_FILE_NOT_FOUND",
                "INVALID_SEQNUM",
                "SEQNUM_NOT_FOUND",
                "INVALID_LEVEL",
                "FDB_OPEN_FILE_FAIL",
                "FDB_OPEN_KVS_FAIL",
                "FDB_SET_FAIL",
                "FDB_COMMIT_FAIL",
                "LOG_NOT_SYNCED",
                "ALREADY_PURGED",
                "KEY_NOT_FOUND",
                "TABLE_NOT_FOUND",
                "ITERATOR_INIT_FAIL",
                "OUT_OF_RANGE",
                "ALREADY_LOADED",
                "FILE_NOT_EXIST",
                "KVS_NOT_FOUND",
                "NOT_KV_PAIR",
                "ALREADY_CLOSED",
                "ALREADY_SHUTDOWN",
                "INVALID_HANDLE_USAGE",
                "LOG_NOT_EXIST",
                "OPERATION_IN_PROGRESS",
                "ALREADY_REMOVED",
                "WRITE_VIOLATION",
                "TABLES_ARE_DISABLED",
                "INVALID_CHECKPOINT",
                "INVALID_SNAPSHOT",
                "FDB_CLOSE_FAIL",
                "ALREADY_FLUSHED",
                "DB_HANDLE_NOT_FOUND",
                "NULL_FILEOPS_HANDLE",
                "INVALID_FILE_DESCRIPTOR",
                "FILE_WRITE_SIZE_MISMATCH",
                "FILE_READ_SIZE_MISMATCH",
                "CHECKSUM_ERROR",
                "FILE_CORRUPTION",
                "INVALID_RECORD",
                "INVALID_MODE",
                "COMPACTION_IS_NOT_ALLOWED",
                "SNAPSHOT_NOT_FOUND",
                "FILE_IS_NOT_IMMUTABLE",
                "MANUAL_COMPACTION_OPEN_FAILED",
                "FDB_KVS_CLOSE_FAIL",
                "COMPACTION_CANCELLED",
                "READ_VIOLATION",
                "TIMEOUT",
                "EVICTION_FAILED",
                "INVALID_OFFSET",
                "HANDLE_IS_BEING_CLOSED",
                "ROLLBACK_IN_PROGRESS",
                "DIRECT_IO_NOT_SUPPORTED",
                "NOT_IMPLEMENTED",
                "FILE_SIZE_MISMATCH",
                "INCOMPLETE_LOG",
                "UNKNOWN_LOG_FLAG",
                "EMPTY_BATCH",
                "INVALID_CONFIG",
                "DECOMPRESSION_FAILED",
                "OPERATION_STOPPED",
                "MANIFEST_NOT_EXIST"
                } );
        uint32_t index = -val;
        if (index < names.size()) {
            return names[index];
        } else if (ERROR == val) {
            return "ERROR";
        } else {
            return "UNKNOWN_STATUS";
        }
    }

private:
    Value val;
};

} // namespace jungle


