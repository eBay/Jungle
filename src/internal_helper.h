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

#include "configs.h"
#include "endian_encode.h"
#include "fileops_base.h"
#include "libjungle/jungle.h"
#include "murmurhash3.h"

#include <atomic>
#include <chrono>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include <assert.h>
#include <inttypes.h>

#define __MACRO_TO_STR(arg) #arg
#define _MACRO_TO_STR(arg)  __MACRO_TO_STR(arg)

namespace jungle {

static Status lastRwStatus;

class RwSerializer {
public:
    static Status lastStatus;

    // Memory buffer mode.
    RwSerializer(void* dst_ptr, size_t _len)
        : dstPtr(static_cast<uint8_t*>(dst_ptr))
        , rBuf(nullptr)
        , fOps(nullptr)
        , dstFile(nullptr)
        , len(_len)
        , curPos(0)
        , supportAppend(false)
        {}

    // SizedBuf mode (same as memory buffer mode).
    RwSerializer(const SizedBuf& s_buf)
        : dstPtr(static_cast<uint8_t*>(s_buf.data))
        , rBuf(nullptr)
        , fOps(nullptr)
        , dstFile(nullptr)
        , len(s_buf.size)
        , curPos(0)
        , supportAppend(false)
        {}

    // Resizable SizedBuf mode.
    RwSerializer(SizedBuf* s_buf)
        : dstPtr(static_cast<uint8_t*>(s_buf->data))
        , rBuf(s_buf)
        , fOps(nullptr)
        , dstFile(nullptr)
        , len(s_buf->size)
        , curPos(0)
        , supportAppend(false)
        {}

    // File mode.
    RwSerializer(FileOps* f_ops, FileHandle* dst_file, bool _supportAppend = false)
        : dstPtr(nullptr)
        , rBuf(nullptr)
        , fOps(f_ops)
        , dstFile(dst_file)
        , len(0)
        , curPos(0)
        , supportAppend(_supportAppend)
        {}

    size_t size();
    size_t pos() const;
    size_t pos(size_t new_pos);
    void chkResize(size_t new_data_size);
    Status put(const void* data, size_t size);
    Status putSb(const SizedBuf& s_buf);
    Status putU64(uint64_t val);
    Status putU32(uint32_t val);
    Status putU16(uint16_t val);
    Status putU8(uint8_t val);
    Status get(void* data, size_t size);
    Status getSb(SizedBuf& s_buf_out, bool clone = true);
    uint64_t getU64(Status& s_out = lastRwStatus);
    uint32_t getU32(Status& s_out = lastRwStatus);
    uint16_t getU16(Status& s_out = lastRwStatus);
    uint8_t getU8(Status& s_out = lastRwStatus);
    bool available(size_t nbyte);

private:
    uint8_t* dstPtr;
    SizedBuf* rBuf;
    FileOps* fOps;
    FileHandle* dstFile;
    size_t len;
    size_t curPos;
    bool supportAppend;
};

class BackupRestore {
public:
    static Status copyFile(FileOps* f_ops,
                           const std::string& src_file,
                           const std::string& dst_file,
                           bool call_fsync);
    static Status backup(FileOps* f_ops,
                         const std::string& filename,
                         bool call_fsync);
    static Status backup(FileOps* f_ops,
                         const std::string& filename,
                         const SizedBuf& ctx,
                         size_t length,
                         bool call_fsync);
    static Status restore(FileOps* f_ops,
                          const std::string& filename);
};

struct Timer {
    Timer(size_t duration_ms = 0)
        : durationUs(0)
    {
        if (duration_ms) setDurationMs(duration_ms);
        reset();
    }
    void reset() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> l(lock);
        tCreated = cur;
    }
    void setDurationMs(size_t to) {
        durationUs = to * 1000;
    }
    bool timeout() const {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> l(lock);
        std::chrono::duration<double> elapsed = cur - tCreated;
        return (durationUs < elapsed.count() * 1000000);
    }
    bool timeoutAndReset() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> l(lock);
        std::chrono::duration<double> elapsed = cur - tCreated;
        if (durationUs < elapsed.count() * 1000000) {
            tCreated = cur;
            return true;
        }
        return false;
    }
    uint64_t getUs() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> l(lock);
        std::chrono::duration<double> elapsed = cur - tCreated;
        return (uint64_t)1000000 * elapsed.count();
    }
    uint64_t getMs() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> l(lock);
        std::chrono::duration<double> elapsed = cur - tCreated;
        return (uint64_t)1000 * elapsed.count();
    }
    double getSec() {
        auto cur = std::chrono::system_clock::now();
        std::lock_guard<std::mutex> l(lock);
        std::chrono::duration<double> elapsed = cur - tCreated;
        return elapsed.count();
    }
    static void sleepUs(size_t us) {
        std::this_thread::sleep_for(std::chrono::microseconds(us));
    }
    static void sleepMs(size_t ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    std::chrono::time_point<std::chrono::system_clock> tCreated;
    size_t durationUs;
    mutable std::mutex lock;
};

struct VerboseLog {
    VerboseLog(size_t time_window_ms)
        : timer(time_window_ms)
        , numSuppressed(0)
        {}

    bool checkPrint(int& num_suppressed_out) {
        if (timer.timeoutAndReset()) {
            num_suppressed_out = numSuppressed;
            numSuppressed.fetch_sub(num_suppressed_out);
            return true;
        }
        numSuppressed.fetch_add(1);
        return false;
    }

    Timer timer;
    std::atomic<int> numSuppressed;
};

#define _SCU32(val) static_cast<uint32_t>(val)
#define _SCU64(val) static_cast<uint64_t>(val)

// Error Pass
#define EP(cmd)         \
    s = cmd;            \
    if (!s) return s    \

// Try Catch
#define TC(cmd)     \
    s = cmd;        \
    if (!s) throw s \

// Error Break
#define EB(cmd, msg)            \
    s = cmd;                    \
    if (!s) { m = msg; break; } \

#define EP_(cmd)        \
    cmd;                \
    if (!s) return s    \

#define TC_(cmd)    \
    cmd;            \
    if (!s) throw s \

#define EB_(cmd, msg)           \
    cmd;                        \
    if (!s) { m = msg; break; } \

// Delete / free and clear the pointer.
#define DELETE(ptr) \
    delete ptr;     \
    ptr = nullptr   \

#define FREE(ptr)   \
    free(ptr);      \
    ptr = nullptr   \

// If-clause that can use `break` in the middle.
#define IF(cond)                        \
    for ( bool __first_loop__ = true;   \
          __first_loop__ && (cond);     \
          __first_loop__ = false )      \

using mGuard = std::unique_lock<std::mutex>;

static const std::memory_order MOR = std::memory_order_relaxed;
static const std::memory_order MOSC = std::memory_order_seq_cst;

static const uint64_t NOT_INITIALIZED = static_cast<uint64_t>(-1);

// fe 00 00 00 00 00 00 00
static const uint64_t PADDING_HEADER_FLAG = 0xfeUL << 56;

inline bool valid_number(uint64_t seq) {
    return (seq != NOT_INITIALIZED);
}

inline std::string _seq_str(uint64_t seqnum) {
    if (!valid_number(seqnum)) {
        return "(NIL)";
    }
    return std::to_string(seqnum);
}


inline Status append_file(FileOps* ops, FileHandle* fhandle,
                          const void* data, size_t size,
                          uint64_t start, uint64_t& offset)
{
    Status s;
    s = ops->pwrite(fhandle, data, size, start + offset);
    if (s) offset += size;
    return s;
}
inline Status append_file_64(FileOps* ops, FileHandle* fhandle,
                             uint64_t val, uint64_t start, uint64_t& offset)
{
    uint64_t e64 = _enc(val);
    return append_file(ops, fhandle, &e64, sizeof(e64), start, offset);
}
inline Status append_file_32(FileOps* ops, FileHandle* fhandle,
                             uint32_t val, uint64_t start, uint64_t& offset)
{
    uint32_t e32 = _enc(val);
    return append_file(ops, fhandle, &e32, sizeof(e32), start, offset);
}
inline Status append_file_16(FileOps* ops, FileHandle* fhandle,
                             uint16_t val, uint64_t start, uint64_t& offset)
{
    uint16_t e16 = _enc(val);
    return append_file(ops, fhandle, &e16, sizeof(e16), start, offset);
}
inline Status append_file_8(FileOps* ops, FileHandle* fhandle,
                            uint8_t val, uint64_t start, uint64_t& offset)
{
    return append_file(ops, fhandle, &val, sizeof(val), start, offset);
}

// Real append file
inline Status append_file(FileOps* ops, FileHandle* fhandle,
                          const void* data, size_t size)
{
    Status s;
    s = ops->append(fhandle, data, size);
    return s;
}
inline Status append_file_64(FileOps* ops, FileHandle* fhandle, uint64_t val)
{
    uint64_t e64 = _enc(val);
    return append_file(ops, fhandle, &e64, sizeof(e64));
}
inline Status append_file_32(FileOps* ops, FileHandle* fhandle, uint32_t val)
{
    uint32_t e32 = _enc(val);
    return append_file(ops, fhandle, &e32, sizeof(e32));
}
inline Status append_file_16(FileOps* ops, FileHandle* fhandle, uint16_t val)
{
    uint16_t e16 = _enc(val);
    return append_file(ops, fhandle, &e16, sizeof(e16));
}
inline Status append_file_8(FileOps* ops, FileHandle* fhandle, uint8_t val)
{
    return append_file(ops, fhandle, &val, sizeof(val));
}


inline Status read_file(FileOps* ops, FileHandle* fhandle,
                        void* data, size_t size,
                        uint64_t start, uint64_t& offset)
{
    Status s;
    s = ops->pread(fhandle, data, size, start + offset);
    if (s) offset += size;
    return s;
}
inline Status read_file_64(FileOps* ops, FileHandle* fhandle,
                           uint64_t& val, uint64_t start, uint64_t& offset)
{
    uint64_t e64;
    Status s = read_file(ops, fhandle, &e64, sizeof(e64), start, offset);
    if (!s) return s;
    val = _dec(e64);
    return s;
}
inline Status read_file_32(FileOps* ops, FileHandle* fhandle,
                           uint32_t& val, uint64_t start, uint64_t& offset)
{
    uint32_t e32;
    Status s = read_file(ops, fhandle, &e32, sizeof(e32), start, offset);
    if (!s) return s;
    val = _dec(e32);
    return s;
}
inline Status read_file_16(FileOps* ops, FileHandle* fhandle,
                           uint16_t& val, uint64_t start, uint64_t& offset)
{
    uint16_t e16;
    Status s = read_file(ops, fhandle, &e16, sizeof(e16), start, offset);
    if (!s) return s;
    val = _dec(e16);
    return s;
}
inline Status read_file_8(FileOps* ops, FileHandle* fhandle,
                          uint8_t& val, uint64_t start, uint64_t& offset)
{
    Status s = read_file(ops, fhandle, &val, sizeof(val), start, offset);
    return s;
}


inline void append_mem(void* dst, const void* data, size_t size,
                       uint64_t start, uint64_t& offset)
{
    uint8_t* ptr = static_cast<uint8_t*>(dst);
    memcpy(ptr + start + offset, data, size);
    offset += size;
}
inline void append_mem_64(void* dst, uint64_t val,
                          uint64_t start, uint64_t& offset)
{
    uint64_t e64 = _enc(val);
    return append_mem(dst, &e64, sizeof(e64), start, offset);
}
inline void append_mem_32(void* dst, uint32_t val,
                          uint64_t start, uint64_t& offset)
{
    uint32_t e32 = _enc(val);
    return append_mem(dst, &e32, sizeof(e32), start, offset);
}
inline void append_mem_16(void* dst, uint16_t val,
                          uint64_t start, uint64_t& offset)
{
    uint16_t e16 = _enc(val);
    return append_mem(dst, &e16, sizeof(e16), start, offset);
}
inline void append_mem_8(void* dst, uint8_t val,
                         uint64_t start, uint64_t& offset)
{
    return append_mem(dst, &val, sizeof(val), start, offset);
}


inline void read_mem(void* dst, void* data, size_t size,
                     uint64_t start, uint64_t& offset)
{
    uint8_t* ptr = static_cast<uint8_t*>(dst);
    memcpy(data, ptr + start + offset, size);
    offset += size;
}
inline void read_mem_64(void* dst, uint64_t& val,
                        uint64_t start, uint64_t& offset)
{
    uint64_t e64;
    read_mem(dst, &e64, sizeof(e64), start, offset);
    val = _dec(e64);
}
inline void read_mem_32(void* dst, uint32_t& val,
                        uint64_t start, uint64_t& offset)
{
    uint32_t e32;
    read_mem(dst, &e32, sizeof(e32), start, offset);
    val = _dec(e32);
}
inline void read_mem_16(void* dst, uint16_t& val,
                        uint64_t start, uint64_t& offset)
{
    uint16_t e16;
    read_mem(dst, &e16, sizeof(e16), start, offset);
    val = _dec(e16);
}
inline void read_mem_8(void* dst, uint8_t& val,
                       uint64_t start, uint64_t& offset)
{
    read_mem(dst, &val, sizeof(val), start, offset);
}

inline bool contains_null(void* a, void *b) {
    return !a || !b;
}

inline int cmp_null_chk(void* a, void *b) {
    if (!a && !b) return  0;    // Both are null:    a == b
    if (!a &&  b) return -1;    // Only `a` is null: a  < b.
    if ( a && !b) return  1;    // Only `b` is null: a  > b.
    assert(0);
    return 0xff;
}

inline size_t getMurmurHash32(const SizedBuf& data) {
    uint32_t output = 0;
    MurmurHash3_x86_32(data.data, data.size, 0, &output);
    return output;
}

inline size_t getMurmurHash(const SizedBuf& data, size_t limit) {
    uint32_t output = 0;
    MurmurHash3_x86_32(data.data, data.size, 0, &output);
    return output % limit;
}

inline uint64_t getMurmurHash64(const SizedBuf& data) {
    uint64_t output[2];
    MurmurHash3_x64_128(data.data, data.size, 0, output);
    return output[0];
}

inline const char* getOnOffStr(bool cond) {
    if (cond) return "ON";
    else return "OFF";
}

#define CMP_NULL_CHK(a, b) \
    if ( contains_null( (a), (b) ) ) return cmp_null_chk( (a), (b) );

class HexDump {
public:
    static std::string toString(const std::string& str);
    static std::string toString(const void* pd, size_t len);
    static std::string toString(const SizedBuf& buf);
    static std::string rStr(const std::string& str, size_t limit = 16);
    static SizedBuf hexStr2bin(const std::string& hexstr);
};

class FileMgr {
public:
    static int scan(const std::string& path,
                    std::vector<std::string>& files_out);
    static std::string filePart(const std::string& full_path);
    static bool exist(const std::string& path);
    static uint64_t fileSize(const std::string& path);
    static int remove(const std::string& path);
    static int removeDir(const std::string& path);
    static int copy(const std::string& from, const std::string& to);
    static int move(const std::string& from, const std::string& to);
    static int mkdir(const std::string& path);
    static uint64_t dirSize(const std::string& path,
                            bool recursive = false);
};

template<typename T>
class GcDelete {
public:
    GcDelete(T& _src) : done(false), src(_src) {}
    ~GcDelete() {
        gcNow();
    }
    void gcNow() {
        if (!done) {
            delete src;
            src = nullptr;
            done = true;
        }
    }
private:
    bool done;
    T& src;
};

class RndGen {
public:
    static size_t fromProbDist(std::vector<size_t>& prob_dist);
};

class Formatter {
public:
    static std::string usToString(uint64_t us, size_t precision = 1) {
        std::stringstream ss;
        if (us < 1000) {
            // us
            ss << std::fixed << std::setprecision(0) << us << " us";
        } else if (us < 1000000) {
            // ms
            double tmp = static_cast<double>(us / 1000.0);
            ss << std::fixed << std::setprecision(precision) << tmp << " ms";
        } else if (us < (uint64_t)600 * 1000000) {
            // second: 1 s -- 600 s (10 mins)
            double tmp = static_cast<double>(us / 1000000.0);
            ss << std::fixed << std::setprecision(precision) << tmp << " s";
        } else {
            // minute
            double tmp = static_cast<double>(us / 60.0 / 1000000.0);
            ss << std::fixed << std::setprecision(0) << tmp << " m";
        }
        return ss.str();
    }

    static std::string countToString(uint64_t count, size_t precision = 1) {
        std::stringstream ss;
        if (count < 1000) {
            ss << count;
        } else if (count < 1000000) {
            // K
            double tmp = static_cast<double>(count / 1000.0);
            ss << std::fixed << std::setprecision(precision) << tmp << "K";
        } else if (count < (uint64_t)1000000000) {
            // M
            double tmp = static_cast<double>(count / 1000000.0);
            ss << std::fixed << std::setprecision(precision) << tmp << "M";
        } else {
            // B
            double tmp = static_cast<double>(count / 1000000000.0);
            ss << std::fixed << std::setprecision(precision) << tmp << "B";
        }
        return ss.str();
    }

    static std::string sizeToString(uint64_t size, size_t precision = 1) {
        std::stringstream ss;
        if (size < 1024) {
            ss << size << " B";
        } else if (size < 1024*1024) {
            // K
            double tmp = static_cast<double>(size / 1024.0);
            ss << std::fixed << std::setprecision(precision) << tmp << " KiB";
        } else if (size < (uint64_t)1024*1024*1024) {
            // M
            double tmp = static_cast<double>(size / 1024.0 / 1024.0);
            ss << std::fixed << std::setprecision(precision) << tmp << " MiB";
        } else {
            // B
            double tmp = static_cast<double>(size / 1024.0 / 1024.0 / 1024.0);
            ss << std::fixed << std::setprecision(precision) << tmp << " GiB";
        }
        return ss.str();
    }

};

class GcFunc {
public:
    using Func = std::function< void() >;

    GcFunc(Func _func) : done(false), func(_func) {}
    ~GcFunc() { gcNow(); }
    void gcNow() {
        if (!done) {
            func();
            done = true;
        }
    }
private:
    bool done;
    Func func;
};

class StrHelper {
public:
    // Replace all `before`s in `src_str` with `after.
    // e.g.) before="a", after="A", src_str="ababa"
    //       result: "AbAbA"
    static std::string replace(const std::string& src_str,
                               const std::string& before,
                               const std::string& after)
    {
        size_t last = 0;
        size_t pos = src_str.find(before, last);
        std::string ret;
        while (pos != std::string::npos) {
            ret += src_str.substr(last, pos - last);
            ret += after;
            last = pos + before.size();
            pos = src_str.find(before, last);
        }
        if (last < src_str.size()) {
            ret += src_str.substr(last);
        }
        return ret;
    }

    // e.g.)
    //   src = "a,b,c", delim = ","
    //   result = {"a", "b", "c"}
    static std::vector<std::string> tokenize(const std::string& src,
                                             const std::string& delim)
    {
        std::vector<std::string> ret;
        size_t last = 0;
        size_t pos = src.find(delim, last);
        while (pos != std::string::npos) {
            ret.push_back( src.substr(last, pos - last) );
            last = pos + delim.size();
            pos = src.find(delim, last);
        }
        if (last < src.size()) {
            ret.push_back( src.substr(last) );
        }
        return ret;
    }

    // Trim heading whitespace and trailing whitespace
    // e.g.)
    //   src = " a,b,c ", whitespace =" "
    //   result = "a,b,c"
    static std::string trim(const std::string& src,
                            const std::string whitespace = " \t\n")
    {
        // start pos
        const size_t pos = src.find_first_not_of(whitespace);
        if (pos == std::string::npos)
            return "";

        const size_t last = src.find_last_not_of(whitespace);
        const size_t len = last - pos + 1;

        return src.substr(pos, len);
    }
};

} // namespace jungle

