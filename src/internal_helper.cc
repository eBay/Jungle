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

#include "internal_helper.h"

#include "fileops_base.h"
#include "hex_dump.h"
#include "libjungle/jungle.h"

#include <set>
#include <string>

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

// LCOV_EXCL_START

namespace jungle {

size_t RwSerializer::size() {
    if (len) return len;
    return fOps->eof(dstFile);
}

size_t RwSerializer::pos() const {
    return curPos;
}

size_t RwSerializer::pos(size_t new_pos) {
    if (len) {
        assert(new_pos <= len);
    }
    curPos = new_pos;
    return curPos;
}

void RwSerializer::chkResize(size_t new_data_size) {
    // Not resizable mode.
    if (!rBuf) return;

    if (rBuf->empty()) {
        // Allocate 32 bytes if empty.
        rBuf->alloc(32);
    }

    size_t new_buf_size = rBuf->size;
    while (curPos + new_data_size > new_buf_size) {
        // Double the size.
        new_buf_size *= 2;
    }
    rBuf->resize(new_buf_size);
    dstPtr = rBuf->data;
    len = rBuf->size;
}

Status RwSerializer::put(const void* data, size_t size) {
    uint64_t off = pos();
    Status s;
    chkResize(size);
    if (dstPtr) {
        append_mem(dstPtr, data, size, 0, off);
    } else if (supportAppend) {
        s = append_file(fOps, dstFile, data, size);
    } else {
        s = append_file(fOps, dstFile, data, size, 0, off);
    }
    if (s) pos(off);
    return s;
}

Status RwSerializer::putSb(const SizedBuf& s_buf) {
    Status s;
    s = putU32(s_buf.size);
    if (!s) return s;

    s = put(s_buf.data, s_buf.size);
    if (!s) {
        // Rollback.
        pos( pos() - sizeof(uint32_t) );
    }
    return s;
}

Status RwSerializer::putU64(uint64_t val) {
    uint64_t off = pos();
    Status s;
    chkResize( sizeof(val) );
    if (dstPtr) {
        append_mem_64(dstPtr, val, 0, off);
    } else if (supportAppend) {
        s = append_file_64(fOps, dstFile, val);
    } else {
        s = append_file_64(fOps, dstFile, val, 0, off);
    }
    if (s) pos(off);
    return s;
}

Status RwSerializer::putU32(uint32_t val) {
    uint64_t off = pos();
    Status s;
    chkResize( sizeof(val) );
    if (dstPtr) {
        append_mem_32(dstPtr, val, 0, off);
    } else if (supportAppend) {
        s = append_file_32(fOps, dstFile, val);
    } else {
        s = append_file_32(fOps, dstFile, val, 0, off);
    }
    if (s) pos(off);
    return s;
}

Status RwSerializer::putU16(uint16_t val) {
    uint64_t off = pos();
    Status s;
    chkResize( sizeof(val) );
    if (dstPtr) {
        append_mem_16(dstPtr, val, 0, off);
    } else if (supportAppend) {
        s = append_file_16(fOps, dstFile, val);
    } else {
        s = append_file_16(fOps, dstFile, val, 0, off);
    }
    if (s) pos(off);
    return s;
}

Status RwSerializer::putU8(uint8_t val) {
    uint64_t off = pos();
    Status s;
    chkResize( sizeof(val) );
    if (dstPtr) {
        append_mem_8(dstPtr, val, 0, off);
    } else if (supportAppend) {
        s = append_file_8(fOps, dstFile, val);
    } else {
        s = append_file_8(fOps, dstFile, val, 0, off);
    }
    if (s) pos(off);
    return s;
}

Status RwSerializer::get(void* data, size_t size) {
    uint64_t off = pos();
    Status s;
    if (dstPtr) {
        if (curPos + size > len) {
            s = Status::READ_VIOLATION;
        } else {
            read_mem(dstPtr, data, size, 0, off);
        }
    } else {
        s = read_file(fOps, dstFile, data, size, 0, off);
    }
    if (s) pos(off);
    return s;
}

Status RwSerializer::getSb(SizedBuf& s_buf_out, bool clone) {
    Status s;
    uint32_t len = getU32(s);
    s_buf_out.clear();
    if (len) {
        if (clone) {
            s_buf_out.alloc(len);
            s = get(s_buf_out.data, len);
            if (!s) s_buf_out.free();
        } else {
            s_buf_out.data = dstPtr + curPos;
            s_buf_out.size = len;
            pos( pos() + len );
        }
    }
    return s;
}

uint64_t RwSerializer::getU64(Status& s_out) {
    uint64_t off = pos();
    uint64_t val = 0;
    if (dstPtr) {
        if (curPos + sizeof(val) > len) {
            s_out = Status::READ_VIOLATION;
        } else {
            read_mem_64(dstPtr, val, 0, off);
        }
    } else {
        s_out = read_file_64(fOps, dstFile, val, 0, off);
    }
    if (s_out) pos(off);
    return val;
}

uint32_t RwSerializer::getU32(Status& s_out) {
    uint64_t off = pos();
    uint32_t val = 0;
    if (dstPtr) {
        if (curPos + sizeof(val) > len) {
            s_out = Status::READ_VIOLATION;
        } else {
            read_mem_32(dstPtr, val, 0, off);
        }
    } else {
        s_out = read_file_32(fOps, dstFile, val, 0, off);
    }
    if (s_out) pos(off);
    return val;
}

uint16_t RwSerializer::getU16(Status& s_out) {
    uint64_t off = pos();
    uint16_t val = 0;
    if (dstPtr) {
        if (curPos + sizeof(val) > len) {
            s_out = Status::READ_VIOLATION;
        } else {
            read_mem_16(dstPtr, val, 0, off);
        }
    } else {
        s_out = read_file_16(fOps, dstFile, val, 0, off);
    }
    if (s_out) pos(off);
    return val;
}

uint8_t RwSerializer::getU8(Status& s_out) {
    uint64_t off = pos();
    uint8_t val = 0;
    if (dstPtr) {
        if (curPos + sizeof(val) > len) {
            s_out = Status::READ_VIOLATION;
        } else {
            read_mem_8(dstPtr, val, 0, off);
        }
    } else {
        s_out = read_file_8(fOps, dstFile, val, 0, off);
    }
    if (s_out) pos(off);
    return val;
}

bool RwSerializer::available(size_t nbyte) {
    if (dstPtr) {
        return (pos() + nbyte) <= len;
    } else if (fOps) {
        return true;
    } else {
        return false;
    }
}

Status BackupRestore::copyFile(FileOps* f_ops,
                               const std::string& src_file,
                               const std::string& dst_file,
                               bool call_fsync)
{
    Status s;
    if (!f_ops->exist(src_file)) return Status::FILE_NOT_EXIST;

    FileHandle* s_file = nullptr;
    FileHandle* d_file = nullptr;

   try {
    TC(f_ops->open(&s_file, src_file));
    TC(f_ops->open(&d_file, dst_file));

    size_t file_size = f_ops->eof(s_file);
    SizedBuf tmp_buf(file_size);
    SizedBuf::Holder h_tmp_buf(tmp_buf);

    TC( f_ops->pread(s_file, tmp_buf.data, tmp_buf.size, 0) );
    TC( f_ops->pwrite(d_file, tmp_buf.data, tmp_buf.size, 0) );
    f_ops->ftruncate(d_file, file_size);
    if (call_fsync) {
        f_ops->fsync(d_file);
    }

    f_ops->close(s_file);
    f_ops->close(d_file);
    delete s_file;
    delete d_file;

    return s;

   } catch (Status s) {
    if (s_file) {
        f_ops->close(s_file);
        delete s_file;
    }
    if (d_file) {
        f_ops->close(d_file);
        delete d_file;
    }
    return s;
   }
}

Status BackupRestore::backup(FileOps* f_ops,
                             const std::string& filename,
                             bool call_fsync)
{
    return copyFile(f_ops, filename, filename + ".bak", call_fsync);
}

Status BackupRestore::backup(FileOps* f_ops,
                             const std::string& filename,
                             const SizedBuf& ctx,
                             size_t length,
                             size_t bytes_to_skip,
                             bool call_fsync)
{
    Status s;
    std::string dst_file = filename + ".bak";

    FileHandle* d_file = nullptr;

   try {
    TC( f_ops->open(&d_file, dst_file) );
    if (length > bytes_to_skip) {
        TC( f_ops->pwrite( d_file,
                           ctx.data + bytes_to_skip,
                           length - bytes_to_skip,
                           bytes_to_skip ) );
    }
    f_ops->ftruncate(d_file, length);
    if (call_fsync) {
        f_ops->fsync(d_file);
    }
    f_ops->close(d_file);
    delete d_file;
    return s;

   } catch (Status s) {
    if (d_file) {
        f_ops->close(d_file);
        delete d_file;
    }
    return s;
   }
}

Status BackupRestore::backup(FileOps* f_ops,
                             FileHandle** file,
                             const std::string& filename,
                             const SizedBuf& ctx,
                             size_t length,
                             size_t bytes_to_skip,
                             bool call_fsync)
{
    Status s;
    std::string dst_file = filename + ".bak";

   try {
    if (!f_ops->exist(dst_file)) {
        if (*file) {
            f_ops->close(*file);
            delete *file;
            *file = nullptr;
        }
    }

    if (!(*file)) {
        TC( f_ops->open(file, dst_file) );
    }

    size_t file_size = f_ops->eof(*file);
    if (length > bytes_to_skip) {
        TC( f_ops->pwrite( *file,
                        ctx.data + bytes_to_skip,
                        length - bytes_to_skip,
                        bytes_to_skip ) );
    }
    if (file_size > length) {
        f_ops->ftruncate(*file, length);
    }

    if (call_fsync) {
        f_ops->fsync(*file);
    }
    return s;
   } catch (Status s) {
    if (*file) {
        f_ops->close(*file);
        delete *file;
        *file = nullptr;
    }
    return s;
   }
}

Status BackupRestore::restore(FileOps* f_ops,
                              const std::string& filename)
{
    return copyFile(f_ops, filename + ".bak", filename, true);
}

std::string HexDump::toString(const std::string& str) {
    return toString(str.data(), str.size());
}

std::string HexDump::toString(const void* pd, size_t len) {
    char* buffer;
    size_t buffer_len;
    print_hex_options opt = PRINT_HEX_OPTIONS_INITIALIZER;
    opt.actual_address = 0;
    opt.enable_colors = 0;
    print_hex_to_buf(&buffer, &buffer_len,
                     pd, len,
                     opt);
    std::string s = std::string(buffer);
    free(buffer);
    return s;
}

std::string HexDump::toString(const SizedBuf& buf) {
    return toString(buf.data, buf.size);
}

std::string HexDump::rStr(const std::string& str, size_t limit) {
    std::stringstream ss;
    size_t size = std::min(str.size(), limit);
    for (size_t ii=0; ii<size; ++ii) {
        char cc = str[ii];
        if (0x20 <= cc && cc <= 0x7d) {
            ss << cc;
        } else {
            ss << '.';
        }
    }
    ss << " (" << str.size() << ")";
    return ss.str();
}

SizedBuf HexDump::hexStr2bin(const std::string& hexstr) {
    if (hexstr.size() == 0 || hexstr.size() % 2) {
        return SizedBuf();
    }

    SizedBuf key_buf;
    key_buf.alloc(hexstr.size() / 2);
    for (size_t ii=0; ii<hexstr.size(); ++ii) {
        if ( ( hexstr[ii] >= '0' &&
               hexstr[ii] <= '9' ) ||
             ( hexstr[ii] >= 'a' &&
               hexstr[ii] <= 'f' ) ||
             ( hexstr[ii] >= 'A' &&
               hexstr[ii] <= 'F' ) ) {
            if (ii && ii % 2 == 1) {
                std::string cur_hex = hexstr.substr(ii - 1, 2);
                key_buf.data[ii/2] = std::stoul(cur_hex, nullptr, 16);
            }

        } else {
            key_buf.free();
            return SizedBuf();
        }
    }
    return key_buf;
}

int FileMgr::scan(const std::string& path,
                  std::vector<std::string>& files_out)
{
    DIR *dir_info = nullptr;
    struct dirent *dir_entry = nullptr;

    dir_info = opendir(path.c_str());
    while ( dir_info && (dir_entry = readdir(dir_info)) ) {
        files_out.push_back(dir_entry->d_name);
    }
    if (dir_info) {
        closedir(dir_info);
    }
    return 0;
}

std::string FileMgr::filePart(const std::string& full_path) {
    size_t pos = full_path.rfind("/");
    if (pos == std::string::npos) return full_path;
    return full_path.substr(pos + 1, full_path.size() - pos - 1);
}

bool FileMgr::exist(const std::string& path) {
    struct stat st;
    int result = stat(path.c_str(), &st);
    return (result == 0);
}

uint64_t FileMgr::fileSize(const std::string& path) {
    struct stat st;
    int result = stat(path.c_str(), &st);
    if (result != 0) return 0;
    return st.st_size;
}

int FileMgr::remove(const std::string& path) {
    if (!exist(path)) return 0;
    return ::remove(path.c_str());
}

int FileMgr::removeDir(const std::string& path) {
    if (!exist(path)) return 0;
    // TODO: non-Posix OS.
    std::string cmd = "rm -rf " + path;
    return ::system(cmd.c_str());
}

int FileMgr::copy(const std::string& from, const std::string& to) {
    std::string cmd = "cp -R " + from + " " + to + " 2> /dev/null";
    FILE* fp = popen(cmd.c_str(), "r");
    return pclose(fp);
}

int FileMgr::move(const std::string& from, const std::string& to) {
    std::string cmd = "mv " + from + " " + to + " 2> /dev/null";
    FILE* fp = popen(cmd.c_str(), "r");
    return pclose(fp);
}

int FileMgr::mkdir(const std::string& path) {
    return ::mkdir(path.c_str(), 0755);
}

uint64_t FileMgr::dirSize(const std::string& path,
                          bool recursive)
{
    uint64_t ret = 0;
    DIR *dir_info = nullptr;
    struct dirent *dir_entry = nullptr;

    dir_info = opendir(path.c_str());
    while ( dir_info && (dir_entry = readdir(dir_info)) ) {
        std::string d_name = dir_entry->d_name;
        if (d_name == "." || d_name == "..") continue;

        std::string full_path = path + "/" + d_name;

        if (dir_entry->d_type == DT_REG) {
            struct stat st;
            if (stat(full_path.c_str(), &st) == 0) {
                ret += st.st_size;
            }

        } else if (recursive && dir_entry->d_type == DT_DIR) {
            ret += dirSize(full_path, recursive);
        }
    }
    if (dir_info) {
        closedir(dir_info);
    }
    return ret;
}

size_t RndGen::fromProbDist(std::vector<size_t>& prob_dist) {
    uint64_t sum = 0;
    for (size_t& prob: prob_dist) sum += (prob * 65536);
    assert(sum > 0);

    uint64_t rr = rand() % sum;
    uint64_t cnt = 0;
    for (size_t ii=0; ii<prob_dist.size(); ++ii) {
        cnt += (prob_dist[ii] * 65536);
        if (rr < cnt) return ii;
    }
    return prob_dist.size() - 1;
}

size_t PrimeNumber::getUpper(size_t value) {
    static const std::set<size_t> prime_numbers =
        { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29,
          31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
          73, 79, 83, 89, 97, 101, 103, 107, 109, 113,
          127, 131, 137, 139, 149, 151, 157, 163, 167, 173,
          179, 181, 191, 193, 197, 199, 211, 223, 227, 229,
          233, 239, 241, 251, 257, 263, 269, 271, 277, 281,
          283, 293, 307, 311, 313, 317, 331, 337, 347, 349,
          353, 359, 367, 373, 379, 383, 389, 397, 401, 409,
          419, 421, 431, 433, 439, 443, 449, 457, 461, 463,
          467, 479, 487, 491, 499, 503, 509, 521, 523, 541,
          547, 557, 563, 569, 571, 577, 587, 593, 599, 601,
          607, 613, 617, 619, 631, 641, 643, 647, 653, 659,
          661, 673, 677, 683, 691, 701, 709, 719, 727, 733,
          739, 743, 751, 757, 761, 769, 773, 787, 797, 809,
          811, 821, 823, 827, 829, 839, 853, 857, 859, 863,
          877, 881, 883, 887, 907, 911, 919, 929, 937, 941,
          947, 953, 967, 971, 977, 983, 991, 997 };

    if (value >= *prime_numbers.rbegin()) return *prime_numbers.rbegin();

    // Equal to or greater than.
    return *prime_numbers.lower_bound(value);
}

} // namespace jungle

// LCOV_EXCL_STOP

