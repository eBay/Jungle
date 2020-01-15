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

#include <cstring>
#include <ctime>
#include <random>

#include "fileops_directio.h"
#include "fileops_posix.h"

#include "test_common.h"

using namespace jungle;

#define DEFAULT_ALIGNMENT 512
#define DEFAULT_BUFFER_SIZE 16384

int read_write_without_open_test()
{
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle = nullptr;

    Status s;
    char buf[256];

    s = ops->pread(fhandle, buf, 256, 0);
    CHK_NOT(s);

    memset(buf, 'x', 256);
    s = ops->append(fhandle, buf, 256);
    CHK_NOT(s);

    s = ops->flush(fhandle);
    CHK_NOT(s);

    s = ops->close(fhandle);
    CHK_NOT(s);

    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int normal_read_write_test()
{
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    char buf[256];

    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);
    CHK_OK(fhandle->isOpened());

    memset(buf, 'x', 256);
    // DirectIO random write not supported
    s = ops->pwrite(fhandle, buf, 256, 0);
    CHK_NOT(s);
    s = ops->append(fhandle, buf, 256);
    CHK_OK(s);

    s = ops->flush(fhandle);
    CHK_OK(s);

    memset(buf, 'a', 256);
    s = ops->pread(fhandle, buf, 256, 0);
    CHK_OK(s);

    char buf_chk[256];
    memset(buf_chk, 'x', 256);
    CHK_EQ(0, memcmp(buf_chk, buf, 256));

    // Check file size
    cs_off_t offset = ops->eof(fhandle);
    CHK_EQ(DEFAULT_ALIGNMENT, offset);

    // Check padding bytes
    memset(buf, 'a', 256);
    s = ops->pread(fhandle, buf, 256, 256);
    CHK_OK(s);
    uint64_t val = 0;
    uint64_t offset_tmp = 0;
    read_mem_64(buf, val, 0, offset_tmp);
    CHK_EQ(PADDING_HEADER_FLAG, val);

    // Read beyond file size
    s = ops->pread(fhandle, buf, 300, 256);
    CHK_NOT(s);

    s = ops->close(fhandle);
    CHK_OK(s);
    CHK_NOT(fhandle->isOpened());

    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

uint32_t randint(uint32_t begin_inclusive, uint32_t end_inclusive) {
    assert(begin_inclusive <= end_inclusive);
    return std::rand() % (end_inclusive - begin_inclusive + 1) + begin_inclusive;
}

void _set_circling_data(uint8_t* buf, size_t size, size_t int_seq = 0) {
    uint8_t seq = static_cast<uint8_t>(int_seq % 256);
    for (uint32_t i = 0; i < size; i++) {
        buf[i] = seq;
        seq++;
    }
}

int _check_circling_data(uint8_t* buf, size_t size, size_t int_seq = 0) {
    uint8_t seq = static_cast<uint8_t>(int_seq % 256);
    for (uint32_t i = 0; i < size; i++) {
        CHK_EQ(seq, buf[i]);
        seq++;
    }
    return 0;
}

size_t _file_size(size_t data_size) {
    size_t need_align = data_size % DEFAULT_ALIGNMENT;
    if (0 == need_align) {
        return data_size;
    } else {
        // Make sure enougth room for padding bytes
        return need_align <= (DEFAULT_ALIGNMENT - 8)
               ? data_size - need_align + DEFAULT_ALIGNMENT
               : data_size - need_align + 2 * DEFAULT_ALIGNMENT;
    }
}

int unaligned_read_write_test()
{
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);
    CHK_OK(fhandle->isOpened());

    // Prepare write buffer
    size_t buf_size = static_cast<size_t>(randint(1, 10)
                                          * DEFAULT_BUFFER_SIZE
                                          + randint(0, DEFAULT_ALIGNMENT - 1));
    uint8_t* buf = new uint8_t[buf_size];
    uint8_t* buf_chk = new uint8_t[buf_size];
    _set_circling_data(buf, buf_size);
    _check_circling_data(buf, buf_size);

    // Append into file with random size
    size_t buf_pos = 0;
    bool truncated = false;
    size_t truncate_pos = randint(1, buf_size - 1);
    while (buf_pos < buf_size) {
        size_t slice = randint(1, buf_size - buf_pos);
        s = ops->append(fhandle, buf + buf_pos, slice);
        CHK_OK(s);
        buf_pos += slice;

        // Truncate
        if (!truncated && buf_pos > truncate_pos) {
            truncated = true;
            buf_pos = truncate_pos;

            s = ops->flush(fhandle);
            CHK_OK(s);

            s = ops->ftruncate(fhandle, truncate_pos);
            CHK_OK(s);
            // Check file size
            cs_off_t file_size = ops->eof(fhandle);
            CHK_EQ(truncate_pos, file_size);
            // Read all
            s = ops->pread(fhandle, buf_chk, truncate_pos, 0);
            CHK_OK(s);
            _check_circling_data(buf_chk, truncate_pos);
            // Read tail
            s = ops->pread(fhandle, buf_chk, 1, truncate_pos - 1);
            CHK_OK(s);
            _check_circling_data(buf_chk, 1, truncate_pos - 1);
            // Read beyond file size
            s = ops->pread(fhandle, buf_chk, 1, truncate_pos);
            CHK_NOT(s);
        }
    }
    s = ops->flush(fhandle);
    CHK_OK(s);

    // Check file size
    cs_off_t file_size = ops->eof(fhandle);
    CHK_EQ(_file_size(buf_size), file_size);

    // Read all
    s = ops->pread(fhandle, buf_chk, buf_size, 0);
    CHK_OK(s);
    _check_circling_data(buf_chk, buf_size);

    // Check padding bytes
    if (buf_size % DEFAULT_ALIGNMENT != 0) {
        s = ops->pread(fhandle, buf_chk, 8, buf_size);
        CHK_OK(s);
        uint64_t val = 0;
        uint64_t offset_tmp = 0;
        read_mem_64(buf_chk, val, 0, offset_tmp);
        CHK_EQ(PADDING_HEADER_FLAG, val);
    }

    // Randomly read
    for (int i = 0; i < 100; i++) {
        size_t offset = randint(0, buf_size - 1);
        size_t slice = randint(1, buf_size - offset);
        s = ops->pread(fhandle, buf_chk, slice, offset);
        CHK_OK(s);
        _check_circling_data(buf_chk, slice, offset);
    }

    s = ops->close(fhandle);
    CHK_OK(s);
    CHK_NOT(fhandle->isOpened());

    delete[] buf;
    delete[] buf_chk;
    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int unaligned_file_read_test()
{
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    // Prepare write buffer
    size_t buf_size = static_cast<size_t>(randint(1, 10)
                                          * DEFAULT_BUFFER_SIZE
                                          + randint(1, DEFAULT_ALIGNMENT - 1));
    uint8_t* buf = new uint8_t[buf_size];
    uint8_t* buf_chk = new uint8_t[buf_size];
    _set_circling_data(buf, buf_size);
    _check_circling_data(buf, buf_size);

    // Generate unaligned file
    {
        FileOps* ops = new FileOpsPosix();
        FileHandle* fhandle;

        s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
        CHK_OK(s);
        CHK_OK(fhandle->isOpened());

        s = ops->pwrite(fhandle, buf, buf_size, 0);
        CHK_OK(s);

        s = ops->close(fhandle);
        CHK_OK(s);
        CHK_NOT(fhandle->isOpened());

        delete fhandle;
        delete ops;
    }

    // Open unaligned file with direct-io
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);
    CHK_OK(fhandle->isOpened());

    // Check file size
    cs_off_t file_size = ops->eof(fhandle);
    CHK_EQ(buf_size, file_size);

    // Read all
    s = ops->pread(fhandle, buf_chk, buf_size, 0);
    CHK_OK(s);
    _check_circling_data(buf_chk, buf_size);

    // Randomly read
    for (int i = 0; i < 100; i++) {
        size_t offset = randint(0, buf_size - 1);
        size_t slice = randint(1, buf_size - offset);
        s = ops->pread(fhandle, buf_chk, slice, offset);
        CHK_OK(s);
        _check_circling_data(buf_chk, slice, offset);
    }

    s = ops->close(fhandle);
    CHK_OK(s);
    CHK_NOT(fhandle->isOpened());

    delete[] buf;
    delete[] buf_chk;
    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int unaligned_file_write_test()
{
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    // Prepare write buffer
    size_t buf_size = static_cast<size_t>(randint(1, 10)
                                          * DEFAULT_BUFFER_SIZE
                                          + randint(1, DEFAULT_ALIGNMENT - 1));
    uint8_t* buf = new uint8_t[buf_size];
    uint8_t* buf_chk = new uint8_t[buf_size];
    _set_circling_data(buf, buf_size);
    _check_circling_data(buf, buf_size);

    // Generate unaligned file
    {
        FileOps* ops = new FileOpsPosix();
        FileHandle* fhandle;

        s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
        CHK_OK(s);
        CHK_OK(fhandle->isOpened());

        s = ops->pwrite(fhandle, buf, buf_size, 0);
        CHK_OK(s);

        s = ops->close(fhandle);
        CHK_OK(s);
        CHK_NOT(fhandle->isOpened());

        delete fhandle;
        delete ops;
    }

    // Open unaligned file with direct-io
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);
    CHK_OK(fhandle->isOpened());

    // Append into file with random size
    size_t buf_pos = 0;
    while (buf_pos < buf_size) {
        size_t slice = randint(1, buf_size - buf_pos);
        s = ops->append(fhandle, buf + buf_pos, slice);
        CHK_OK(s);
        buf_pos += slice;
    }
    s = ops->flush(fhandle);
    CHK_OK(s);

    // Check file size
    cs_off_t file_size = ops->eof(fhandle);
    CHK_EQ(_file_size(2 * buf_size), file_size);

    // Read all
    s = ops->pread(fhandle, buf_chk, buf_size, 0);
    CHK_OK(s);
    _check_circling_data(buf_chk, buf_size);
    s = ops->pread(fhandle, buf_chk, buf_size, buf_size);
    CHK_OK(s);
    _check_circling_data(buf_chk, buf_size);

    s = ops->close(fhandle);
    CHK_OK(s);
    CHK_NOT(fhandle->isOpened());

    delete[] buf;
    delete[] buf_chk;
    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int fsync_test()
{
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    char buf[256];

    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->append(fhandle, buf, 256);
    CHK_OK(s);

    s = ops->flush(fhandle);
    CHK_OK(s);

    s = ops->fsync(fhandle);
    CHK_OK(s);

    s = ops->close(fhandle);
    CHK_OK(s);

    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int exist_test() {
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);
    std::string not_exist_filename = "not_exist";

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    char buf[256];

    // Open, write something, close.
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->append(fhandle, buf, 256);
    CHK_OK(s);

    s = ops->close(fhandle);
    CHK_OK(s);

    // Should exist.
    CHK_OK(ops->exist(filename.c_str()));

    // Should not exist.
    CHK_NOT(ops->exist(not_exist_filename.c_str()));

    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int remove_test() {
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    char buf[256];

    // Open, write something, close.
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->append(fhandle, buf, 256);
    CHK_OK(s);

    s = ops->close(fhandle);
    CHK_OK(s);

    // Remove it.
    s = ops->remove(filename.c_str());
    CHK_OK(s);

    // Should not exist.
    CHK_NOT(ops->exist(filename.c_str()));

    delete fhandle;
    delete ops;

    return 0;
}

int mkdir_test() {
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string dirname = TestSuite::getTestFileName(prefix);
    std::string filename = dirname + "/file";

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;
    char buf[256];

    // Directory should not exist
    CHK_NOT(ops->exist(dirname.c_str()));

    // Make a directory
    s = ops->mkdir(dirname.c_str());
    CHK_OK(s);

    // Open, write something, close.
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->append(fhandle, buf, 256);
    CHK_OK(s);

    s = ops->close(fhandle);
    CHK_OK(s);

    // Remove file.
    s = ops->remove(filename.c_str());
    CHK_OK(s);

    // Remove directory.
    s = ops->remove(dirname.c_str());
    CHK_OK(s);

    // Directory should not exist
    CHK_NOT(ops->exist(dirname.c_str()));

    delete fhandle;
    delete ops;

    return 0;
}

int get_ops_test() {
    const std::string prefix = "file_directio_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps* ops = new FileOpsDirectIO(nullptr);
    FileHandle* fhandle;

    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    // Return value of ops() should be same to the original ops.
    CHK_EQ(ops, fhandle->ops());

    // Call close() using ops() member function,
    // instead of ops pointer.
    s = fhandle->ops()->close(fhandle);
    CHK_OK(s);

    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int main(int argc, char** argv) {
    std::srand(std::time(0));
    TestSuite test(argc, argv);

    test.doTest("read write without open test", read_write_without_open_test);
    test.doTest("normal read write test", normal_read_write_test);
    test.doTest("unaligned read write test", unaligned_read_write_test);
    test.doTest("unaligned file read test", unaligned_file_read_test);
    test.doTest("unaligned file write test", unaligned_file_write_test);
    test.doTest("file fsync test", fsync_test);
    test.doTest("file exist test", exist_test);
    test.doTest("file remove test", remove_test);
    test.doTest("file mkdir test", mkdir_test);
    test.doTest("get ops test", get_ops_test);

    return 0;
}
