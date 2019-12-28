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

#include <stdio.h>
#include <string.h>

#include "fileops_posix.h"

#include "test_common.h"

using namespace jungle;

int read_write_without_open_test()
{
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    FileOps *ops = new FileOpsPosix();
    FileHandle* fhandle = nullptr;

    Status s;
    char buf[256];

    s = ops->pread(fhandle, buf, 256, 0);
    CHK_NOT(s);

    memset(buf, 'x', 256);
    s = ops->pwrite(fhandle, buf, 256, 0);
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
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps* ops = new FileOpsPosix();
    FileHandle* fhandle;
    char buf[256];

    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);
    CHK_OK(fhandle->isOpened());

    memset(buf, 'x', 256);
    s = ops->pwrite(fhandle, buf, 256, 0);
    CHK_OK(s);

    s = ops->flush(fhandle);
    CHK_OK(s);

    memset(buf, 'a', 256);
    s = ops->pread(fhandle,buf, 256, 0);
    CHK_OK(s);

    char buf_chk[256];
    memset(buf_chk, 'x', 256);
    CHK_EQ(0, memcmp(buf_chk, buf, 256));

    s = ops->ftruncate(fhandle, 111);
    CHK_OK(s);
    cs_off_t file_size = ops->eof(fhandle);
    CHK_EQ(111, file_size);

    s = ops->close(fhandle);
    CHK_OK(s);
    CHK_NOT(fhandle->isOpened());

    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int eof_test()
{
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps *ops = new FileOpsPosix();
    FileHandle* fhandle;
    char buf[256];

    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->pwrite(fhandle, buf, 256, 0);
    CHK_OK(s);

    s = ops->flush(fhandle);
    CHK_OK(s);

    cs_off_t offset = ops->eof(fhandle);
    CHK_EQ(256, offset);

    s = ops->close(fhandle);
    CHK_OK(s);

    delete fhandle;
    delete ops;
    TestSuite::clearTestFile(prefix);

    return 0;
}

int fsync_test()
{
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps *ops = new FileOpsPosix();
    FileHandle* fhandle;
    char buf[256];

    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->pwrite(fhandle, buf, 256, 0);
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
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);
    std::string not_exist_filename = "not_exist";

    Status s;
    FileOps *ops = new FileOpsPosix();
    FileHandle* fhandle;
    char buf[256];

    // Open, write something, close.
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->pwrite(fhandle, buf, 256, 0);
    CHK_OK(s);

    s = ops->flush(fhandle);
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
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps *ops = new FileOpsPosix();
    FileHandle* fhandle;
    char buf[256];

    // Open, write something, close.
    s = ops->open(&fhandle, filename.c_str(), FileOps::NORMAL);
    CHK_OK(s);

    memset(buf, 'x', 256);
    s = ops->pwrite(fhandle, buf, 256, 0);
    CHK_OK(s);

    s = ops->flush(fhandle);
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
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string dirname = TestSuite::getTestFileName(prefix);
    std::string filename = dirname + "/file";

    Status s;
    FileOps *ops = new FileOpsPosix();
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
    s = ops->pwrite(fhandle, buf, 256, 0);
    CHK_OK(s);

    s = ops->flush(fhandle);
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
    const std::string prefix = "file_posix_test";
    TestSuite::clearTestFile(prefix);
    std::string filename = TestSuite::getTestFileName(prefix);

    Status s;
    FileOps *ops = new FileOpsPosix();
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
    TestSuite test(argc, argv);

    test.doTest("read write without open test", read_write_without_open_test);
    test.doTest("normal read write test", normal_read_write_test);
    test.doTest("file EOF test", eof_test);
    test.doTest("file fsync test", fsync_test);
    test.doTest("file exist test", exist_test);
    test.doTest("file remove test", remove_test);
    test.doTest("file mkdir test", mkdir_test);
    test.doTest("get ops test", get_ops_test);

    return 0;
}
