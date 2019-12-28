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

#include "test_common.h"

#include "internal_helper.h"

#include <libjungle/keyvalue.h>

#include <stdio.h>
#include <string.h>

using namespace jungle;

int sb_empty_test() {
    SizedBuf sb;

    CHK_EQ(0, sb.size);
    CHK_NULL(sb.data);

    return 0;
}

int sb_normal_test() {
    SizedBuf sb;
    char str[16];
    char str2[8];

    sb = SizedBuf(16, str);
    CHK_EQ(16, sb.size);
    CHK_NONNULL(sb.data);

    sb.set(8, str2);
    CHK_EQ(8, sb.size);
    CHK_EQ((void*)str2, sb.data);

    return 0;
}

int sb_clone_test() {
    SizedBuf sb;
    SizedBuf sb_clone(sb);

    CHK_EQ(sb.size, sb_clone.size);
    CHK_EQ(sb.data, sb_clone.data);

    return 0;
}

int sb_string_test() {
    SizedBuf sb;

    std::string str1 = "test_string";
    std::string str2 = "hello world";

    sb = SizedBuf(str1);
    CHK_EQ(str1.size(), sb.size);
    CHK_OK(!memcmp(str1.c_str(), sb.data, sb.size));

    sb.set(str2);
    CHK_EQ(str2.size(), sb.size);
    CHK_OK(!memcmp(str2.c_str(), sb.data, sb.size));

    std::string str3 = sb.toString();
    CHK_EQ(str2, str3);

    return 0;
}

int sb_equal_test() {
    std::string str1 = "test_string";
    std::string str2 = "test_string";
    std::string str3 = "!!test_string";
    SizedBuf sb1(str1), sb2(str2), sb3(str3);

    CHK_OK((void*)sb1.data != (void*)sb2.data);
    CHK_OK((void*)sb2.data != (void*)sb3.data);
    CHK_OK((void*)sb3.data != (void*)sb1.data);

    CHK_OK(sb1 == sb2);
    CHK_OK(sb1 != sb3);
    CHK_OK(sb2 != sb3);

    return 0;
}

int sb_equal_empty_test() {
    std::string str3 = "!!test_string";
    SizedBuf sb1, sb2, sb3(str3);

    CHK_OK((void*)sb2.data != (void*)sb3.data);
    CHK_OK((void*)sb3.data != (void*)sb1.data);

    CHK_OK(sb1 == sb2);
    CHK_OK(sb1 != sb3);
    CHK_OK(sb2 != sb3);

    return 0;
}

int sb_clear_test()
{
    SizedBuf sb;

    std::string str = "test_string";
    sb = SizedBuf(str);
    CHK_EQ(str.size(), sb.size);
    CHK_OK(!memcmp(str.c_str(), sb.data, sb.size));

    sb.clear();
    CHK_EQ(0, sb.size);
    CHK_NULL(sb.data);

    return 0;
}

int sb_free_test()
{
    std::string str = "test_string";
    SizedBuf sb(str);
    // Should fail.
    CHK_NOT( sb.free() );

    char str_raw[] = "test";
    SizedBuf sb2(4, str_raw);
    // Should fail.
    CHK_NOT( sb.free() );

    SizedBuf sb3(4);
    CHK_TRUE( sb3.free() );

    char* str_raw2 = (char*)malloc(4);
    SizedBuf sb4(4, str_raw2);
    CHK_NOT( sb4.free() );

    sb4.setNeedToFree();
    CHK_TRUE( sb4.free() );

    char* str_raw3 = new char[4];
    SizedBuf sb5(4, str_raw3);
    CHK_NOT( sb5.free() );

    sb5.setNeedToDelete();
    CHK_TRUE( sb5.free() );

    return 0;
}

int sb_compare_test() {
    SizedBuf aa("aa");
    SizedBuf aaa("aaa");
    SizedBuf abc("abc");

    CHK_OK(aa == aa);
    CHK_NOT(aa == aaa);
    CHK_NOT(aa == abc);

    CHK_OK(aa < aaa);
    CHK_OK(aa < abc);
    CHK_OK(aaa < abc);
    CHK_NOT(aa < aa);

    CHK_NOT(aaa < aa);
    CHK_NOT(abc < aa);
    CHK_NOT(abc < aaa);

    CHK_OK(aa <= aaa);
    CHK_OK(aa <= abc);
    CHK_OK(aaa <= abc);
    CHK_OK(aa <= aa);

    CHK_NOT(aaa <= aa);
    CHK_NOT(abc <= aa);
    CHK_NOT(abc <= aaa);

    CHK_OK(aaa > aa);
    CHK_OK(abc > aa);
    CHK_OK(abc > aaa);
    CHK_NOT(aa > aa);

    CHK_NOT(aa > aaa);
    CHK_NOT(aa > abc);
    CHK_NOT(aaa > abc);

    CHK_OK(aaa >= aa);
    CHK_OK(abc >= aa);
    CHK_OK(abc >= aaa);
    CHK_OK(aa >= aa);

    CHK_NOT(aa >= aaa);
    CHK_NOT(aa >= abc);
    CHK_NOT(aaa >= abc);

    return 0;
}

int sb_compare_empty_test() {
    SizedBuf aa("aa");
    SizedBuf empty;

    CHK_OK(empty == empty);
    CHK_OK(empty <= empty);
    CHK_OK(empty >= empty);

    CHK_OK(empty < aa);
    CHK_OK(empty <= aa);
    CHK_OK(aa > empty);
    CHK_OK(aa >= empty);

    CHK_NOT(empty > aa);
    CHK_NOT(empty >= aa);
    CHK_NOT(aa < empty);
    CHK_NOT(aa <= empty);

    return 0;
}

int sb_cmp_func_test() {
    SizedBuf aa("aa");
    SizedBuf aaa("aaa");
    SizedBuf abc("abc");
    SizedBuf empty;

    CHK_EQ(0, SizedBuf::cmp(aa, aa));
    CHK_EQ(0, SizedBuf::cmp(empty, empty));

    CHK_SM(SizedBuf::cmp(empty, aa), 0);
    CHK_SM(SizedBuf::cmp(aa, aaa), 0);
    CHK_SM(SizedBuf::cmp(aaa, abc), 0);
    CHK_SM(SizedBuf::cmp(aa, abc), 0);

    CHK_GT(SizedBuf::cmp(aa, empty), 0);
    CHK_GT(SizedBuf::cmp(aaa, aa), 0);
    CHK_GT(SizedBuf::cmp(abc, aaa), 0);
    CHK_GT(SizedBuf::cmp(abc, aa), 0);

    return 0;
}

int rw_serializer_test() {
    SizedBuf buf(128);
    SizedBuf::Holder h_buf(buf);

    SizedBuf aaa("aaa");
    RwSerializer ww(buf);

    CHK_Z( ww.putSb(aaa) );
    CHK_Z( ww.putU8(1) );
    CHK_Z( ww.putU16(2) );
    CHK_Z( ww.putU32(3) );
    CHK_Z( ww.putU64(4) );

    RwSerializer rr(buf);
    SizedBuf tmp;
    SizedBuf::Holder h_tmp(tmp);
    CHK_Z( rr.getSb(tmp) );
    CHK_EQ(aaa, tmp);
    CHK_EQ(1, rr.getU8());
    CHK_EQ(2, rr.getU16());
    CHK_EQ(3, rr.getU32());
    CHK_EQ(4, rr.getU64());

    return 0;
}

int resizable_rw_serializer_test() {
    SizedBuf buf(4);
    SizedBuf::Holder h_buf(buf);

    SizedBuf aaa("aaa");
    RwSerializer ww(&buf);

    CHK_Z( ww.putSb(aaa) );
    CHK_Z( ww.putU8(1) );
    CHK_Z( ww.putU16(2) );
    CHK_Z( ww.putU32(3) );
    CHK_Z( ww.putU64(4) );

    RwSerializer rr(buf);
    SizedBuf tmp;
    SizedBuf::Holder h_tmp(tmp);
    CHK_Z( rr.getSb(tmp) );
    CHK_EQ(aaa, tmp);
    CHK_EQ(1, rr.getU8());
    CHK_EQ(2, rr.getU16());
    CHK_EQ(3, rr.getU32());
    CHK_EQ(4, rr.getU64());

    return 0;
}

int empty_rw_serializer_test() {
    SizedBuf buf;
    SizedBuf::Holder h_buf(buf);

    SizedBuf aaa("hello world 1234567890");
    RwSerializer ww(&buf);

    CHK_Z( ww.putSb(aaa) );
    CHK_Z( ww.putU8(1) );
    CHK_Z( ww.putU16(2) );
    CHK_Z( ww.putU32(3) );
    CHK_Z( ww.putU64(4) );

    RwSerializer rr(buf);
    SizedBuf tmp;
    SizedBuf::Holder h_tmp(tmp); // dummy holder, should do nothing.
    CHK_Z( rr.getSb(tmp, false) );
    CHK_EQ(aaa, tmp);
    CHK_EQ(1, rr.getU8());
    CHK_EQ(2, rr.getU16());
    CHK_EQ(3, rr.getU32());
    CHK_EQ(4, rr.getU64());

    return 0;
}

int kv_empty_test()
{
    KV kv;
    SizedBuf sb_empty;

    CHK_EQ(sb_empty, kv.key);
    CHK_EQ(sb_empty, kv.value);

    return 0;
}

int main(int argc, char** argv) {
    TestSuite test(argc, argv);

    test.doTest("SizedBuf empty test", sb_empty_test);
    test.doTest("SizedBuf normal test", sb_normal_test);
    test.doTest("SizedBuf clone test", sb_clone_test);
    test.doTest("SizedBuf string test", sb_string_test);
    test.doTest("SizedBuf clear test", sb_clear_test);
    test.doTest("SizedBuf free test", sb_free_test);
    test.doTest("SizedBuf equal test", sb_equal_test);
    test.doTest("SizedBuf equal empty test", sb_equal_empty_test);
    test.doTest("SizedBuf comparison test", sb_compare_test);
    test.doTest("SizedBuf comparison with empty test", sb_compare_empty_test);
    test.doTest("SizedBuf cmp function test", sb_cmp_func_test);

    test.doTest("RW Serializer test", rw_serializer_test);
    test.doTest("Resizable RW Serializer test", resizable_rw_serializer_test);
    test.doTest("Empty RW Serializer test", empty_rw_serializer_test);

    test.doTest("KV empty test", kv_empty_test);

    return 0;
}
