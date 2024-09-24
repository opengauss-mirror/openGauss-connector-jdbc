/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * test utf8 encoding
 *
 * @author zhangting
 * @since  2024-09-24
 */
@RunWith(Parameterized.class)
public class UTF8EncodingTest {
    private static final int STEP = 8 * 1024;

    /**
     * character encoding
     */
    @Parameterized.Parameter(0)
    public Encoding encoding;

    /**
     * test data
     */
    @Parameterized.Parameter(1)
    public String str;

    /**
     * test data
     */
    @Parameterized.Parameter(2)
    public String shortStr;

    /**
     * Construct data
     *
     * @return data
     */
    @Parameterized.Parameters(name = "string={2}, encoding={0}")
    public static Iterable<Object[]> data() {
        final StringBuilder reallyLongString = new StringBuilder(1024 * 1024);
        for (int i = 0; i < 185000; ++i) {
            reallyLongString.append(i);
        }

        final List<String> strs = new ArrayList<>(150);
        strs.add("short simple");
        strs.add("longer but still not really all that long");
        strs.add(reallyLongString.toString());

        // add multi-byte to end of a long string
        strs.add(reallyLongString.append('\u03C0').toString());
        strs.add(reallyLongString.delete((32 * 1024) + 5, reallyLongString.capacity() - 1).toString());

        // add high order char to end of mid length string
        strs.add(reallyLongString.append('\u00DC').toString());
        strs.add(reallyLongString.delete((16 * 1024) + 5, reallyLongString.capacity() - 1).toString());

        // add high order char to end of mid length string
        strs.add(reallyLongString.append('\u00DD').toString());
        strs.add("e\u00E4t \u03A3 \u03C0 \u798F, it is good");

        for (int i = 1; i < 0xd800; i += STEP) {
            int count = (i + STEP) > 0xd800 ? 0xd800 - i : STEP;
            char[] testChars = new char[count];
            for (int j = 0; j < count; ++j) {
                testChars[j] = (char) (i + j);
            }

            strs.add(new String(testChars));
        }

        for (int i = 0xe000; i < 0x10000; i += STEP) {
            int count = (i + STEP) > 0x10000 ? 0x10000 - i : STEP;
            char[] testChars = new char[count];
            for (int j = 0; j < count; ++j) {
                testChars[j] = (char) (i + j);
            }

            strs.add(new String(testChars));
        }

        for (int i = 0x10000; i < 0x110000; i += STEP) {
            int count = (i + STEP) > 0x110000 ? 0x110000 - i : STEP;
            char[] testChars = new char[count * 2];
            for (int j = 0; j < count; ++j) {
                testChars[j * 2] = (char) (0xd800 + ((i + j - 0x10000) >> 10));
                testChars[j * 2 + 1] = (char) (0xdc00 + ((i + j - 0x10000) & 0x3ff));
            }

            strs.add(new String(testChars));
        }

        final List<Object[]> data = new ArrayList<>(strs.size() * 2);
        for (String str : strs) {
            if (str != null && str.length() > 1000) {
                str = str.substring(0, 100) + "...(" + str.length() + " chars)";
            }
            data.add(new Object[] {Encoding.getDatabaseEncoding("UNICODE"), str, str});
        }
        return data;
    }

    @Test
    public void test() throws IOException {
        final byte[] encoded = encoding.encode(str);
        assertEquals(str, encoding.decode(encoded));
    }
}
