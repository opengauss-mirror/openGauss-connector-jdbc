/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class OidToStringTest {
    @Parameterized.Parameter(0)
    public int value;
    @Parameterized.Parameter(1)
    public String expected;

    @Parameterized.Parameters(name = "expected={1}, value={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(OidValueOfTest.types);
    }

    @Test
    public void run() {
        Assert.assertEquals(expected, Oid.toString(value));
    }
}
