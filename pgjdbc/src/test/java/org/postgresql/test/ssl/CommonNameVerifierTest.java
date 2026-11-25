/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.test.ssl;

import org.postgresql.ssl.PGjdbcHostnameVerifier;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class CommonNameVerifierTest {

  private final String a;
  private final String b;
  private final int expected;

  public CommonNameVerifierTest(String a, String b, int expected) {
    this.a = a;
    this.b = b;
    this.expected = expected;
  }

  @Parameterized.Parameters(name = "a={0}, b={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"com", "host.com", -1},
        {"*.com", "host.com", -1},
        {"*.com", "*.*.com", -1},
        {"**.com", "*.com", -1},
        {"a.com", "*.host.com", -1},
        {"host.com", "subhost.host.com", -1},
        {"host.com", "host.com", 0}
    });
  }

  @Test
  public void comparePatterns() throws Exception {
    Assert.assertEquals(a + " vs " + b,
        expected, PGjdbcHostnameVerifier.HOSTNAME_PATTERN_COMPARATOR.compare(a, b));

    Assert.assertEquals(b + " vs " + a,
        -expected, PGjdbcHostnameVerifier.HOSTNAME_PATTERN_COMPARATOR.compare(b, a));
  }
}
