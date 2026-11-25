/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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
import org.postgresql.ssl.jdbc4.LibPQFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class LibPQFactoryHostNameTest {

  private final String hostname;
  private final String pattern;
  private final boolean expected;

  public LibPQFactoryHostNameTest(String hostname, String pattern, boolean expected) {
    this.hostname = hostname;
    this.pattern = pattern;
    this.expected = expected;
  }

  @Parameterized.Parameters(name = "host={0}, pattern={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"host.com", "pattern.com", false},
        {"host.com", ".pattern.com", false},
        {"host.com", "*.pattern.com", false},
        {"host.com", "*.host.com", false},
        {"a.com", "*.host.com", false},
        {".a.com", "*.host.com", false},
        {"longhostname.com", "*.com", true},
        {"longhostname.ru", "*.com", false},
        {"host.com", "host.com", true},
        {"sub.host.com", "host.com", false},
        {"sub.host.com", "sub.host.com", true},
        {"sub.host.com", "*.host.com", true},
        {"Sub.host.com", "sub.host.com", true},
        {"sub.host.com", "Sub.host.com", true},
        {"sub.host.com", "*.hoSt.com", true},
        {"*.host.com", "host.com", false},
        {"sub.sub.host.com", "*.host.com", false}, // Wildcard should cover just one level
        {"com", "*", false}, // Wildcard should have al least one dot
    });
  }

  @Test
  public void checkPattern() throws Exception {
    Assert.assertEquals(hostname + ", pattern: " + pattern,
        expected, LibPQFactory.verifyHostName(hostname, pattern));

    Assert.assertEquals(hostname + ", pattern: " + pattern,
        expected, PGjdbcHostnameVerifier.INSTANCE.verifyHostName(hostname, pattern));
  }
}
