/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
