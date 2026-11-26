/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
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
