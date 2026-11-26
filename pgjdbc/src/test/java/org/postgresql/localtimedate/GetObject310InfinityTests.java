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

package org.postgresql.localtimedate;
import org.postgresql.core.ServerVersion;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.test.jdbc2.BaseTest4;
import org.postgresql.test.jdbc2.BaseTest4PG;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class GetObject310InfinityTests extends BaseTest4PG {
  private final String expression;
  private final String pgType;
  private final Class<?> klass;
  private final Object expectedValue;

  public GetObject310InfinityTests(BinaryMode binaryMode, String expression,
      String pgType, Class<?> klass, Object expectedValue) {
    setBinaryMode(binaryMode);
    this.expression = expression;
    this.pgType = pgType;
    this.klass = klass;
    this.expectedValue = expectedValue;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeTrue("PostgreSQL 8.3 does not support 'infinity' for 'date'",
        !"date".equals(pgType) || TestUtil.haveMinimumServerVersion(con, ServerVersion.v8_4));
  }

  @Parameterized.Parameters(name = "binary = {0}, expr = {1}, pgType = {2}, klass = {3}")
  public static Iterable<Object[]> data() throws IllegalAccessException {
    Collection<Object[]> ids = new ArrayList<Object[]>();
    for (BinaryMode binaryMode : BinaryMode.values()) {
      for (String expression : Arrays.asList("-infinity", "infinity")) {
        for (String pgType : Arrays.asList("date", "timestamp",
            "timestamp with time zone")) {
          for (Class<?> klass : Arrays.asList(LocalDate.class, LocalDateTime.class,
              OffsetDateTime.class)) {
            if (klass.equals(LocalDate.class) && !pgType.equals("date")) {
              continue;
            }
            if (klass.equals(LocalDateTime.class) && !pgType.startsWith("timestamp")) {
              continue;
            }
            if (klass.equals(OffsetDateTime.class) && !pgType.startsWith("timestamp")) {
              continue;
            }
            if (klass.equals(LocalDateTime.class) && pgType.equals("timestamp with time zone")) {
              // org.postgresql.util.PSQLException: Cannot convert the column of type TIMESTAMPTZ to requested type timestamp.
              continue;
            }
            Field field = null;
            try {
              field = klass.getField(expression.startsWith("-") ? "MIN" : "MAX");
            } catch (NoSuchFieldException e) {
              throw new IllegalStateException("No min/max field in " + klass, e);
            }
            Object expected = field.get(null);
            ids.add(new Object[]{binaryMode, expression, pgType, klass, expected});
          }
        }
      }
    }
    return ids;
  }

  @Test
  public void test() throws SQLException {
    PreparedStatement stmt = con.prepareStatement("select '" + expression + "'::" + pgType);
    ResultSet rs = stmt.executeQuery();
    rs.next();
    Object res = rs.getObject(1, klass);
    Assert.assertEquals(expectedValue, res);
  }
}
