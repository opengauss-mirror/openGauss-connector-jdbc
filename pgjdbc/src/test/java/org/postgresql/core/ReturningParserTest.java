/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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

package org.postgresql.core;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ReturningParserTest {
  private final String columnName;
  private final String returning;
  private final String prefix;
  private final String suffix;

  public ReturningParserTest(String columnName, String returning, String prefix, String suffix) {
    this.columnName = columnName;
    this.returning = returning;
    this.prefix = prefix;
    this.suffix = suffix;
  }


  @Parameterized.Parameters(name = "columnName={2} {0} {3}, returning={2} {1} {3}")
  public static Iterable<Object[]> data() {
    Collection<Object[]> ids = new ArrayList<Object[]>();

    String[] delimiters = {"", "_", "3", "*", " "};

    for (String columnName : new String[]{"returning", "returningreturning"}) {
      for (String prefix : delimiters) {
        for (String suffix : delimiters) {
          for (String returning : new String[]{"returning", "returningreturning"}) {
            ids.add(new Object[]{columnName, returning, prefix, suffix});
          }
        }
      }
    }
    return ids;
  }

  @Test
  public void test() throws SQLException {
    String query =
        "insert into\"prep\"(a, " + prefix + columnName + suffix + ")values(1,2)" + prefix
            + returning + suffix;
    List<NativeQuery> qry = Parser.parseJdbcSql(query, true, true, true, true, true);
    boolean returningKeywordPresent = qry.get(0).command.isReturningKeywordPresent();

    boolean expectedReturning = this.returning.equalsIgnoreCase("returning")
        && (prefix.isEmpty() || !Character.isJavaIdentifierStart(prefix.charAt(0)))
        && (suffix.isEmpty() || !Character.isJavaIdentifierPart(suffix.charAt(0)));
    if (expectedReturning != returningKeywordPresent) {
      Assert.assertEquals("Wrong <returning_clause> detected in SQL " + query,
          expectedReturning,
          returningKeywordPresent);
    }
  }

}
