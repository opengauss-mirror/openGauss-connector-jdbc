/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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
