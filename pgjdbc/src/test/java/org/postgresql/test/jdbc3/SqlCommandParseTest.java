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

package org.postgresql.test.jdbc3;

import static org.junit.Assert.assertEquals;

import org.postgresql.core.NativeQuery;
import org.postgresql.core.Parser;
import org.postgresql.core.SqlCommandType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class SqlCommandParseTest {
  @Parameterized.Parameter(0)
  public SqlCommandType type;
  @Parameterized.Parameter(1)
  public String sql;

  @Parameterized.Parameters(name = "expected={0}, sql={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {SqlCommandType.INSERT, "insert/**/ into table(select) values(1)"},
        {SqlCommandType.SELECT, "select'abc'/**/ as insert"},
        {SqlCommandType.INSERT, "INSERT/*fool /*nest comments -- parser*/*/ INTO genkeys (b,c) VALUES ('a', 2), ('b', 4) SELECT"},
        {SqlCommandType.INSERT, "with update as (update foo set (a=?,b=?,c=?)) insert into table(select) values(1)"},
        {SqlCommandType.INSERT, "with update as (update foo set (a=?,b=?,c=?)) insert into table(select) select * from update"},
        {SqlCommandType.INSERT, "with update as (update foo set (a=?,b=?,c=?)) insert/**/ into table(select) values(1)"},
        {SqlCommandType.INSERT, "with update as (update foo set (a=?,b=?,c=?)) insert /**/ into table(select) values(1)"},
        {SqlCommandType.SELECT, "with update as (update foo set (a=?,b=?,c=?)) insert --\nas () select 1"},
        {SqlCommandType.SELECT, "with update as (update foo set (a=?,b=?,c=?)) insert --\n/* dfhg \n*/\nas () select 1"},
        {SqlCommandType.SELECT, "WITH x as (INSERT INTO genkeys(a,b,c) VALUES (1, 'a', 2) returning  returning a, b) select * from x"},
        // No idea if it works, but it should be parsed as WITH
        {SqlCommandType.WITH, "with update as (update foo set (a=?,b=?,c=?)) copy from stdin"},
    });
  }

  @Test
  public void run() throws SQLException {
    List<NativeQuery> queries;
    queries = Parser.parseJdbcSql(sql, true, true, false, true, true);
    NativeQuery query = queries.get(0);
    assertEquals(sql, type, query.command.getType());
  }
}
