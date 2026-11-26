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
