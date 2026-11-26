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

package org.postgresql.core;

import org.postgresql.util.PSQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class CommandCompleteParserTest {

  @Parameterized.Parameter(0)
  public String input;
  @Parameterized.Parameter(1)
  public long oid;
  @Parameterized.Parameter(2)
  public long rows;

  @Parameterized.Parameters(name = "input={0}, oid={1}, rows={2}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"SELECT 0", 0, 0},
        {"SELECT -42", 0, 0},
        {"SELECT", 0, 0},
        {"", 0, 0},
        {"A", 0, 0},
        {"SELECT 42", 0, 42},
        {"UPDATE 43 42", 43, 42},
        {"UPDATE 43 " + Long.MAX_VALUE, 43, Long.MAX_VALUE},
        {"UPDATE " + Long.MAX_VALUE + " " + Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE},
        {"UPDATE " + (Long.MAX_VALUE / 10) + " " + (Long.MAX_VALUE / 10), (Long.MAX_VALUE / 10),
            (Long.MAX_VALUE / 10)},
        {"UPDATE " + (Long.MAX_VALUE / 100) + " " + (Long.MAX_VALUE / 100), (Long.MAX_VALUE / 100),
            (Long.MAX_VALUE / 100)},
        {"CREATE TABLE " + (Long.MAX_VALUE / 100) + " " + (Long.MAX_VALUE / 100),
            (Long.MAX_VALUE / 100), (Long.MAX_VALUE / 100)},
        {"CREATE TABLE", 0, 0},
        {"CREATE OR DROP OR DELETE TABLE 42", 0, 42},
    });
  }

  @Test
  public void run() throws PSQLException {
    CommandCompleteParser expected = new CommandCompleteParser();
    CommandCompleteParser actual = new CommandCompleteParser();
    expected.set(oid, rows);
    actual.parse(input);
    Assert.assertEquals(input, expected, actual);
  }
}
