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
public class CommandCompleteParserNegativeTest {

  @Parameterized.Parameter(0)
  public String input;

  @Parameterized.Parameters(name = "input={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {"SELECT 0_0 42"},
        {"SELECT 42 0_0"},
        {"SELECT 0_0 0_0"},
    });
  }

  @Test
  public void run() throws PSQLException {
    CommandCompleteParser parser = new CommandCompleteParser();
    try {
      parser.parse(input);
      Assert.fail("CommandCompleteParser should throw NumberFormatException for " + input);
    } catch (PSQLException e) {
      Throwable cause = e.getCause();
      if (cause == null) {
        throw e;
      }
      if (!(cause instanceof NumberFormatException)) {
        throw e;
      }
      // NumerFormatException is expected
    }
  }
}
