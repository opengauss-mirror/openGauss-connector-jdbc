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
