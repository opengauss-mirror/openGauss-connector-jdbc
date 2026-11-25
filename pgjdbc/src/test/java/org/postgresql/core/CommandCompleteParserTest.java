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
