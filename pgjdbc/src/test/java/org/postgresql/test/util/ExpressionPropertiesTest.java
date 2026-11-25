/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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

package org.postgresql.test.util;

import org.postgresql.util.ExpressionProperties;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ExpressionPropertiesTest {
  @Test
  public void simpleReplace() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("server", "app1");
    p.put("file", "pgjdbc_${server}.txt");
    Assert.assertEquals("${server} should be replaced", "pgjdbc_app1.txt", p.getProperty("file"));
  }

  @Test
  public void replacementMissing() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("file", "pgjdbc_${server}.txt");
    Assert.assertEquals("${server} should be kept as is as there is no replacement",
        "pgjdbc_${server}.txt", p.getProperty("file"));
  }

  @Test
  public void multipleReplacements() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("server", "app1");
    p.put("file", "${server}${server}${server}${server}${server}");
    Assert.assertEquals("All the ${server} entries should be replaced",
        "app1app1app1app1app1", p.getProperty("file"));
  }

  @Test
  public void multipleParentProperties() {
    Properties p1 = new Properties();
    p1.setProperty("server", "app1_${app.type}");
    Properties p2 = new Properties();
    p2.setProperty("app.type", "production");

    ExpressionProperties p = new ExpressionProperties(p1, p2);
    p.put("file", "pgjdbc_${server}.txt");

    Assert.assertEquals("All the ${...} entries should be replaced",
        "pgjdbc_app1_production.txt", p.getProperty("file"));
  }

  @Test
  public void rawValue() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("server", "app1");
    p.put("file", "${server}${server}${server}${server}${server}");
    Assert.assertEquals("No replacements in raw value expected",
        "${server}${server}${server}${server}${server}", p.getRawPropertyValue("file"));
  }
}
