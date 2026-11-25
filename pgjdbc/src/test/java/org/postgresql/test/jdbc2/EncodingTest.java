/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.postgresql.core.Encoding;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Locale;

/**
 * Tests for the Encoding class.
 */
public class EncodingTest {

  @Test
  public void testCreation() throws Exception {
    Encoding encoding = Encoding.getDatabaseEncoding("UTF8");
    assertEquals("UTF", encoding.name().substring(0, 3).toUpperCase(Locale.US));
    encoding = Encoding.getDatabaseEncoding("SQL_ASCII");
    assertTrue(encoding.name().toUpperCase(Locale.US).contains("ASCII"));
    assertEquals("When encoding is unknown the default encoding should be used",
        Encoding.defaultEncoding(), Encoding.getDatabaseEncoding("UNKNOWN"));
  }

  @Test
  public void testTransformations() throws Exception {
    Encoding encoding = Encoding.getDatabaseEncoding("UTF8");
    assertTrue(encoding.hasAsciiNumbers());
    assertEquals("ab", encoding.decode(new byte[]{97, 98}));

    assertEquals(2, encoding.encode("ab").length);
    assertEquals(97, encoding.encode("a")[0]);
    assertEquals(98, encoding.encode("b")[0]);

    encoding = Encoding.defaultEncoding();
    assertEquals("a".getBytes()[0], encoding.encode("a")[0]);
    assertEquals(new String(new byte[]{97}), encoding.decode(new byte[]{97}));
  }

  @Test
  public void testReader() throws Exception {
    Encoding encoding = Encoding.getDatabaseEncoding("SQL_ASCII");
    InputStream stream = new ByteArrayInputStream(new byte[]{97, 98});
    Reader reader = encoding.getDecodingReader(stream);
    assertEquals(97, reader.read());
    assertEquals(98, reader.read());
    assertEquals(-1, reader.read());
  }

  @Test
  public void testEncodingWithNull() {
    try {
      Encoding encoding = Encoding.getDatabaseEncoding(null);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Null charset name");
    }
  }

  @Test
  public void testGetJVMEncoding() {
    Encoding encoding = Encoding.getJVMEncoding("UTF8");
    assertEquals(encoding.name(), "UTF-8");

    Encoding encodingGBK = Encoding.getJVMEncoding("GBK");
    assertEquals(encodingGBK.name(), "GBK");

    Encoding encodingWrong = Encoding.getJVMEncoding("encodingWrong");
    assertEquals(encodingWrong.name(), Charset.defaultCharset().name());
  }

  @Test
  public void testDefaultEncoding() {
    Charset.defaultCharset();
    assertEquals(Encoding.defaultEncoding().name(), Charset.defaultCharset().name());
  }
}
