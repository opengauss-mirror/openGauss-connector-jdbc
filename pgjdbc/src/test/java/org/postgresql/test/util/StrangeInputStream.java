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

package org.postgresql.test.util;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * {@link InputStream} implementation that reads less data than is provided in the destination
 * array. This allows to stress test {@link org.postgresql.copy.CopyManager} or other consumers.
 */
public class StrangeInputStream extends FilterInputStream {
  private Random rand; // generator of fun events

  public StrangeInputStream(InputStream is) throws FileNotFoundException {
    super(is);
    rand = new Random();
    long seed = Long.getLong("StrangeInputStream.seed", System.currentTimeMillis());
    System.out
        .println("Using seed = " + seed + " for StrangeInputStream. Set -DStrangeInputStream.seed="
            + seed + " to reproduce the test");
    rand.setSeed(seed);
  }

  @Override
  public int read(byte[] b) throws IOException {
    int maxRead = rand.nextInt(b.length);
    return super.read(b, 0, maxRead);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int maxRead = rand.nextInt(len);
    return super.read(b, off, maxRead);
  }
}
