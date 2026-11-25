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

package org.postgresql.largeobject;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * This implements a basic output stream that writes to a LargeObject.
 */
public class BlobOutputStream extends OutputStream {
  /**
   * The parent LargeObject.
   */
  private LargeObject lo;

  /**
   * Buffer.
   */
  private byte[] buf;

  /**
   * Size of the buffer (default 1K).
   */
  private int bsize;

  /**
   * Position within the buffer.
   */
  private int bpos;

  /**
   * Create an OutputStream to a large object.
   *
   * @param lo LargeObject
   */
  public BlobOutputStream(LargeObject lo) {
    this(lo, 1024);
  }

  /**
   * Create an OutputStream to a large object.
   *
   * @param lo LargeObject
   * @param bsize The size of the buffer used to improve performance
   */
  public BlobOutputStream(LargeObject lo, int bsize) {
    this.lo = lo;
    this.bsize = bsize;
    buf = new byte[bsize];
    bpos = 0;
  }

  public void write(int b) throws java.io.IOException {
    checkClosed();
    try {
      if (bpos >= bsize) {
        lo.write(buf);
        bpos = 0;
      }
      buf[bpos++] = (byte) b;
    } catch (SQLException se) {
      throw new IOException(se.toString());
    }
  }

  public void write(byte[] buf, int off, int len) throws java.io.IOException {
    checkClosed();
    try {
      // If we have any internally buffered data, send it first
      if (bpos > 0) {
        flush();
      }

      if (off == 0 && len == buf.length) {
        lo.write(buf); // save a buffer creation and copy since full buffer written
      } else {
        lo.write(buf, off, len);
      }
    } catch (SQLException se) {
      throw new IOException(se.toString());
    }
  }


  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. The general
   * contract of <code>flush</code> is that calling it is an indication that, if any bytes
   * previously written have been buffered by the implementation of the output stream, such bytes
   * should immediately be written to their intended destination.
   *
   * @throws IOException if an I/O error occurs.
   */
  public void flush() throws IOException {
    checkClosed();
    try {
      if (bpos > 0) {
        lo.write(buf, 0, bpos);
      }
      bpos = 0;
    } catch (SQLException se) {
      throw new IOException(se.toString());
    }
  }

  public void close() throws IOException {
    if (lo != null) {
      try {
        flush();
        lo.close();
        lo = null;
      } catch (SQLException se) {
        throw new IOException(se.toString());
      }
    }
  }

  private void checkClosed() throws IOException {
    if (lo == null) {
      throw new IOException("BlobOutputStream is closed");
    }
  }
}
