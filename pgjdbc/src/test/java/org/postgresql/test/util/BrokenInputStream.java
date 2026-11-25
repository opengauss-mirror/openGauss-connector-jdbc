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

import java.io.IOException;
import java.io.InputStream;

public class BrokenInputStream extends InputStream {

  private InputStream _is;
  private long _numRead;
  private long _breakOn;

  public BrokenInputStream(InputStream is, long breakOn) {
    _is = is;
    _breakOn = breakOn;
    _numRead = 0;
  }

  public int read() throws IOException {
    if (_breakOn > _numRead++) {
      throw new IOException("I was told to break on " + _breakOn);
    }

    return _is.read();
  }
}
