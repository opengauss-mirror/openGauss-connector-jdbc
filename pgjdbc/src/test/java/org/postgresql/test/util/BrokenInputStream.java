/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
