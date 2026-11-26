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
