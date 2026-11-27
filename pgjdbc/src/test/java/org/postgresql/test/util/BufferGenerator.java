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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Random;

/**
 * Created by amozhenin on 30.09.2015.
 */
public class BufferGenerator {
  public static final int ROW_COUNT = 100000;

  public static void main(String[] args) throws Exception {
    Random random = new Random();
    random.setSeed(new Date().getTime());
    OutputStream out = null;
    try {
      File outFile = new File("target", "buffer.txt");
      outFile.getParentFile().mkdir();
      out = new BufferedOutputStream(new FileOutputStream(outFile));
      for (long i = 0; i < ROW_COUNT; i++) {
        StringBuffer line = new StringBuffer();
        line.append("VERY_LONG_LINE_TO_ASSIST_IN_DETECTION_OF_ISSUE_366_#_").append(i).append('\t');
        int letter = random.nextInt(26); // don't really care about uniformity for a test
        char character = (char) ((int) 'A' + letter); // black magic
        line.append("VERY_LONG_STRING_TO_REPRODUCE_ISSUE_366_").append(character).append(character);
        line.append(character).append('\t').append(random.nextDouble()).append('\n');
        out.write(line.toString().getBytes("UTF-8"));
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

}
