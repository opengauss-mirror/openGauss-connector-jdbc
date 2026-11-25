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
