/*
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

package org.postgresql.util;

import java.io.Writer;

import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class WriterHandler extends Handler {

  private boolean doneHeader;

  private Writer writer;

  public WriterHandler(Writer inWriter) {
    super();
    setLevel(Level.INFO);
    setFilter(null);
    setFormatter(new SimpleFormatter());
    writer = inWriter;
  }

  public WriterHandler(Writer inWriter, Formatter formatter) {
    super();
    setLevel(Level.INFO);
    setFilter(null);
    setFormatter(formatter);
    writer = inWriter;

  }

  public synchronized void publish(final LogRecord record) {
    if (!isLoggable(record)) {
      return;
    }
    String msg;
    try {
      msg = getFormatter().format(record);
    } catch (Exception ex) {
      // We don't want to throw an exception here, but we
      // report the exception to any registered ErrorManager.
      reportError(null, ex, ErrorManager.FORMAT_FAILURE);
      return;
    }

    try {
      if (!doneHeader) {
        writer.write(getFormatter().getHead(this));
        doneHeader = true;
      }
      writer.write(msg);
    } catch (Exception ex) {
      // We don't want to throw an exception here, but we
      // report the exception to any registered ErrorManager.
      reportError(null, ex, ErrorManager.WRITE_FAILURE);
    }
  }

  public boolean isLoggable(final LogRecord record) {
    if (writer == null || record == null) {
      return false;
    }
    return super.isLoggable(record);
  }

  public synchronized void flush() {
    if (writer != null) {
      try {
        writer.flush();
      } catch (Exception ex) {
        // We don't want to throw an exception here, but we
        // report the exception to any registered ErrorManager.
        reportError(null, ex, ErrorManager.FLUSH_FAILURE);
      }
    }
  }

  private synchronized void flushAndClose() throws SecurityException {

    if (writer != null) {
      try {
        if (!doneHeader) {
          writer.write(getFormatter().getHead(this));
          doneHeader = true;
        }
        writer.write(getFormatter().getTail(this));
        writer.flush();
        writer.close();
      } catch (Exception ex) {
        // We don't want to throw an exception here, but we
        // report the exception to any registered ErrorManager.
        reportError(null, ex, ErrorManager.CLOSE_FAILURE);
      }
      writer = null;

    }
  }

  public synchronized void close() throws SecurityException {
    flushAndClose();
  }
}
