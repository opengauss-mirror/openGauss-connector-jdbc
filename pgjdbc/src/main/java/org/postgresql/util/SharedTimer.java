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

package org.postgresql.util;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedTimer {
  // Incremented for each Timer created, this allows each to have a unique Timer name
  private static final AtomicInteger timerCount = new AtomicInteger(0);

  private static Log LOGGER = Logger.getLogger(SharedTimer.class.getName());
  private volatile Timer timer = null;
  private final AtomicInteger refCount = new AtomicInteger(0);

  public SharedTimer() {
  }

  public int getRefCount() {
    return refCount.get();
  }

  public synchronized Timer getTimer() {
    if (timer == null) {
      int index = timerCount.incrementAndGet();

      /*
       Temporarily switch contextClassLoader to the one that loaded this driver to avoid TimerThread preventing current
       contextClassLoader - which may be the ClassLoader of a web application - from being GC:ed.
       */
      final ClassLoader prevContextCL = Thread.currentThread().getContextClassLoader();
      try {
        /*
         Scheduled tasks whould not need to use .getContextClassLoader, so we just reset it to null
         */
        Thread.currentThread().setContextClassLoader(null);

        timer = new Timer("PostgreSQL-JDBC-SharedTimer-" + index, true);
      } finally {
        Thread.currentThread().setContextClassLoader(prevContextCL);
      }
    }
    refCount.incrementAndGet();
    return timer;
  }

  public synchronized void releaseTimer() {
    int count = refCount.decrementAndGet();
    if (count > 0) {
      // There are outstanding references to the timer so do nothing
      LOGGER.trace("Outstanding references still exist so not closing shared Timer");
    } else if (count == 0) {
      // This is the last usage of the Timer so cancel it so it's resources can be release.
      LOGGER.trace("No outstanding references to shared Timer, will cancel and close it");
      if (timer != null) {
        timer.cancel();
        timer = null;
      }
    } else {
      // Should not get here under normal circumstance, probably a bug in app code.
      LOGGER.warn(
          "releaseTimer() called too many times; there is probably a bug in the calling code");
      refCount.set(0);
    }
  }
}
