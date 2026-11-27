/*
 * Copyright (c) 2019, PostgreSQL Global Development Group
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

package org.postgresql.localtimedate;
import static org.junit.Assert.assertEquals;

import org.postgresql.core.Provider;
import org.postgresql.jdbc.TimestampUtils;

import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalTime;
import java.util.TimeZone;

public class TimestampUtilsTest {
  @Test
  public void testToStringOfLocalTime() {
    TimestampUtils timestampUtils = createTimestampUtils();

    assertEquals("00:00:00.0", timestampUtils.toString(LocalTime.parse("00:00:00")));
    assertEquals("00:00:00.1", timestampUtils.toString(LocalTime.parse("00:00:00.1")));
    assertEquals("00:00:00.12", timestampUtils.toString(LocalTime.parse("00:00:00.12")));
    assertEquals("00:00:00.123", timestampUtils.toString(LocalTime.parse("00:00:00.123")));
    assertEquals("00:00:00.1234", timestampUtils.toString(LocalTime.parse("00:00:00.1234")));
    assertEquals("00:00:00.12345", timestampUtils.toString(LocalTime.parse("00:00:00.12345")));
    assertEquals("00:00:00.123456", timestampUtils.toString(LocalTime.parse("00:00:00.123456")));

    assertEquals("00:00:00.999999", timestampUtils.toString(LocalTime.parse("00:00:00.999999")));
    assertEquals("00:00:00.999999", timestampUtils.toString(LocalTime.parse("00:00:00.999999499"))); // 499 NanoSeconds
    assertEquals("00:00:01.0", timestampUtils.toString(LocalTime.parse("00:00:00.999999500"))); // 500 NanoSeconds

    assertEquals("23:59:59.0", timestampUtils.toString(LocalTime.parse("23:59:59")));
    assertEquals("23:59:59.999999", timestampUtils.toString(LocalTime.parse("23:59:59.999999"))); // 0 NanoSeconds
    assertEquals("23:59:59.999999", timestampUtils.toString(LocalTime.parse("23:59:59.999999499"))); // 499 NanoSeconds
    assertEquals("24:00:00", timestampUtils.toString(LocalTime.parse("23:59:59.999999500")));// 500 NanoSeconds
    assertEquals("24:00:00", timestampUtils.toString(LocalTime.parse("23:59:59.999999999")));// 999 NanoSeconds
  }

  @Test
  public void testToLocalTime() throws SQLException {
    TimestampUtils timestampUtils = createTimestampUtils();

    assertEquals(LocalTime.parse("00:00:00"), timestampUtils.toLocalTime("00:00:00"));

    assertEquals(LocalTime.parse("00:00:00.1"), timestampUtils.toLocalTime("00:00:00.1"));
    assertEquals(LocalTime.parse("00:00:00.12"), timestampUtils.toLocalTime("00:00:00.12"));
    assertEquals(LocalTime.parse("00:00:00.123"), timestampUtils.toLocalTime("00:00:00.123"));
    assertEquals(LocalTime.parse("00:00:00.1234"), timestampUtils.toLocalTime("00:00:00.1234"));
    assertEquals(LocalTime.parse("00:00:00.12345"), timestampUtils.toLocalTime("00:00:00.12345"));
    assertEquals(LocalTime.parse("00:00:00.123456"), timestampUtils.toLocalTime("00:00:00.123456"));
    assertEquals(LocalTime.parse("00:00:00.999999"), timestampUtils.toLocalTime("00:00:00.999999"));

    assertEquals(LocalTime.parse("23:59:59"), timestampUtils.toLocalTime("23:59:59"));
    assertEquals(LocalTime.parse("23:59:59.999999"), timestampUtils.toLocalTime("23:59:59.999999")); // 0 NanoSeconds
    assertEquals(LocalTime.parse("23:59:59.9999999"), timestampUtils.toLocalTime("23:59:59.9999999")); // 900 NanoSeconds
    assertEquals(LocalTime.parse("23:59:59.99999999"), timestampUtils.toLocalTime("23:59:59.99999999")); // 990 NanoSeconds
    assertEquals(LocalTime.parse("23:59:59.999999998"), timestampUtils.toLocalTime("23:59:59.999999998")); // 998 NanoSeconds
    assertEquals(LocalTime.parse("23:59:59.999999999"), timestampUtils.toLocalTime("24:00:00"));
  }

  private TimestampUtils createTimestampUtils() {
    return new TimestampUtils(true, (Provider<TimeZone>) TimeZone::getDefault);
  }
}
