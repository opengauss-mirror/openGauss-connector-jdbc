/*
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

package org.postgresql.jdbc;

import java.math.BigDecimal;

/**
 * oGRAC Misc utils for handling time and date values.
 *
 * @author zhangting
 * @since  2025-09-01
 */
public class ORTimestampUtils {
    private static final long ONE_DAY_US = 24 * 60 * 60 * 1000 * 1000L;
    private static final long ONE_HOUR_US = 60 * 60 * 1000 * 1000L;
    private static final long ONE_MINUTE_US = 60 * 1000 * 1000L;
    private static final long ONE_SECONDS_US = 1000 * 1000L;

    /**
     * Returns the given us time value as year-month String
     *
     * @param ymTime year-month us value
     * @return year-month date String
     */
    public static String getDateYearMonth(int ymTime) {
        int usTime = ymTime;
        if (usTime < 0) {
            usTime = Math.abs(usTime);
        }
        int year = usTime / 12;
        int month = usTime % 12;
        StringBuilder ymValue = new StringBuilder();
        ymValue.append(year).append('-');
        if (month < 10) {
            ymValue.append('0');
        }
        ymValue.append(month);
        return ymValue.toString();
    }

    /**
     * Returns the given us time value as day-hms String
     *
     * @param dhTime day-hms us value
     * @return day-hms date String
     */
    public static String getDateDayHMS(long dhTime) {
        StringBuilder value = new StringBuilder();
        long usTime = dhTime;
        if (usTime < 0) {
            usTime = Math.abs(usTime);
            value.append('-');
        }
        int day = (int) (usTime / ONE_DAY_US);
        usTime = usTime % ONE_DAY_US;
        int hour = (int) (usTime / ONE_HOUR_US);
        usTime = usTime % ONE_HOUR_US;
        int min = (int) (usTime / ONE_MINUTE_US);
        usTime = usTime % ONE_MINUTE_US;
        int sec = (int) (usTime / ONE_SECONDS_US);
        usTime = usTime % ONE_SECONDS_US;

        value.append(day).append(' ');
        value.append(hour).append(':');
        value.append(min).append(':').append(sec);
        if (usTime <= 0) {
            return value.append(".0").toString();
        }
        String milliSec = String.format("%06d", usTime);
        String us = "0." + milliSec;
        BigDecimal usBd = new BigDecimal(us);
        String usFormat = usBd.stripTrailingZeros().toPlainString();
        return value.append(usFormat.substring(1)).toString();
    }
}
