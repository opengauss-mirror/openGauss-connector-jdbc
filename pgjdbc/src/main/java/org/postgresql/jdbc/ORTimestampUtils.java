/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
