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
