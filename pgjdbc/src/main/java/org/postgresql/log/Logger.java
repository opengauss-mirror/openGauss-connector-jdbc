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

package org.postgresql.log;

import java.sql.SQLException;

public class Logger {

    private static final String JDK_LOGGER = "JdkLogger";
    private static String loggerName;

    public static Log getLogger(String name){
        try {
            if (loggerName == null || loggerName.equals("")) {
                return LogFactory.getLogger(JDK_LOGGER, name);
            } else {
                return LogFactory.getLogger(loggerName, name);
            }
        } catch (SQLException e) {
            //ignore this exception for JdkLogger always exists
            System.err.println("ERROR: getLogger " + loggerName + " failed, pls check properties of logger, \n" +sqlErrorDetail(e));
            return null;
        }
    }

    public static boolean isUsingJDKLogger() {
        if (loggerName == null || loggerName.equals("")) {
            return true;
        }
        String logger = simplifyLoggerName(loggerName);
        return logger.equals(JDK_LOGGER);
    }

    private static String simplifyLoggerName(String fullName) {
        if (!fullName.contains(".")) {
            return fullName;
        }
        int startIndex = fullName.lastIndexOf('.');
        if (startIndex > 0) {
            return fullName.substring(startIndex + 1);
        }
        return "";
    }

    public static synchronized void setLoggerName(String logger) {
        loggerName = logger;
    }


    private static String sqlErrorDetail(SQLException e){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("error message: ").append(e.getMessage()).append("\n")
                .append("error cause: ").append(e.getCause()).append("\n")
                .append("error state: ").append(e.getSQLState()).append("\n");
        stringBuilder.append("stack trace:");
        for(StackTraceElement element : e.getStackTrace()){
            stringBuilder.append("    ").append(element.toString()).append("\n");
        }
        return stringBuilder.toString();
    }

}
