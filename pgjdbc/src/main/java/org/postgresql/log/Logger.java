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
