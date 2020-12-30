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
