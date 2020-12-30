package org.postgresql.log;


import java.util.logging.Level;
import java.util.logging.Logger;

public class JdkLogger implements Log {
    private Logger jdkLogger = null;

    public JdkLogger(String name) {
        this.jdkLogger = Logger.getLogger(name);
    }

    @Override
    public boolean isDebugEnabled() {
        return this.jdkLogger.isLoggable(Level.FINE);
    }

    @Override
    public boolean isWarnEnabled() {
        return this.jdkLogger.isLoggable(Level.WARNING);
    }

    @Override
    public boolean isErrorEnabled() {
        return this.jdkLogger.isLoggable(Level.SEVERE);
    }

    @Override
    public boolean isFatalEnabled() {
        return this.jdkLogger.isLoggable(Level.SEVERE);
    }

    @Override
    public boolean isInfoEnabled() {
        return this.jdkLogger.isLoggable(Level.INFO);
    }

    @Override
    public boolean isTraceEnabled() {
        return this.jdkLogger.isLoggable(Level.FINEST);
    }

    @Override
    public void debug(Object msg) {
        logInner(Level.FINE, msg, null);
    }

    @Override
    public void debug(Object msg, Throwable throwable) {
        logInner(Level.FINE, msg, throwable);
    }

    @Override
    public void warn(Object msg) {
        logInner(Level.WARNING, msg, null);
    }

    @Override
    public void warn(Object msg, Throwable throwable) {
        logInner(Level.WARNING, msg, throwable);
    }

    @Override
    public void trace(Object msg) {
        logInner(Level.FINEST, msg, null);
    }

    @Override
    public void trace(Object msg, Throwable throwable) {
        logInner(Level.FINEST, msg, throwable);
    }

    @Override
    public void info(Object msg) {
        logInner(Level.INFO, msg, null);
    }

    @Override
    public void info(Object msg, Throwable throwable) {
        logInner(Level.INFO, msg, throwable);
    }

    @Override
    public void fatal(Object msg) {
        logInner(Level.SEVERE, msg, null);
    }

    @Override
    public void fatal(Object msg, Throwable throwable) {
        logInner(Level.SEVERE, msg, throwable);
    }

    @Override
    public void error(Object msg) {
        logInner(Level.SEVERE, msg, null);
    }

    @Override
    public void error(Object msg, Throwable throwable) {
        logInner(Level.SEVERE, msg, throwable);
    }

    private static final int getFrameIndex(StackTraceElement[] stackTrace) {
        for (int i = 0; i < stackTrace.length; i++) {
            String className = stackTrace[i].getClassName();
            String packageName = LogFactory.getPackageName(Log.class);

            if (!className.startsWith(packageName)) {
                return i;
            }
        }
        return 0;
    }

    private void logInner(Level level, Object msg, Throwable throwable) {
        if (this.jdkLogger.isLoggable(level)) {
            String message = null;
            String methodName = "NULL";
            String className = "NULL";

            Throwable locationException = new Throwable();
            StackTraceElement[] stackTrace = locationException.getStackTrace();

            int frameIndex = getFrameIndex(stackTrace);

            if (frameIndex != 0) {
                className = stackTrace[frameIndex].getClassName();
                methodName = stackTrace[frameIndex].getMethodName();
            }

            message = String.valueOf(msg);

            if (throwable == null) {
                this.jdkLogger.logp(level, className, methodName, message);
            } else {
                this.jdkLogger.logp(level, className, methodName, message, throwable);
            }
        }
    }
}
