package org.postgresql.log;


public interface Log {
    boolean isDebugEnabled();

    boolean isWarnEnabled();

    boolean isTraceEnabled();

    boolean isInfoEnabled();

    boolean isErrorEnabled();

    boolean isFatalEnabled();

    void debug(Object msg);

    void debug(Object msg, Throwable throwable);

    void warn(Object msg);

    void warn(Object msg, Throwable throwable);

    void trace(Object msg);

    void trace(Object msg, Throwable throwable);

    void info(Object msg);

    void info(Object msg, Throwable throwable);

    void fatal(Object msg);

    void fatal(Object msg, Throwable throwable);

    void error(Object msg);

    void error(Object msg, Throwable throwable);
}
