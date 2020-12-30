package org.postgresql.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4JLogger implements Log {
    private Logger logger;

    public Slf4JLogger(String name) {
        this.logger = LoggerFactory.getLogger(name);
    }

    @Override
    public boolean isDebugEnabled() {
        return this.logger.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return this.logger.isTraceEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return this.logger.isInfoEnabled();
    }

    @Override
    public boolean isFatalEnabled() {
        return this.logger.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return this.logger.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return this.logger.isWarnEnabled();
    }

    @Override
    public void debug(Object msg) {
        this.logger.debug(msg.toString());
    }

    @Override
    public void debug(Object msg, Throwable throwable) {
        this.logger.debug(msg.toString(), throwable);
    }

    @Override
    public void error(Object msg) {
        this.logger.error(msg.toString());
    }

    @Override
    public void error(Object msg, Throwable throwable) {
        this.logger.error(msg.toString(), throwable);
    }

    @Override
    public void fatal(Object msg) {
        this.logger.error(msg.toString());
    }

    @Override
    public void fatal(Object msg, Throwable throwable) {
        this.logger.error(msg.toString(), throwable);
    }

    @Override
    public void info(Object msg) {
        this.logger.info(msg.toString());
    }

    @Override
    public void info(Object msg, Throwable throwable) {
        this.logger.info(msg.toString(), throwable);
    }

    @Override
    public void trace(Object msg) {
        this.logger.trace(msg.toString());
    }

    @Override
    public void trace(Object msg, Throwable throwable) {
        this.logger.trace(msg.toString(), throwable);
    }

    @Override
    public void warn(Object msg) {
        this.logger.warn(msg.toString());
    }

    @Override
    public void warn(Object msg, Throwable throwable) {
        this.logger.warn(msg.toString(), throwable);
    }

}
