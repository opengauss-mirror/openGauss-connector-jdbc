package org.postgresql.log;

/**
 * Tracker interface class.
 *
 * @author opengauss
 * @since 2021-1-5
 */
public interface Tracer {
    /**
     * Used to obtain the globally unique trace id.
     *
     * @return trace id.
     */
    String getTraceId();
}
