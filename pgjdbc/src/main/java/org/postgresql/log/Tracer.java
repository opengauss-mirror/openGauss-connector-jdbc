package org.postgresql.log;

/**
 * Tracker interface class.
 *
 */
public interface Tracer {
    /**
     * Used to obtain the globally unique trace id.
     *
     * @return trace id.
     */
    String getTraceId();
}
