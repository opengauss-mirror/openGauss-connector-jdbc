/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package org.postgresql.readwritesplitting;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Method invocation recorder.
 *
 * @since 2023-11-20
 * @param <T> type of target
 */
public final class MethodInvocationRecorder<T> {
    private final Map<String, ForceExecuteCallback<T>> methodInvocations = new LinkedHashMap<>();

    /**
     * Record method invocation.
     *
     * @param methodName method name
     * @param callback callback
     */
    public void record(final String methodName, final ForceExecuteCallback<T> callback) {
        methodInvocations.put(methodName, callback);
    }

    /**
     * Replay methods invocation.
     *
     * @param target target object
     * @throws SQLException SQL Exception
     */
    public void replay(final T target) throws SQLException {
        for (ForceExecuteCallback<T> each : methodInvocations.values()) {
            each.execute(target);
        }
    }
}
