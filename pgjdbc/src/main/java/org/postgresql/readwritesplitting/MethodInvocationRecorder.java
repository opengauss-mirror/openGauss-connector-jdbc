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
