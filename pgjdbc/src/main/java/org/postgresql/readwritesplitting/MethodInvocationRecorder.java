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
