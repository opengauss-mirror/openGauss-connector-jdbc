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

/**
 * Force execute callback.
 *
 * @since 2023-11-20
 * @param <T> type of target to be executed
 */
public interface ForceExecuteCallback<T> {
    /**
     * Execute.
     *
     * @param target target to be executed
     * @throws SQLException SQL exception
     */
    void execute(T target) throws SQLException;
}
