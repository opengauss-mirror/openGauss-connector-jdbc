/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
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
