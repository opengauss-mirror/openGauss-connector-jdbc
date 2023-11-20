/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package org.postgresql.readwritesplitting;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Force execute template.
 *
 * @since 2023-11-20
 * @param <T> type of targets to be executed
 */
public final class ForceExecuteTemplate<T> {
    /**
     * Force execute.
     *
     * @param targets targets to be executed
     * @param callback force execute callback
     * @throws SQLException throw SQL exception after all targets are executed
     */
    public void execute(final Collection<T> targets, final ForceExecuteCallback<T> callback) throws SQLException {
        Collection<SQLException> exceptions = new LinkedList<>();
        for (T each : targets) {
            try {
                callback.execute(each);
            } catch (final SQLException ex) {
                exceptions.add(ex);
            }
        }
        throwSQLExceptionIfNecessary(exceptions);
    }

    private void throwSQLExceptionIfNecessary(final Collection<SQLException> exceptions) throws SQLException {
        if (exceptions.isEmpty()) {
            return;
        }
        SQLException ex = new SQLException("");
        exceptions.forEach(ex::setNextException);
        throw ex;
    }
}
