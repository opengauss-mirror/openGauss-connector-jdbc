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
