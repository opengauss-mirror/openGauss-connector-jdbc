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

package org.postgresql.jdbc;

import org.postgresql.core.Utils;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * Savepoint
 *
 * @author zhangting
 * @since  2026-06-23
 */
public class ORSavepoint implements Savepoint {
    private int id = -1;
    private String name = null;
    private boolean isValid;

    /**
     * ORSavepoint constructor
     *
     * @param id savepointId
     */
    ORSavepoint(int id) {
        this.isValid = true;
        this.id = id;
    }

    /**
     * ORSavepoint constructor
     *
     * @param name savepointName
     */
    ORSavepoint(String name) {
        this.isValid = true;
        this.name = name;
    }

    /**
     * release savepoint
     */
    public void release() {
        this.isValid = false;
    }

    /**
     * get savepoint name
     *
     * @return savepoint name
     * @throws SQLException if a database access error occurs
     */
    public String getSavepointName() throws SQLException {
        if (!isValid) {
            throw new PSQLException(GT.tr("Savepoint is invalid, it has been released."),
                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
        }
        if (name == null) {
            return name;
        }
        return Utils.escapeIdentifier(null, name).toString();
    }

    /**
     * get savepoint id
     *
     * @return savepoint id
     * @throws SQLException if a database access error occurs
     */
    public int getSavepointId() throws SQLException {
        if (!isValid) {
            throw new PSQLException(GT.tr("Savepoint is invalid, it has been released."),
                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
        }
        return this.id;
    }
}
