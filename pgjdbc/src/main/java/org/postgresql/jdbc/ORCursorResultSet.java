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

import org.postgresql.core.ORField;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.List;

/**
 * cursor ResultSet
 *
 * @author zhangting
 * @since  2025-09-29
 */
public class ORCursorResultSet extends ORResultSet {
    private int cursorMode;
    private boolean isFetched;

    /**
     * ORCursorResultSet constructor
     *
     * @param orStatement orStatement
     * @param cursorMode cursorMode
     * @throws SQLException if a database access error occurs
     */
    public ORCursorResultSet(ORStatement orStatement, int cursorMode) throws SQLException {
        super(orStatement);
        this.isFetched = false;
        this.cursorMode = cursorMode;
    }

    /**
     * get statement
     *
     * @return statement
     */
    public ORStatement getStatement() {
        return statement;
    }

    /**
     * is fetched
     *
     * @return isFetched
     */
    public boolean isFetched() {
        return isFetched;
    }

    /**
     * set isFetched
     *
     * @param isFetched is fetched
     */
    public void setFetched(boolean isFetched) {
        this.isFetched = isFetched;
    }

    /**
     * get cursorMode
     *
     * @return cursorMode
     */
    public int getCursorMode() {
        return cursorMode;
    }

    /**
     * set cursor fetch result
     *
     * @param total total rows
     * @param orFields fields
     * @param valueLens value length
     * @param dataRows data rows
     * @param hasRemain has remain
     */
    public void setCursorFetchInfo(int total, ORField[] orFields, List<int[]> valueLens,
                                   List<byte[][]> dataRows, boolean hasRemain) {
        this.orFields = orFields;
        setFetchInfo(total, valueLens, dataRows, hasRemain);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        boolean hasNext = this.hasNext();
        if (hasRemain && !hasNext) {
            try {
                this.statement.connection.getQueryExecutor().fetchCursor(this);
            } catch (SQLException e) {
                throw new PSQLException("fetch more rows failed.", PSQLState.IO_ERROR);
            }
            hasNext = hasNext();
        }
        return hasNext;
    }
}
