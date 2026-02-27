/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
