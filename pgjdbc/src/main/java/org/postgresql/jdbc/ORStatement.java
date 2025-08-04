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

import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORCachedQuery;
import org.postgresql.core.ORField;
import org.postgresql.core.ORParameterList;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * the simple sql query statement.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORStatement implements Statement {
    /**
     * connection info
     */
    protected final ORBaseConnection connection;

    /**
     * the execution results
     */
    protected ResultSet rs = null;

    /**
     * parameter list in batch execution
     */
    protected List<ORParameterList> parametersList = null;

    /**
     * the batch sqls
     */
    protected List<String> sqls = new ArrayList();

    private int queryMode;
    private int updateCount;
    private int queryFlag;
    private List<Long> resultSets = new LinkedList();
    private ORField[] field;
    private volatile boolean isClosed;
    private int fetchSize;
    private int mark = -1;

    /**
     * statement constructor
     *
     * @param conn connection
     */
    ORStatement(ORConnection conn) {
        this.connection = conn;
        if (conn.getFetchSize() > 0) {
            fetchSize = conn.getFetchSize();
        }
    }

    /**
     * get batch parameters
     *
     * @return batchParameters
     */
    public List<ORParameterList> getParametersList() {
        return parametersList;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        verifyClosed();
        String exception = "No results were returned by the query.";
        if (sql == null || sql.length() == 0) {
            throw new PSQLException(GT.tr("sql is invalid"), PSQLState.INVALID_PARAMETER_VALUE);
        }
        ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sql, false);
        execute(cachedQuery, null);
        if (rs == null) {
            throw new PSQLException(GT.tr(exception), PSQLState.NO_DATA);
        }
        return rs;
    }

    /**
     * check if statement has been closed
     *
     * @throws SQLException if a database access error occurs
     */
    protected void verifyClosed() throws SQLException {
        if (isClosed()) {
            throw new PSQLException(GT.tr("This statement has been closed."),
                    PSQLState.OBJECT_NOT_IN_STATE);
        }
    }

    /**
     * set field
     *
     * @param field field info
     */
    public void setField(ORField[] field) {
        this.field = field;
    }

    /**
     * set updateCount
     *
     * @param updateCount updateCount
     */
    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

    @Override
    public void close() throws SQLException {
        synchronized (this) {
            if (isClosed) {
                return;
            }
        }
        try {
            if (this.mark != -1) {
                connection.getQueryExecutor().freeStatement(this);
            }
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
        isClosed = true;
    }

    /**
     * get queryMode
     *
     * @return queryMode
     */
    public int getQueryMode() {
        return this.queryMode;
    }

    @Override
    public int getUpdateCount() {
        return this.updateCount;
    }

    /**
     * get field
     *
     * @return field
     */
    public ORField[] getField() {
        return field;
    }

    /**
     * get mark
     *
     * @return mark
     */
    public int getMark() {
        return mark;
    }

    @Override
    public void setMaxFieldSize(int max) {
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) {
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        verifyClosed();
        ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sql, false);
        execute(cachedQuery, null);
        return getUpdateCount();
    }

    @Override
    public void setEscapeProcessing(boolean isEnable) {
    }

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    /**
     * set statement mark
     *
     * @param mark statementMark
     */
    public void setMark(int mark) {
        this.mark = mark;
    }

    @Override
    public void setQueryTimeout(int seconds) {
    }

    @Override
    public void cancel() throws SQLException {
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void setCursorName(String name) {
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        verifyClosed();
        ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sql, false);
        execute(cachedQuery, null);
        return rs != null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return rs;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    /**
     * set queryMode
     *
     * @param queryMode queryMode
     */
    public void setQueryMode(int queryMode) {
        this.queryMode = queryMode;
    }

    @Override
    public void setFetchDirection(int direction) {
    }

    @Override
    public int getFetchDirection() {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        verifyClosed();
        if (rows < 0) {
            throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        verifyClosed();
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() {
        return 0;
    }

    @Override
    public int getResultSetType() {
        return 0;
    }

    /**
     * set queryFlag
     *
     * @param queryFlag queryFlag
     */
    public void setQueryFlag(int queryFlag) {
        this.queryFlag = queryFlag;
    }

    private void reset() {
        rs = null;
        updateCount = -1;
        resultSets.clear();
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public int[] executeBatch() throws SQLException {
        verifyClosed();
        int[] updateCounts = new int[sqls.size()];
        for (int i = 0; i < sqls.size(); i++) {
            this.reset();
            ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sqls.get(i), false);
            execute(cachedQuery, null);
            updateCounts[i] = getUpdateCount();
        }
        return updateCounts;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            return executeUpdate(sql);
        }
        throw new PSQLException(GT.tr("Returning autogenerated keys by column "
                + "index is not supported."), PSQLState.NOT_IMPLEMENTED);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    /**
     * execute sql query
     *
     * @param cachedQuery sql query info
     * @param batchParameters list of parameters for prepare mode
     * @throws SQLException if a database access error occurs
     */
    protected void execute(ORCachedQuery cachedQuery, List<ORParameterList> batchParameters) throws SQLException {
        connection.getQueryExecutor().execute(cachedQuery, batchParameters);
        this.rs = cachedQuery.getRs();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    /**
     * fetch more rows from server
     *
     * @param rs resultSet
     * @param sql execute sql
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    public void fetch(ORResultSet rs, String sql) throws IOException, SQLException {
        ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sql, false);
        cachedQuery.setRs(rs);
        connection.getQueryExecutor().fetch(cachedQuery);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        verifyClosed();
        sqls.add(sql);
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void clearBatch() {
        this.sqls.clear();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public void closeOnCompletion() {
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public void setPoolable(boolean isPoolable) {
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes != null && columnIndexes.length == 0) {
            return execute(sql);
        }
        throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
                PSQLState.NOT_IMPLEMENTED);
    }
}