/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd
 */

package org.postgresql.jdbc;

import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORCachedQuery;
import org.postgresql.core.ORField;
import org.postgresql.core.ORParameterList;
import org.postgresql.core.OutParams;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.DriverManager;
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
     * the resultSet type to return
     */
    protected final int resultSetType;

    /**
     * is it updateAble or not
     */
    protected final int resultSetConcurrency;

    /**
     * whether this holdability will remain open when the current transaction is committed.
     */
    protected final int resultSetHoldability;

    /**
     * the execution results
     */
    protected ResultSet rs = null;

    /**
     * the callable results
     */
    protected ORResultSet callableRs = null;

    /**
     * parameter list in batch execution
     */
    protected List<ORParameterList> parametersList = null;

    /**
     * the batch sqls
     */
    protected List<String> sqls = new ArrayList();

    /**
     * max field size
     */
    protected int maxfieldSize = 0;

    /**
     * result Sets
     */
    protected List<ResultSet> resultSets = new ArrayList<>();

    /**
     * max rows
     */
    protected int maxrows = 0;

    /**
     * query timeout
     */
    protected int timeout = 0;

    /**
     * auto generated keys
     */
    protected ResultSet generatedKeys = null;

    /**
     * out params
     */
    protected OutParams outParams;

    /**
     * statement id
     */
    protected int mark = -1;

    /**
     * queryMode
     */
    protected int queryMode;

    /**
     * cursorIds
     */
    protected List<Long> cursorSets = new LinkedList();
    private int updateCount;
    private int queryFlag;
    private ORField[] field;
    private int rsIndex = 0;
    private volatile boolean isClosed;
    private int fetchSize;

    /**
     * statement constructor
     *
     * @param conn connection
     * @param resultSetType resultSetType
     * @param resultSetConcurrency resultSetConcurrency
     * @param resultSetHoldability resultSetHoldability
     */
    ORStatement(ORConnection conn, int resultSetType, int resultSetConcurrency,
                int resultSetHoldability) {
        this.connection = conn;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
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
        if (sql == null || sql.isEmpty()) {
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
            try {
                if (this.mark != -1) {
                    connection.getQueryExecutor().freeStatement(this);
                }
            } catch (IOException e) {
                throw new SQLException(e.getMessage());
            }

            for (int i = 0; i < resultSets.size(); i++) {
                ResultSet resultSet = resultSets.get(i);
                if (resultSet != null) {
                    resultSet.close();
                }
            }
            resultSets.clear();
            rs = null;
            if (generatedKeys != null) {
                generatedKeys.close();
            }
            isClosed = true;
        }
    }

    /**
     * get queryMode
     *
     * @return queryMode
     */
    public int getQueryMode() {
        return this.queryMode;
    }

    /**
     * set callable ResultSet
     *
     * @param callableRs callable ResultSet
     */
    public void setCallableRs(ORResultSet callableRs) {
        this.callableRs = callableRs;
    }

    @Override
    public int getUpdateCount() {
        return this.updateCount;
    }

    /**
     * get out params
     *
     * @return out params
     */
    public OutParams getOutParams() {
        return this.outParams;
    }

    /**
     * set out params
     *
     * @param outParams out params
     */
    public void setOutParams(OutParams outParams) {
        this.outParams = outParams;
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
    public void setMaxFieldSize(int max) throws SQLException {
        verifyClosed();
        if (max < 0) {
            throw new SQLException("The max field size should be greater than 0.");
        }
        this.maxfieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        verifyClosed();
        return maxrows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        verifyClosed();
        if (max < 0) {
            throw new SQLException("The max rows should be greater than 0.");
        }
        this.maxrows = max;
    }

    @Override
    public int getMaxFieldSize() {
        return maxfieldSize;
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
    public int getQueryTimeout() throws SQLException {
        verifyClosed();
        return timeout / 1000;
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
    public void setQueryTimeout(int seconds) throws SQLException {
        verifyClosed();
        if (seconds < 0) {
            throw new SQLException("Query timeout should be greater than 0.");
        }
        timeout = seconds * 1000;
    }

    @Override
    public void cancel() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(connection.getUrl(), connection.getClientInfo());
            if (conn instanceof ORConnection) {
                ((ORConnection) conn).getQueryExecutor().cancel();
            }
        } catch (IOException | SQLException e) {
            throw new SQLException("cancel failed: " + e.getMessage());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
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
        synchronized (this) {
            verifyClosed();
            if (rsIndex < resultSets.size() - 1) {
                resultSets.get(rsIndex).close();
                rsIndex++;
                rs = resultSets.get(rsIndex);
                return true;
            }
            return false;
        }
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
        return resultSetHoldability;
    }

    @Override
    public int getResultSetType() {
        return resultSetType;
    }

    /**
     * set queryFlag
     *
     * @param queryFlag queryFlag
     */
    public void setQueryFlag(int queryFlag) {
        this.queryFlag = queryFlag;
    }

    /**
     * get queryFlag
     *
     * @return QueryFlag
     */
    public int getQueryFlag() {
        return queryFlag;
    }

    private void init() {
        updateCount = -1;
        rsIndex = 0;
        cursorSets.clear();
        rs = null;
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

    /**
     * add cursor
     *
     * @param cursorId cursorId
     */
    public void addCursor(long cursorId) {
        cursorSets.add(cursorId);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        verifyClosed();
        synchronized (this) {
            if (generatedKeys != null) {
                return generatedKeys;
            }
            throw new PSQLException(GT.tr("No generated keys results were returned"), PSQLState.NO_DATA);
        }
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
        init();
        generatedKeys = null;
        connection.getQueryExecutor().execute(cachedQuery, batchParameters);
        ResultSet resultSet = cachedQuery.getRs();
        if (resultSet != null) {
            resultSets.add(resultSet);
            this.rs = resultSet;
        }
        rsIndex = resultSets.size() - 1;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        synchronized (this) {
            verifyClosed();
            if (rsIndex >= resultSets.size() - 1) {
                return false;
            }

            if (current == Statement.CLOSE_ALL_RESULTS) {
                for (int i = 0; i <= rsIndex; i++) {
                    resultSets.get(i).close();
                }
            }
            rsIndex++;
            rs = resultSets.get(rsIndex);
            return true;
        }
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
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS) {
            return executeUpdate(sql);
        }
        ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sql, false);
        cachedQuery.setAutoGeneratedKeys(true);
        execute(cachedQuery, null);
        this.generatedKeys = cachedQuery.getGeneratedKeys();
        return getUpdateCount();
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
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS) {
            return execute(sql);
        }
        ORCachedQuery cachedQuery = new ORCachedQuery(connection, this, sql, false);
        cachedQuery.setAutoGeneratedKeys(true);
        execute(cachedQuery, null);
        this.generatedKeys = cachedQuery.getGeneratedKeys();
        return rs != null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return resultSetHoldability;
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