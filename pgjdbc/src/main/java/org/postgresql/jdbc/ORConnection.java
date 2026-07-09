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

import org.postgresql.Driver;
import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORQueryExecutor;
import org.postgresql.core.ConnectionFactory;
import org.postgresql.core.ORStream;
import org.postgresql.core.types.PGBlob;
import org.postgresql.core.types.PGClob;
import org.postgresql.core.ORCachedQuery;
import org.postgresql.core.ORDataType;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.GT;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Clob;
import java.sql.Blob;
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.Array;
import java.sql.Struct;
import java.sql.Connection;
import java.io.IOException;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * create and return a connection with oGRAC.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORConnection implements ORBaseConnection {
    private static Log LOGGER = Logger.getLogger(ORConnection.class.getName());

    private final TimestampUtils timestampUtils;
    private ORQueryExecutor queryExecutor;
    private ORStream orStream;
    private Properties properties;
    private boolean isAutoCommit = true;
    private int fetchSize = -1;
    private boolean isSsl;
    private String enabledCipherSuites;
    private int savepointId = 0;
    private boolean isOnlySSL;
    private HostSpec hostSpec;
    private String url;
    private String user;
    private boolean isReadOnly = false;
    private int orHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    private String catalog;
    private DatabaseMetaData metadata;
    private int isolationLevel;

    /**
     * connection constructor
     *
     * @param hostsInfo hosts info
     * @param user user
     * @param info properties
     * @param url url
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    public ORConnection(HostSpec[] hostsInfo, String user, Properties info, String url)
            throws SQLException, IOException {
        try {
            if (info.getProperty("fetchsize") != null) {
                fetchSize = Integer.parseInt(info.getProperty("fetchsize"));
                if (fetchSize < 0) {
                    fetchSize = -1;
                }
            }
        } catch (NumberFormatException e) {
            throw new SQLException("fetchsize value error: " + e.getMessage());
        }
        this.url = url;
        this.properties = info;
        this.user = user;
        this.isSsl = Boolean.valueOf(info.getProperty("ssl", "true"));
        this.enabledCipherSuites = info.getProperty("enabledCipherSuites", "");
        this.isOnlySSL = Boolean.valueOf(info.getProperty("onlySSL", "false"));
        ConnectionFactory.openORConnection(hostsInfo, this, info);
        timestampUtils = new TimestampUtils(true, null);
    }

    @Override
    public TimestampUtils getTimestampUtils() {
        return timestampUtils;
    }

    /**
     * get fetchSize
     *
     * @return fetchSize
     */
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public HostSpec getHostSpec() {
        return hostSpec;
    }

    public String getUser() {
        return user;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public void setHostSpec(HostSpec hostSpec) {
        this.hostSpec = hostSpec;
    }

    @Override
    public boolean isSsl() {
        return isSsl;
    }

    /**
     * set fetchSize
     *
     * @param fetchSize fetchSize
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "nativeSQL(String)");
    }

    @Override
    public void setAutoCommit(boolean isAutoCommit) throws SQLException {
        this.isAutoCommit = isAutoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.isAutoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        try {
            queryExecutor.commit();
        } catch (IOException e) {
            throw new PSQLException("transaction commit failed.", PSQLState.IO_ERROR);
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        try {
            queryExecutor.rollback();
        } catch (IOException e) {
            throw new PSQLException("transaction rollback failed.", PSQLState.IO_ERROR);
        }
    }

    @Override
    public void close() {
        try {
            if (queryExecutor != null) {
                queryExecutor.close();
            }
        } finally {
            if (orStream != null) {
                try {
                    orStream.flush();
                    orStream.close();
                } catch (IOException e) {
                    LOGGER.warn("IOException occur on close stream: ", e);
                }
            }
        }
    }

    @Override
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return queryExecutor.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        if (metadata == null) {
            metadata = new ORDatabaseMetaData(this);
        }
        return metadata;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public void setReadOnly(boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != Connection.TRANSACTION_SERIALIZABLE && level != Connection.TRANSACTION_READ_COMMITTED) {
            throw new SQLException("Transaction isolation level " + level + " not supported.");
        }

        String isolation = null;
        if (level == Connection.TRANSACTION_SERIALIZABLE) {
            isolation = "serializable";
        } else {
            isolation = "read committed";
        }
        String sql = "set transaction isolation level " + isolation;
        try (Statement stmt = this.createStatement()) {
            stmt.execute(sql);
        }
        this.isolationLevel = level;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getWarnings()");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    /**
     * Check whether the database connection is closed
     *
     * @throws SQLException if a database access error occurs
     */
    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new PSQLException("This connection has been closed.",
                    PSQLState.CONNECTION_DOES_NOT_EXIST);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkClosed();
        return new ORPreparedStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public ORQueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    @Override
    public void setQueryExecutor(ORQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public ORStream getORStream() {
        return orStream;
    }

    public void setOrStream(ORStream orStream) {
        this.orStream = orStream;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return Collections.emptyMap();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        switch (holdability) {
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                orHoldability = holdability;
                break;
            default:
                throw new SQLException("Holdability " + holdability + " is invalid");
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        if (getAutoCommit()) {
            throw new SQLException("Cannot establish a savepoint in auto-commit mode.");
        }

        if (name == null || name.trim().isEmpty()) {
            throw new PSQLException(GT.tr("SavePoint name is invalid."),
                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
        }
        ORSavepoint savepoint = new ORSavepoint(name);
        String sql = "SAVEPOINT " + savepoint.getSavepointName();
        try (Statement stmt = this.createStatement()) {
            stmt.execute(sql);
        }
        return savepoint;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        checkClosed();
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public int getTransactionIsolation() {
        return isolationLevel;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        checkClosed();
        return new ORCallableStatement(this, sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        ORPreparedStatement preparedStatement = new ORPreparedStatement(this, sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, getHoldability());
        ORCachedQuery cachedQuery = preparedStatement.getPreparedQuery();
        cachedQuery.setAutoGeneratedKeys(true);
        return preparedStatement;
    }

    @Override
    public void setClientInfo(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        return new PGBlob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw Driver.notImplemented(this.getClass(), "createSQLXML()");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        String sql = null;
        if (savepoint.getSavepointName() == null) {
            sql = "ROLLBACK TO SAVEPOINT Gauss_" + savepoint.getSavepointId();
        } else {
            sql = "ROLLBACK TO SAVEPOINT " + savepoint.getSavepointName();
        }

        Statement stmt = null;
        try {
            stmt = this.createStatement();
            stmt.execute(sql);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        ORPreparedStatement ps = new ORPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, getHoldability());
        if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
            ps.getPreparedQuery().setAutoGeneratedKeys(true);
        }
        return ps;
    }

    @Override
    public void setClientInfo(String name, String value) {
        this.properties.setProperty(name, value);
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw Driver.notImplemented(this.getClass(), "createNClob()");
    }

    @Override
    public String getClientInfo(String name) {
        return this.properties.getProperty(name);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        if (savepoint instanceof ORSavepoint) {
            ORSavepoint orSavepoint = (ORSavepoint) savepoint;
            String sql = null;
            if (orSavepoint.getSavepointName() == null) {
                sql = "RELEASE SAVEPOINT Gauss_" + orSavepoint.getSavepointId();
            } else {
                sql = "RELEASE SAVEPOINT " + orSavepoint.getSavepointName();
            }

            Statement stmt = null;
            try {
                stmt = this.createStatement();
                stmt.execute(sql);
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
            orSavepoint.release();
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setTypeMap(Map<String, Class<?>>)");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        ORArray arr = new ORArray();
        Integer type = ORDataType.getJavaType(typeName);
        arr.setType(type);
        arr.setValue(elements);
        return arr;
    }

    @Override
    public int getHoldability() {
        return orHoldability;
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw Driver.notImplemented(this.getClass(), "clearWarnings()");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "createStruct(String, Object[])");
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        return new PGClob();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        if (schema == null || schema.contains(";")) {
            throw new PSQLException(GT.tr("Invalid schema name."),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }

        String sql = "ALTER SESSION SET CURRENT_SCHEMA = " + schema;
        try (Statement stmt = createStatement()) {
            stmt.execute(sql);
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        int timeout = 0;
        try {
            if (orStream != null) {
                timeout = orStream.getSocket().getSoTimeout();
            }
        } catch (IOException e) {
            throw new PSQLException(GT.tr("Unable to get network timeout."),
                    PSQLState.COMMUNICATION_ERROR, e);
        }
        return timeout;
    }

    @Override
    public Properties getClientInfo() {
        return properties;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        if (getAutoCommit()) {
            throw new SQLException("Cannot establish a savepoint in auto-commit mode.");
        }

        ORSavepoint savepoint = new ORSavepoint(savepointId++);
        String sql = "SAVEPOINT Gauss_" + savepoint.getSavepointId();
        try (Statement stmt = createStatement()) {
            stmt.execute(sql);
        }
        return savepoint;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        String schema = null;
        String sql = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')";
        try (Statement stmt = this.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    schema = rs.getString(1);
                }
            }
        }
        return schema;
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        if (milliseconds < 0) {
            throw new PSQLException(GT.tr("Network timeout must be a value greater than or equal to 0."),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }

        if (orStream == null) {
            return;
        }
        try {
            orStream.getSocket().setSoTimeout(milliseconds);
        } catch (IOException e) {
            throw new PSQLException(GT.tr("Failed to set network timeout."),
                    PSQLState.COMMUNICATION_ERROR, e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "unwrap(Class<T>)");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "isWrapperFor(Class<?>)");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (isClosed()) {
            return;
        }

        AbortConnection abortConnection = new AbortConnection();
        if (executor == null) {
            abortConnection.run();
        } else {
            executor.execute(abortConnection);
        }
    }

    private class AbortConnection implements Runnable {
        @Override
        public void run() {
            close();
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new PSQLException(GT.tr("timeout is invalid."), PSQLState.INVALID_PARAMETER_VALUE);
        }
        if (isClosed()) {
            return false;
        }

        Statement stmt = null;
        int originalTime = 0;
        try {
            originalTime = orStream.getSocket().getSoTimeout();
            if (timeout > 0) {
                orStream.getSocket().setSoTimeout(timeout * 1000);
            }
            stmt = createStatement();
            stmt.execute("select 0");
            return true;
        } catch (IOException | SQLException e) {
            throw new SQLException("Failed to detect connection status", e);
        } finally {
            try {
                orStream.getSocket().setSoTimeout(originalTime);
            } catch (IOException e) {
                LOGGER.warn("Failed to set timeout, error: " + e.getMessage());
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                LOGGER.warn("Failed to close statement, error: " + e.getMessage());
            }
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        checkClosed();
        return new ORStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }
}
