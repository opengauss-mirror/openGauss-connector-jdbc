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

import org.postgresql.PGNotification;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORQueryExecutor;
import org.postgresql.core.ConnectionFactory;
import org.postgresql.core.ORStream;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.PGobject;
import org.postgresql.util.GT;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
import java.io.IOException;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * create and return a connection with CT.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORConnection implements ORBaseConnection {
    private static Log LOGGER = Logger.getLogger(PgConnection.class.getName());
    private static final int BUFFER_SIZE = 8192;

    private final TimestampUtils timestampUtils;
    private ORQueryExecutor queryExecutor;
    private ORStream orStream;
    private Properties properties;
    private boolean isAutoCommit = true;
    private int fetchSize = -1;
    private boolean isSsl;
    private String enabledCipherSuites;
    private boolean isOnlySSL;

    /**
     * connection constructor
     *
     * @param hostSpecs host info
     * @param user user
     * @param info properties
     * @param url url
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    public ORConnection(HostSpec[] hostSpecs,
                        String user,
                        Properties info,
                        String url) throws SQLException, IOException {
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
        this.properties = info;
        this.isSsl = Boolean.valueOf(info.getProperty("ssl", "true"));
        this.enabledCipherSuites = info.getProperty("enabledCipherSuites", "");
        this.isOnlySSL = Boolean.valueOf(info.getProperty("onlySSL", "false"));

        SocketAddress socketAddress = new InetSocketAddress(info.getProperty("PGHOST").toString(),
                Integer.valueOf(info.getProperty("PGPORT")));
        this.orStream = new ORStream(socketAddress, getBufferSize());
        ConnectionFactory.openORConnection(this, info, orStream);
        timestampUtils = new TimestampUtils(true, null);
    }

    @Override
    public TimestampUtils getTimestampUtils() {
        return timestampUtils;
    }

    @Override
    public int getBufferSize() {
        return BUFFER_SIZE;
    }

    /**
     * get fetchSize
     *
     * @return fetchSize
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * is it ssl
     *
     * @return is ssl
     */
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
        return new ORPreparedStatement(this, sql, 2);
    }

    @Override
    public String nativeSQL(String sql) {
        return null;
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
            throw new PSQLException(GT.tr("transaction commit failed."), PSQLState.IO_ERROR);
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        try {
            queryExecutor.rollback();
        } catch (IOException e) {
            throw new PSQLException(GT.tr("transaction rollback failed."), PSQLState.IO_ERROR);
        }
    }

    @Override
    public void close() throws SQLException {
        if (queryExecutor == null) {
            return;
        }
        queryExecutor.close();
    }

    @Override
    public void setCatalog(String catalog) {
    }

    @Override
    public boolean isClosed() throws SQLException {
        return queryExecutor.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public void setReadOnly(boolean isReadOnly) {
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setTransactionIsolation(int level) {
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }


    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new PSQLException(GT.tr("This connection has been closed."),
                    PSQLState.CONNECTION_DOES_NOT_EXIST);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return new ORPreparedStatement(this, sql, 2);
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

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return null;
    }

    @Override
    public void setHoldability(int holdability) {
    }

    @Override
    public Savepoint setSavepoint(String name) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public int getTransactionIsolation() {
        return 0;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(Properties properties) {
    }

    @Override
    public Blob createBlob() {
        return null;
    }

    @Override
    public SQLXML createSQLXML() {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) {
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(String name, String value) {
    }

    @Override
    public NClob createNClob() {
        return null;
    }

    @Override
    public String getClientInfo(String name) {
        return this.properties.getProperty(name);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) {
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public int getHoldability() {
        return 0;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        return null;
    }

    @Override
    public Clob createClob() {
        return null;
    }

    @Override
    public void setSchema(String schema) {
    }

    @Override
    public int getNetworkTimeout() {
        return 0;
    }

    @Override
    public Properties getClientInfo() {
        return properties;
    }

    @Override
    public Savepoint setSavepoint() {
        return null;
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public Array createArrayOf(String typeName, Object elements) throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        checkClosed();
        return new ORStatement(this);
    }

    @Override
    public PGNotification[] getNotifications() {
        return new PGNotification[0];
    }

    @Override
    public PGNotification[] getNotifications(int timeoutMillis) {
        return new PGNotification[0];
    }

    @Override
    public CopyManager getCopyAPI() {
        return null;
    }

    @Override
    public LargeObjectManager getLargeObjectAPI() {
        return null;
    }

    @Override
    public Fastpath getFastpathAPI() {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public void addDataType(String type, String className) {
    }

    @Override
    public void addDataType(String type, Class<? extends PGobject> klass) {
    }

    @Override
    public void setPrepareThreshold(int threshold) {
    }

    @Override
    public int getPrepareThreshold() {
        return 0;
    }

    @Override
    public void setDefaultFetchSize(int fetchSize) {
    }

    @Override
    public int getDefaultFetchSize() {
        return 0;
    }

    @Override
    public String escapeIdentifier(String identifier) {
        return null;
    }

    @Override
    public String escapeLiteral(String literal) {
        return null;
    }

    @Override
    public PreferQueryMode getPreferQueryMode() {
        return null;
    }

    @Override
    public AutoSave getAutosave() {
        return null;
    }

    @Override
    public void setAutosave(AutoSave autoSave) {
    }

    @Override
    public PGReplicationConnection getReplicationAPI() {
        return null;
    }
}
