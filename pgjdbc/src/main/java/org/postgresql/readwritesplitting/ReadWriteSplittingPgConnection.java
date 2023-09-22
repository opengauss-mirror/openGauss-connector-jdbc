/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package org.postgresql.readwritesplitting;

import org.postgresql.PGProperty;
import org.postgresql.core.v3.ConnectionFactoryImpl;
import org.postgresql.hostchooser.HostRequirement;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.ReadWriteSplittingPgPreparedStatement;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Read write splitting pg connection.
 *
 * @since 2023-11-20
 */
public class ReadWriteSplittingPgConnection implements Connection {
    private final ReadWriteSplittingHostSpec readWriteSplittingHostSpec;

    private final PgConnectionManager connectionManager;

    private final Log LOGGER = Logger.getLogger(ReadWriteSplittingPgConnection.class.getName());

    private volatile boolean isClosed;

    private boolean isAutoCommit = true;

    /**
     * Constructor.
     *
     * @param hostSpecs host specs
     * @param props props
     * @param user user
     * @param database database
     * @param url url
     * @throws SQLException SQL exception
     */
    public ReadWriteSplittingPgConnection(HostSpec[] hostSpecs, Properties props, String user, String database,
                                          String url) throws SQLException {
        checkRequiredDependencies();
        connectionManager = new PgConnectionManager(props, user, database, url, this);
        readWriteSplittingHostSpec = new ReadWriteSplittingHostSpec(getWriteDataSourceAddress(props, hostSpecs),
                hostSpecs, getTargetServerTypeParam(props), props);
    }

    private static void checkRequiredDependencies() throws PSQLException {
        if (!isClassPresent("org.apache.shardingsphere.sql.parser.api.SQLParserEngine")) {
            throw new PSQLException("When enableStatementLoadBalance=true, the dependency "
                    + "shardingsphere-parser-sql-engine does not exist and this function cannot be used.",
                    PSQLState.UNEXPECTED_ERROR);
        }
        if (!isClassPresent("org.apache.shardingsphere.sql.parser.opengauss.parser.OpenGaussParserFacade")) {
            throw new PSQLException("When enableStatementLoadBalance=true, the dependency "
                    + "shardingsphere-parser-sql-opengauss does not exist and this function cannot be used.",
                    PSQLState.UNEXPECTED_ERROR);
        }
    }

    /**
     * Get target server type param.
     *
     * @param className Class name
     * @return Whether class is present
     */
    public static boolean isClassPresent(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException ignored) {
            // Class or one of its dependencies is not present
            return false;
        }
    }

    private HostSpec getWriteDataSourceAddress(Properties props, HostSpec[] hostSpecs) throws SQLException {
        String writeDataSourceAddress = PGProperty.WRITE_DATA_SOURCE_ADDRESS.get(props);
        if (writeDataSourceAddress.trim().isEmpty()) {
            return getWriteAddressByEstablishingConnections(hostSpecs);
        }
        String[] hostSpec = writeDataSourceAddress.split(":");
        return new HostSpec(hostSpec[0], Integer.parseInt(hostSpec[1]));
    }

    private HostSpec getWriteAddressByEstablishingConnections(HostSpec[] hostSpecs) throws SQLException {
        for (HostSpec each : hostSpecs) {
            PgConnection connection = getConnectionManager().getConnection(each);
            ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl();
            try {
                if (connectionFactory.isMaster(connection.getQueryExecutor())) {
                    return each;
                }
            } catch (IOException ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Error obtaining node role " + ex.getMessage());
                    LOGGER.debug(ex.getStackTrace());
                }
            }
        }
        throw new PSQLException(GT.tr("No write address found"), PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }

    private HostRequirement getTargetServerTypeParam(Properties info) throws PSQLException {
        HostRequirement targetServerType;
        String targetServerTypeStr = PGProperty.TARGET_SERVER_TYPE.get(info);
        try {
            targetServerType = HostRequirement.getTargetServerType(targetServerTypeStr);
        } catch (IllegalArgumentException ex) {
            throw new PSQLException(
                    GT.tr("Invalid targetServerType value: {0}", targetServerTypeStr),
                    PSQLState.CONNECTION_UNABLE_TO_CONNECT);
        }
        return targetServerType;
    }

    /**
     * Get read write splitting host spec.
     *
     * @return read write splitting host spec
     */
    public ReadWriteSplittingHostSpec getReadWriteSplittingHostSpec() {
        return readWriteSplittingHostSpec;
    }

    /**
     * Get connection manager.
     *
     * @return the connectionManager
     */
    public PgConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return new ReadWriteSplittingPgStatement(this, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new ReadWriteSplittingPgPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new ReadWriteSplittingPgPreparedStatement(this, sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new ReadWriteSplittingPgPreparedStatement(this, sql, resultSetType,
                resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new ReadWriteSplittingPgPreparedStatement(this, sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new ReadWriteSplittingPgPreparedStatement(this, sql, columnNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return new ReadWriteSplittingPgPreparedStatement(this, sql, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return connectionManager.getCurrentConnection().prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connectionManager.getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return connectionManager.getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public void setAutoCommit(boolean isAutoCommit) throws SQLException {
        this.isAutoCommit = isAutoCommit;
        connectionManager.setAutoCommit(isAutoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return isAutoCommit;
    }

    @Override
    public void commit() throws SQLException {
        connectionManager.commit();
    }

    @Override
    public void rollback() throws SQLException {
        connectionManager.rollback();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return connectionManager.getCurrentConnection().setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return connectionManager.getCurrentConnection().setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        connectionManager.getCurrentConnection().rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        connectionManager.getCurrentConnection().releaseSavepoint(savepoint);
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
        connectionManager.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return connectionManager.isValid(timeout);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connectionManager.setSchema(schema);
    }

    @Override
    public void setReadOnly(boolean isReadOnly) throws SQLException {
        connectionManager.setReadOnly(isReadOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connectionManager.getCurrentConnection().isReadOnly();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        connectionManager.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return connectionManager.getCurrentConnection().getTransactionIsolation();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return connectionManager.getCurrentConnection().nativeSQL(sql);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return connectionManager.getCurrentConnection().getMetaData();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        connectionManager.getCurrentConnection().setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return connectionManager.getCurrentConnection().getCatalog();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connectionManager.getCurrentConnection().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        connectionManager.getCurrentConnection().clearWarnings();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return connectionManager.getCurrentConnection().getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        connectionManager.getCurrentConnection().setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        connectionManager.getCurrentConnection().setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return connectionManager.getCurrentConnection().getHoldability();
    }

    @Override
    public Clob createClob() throws SQLException {
        return connectionManager.getCurrentConnection().createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connectionManager.getCurrentConnection().createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connectionManager.getCurrentConnection().createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connectionManager.getCurrentConnection().createSQLXML();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            connectionManager.getCurrentConnection().setClientInfo(name, value);
        } catch (SQLException e) {
            throw new SQLClientInfoException(Collections.emptyMap(), e);
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            connectionManager.getCurrentConnection().setClientInfo(properties);
        } catch (SQLException e) {
            throw new SQLClientInfoException(Collections.emptyMap(), e);
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return connectionManager.getCurrentConnection().getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return connectionManager.getCurrentConnection().getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connectionManager.getCurrentConnection().createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return connectionManager.getCurrentConnection().createStruct(typeName, attributes);
    }

    @Override
    public String getSchema() throws SQLException {
        return connectionManager.getCurrentConnection().getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        connectionManager.getCurrentConnection().abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        connectionManager.getCurrentConnection().setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return connectionManager.getCurrentConnection().getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return connectionManager.getCurrentConnection().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return connectionManager.getCurrentConnection().isWrapperFor(iface);
    }
}
