/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

package org.postgresql.quickautobalance;

import org.postgresql.PGProperty;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.SocketFactoryFactory;
import org.postgresql.core.v3.ConnectionFactoryImpl;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.SslMode;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Datanode.
 */
public class DataNode {
    private static Log LOGGER = Logger.getLogger(DataNode.class.getName());

    private static final String USERNAME_OR_PASSWORD_INVALID_ERROR_CODE = "28P01";

    // the host of datanode (ip + port)
    private final HostSpec hostSpec;

    // cached connections
    private final Map<PgConnection, ConnectionInfo> cachedConnectionList;

    // number of cached connections, before set into cachedConnectionList, and after load balance by leastconn.
    private final AtomicInteger cachedCreatingConnectionSize;

    private volatile boolean dataNodeState;

    public DataNode(final HostSpec hostSpec) {
        this.hostSpec = hostSpec;
        this.cachedConnectionList = new ConcurrentHashMap<>();
        this.cachedCreatingConnectionSize = new AtomicInteger(0);
        this.dataNodeState = true;
    }

    /**
     * Set connection state.
     *
     * @param pgConnection pgConnection
     * @param state state
     */
    public void setConnectionState(final PgConnection pgConnection, final StatementCancelState state) {
        ConnectionInfo connectionInfo = cachedConnectionList.get(pgConnection);
        if (connectionInfo != null) {
            connectionInfo.setConnectionState(state);
        }
    }

    /**
     * Set connection.
     *
     * @param pgConnection pgConnection
     * @param properties properties
     * @param hostSpec hostSpec
     */
    public void setConnection(final PgConnection pgConnection, final Properties properties, final HostSpec hostSpec)
        throws PSQLException {
        if (pgConnection == null || properties == null || hostSpec == null) {
            return;
        }
        if (!hostSpec.equals(this.hostSpec)) {
            return;
        }
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        cachedConnectionList.put(pgConnection, connectionInfo);
    }

    /**
     * Get connection info if the connection exits.
     *
     * @param pgConnection pgConnection
     * @return connectionInfo
     */
    public ConnectionInfo getConnectionInfo(PgConnection pgConnection) {
        if (pgConnection == null) {
            return null;
        }
        return cachedConnectionList.get(pgConnection);
    }

    /**
     * Get the size of cachedConnectionList.
     *
     * @return size of cachedConnectionList
     */
    public int getCachedConnectionListSize() {
        return cachedConnectionList.size();
    }

    /**
     * Check dn state and the validity of properties.
     *
     * @param properties properties
     * @return result (dnValid, dnInvalid, propertiesInvalid)
     */
    public CheckDnStateResult checkDnStateAndProperties(Properties properties) {
        boolean isDataNodeValid;
        Properties singleNodeProperties = new Properties();
        PGProperty.USER.set(singleNodeProperties, PGProperty.USER.get(properties));
        PGProperty.PASSWORD.set(singleNodeProperties, PGProperty.PASSWORD.get(properties));
        PGProperty.PG_DBNAME.set(singleNodeProperties, PGProperty.PG_DBNAME.get(properties));
        PGProperty.PG_HOST.set(singleNodeProperties, hostSpec.getHost());
        PGProperty.PG_PORT.set(singleNodeProperties, hostSpec.getPort());
        try {
            isDataNodeValid = checkDnState(singleNodeProperties);
        } catch (PSQLException e) {
            String cause = e.getCause() != null ? e.getCause().getMessage() : "";
            LOGGER.info(GT.tr("Can not try connect to dn: {0}, {1}.", hostSpec.toString(), cause.toString()));
            return CheckDnStateResult.DN_INVALID;
        } catch (InvocationTargetException e) {
            Throwable invocationTargetExceptionCause = e.getCause();
            if (invocationTargetExceptionCause instanceof PSQLException) {
                PSQLException psqlException = (PSQLException) invocationTargetExceptionCause;
                String sqlState = psqlException.getSQLState();
                if (USERNAME_OR_PASSWORD_INVALID_ERROR_CODE.equals(sqlState)) {
                    String cause = e.getCause() != null ? e.getCause().getMessage() : "";
                    LOGGER.info(GT.tr("Cached properties is invalid: {0}.", cause.toString()));
                    return CheckDnStateResult.PROPERTIES_INVALID;
                }
            }
            String cause = e.getCause() != null ? e.getCause().getMessage() : "";
            LOGGER.info(GT.tr("Can not try connect to dn: {0}, {1}.", hostSpec.toString(), cause.toString()));
            return CheckDnStateResult.DN_INVALID;
        }
        if (isDataNodeValid) {
            return CheckDnStateResult.DN_VALID;
        } else {
            return CheckDnStateResult.DN_INVALID;
        }
    }

    /**
     * Filter idle connections from cachedConnectionsList.
     *
     * @param quickAutoBalanceStartTime the time since the start of quickAutoBalance
     * @return idle Connection list
     */
    public List<ConnectionInfo> filterIdleConnections(final long quickAutoBalanceStartTime) {
        synchronized (cachedConnectionList) {
            List<ConnectionInfo> idleConnectionList = new ArrayList<>();
            for (Entry<PgConnection, ConnectionInfo> entry : cachedConnectionList.entrySet()) {
                ConnectionInfo connectionInfo = entry.getValue();
                if (connectionInfo != null && connectionInfo.checkConnectionCanBeClosed(quickAutoBalanceStartTime)) {
                    idleConnectionList.add(connectionInfo);
                }
            }
            return idleConnectionList;
        }
    }

    /**
     * The result of checking dn state.
     */
    public enum CheckDnStateResult {
        DN_VALID,
        DN_INVALID,
        PROPERTIES_INVALID
    }

    public void setDataNodeState(boolean isDnValid) {
        this.dataNodeState = isDnValid;
    }

    public boolean getDataNodeState() {
        return this.dataNodeState;
    }

    /**
     * check a dn of the cluster if valid,
     *
     * @param properties properties
     * @return if the dn is valid.
     * @throws PSQLException psql exception
     * @throws InvocationTargetException invocation target exception
     */
    public boolean checkDnState(Properties properties) throws PSQLException, InvocationTargetException {
        Object pgStream;
        try {
            HostSpec dnHostSpec = new HostSpec(properties.getProperty("PGHOST")
                , Integer.parseInt(properties.getProperty("PGPORT")));
            SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(properties);
            SslMode sslMode = SslMode.of(properties);
            Class<?> classForName = Class.forName("org.postgresql.core.v3.ConnectionFactoryImpl");
            Object object = classForName.newInstance();
            if (!(object instanceof ConnectionFactoryImpl)) {
                LOGGER.error(GT.tr("classForName.newInstance() doesn't instanceof ConnectionFactoryImpl."));
                return false;
            }
            ConnectionFactoryImpl connectionFactory = (ConnectionFactoryImpl) object;
            Method method = connectionFactory.getClass().getDeclaredMethod("tryConnect", String.class,
                String.class, Properties.class, SocketFactory.class, HostSpec.class, SslMode.class);
            method.setAccessible(true);
            pgStream = method.invoke(connectionFactory, properties.getProperty("user"),
                properties.getProperty("PGDBNAME"), properties, socketFactory, dnHostSpec, sslMode);
            if (pgStream instanceof  PGStream) {
                ((PGStream) pgStream).close();
            }
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException |
                 IOException e) {
            throw new PSQLException("The queryExecutor of connection can't execute tryConnect",
                PSQLState.WRONG_OBJECT_TYPE);
        }

        if (pgStream instanceof PGStream) {
            return true;
        } else {
            LOGGER.error(GT.tr("Stream doesn't instanceof PGStream."));
            return false;
        }
    }


    /**
     * Check cached connections validity, and remove invalid connections.
     *
     * @return the amount of removed connections.
     */
    public int checkConnectionsValidity() {
        int num = 0;
        for (Entry<PgConnection, ConnectionInfo> entry : cachedConnectionList.entrySet()) {
            PgConnection pgConnection = entry.getKey();
            ConnectionInfo connectionInfo = entry.getValue();
            if (!connectionInfo.checkConnectionIsValid()) {
                cachedConnectionList.remove(pgConnection);
                num++;
            }
        }
        return num;
    }

    /**
     * Close cached connections, and clear cachedConnectionList.
     * JDBC execute clearCachedConnections when jdbc find an invalid datanode.
     *
     * @return size of cachedConnectionList before cleared
     */
    public int clearCachedConnections() {
        synchronized (cachedConnectionList) {
            int num = cachedConnectionList.size();
            for (Map.Entry<PgConnection, ConnectionInfo> entry : cachedConnectionList.entrySet()) {
                PgConnection pgConnection = entry.getKey();
                if (pgConnection != null) {
                    QueryExecutor queryExecutor = pgConnection.getQueryExecutor();
                    if (queryExecutor != null && !queryExecutor.isClosed()) {
                        queryExecutor.close();
                        queryExecutor.setAvailability(false);
                    }
                } else {
                    LOGGER.error(GT.tr("Fail to close connection, pgConnection = null."));
                }
            }
            cachedConnectionList.clear();
            return num;
        }
    }

    /**
     * Close connection.
     *
     * @param pgConnection pgConnection
     * @return if closed
     */
    public boolean closeConnection(PgConnection pgConnection) {
        if (pgConnection == null) {
            return false;
        }
        ConnectionInfo connectionInfo = cachedConnectionList.remove(pgConnection);
        if (connectionInfo != null) {
            try {
                pgConnection.close();
                return true;
            } catch (SQLException e) {
                LOGGER.info(GT.tr("Connection closed failed."), e);
                return false;
            }
        }
        return false;
    }

    /**
     * get cachedCreatingConnectionSize
     *
     * @return cachedCreatingConnectionSize
     */
    public int getCachedCreatingConnectionSize() {
        return cachedCreatingConnectionSize.get();
    }

    /**
     * increment cachedCreatingConnectionSize
     *
     * @return cachedCreatingConnectionSize after updated
     */
    public int incrementCachedCreatingConnectionSize() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(GT.tr("【quickAutoBalance】incrementCachedCreatingConnectionSize, hostSpec: {0}, before " +
                "increment: {1}", hostSpec.toString(), cachedCreatingConnectionSize.get()));
        }
        return cachedCreatingConnectionSize.incrementAndGet();
    }

    /**
     * decrement cachedCreatingConnectionSize
     *
     * @return cachedCreatingConnectionSize after updated
     */
    public int decrementCachedCreatingConnectionSize() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(GT.tr("【quickAutoBalance】decrementCachedCreatingConnectionSize, hostSpec: {0}, before " +
                "decrement: {1}", hostSpec.toString(), cachedCreatingConnectionSize.get()));
        }
        if (cachedCreatingConnectionSize.get() == 0) {
            // Some of junit tests don't load balance, but setConnection, can generate this error.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(GT.tr("CachedCreatingConnectionSize should not be less than 0, reset to 0."));
            }
            return 0;
        }
        return cachedCreatingConnectionSize.decrementAndGet();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DataNode dataNode = (DataNode) obj;
        return dataNodeState == dataNode.dataNodeState && Objects.equals(hostSpec, dataNode.hostSpec) &&
            Objects.equals(cachedConnectionList, dataNode.cachedConnectionList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostSpec);
    }
}
