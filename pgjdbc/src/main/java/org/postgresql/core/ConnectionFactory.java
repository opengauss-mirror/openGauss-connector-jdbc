/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core;

import org.postgresql.PGProperty;
import org.postgresql.core.v3.ConnectionFactoryImpl;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;


/**
 * Handles protocol-specific connection setup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public abstract class ConnectionFactory {
  private static Log LOGGER = Logger.getLogger(ConnectionFactory.class.getName());

  /**
   * connection info
   */
  protected ORBaseConnection connection;

  public ORBaseConnection getConnection() {
    return connection;
  }

  /**
   * <p>Establishes and initializes a new connection.</p>
   *
   * <p>If the "protocolVersion" property is specified, only that protocol version is tried. Otherwise,
   * all protocols are tried in order, falling back to older protocols as necessary.</p>
   *
   * <p>Currently, protocol versions 3 (7.4+) is supported.</p>
   *
   * @param hostSpecs at least one host and port to connect to; multiple elements for round-robin
   *        failover
   * @param user the username to authenticate with; may not be null.
   * @param database the database on the server to connect to; may not be null.
   * @param info extra properties controlling the connection; notably, "password" if present
   *        supplies the password to authenticate with.
   * @param givenIP an IP for reconnection when using ATF
   * @return the new, initialized, connection
   * @throws SQLException if the connection could not be established.
   */
  public static QueryExecutor openConnection(HostSpec[] hostSpecs, String user,
      String database, Properties info, String... givenIP) throws SQLException {
    String protoName = PGProperty.PROTOCOL_VERSION.get(info);

    if (protoName == null || protoName.isEmpty() || "3".equals(protoName)) {
      ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
      QueryExecutor queryExecutor = connectionFactory.openConnectionImpl(
          hostSpecs, user, database, info, givenIP);
      if (queryExecutor != null) {
        return queryExecutor;
      }
    }

    throw new PSQLException(
        GT.tr("A connection could not be made using the requested protocol {0}.", protoName),
        PSQLState.CONNECTION_UNABLE_TO_CONNECT);
  }

  /**
   * establishes and initializes a new connection with oGRAC
   *
   * @param hostSpecs host info
   * @param connection connection
   * @param info connection info
   * @throws SQLException if a database access error occurs
   * @throws IOException if an I/O error occurs
   */
  public static void openORConnection(HostSpec[] hostSpecs, ORBaseConnection connection,
                                      Properties info) throws SQLException, IOException {
    ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
    connectionFactory.openORConnectionImpl(hostSpecs, connection, info);
  }

  /**
   * Implementation of {@link #openConnection} for a particular protocol version. Implemented by
   * subclasses of {@link ConnectionFactory}.
   *
   * @param hostSpecs at least one host and port to connect to; multiple elements for round-robin
   *        failover
   * @param user the username to authenticate with; may not be null.
   * @param database the database on the server to connect to; may not be null.
   * @param info extra properties controlling the connection; notably, "password" if present
   *        supplies the password to authenticate with.
   * @param givenIP an IP for reconnection when using ATF
   * @return the new, initialized, connection, or <code>null</code> if this protocol version is not
   *         supported by the server.
   * @throws SQLException if the connection could not be established for a reason other than
   *         protocol version incompatibility.
   */
  public abstract QueryExecutor openConnectionImpl(HostSpec[] hostSpecs, String user,
      String database, Properties info, String... givenIP) throws SQLException;

  /**
   * Implementation of connection for oGRAC protocol
   *
   * @param hostSpecs host info
   * @param connection connection
   * @param info connection info
   * @throws SQLException if a database access error occurs
   * @throws IOException if an I/O error occurs
   */
  public abstract void openORConnectionImpl(HostSpec[] hostSpecs, ORBaseConnection connection,
                                            Properties info) throws SQLException, IOException;

  /**
   * Safely close the given stream.
   *
   * @param newStream The stream to close.
   */
  protected void closeStream(PGStream newStream) {
    if (newStream != null) {
      try {
        newStream.close();
      } catch (IOException e) {
        LOGGER.trace("Catch IOException on close:", e);
      }
    }
  }
}
