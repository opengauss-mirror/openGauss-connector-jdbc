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

package org.postgresql.ds;

import org.postgresql.ds.common.BaseDataSource;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * PostgreSQL implementation of ConnectionPoolDataSource. The app server or middleware vendor should
 * provide a DataSource implementation that takes advantage of this ConnectionPoolDataSource. If
 * not, you can use the PostgreSQL implementation known as PoolingDataSource, but that should only
 * be used if your server or middleware vendor does not provide their own. Why? The server may want
 * to reuse the same Connection across all EJBs requesting a Connection within the same Transaction,
 * or provide other similar advanced features.
 *
 * <p>
 * In any case, in order to use this ConnectionPoolDataSource, you must set the property
 * databaseName. The settings for serverName, portNumber, user, and password are optional. Note:
 * these properties are declared in the superclass.
 * </p>
 *
 * <p>
 * This implementation supports JDK 1.3 and higher.
 * </p>
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGConnectionPoolDataSource extends BaseDataSource
    implements ConnectionPoolDataSource, Serializable {
  private boolean defaultAutoCommit = true;

  /**
   * Gets a description of this DataSource.
   */
  public String getDescription() {
    return "ConnectionPoolDataSource from " + org.postgresql.util.DriverInfo.DRIVER_FULL_NAME;
  }

  /**
   * Gets a connection which may be pooled by the app server or middleware implementation of
   * DataSource.
   *
   * @throws java.sql.SQLException Occurs when the physical database connection cannot be
   *         established.
   */
  public PooledConnection getPooledConnection() throws SQLException {
    return new PGPooledConnection(getConnection(), defaultAutoCommit);
  }

  /**
   * Gets a connection which may be pooled by the app server or middleware implementation of
   * DataSource.
   *
   * @throws java.sql.SQLException Occurs when the physical database connection cannot be
   *         established.
   */
  public PooledConnection getPooledConnection(String user, String password) throws SQLException {
    return new PGPooledConnection(getConnection(user, password), defaultAutoCommit);
  }

  /**
   * Gets whether connections supplied by this pool will have autoCommit turned on by default. The
   * default value is <tt>false</tt>, so that autoCommit will be turned off by default.
   *
   * @return true if connections supplied by this pool will have autoCommit
   */
  public boolean isDefaultAutoCommit() {
    return defaultAutoCommit;
  }

  /**
   * Sets whether connections supplied by this pool will have autoCommit turned on by default. The
   * default value is <tt>false</tt>, so that autoCommit will be turned off by default.
   *
   * @param defaultAutoCommit whether connections supplied by this pool will have autoCommit
   */
  public void setDefaultAutoCommit(boolean defaultAutoCommit) {
    this.defaultAutoCommit = defaultAutoCommit;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeBaseObject(out);
    out.writeBoolean(defaultAutoCommit);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    readBaseObject(in);
    defaultAutoCommit = in.readBoolean();
  }
}
