/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
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

package org.postgresql.xa;

import org.postgresql.core.BaseConnection;
import org.postgresql.ds.common.BaseDataSource;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.Reference;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 * XA-enabled DataSource implementation.
 *
 * @author Heikki Linnakangas (heikki.linnakangas@iki.fi)
 */
public class PGXADataSource extends BaseDataSource implements XADataSource {
  /**
   * Gets a connection to the PostgreSQL database. The database is identified by the DataSource
   * properties serverName, databaseName, and portNumber. The user to connect as is identified by
   * the DataSource properties user and password.
   *
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public XAConnection getXAConnection() throws SQLException {
    return getXAConnection(getUser(), getPassword());
  }

  /**
   * Gets a XA-enabled connection to the PostgreSQL database. The database is identified by the
   * DataSource properties serverName, databaseName, and portNumber. The user to connect as is
   * identified by the arguments user and password, which override the DataSource properties by the
   * same name.
   *
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public XAConnection getXAConnection(String user, String password) throws SQLException {
    Connection con = super.getConnection(user, password);
    return new PGXAConnection((BaseConnection) con);
  }

  public String getDescription() {
    return "XA-enabled DataSource from " + org.postgresql.util.DriverInfo.DRIVER_FULL_NAME;
  }

  /**
   * Generates a reference using the appropriate object factory.
   */
  protected Reference createReference() {
    return new Reference(getClass().getName(), PGXADataSourceFactory.class.getName(), null);
  }

}
