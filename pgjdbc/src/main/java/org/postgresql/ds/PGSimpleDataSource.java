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

import javax.sql.DataSource;

/**
 * Simple DataSource which does not perform connection pooling. In order to use the DataSource, you
 * must set the property databaseName. The settings for serverName, portNumber, user, and password
 * are optional. Note: these properties are declared in the superclass.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGSimpleDataSource extends BaseDataSource implements DataSource, Serializable {
  /**
   * Gets a description of this DataSource.
   */
  public String getDescription() {
    return "Non-Pooling DataSource from " + org.postgresql.util.DriverInfo.DRIVER_FULL_NAME;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeBaseObject(out);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    readBaseObject(in);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
      throw org.postgresql.Driver.notImplemented(this.getClass(), "isWrapperFor(Class<?>)");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw org.postgresql.Driver.notImplemented(this.getClass(), "unwrap(Class<T>)");
  }
}
