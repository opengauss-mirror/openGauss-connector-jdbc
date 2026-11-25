/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
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
