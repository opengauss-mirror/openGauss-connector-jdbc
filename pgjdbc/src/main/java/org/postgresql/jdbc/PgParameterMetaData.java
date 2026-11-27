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

package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class PgParameterMetaData implements ParameterMetaData {

  private final BaseConnection _connection;
  private final int[] _oids;

  public PgParameterMetaData(BaseConnection connection, int[] oids) {
    _connection = connection;
    _oids = oids;
  }

  public String getParameterClassName(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().getJavaClass(_oids[param - 1]);
  }

  public int getParameterCount() {
    return _oids.length;
  }

  /**
   * {@inheritDoc} For now report all parameters as inputs. CallableStatements may have one output,
   * but ignore that for now.
   */
  public int getParameterMode(int param) throws SQLException {
    checkParamIndex(param);
    return ParameterMetaData.parameterModeIn;
  }

  public int getParameterType(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().getSQLType(_oids[param - 1]);
  }

  public String getParameterTypeName(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().getPGType(_oids[param - 1]);
  }

  // we don't know this
  public int getPrecision(int param) throws SQLException {
    checkParamIndex(param);
    return 0;
  }

  // we don't know this
  public int getScale(int param) throws SQLException {
    checkParamIndex(param);
    return 0;
  }

  // we can't tell anything about nullability
  public int isNullable(int param) throws SQLException {
    checkParamIndex(param);
    return ParameterMetaData.parameterNullableUnknown;
  }

  /**
   * {@inheritDoc} PostgreSQL doesn't have unsigned numbers
   */
  public boolean isSigned(int param) throws SQLException {
    checkParamIndex(param);
    return _connection.getTypeInfo().isSigned(_oids[param - 1]);
  }

  private void checkParamIndex(int param) throws PSQLException {
    if (param < 1 || param > _oids.length) {
      throw new PSQLException(
          GT.tr("The parameter index is out of range: {0}, number of parameters: {1}.",
              param, _oids.length),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
      throw org.postgresql.Driver.notImplemented(this.getClass(), "isWrapperFor(Class<?>)");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
	  throw org.postgresql.Driver.notImplemented(this.getClass(), "unwrap(Class<?>)");
  }
}
