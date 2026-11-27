/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
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

package org.postgresql.core;

import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public interface TypeInfo {
  void addCoreType(String pgTypeName, Integer oid, Integer sqlType, String javaClass,
      Integer arrayOid);

  void addDataType(String type, Class<? extends PGobject> klass) throws SQLException;

  /**
   * Look up the SQL typecode for a given type oid.
   *
   * @param oid the type's OID
   * @return the SQL type code (a constant from {@link java.sql.Types}) for the type
   * @throws SQLException if an error occurs when retrieving sql type
   */
  int getSQLType(int oid) throws SQLException;

  /**
   * Look up the SQL typecode for a given postgresql type name.
   *
   * @param pgTypeName the server type name to look up
   * @return the SQL type code (a constant from {@link java.sql.Types}) for the type
   * @throws SQLException if an error occurs when retrieving sql type
   */
  int getSQLType(String pgTypeName) throws SQLException;

  /**
   * init all types from table pg_type.
   *
   * @throws SQLException if an error occurs when retrieving sql type
   */
  void setPGTypes() throws SQLException;

  /**
   * Look up the oid for a given postgresql type name. This is the inverse of
   * {@link #getPGType(int)}.
   *
   * @param pgTypeName the server type name to look up
   * @return the type's OID, or 0 if unknown
   * @throws SQLException if an error occurs when retrieving PG type
   */
  int getPGType(String pgTypeName) throws SQLException;

  /**
   * Look up the postgresql type name for a given oid. This is the inverse of
   * {@link #getPGType(String)}.
   *
   * @param oid the type's OID
   * @return the server type name for that OID or null if unknown
   * @throws SQLException if an error occurs when retrieving PG type
   */
  String getPGType(int oid) throws SQLException;

  /**
   * Look up the oid of an array's base type given the array's type oid.
   *
   * @param oid the array type's OID
   * @return the base type's OID, or 0 if unknown
   * @throws SQLException if an error occurs when retrieving array element
   */
  int getPGArrayElement(int oid) throws SQLException;

  /**
   * Determine the oid of the given base postgresql type's array type.
   *
   * @param elementTypeName the base type's
   * @return the array type's OID, or 0 if unknown
   * @throws SQLException if an error occurs when retrieving array type
   */
  int getPGArrayType(String elementTypeName) throws SQLException;

  /**
   * Determine the delimiter for the elements of the given array type oid.
   *
   * @param oid the array type's OID
   * @return the base type's array type delimiter
   * @throws SQLException if an error occurs when retrieving array delimiter
   */
  char getArrayDelimiter(int oid) throws SQLException;

  Iterator<String> getPGTypeNamesWithSQLTypes();

  Class<? extends PGobject> getPGobject(String type);

  String getJavaClass(int oid) throws SQLException;

  String getTypeForAlias(String alias);

  int getPrecision(int oid, int typmod);

  int getScale(int oid, int typmod);

  boolean isCaseSensitive(int oid);

  boolean isSigned(int oid);

  int getDisplaySize(int oid, int typmod);

  int getMaximumPrecision(int oid);

  boolean requiresQuoting(int oid) throws SQLException;

  /**
   * Returns true if particular sqlType requires quoting.
   * This method is used internally by the driver, so it might disappear without notice.
   *
   * @param sqlType sql type as in java.sql.Types
   * @return true if the type requires quoting
   * @throws SQLException if something goes wrong
   */
  boolean requiresQuotingSqlType(int sqlType) throws SQLException;

    /**
     * Returns the attributes sql type list of the object based on oid
     *
     * @param oid the type's OID
     * @return the attributes sql type list
     * @throws SQLException if something goes wrong
     */
    List<Integer> getStructAttributesOid(int oid) throws SQLException;

    /**
     * Returns the attribute name list based on oid
     *
     * @param oid the type's OID
     * @return the attributes name list
     * @throws SQLException if something goes wrong
     */
    Object[] getStructAttributesName(int oid) throws SQLException;
}