/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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

package org.postgresql.util;

import org.postgresql.test.TestUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by davec on 3/14/17.
 */
public class DataBaseCompatibility {

  /**
   * The compatibility enum
   */
  public static enum CompatibilityEnum {
      A("A"),
      MYSQL("B"),
      TERDATA("C"),
      POSTGRES("PG");

      // the type desc
      public final String type;

      CompatibilityEnum(String type) {
        this.type = type;
      }

      /**
       * Build enum by type
       * @param type the type
       * @return enum
       */
      public static CompatibilityEnum str2enum(String type) {
        for (CompatibilityEnum dt: values()) {
          if (dt.type.equals(type)) {
            return dt;
          }
        }
        return A;
      }
  }

  /**
   * Is the A compatibility database
   * @param conn the jdbc connection
   * @return true if A else false
   * @throws SQLException the exception
   */
  public static boolean isADatabase(Connection conn) throws SQLException {
      return CompatibilityEnum.A == getCompatibility(conn);
  }
  /**
   * This is function to get openGauss database compatibility
   * @param conn the jdbc connection
   * @return CompatibilityEnum A:A B:mysql C:TiDB PG:postgresql, default is A
   * @throws SQLException the except from server
   */
  public static CompatibilityEnum getCompatibility(Connection conn) throws SQLException {
    return getCompatibility(conn, conn.getCatalog());
  }
  /**
   * This is function to get openGauss database compatibility
   * @param conn the jdbc connection
   * @param datname the database name
   * @return CompatibilityEnum A:A B:mysql C:TiDB PG:postgresql, default is A
   * @throws SQLException any except from server
   */
  public static CompatibilityEnum getCompatibility(Connection conn, String datname) throws SQLException {
      return CompatibilityEnum.str2enum(communicatDatabase(conn, datname));
  }

  private static String communicatDatabase(Connection conn, String datname) throws SQLException {
    String sql = TestUtil.selectSQL("pg_catalog.pg_database",
            "datcompatibility", "datname=?");
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, datname);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    }
    return "A";
  }
}
