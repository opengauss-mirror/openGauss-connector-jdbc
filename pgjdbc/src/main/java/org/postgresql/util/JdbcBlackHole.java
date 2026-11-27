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

package org.postgresql.util;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class JdbcBlackHole {
  static Log LOGGER = Logger.getLogger(JdbcBlackHole.class.getName());
  public static void close(Connection con) {
    try {
      if (con != null) {
        con.close();
      }
    } catch (SQLException e) {
        /* ignore for now */
        LOGGER.trace("Catch SQLException on close connection :", e);
    }
  }

  public static void close(Statement s) {
    try {
      if (s != null) {
        s.close();
      }
    } catch (SQLException e) {
        /* ignore for now */
        LOGGER.trace("Catch SQLException on close statement :", e);
    }
  }

  public static void close(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
        /* ignore for now */
        LOGGER.trace("Catch SQLException on close resultset :", e);
    }
  }
}
