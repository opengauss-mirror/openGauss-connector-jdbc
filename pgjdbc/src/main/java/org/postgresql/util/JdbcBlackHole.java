/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JdbcBlackHole {
  static Logger LOGGER = Logger.getLogger(JdbcBlackHole.class.getName());
  public static void close(Connection con) {
    try {
      if (con != null) {
        con.close();
      }
    } catch (SQLException e) {
        /* ignore for now */
        LOGGER.log(Level.FINEST, "Catch SQLException on close connection :", e);
    }
  }

  public static void close(Statement s) {
    try {
      if (s != null) {
        s.close();
      }
    } catch (SQLException e) {
        /* ignore for now */
        LOGGER.log(Level.FINEST, "Catch SQLException on close statement :", e);
    }
  }

  public static void close(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
        /* ignore for now */
        LOGGER.log(Level.FINEST, "Catch SQLException on close resultset :", e);
    }
  }
}
