package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.QueryExecutor;
import org.postgresql.util.CompatibilityEnum;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PgDatabase {
  private BaseConnection connection;

  private boolean isDolphin;

  public PgDatabase(BaseConnection connection) {
    this.connection = connection;
  }

  public boolean isDolphin() {
    return isDolphin;
  }

  public void setDolphin() throws SQLException {
    String extensionDolphin = getDolphin("select count(1) from pg_extension where extname = 'dolphin';");
    int dolphinNum = Integer.parseInt(extensionDolphin);
    String compatibility = getDolphin("show dolphin.b_compatibility_mode;");

    if (dolphinNum > 0 && CompatibilityEnum.ON.equals(CompatibilityEnum.valueOf(compatibility.toUpperCase()))) {
      isDolphin = true;
    } else {
      isDolphin = false;
    }
  }

  public String getDolphin(String sql) throws SQLException {
    try (PreparedStatement dbStatement = connection.prepareStatement(sql)) {
      if (!((BaseStatement) dbStatement).executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
        throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
      }
      try (ResultSet rs = dbStatement.getResultSet()) {
        if (rs.next()) {
          return rs.getString(1);
        }
        return "";
      }
    }
  }
}
