package org.postgresql.jdbc;

import org.postgresql.PGProperty;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.QueryExecutor;
import org.postgresql.util.CompatibilityEnum;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.BitOutputEnum;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

public class PgDatabase {
    private PgConnection connection;

    private boolean isDolphin;

    private boolean isDec;

    public PgDatabase(PgConnection connection) {
        this.connection = connection;
    }

    public boolean isDolphin() {
        return isDolphin;
    }

    public boolean isDec() {
        return isDec;
    }

    /**
     * cache dolphin
     *
     * @param info connection param
     * @throws SQLException execute sql exception
     */
    public void setDolphin(Properties info) throws SQLException {
        String extensionDolphin = getDolphin("select count(1) from pg_extension where extname = 'dolphin';");
        int dolphinNum = Integer.parseInt(extensionDolphin);
        String compatibility = getDolphin("show dolphin.b_compatibility_mode;");
        if (compatibility == null || compatibility.isEmpty()) {
            compatibility = "OFF";
        }
        CompatibilityEnum compatibilityEnum = CompatibilityEnum.valueOf(compatibility.toUpperCase(Locale.ROOT));

        if (dolphinNum > 0 && CompatibilityEnum.ON.equals(compatibilityEnum)) {
            isDolphin = true;
            String bitOutput = PGProperty.BIT_OUTPUT.get(info);
            try {
                if (bitOutput == null) {
                    bitOutput = getDolphin("show dolphin.bit_output;");
                } else {
                    updateBitOutput(bitOutput);
                }
                if (BitOutputEnum.DEC.equals(BitOutputEnum.valueOf(bitOutput.toUpperCase(Locale.ROOT)))) {
                    isDec = true;
                }
            } catch (SQLException e) {
                isDec = false;
            }
        } else {
            isDolphin = false;
        }
    }

    /**
     * get dolphin
     *
     * @param sql execute sql
     * @return dolphin of b database
     * @throws SQLException execute sql exception
     */
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

    private void updateBitOutput(String bitOutput) throws SQLException {
        /* set parameter cannot use prepareStatement to set the value */
        try (Statement stmt = connection.createStatement()) {
            String sql = "set dolphin.bit_output to " + bitOutput;
            stmt.execute(sql);
        } catch (SQLException e) {
            throw e;
        }
    }
}
