/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.RowIdLifetime;

/**
 * DatabaseMetaData.
 *
 * @author zhangting
 * @since  2026-06-16
 */
public class ORDatabaseMetaData implements DatabaseMetaData {
    private static final String[] DEFAULT_TABLE = {"DYNAMIC VIEW", "TABLE", "VIEW"};
    private static final int JDBC_MAJOR_VERSION = 7;
    private static final int JDBC_MINOR_VERSION = 0;
    private static final int MAX_SQL_LENGTH = 65535;

    private final ORConnection connection;

    public ORDatabaseMetaData(ORConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public String getUserName() {
        return connection.getUser();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return true;
    }

    @Override
    public String getDatabaseProductVersion() {
        return "7.0.0";
    }

    @Override
    public String getDatabaseProductName() {
        return "oGRAC";
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    @Override
    public String getDriverName() {
        return "oGRAC JDBC Driver";
    }

    @Override
    public String getDriverVersion() {
        return "7.0.0";
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return true;
    }

    @Override
    public int getDriverMajorVersion() {
        return JDBC_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return JDBC_MINOR_VERSION;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getSQLKeywords() {
        return "BLOB,BY,CHAR,CHECK,REAL,FALSE,FLOAT,FOR,BIGINT,RIGHT,CHECKPOINT,CLOB,HAVING,TABLESPACE,THEN,TIMESTAMP,"
                + "SYSDATE,SYSTIMESTAMP,IN,NOT,NOW,NULL,NUMERIC,ON,DAY,VARCHAR,CURRENT_TIMESTAMP,FROM,DECIMAL,DELETE,"
                + "AS,ASC,DUPLICATE,EXECUTE,EXISTS,VALUES,VARBINARY,SELECT,EXPLAIN,FETCH,INTO,IS,LAST_INSERT_ID,LEFT,"
                + "LIKE,LIMIT,MONTH,ROLLBACK,ROWNUM,DROP,OR,BINARY,DESC,DISTINCT,DOUBLE,ORDER,INDEX,INSERT,INT,"
                + "INTEGER,ALL,ALTER,BETWEEN,DATABASE,DATE,DATETIME,AND,COLUMN,COMMIT,CREATE,CURDATE,CURRENT_DATE,"
                + "ADD,GROUP,PARTITION,SYSUTC,TRUE,TRUNCATE,UPDATE,UTC,ELSE,END,SET,SHUTDOWN,TABLE,UNION,VARCHAR2,"
                + "WHEN,WHERE,YEAR";
    }

    @Override
    public String getNumericFunctions() {
        return EscapedFunctions.ROUND + ',' + EscapedFunctions.SIN + ',' + EscapedFunctions.LOG10 + ','
                + EscapedFunctions.ATAN + ',' + EscapedFunctions.CEILING + ',' + EscapedFunctions.PI + ','
                + EscapedFunctions.TRUNCATE + ',' + EscapedFunctions.ASIN + ',' + EscapedFunctions.ATAN2 + ','
                + EscapedFunctions.COS + ',' + EscapedFunctions.MOD + ',' + EscapedFunctions.EXP + ','
                + EscapedFunctions.FLOOR + ',' + EscapedFunctions.POWER + ',' + EscapedFunctions.SQRT + ','
                + EscapedFunctions.SIGN + ',' + EscapedFunctions.ABS + ',' + EscapedFunctions.TAN + ','
                + EscapedFunctions.LOG + ',' + EscapedFunctions.ACOS;
    }

    @Override
    public String getStringFunctions() {
        return EscapedFunctions.UCASE + ',' + EscapedFunctions.CHAR + ',' + EscapedFunctions.ASCII + ','
                + EscapedFunctions.LCASE + ',' + EscapedFunctions.CONCAT + ',' + EscapedFunctions.LENGTH + ','
                + EscapedFunctions.REPLACE + ',' + EscapedFunctions.LTRIM + ',' + EscapedFunctions.SUBSTRING + ','
                + EscapedFunctions.RTRIM;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public String getSystemFunctions() {
        return EscapedFunctions.USER;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public String getTimeDateFunctions() {
        return "GETUTCDATE,CURRENT_TIMESTAMP,NEXT_DAY,UTC_TIMESTAMP,SYS_EXTRACT_UTC,FROM_UNIXTIME,FROM_TZ,"
                + "UTC_DATE,LOCALTIMESTAMP,SECOND,TO_TIMESTAMP,SYSDATE,YEAR,UNIX_TIMESTAMP,CURRENT_DATE,"
                + "CURDATE,ADD_MONTHS,GSCN2DATE,EXTRACT,TIMESTAMPDIFF,HOUR,SYSTIMESTAMP,TO_DATE,LAST_DAY,"
                + "NOW,TRUNC,MONTHS_BETWEEN,TIMESTAMPADD,SLEEP,MINUTE";
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public String getExtraNameCharacters() {
        return "$#";
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return true;
    }

    @Override
    public String getCatalogTerm() {
        return "";
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return EscapedFunctions.USER;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return true;
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public String getCatalogSeparator() {
        return "";
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 32;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public int getMaxStatementLength() {
        return MAX_SQL_LENGTH;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 30;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 30;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 1000;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 30;
    }

    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return true;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 30;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public int getMaxUserNameLength() {
        return 30;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return true;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        String procedure = procedureNamePattern;
        if (procedure == null) {
            procedure = "%";
        }
        String schemaName = null;
        if (schemaPattern == null) {
            schemaName = "%";
        } else if (schemaPattern.isEmpty()) {
            schemaName = connection.getUser();
        } else {
            schemaName = schemaPattern;
        }

        if (catalog == null) {
            return getCatalogNullProcedures(schemaName, procedure);
        } else if (catalog.isEmpty()) {
            return getCatalogEmptyProcedures(schemaName, procedure);
        } else {
            return getCatalogProcedures(catalog, schemaName, procedure);
        }
    }

    private ResultSet getCatalogNullProcedures(String schemaName, String procedure) throws SQLException {
        String sql = "SELECT NULL AS PROCEDURE_CAT, OWNER AS PROCEDURE_SCHEM, OBJECT_NAME AS PROCEDURE_NAME, "
                + "NULL, NULL,NULL, 'STANDALONE PROCEDURE OR FUNCTION' AS REMARKS,  DECODE(OBJECT_TYPE, 'PROCEDURE', "
                + " 1,'FUNCTION', 2, 0)  AS PROCEDURE_TYPE,  NULL AS SPECIFIC_NAME FROM DB_OBJECTS WHERE "
                + "(OBJECT_TYPE = 'PROCEDURE' OR OBJECT_TYPE = 'FUNCTION')  AND OWNER LIKE ?  AND OBJECT_NAME LIKE ? "
                + "UNION ALL SELECT K.PACKAGE_NAME AS PROCEDURE_CAT, K.OWNER AS PROCEDURE_SCHEM, K.OBJECT_NAME AS "
                + "PROCEDURE_NAME, NULL, NULL, NULL, 'PACKAGED PROCEDURE' AS REMARKS, 1 AS PROCEDURE_TYPE, "
                + "NULL AS SPECIFIC_NAME FROM DB_ARGUMENTS K WHERE ARGUMENT_NAME IS NULL  AND DATA_TYPE IS NULL AND "
                + "K.PACKAGE_NAME IS NOT NULL AND K.OWNER LIKE ? AND K.OBJECT_NAME LIKE ? UNION ALL SELECT "
                + "K.PACKAGE_NAME AS PROCEDURE_CAT, K.OWNER AS PROCEDURE_SCHEM, K.OBJECT_NAME AS PROCEDURE_NAME, "
                + "NULL, NULL, NULL, 'PACKAGED PROCEDURE' AS REMARKS, 1 AS PROCEDURE_TYPE, NULL AS SPECIFIC_NAME "
                + "FROM DB_ARGUMENTS K,DB_PL_MANAGER P WHERE K.ARGUMENT_NAME IS NOT NULL AND K.PACKAGE_NAME = "
                + "P.PACKAGE_NAME AND K.OWNER = P.USER_NAME AND K.OBJECT_NAME = P.NAME AND P.TYPE = 'PROCEDURE' AND "
                + "K.PACKAGE_NAME IS NOT NULL AND K.OWNER LIKE ? AND K.OBJECT_NAME LIKE ? UNION ALL SELECT  "
                + "K.PACKAGE_NAME AS PROCEDURE_CAT, K.OWNER AS PROCEDURE_SCHEM, K.OBJECT_NAME AS PROCEDURE_NAME, "
                + "NULL, NULL, NULL, 'PACKAGED FUNCTION' AS REMARKS, 2 AS PROCEDURE_TYPE,  NULL AS SPECIFIC_NAME "
                + "FROM DB_ARGUMENTS K WHERE K.ARGUMENT_NAME IS NULL AND K.IN_OUT = 'OUT' AND   K.DATA_LEVEL = 0 AND "
                + "K.PACKAGE_NAME IS NOT NULL AND K.OWNER LIKE ? AND K.OBJECT_NAME LIKE ? ORDER BY PROCEDURE_SCHEM, "
                + "PROCEDURE_NAME ";
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            for (int i = 1; i <= 8; i++) {
                if (i % 2 == 1) {
                    ps.setString(i, schemaName);
                } else {
                    ps.setString(i, procedure);
                }
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    private ResultSet getCatalogEmptyProcedures(String schemaName, String procedure) throws SQLException {
        String sql = "SELECT NULL AS PROCEDURE_CAT, OWNER AS PROCEDURE_SCHEM, OBJECT_NAME AS PROCEDURE_NAME, NULL, "
                + "NULL, NULL, 'STANDALONE PROCEDURE OR FUNCTION' AS REMARKS,  DECODE(OBJECT_TYPE, 'PROCEDURE', "
                + "1, 'FUNCTION', 2, 0)  AS PROCEDURE_TYPE,  NULL AS SPECIFIC_NAME FROM DB_OBJECTS WHERE "
                + "(OBJECT_TYPE = 'PROCEDURE' OR OBJECT_TYPE = 'FUNCTION')  AND OWNER LIKE ?  AND OBJECT_NAME LIKE ? ";
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            ps.setString(1, schemaName);
            ps.setString(2, procedure);
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    private ResultSet getCatalogProcedures(String catalog, String schemaName, String procedure) throws SQLException {
        String sql = "SELECT K.PACKAGE_NAME AS PROCEDURE_CAT, K.OWNER AS PROCEDURE_SCHEM, K.OBJECT_NAME AS "
                + "PROCEDURE_NAME, NULL, NULL, NULL, 'PACKAGED PROCEDURE' AS REMARKS, 1 AS PROCEDURE_TYPE, "
                + "NULL AS SPECIFIC_NAME FROM DB_ARGUMENTS K WHERE ARGUMENT_NAME IS NULL  AND DATA_TYPE IS NULL AND "
                + "K.PACKAGE_NAME LIKE ?  AND K.OWNER LIKE ? AND K.OBJECT_NAME LIKE ? UNION ALL SELECT "
                + "K.PACKAGE_NAME AS PROCEDURE_CAT, K.OWNER AS PROCEDURE_SCHEM, K.OBJECT_NAME AS PROCEDURE_NAME, "
                + "NULL, NULL, NULL, 'PACKAGED PROCEDURE' AS REMARKS, 1 AS PROCEDURE_TYPE, NULL AS SPECIFIC_NAME "
                + "FROM DB_ARGUMENTS K,DB_PL_MANAGER P WHERE K.ARGUMENT_NAME IS NOT NULL AND K.PACKAGE_NAME = "
                + "P.PACKAGE_NAME AND K.OWNER = P.USER_NAME AND K.OBJECT_NAME = P.NAME AND P.TYPE = 'PROCEDURE' "
                + "AND K.PACKAGE_NAME LIKE ?  AND K.OWNER LIKE ? AND K.OBJECT_NAME LIKE ? UNION ALL SELECT "
                + "K.PACKAGE_NAME AS PROCEDURE_CAT, K.OWNER AS PROCEDURE_SCHEM, K.OBJECT_NAME AS PROCEDURE_NAME, "
                + "NULL, NULL, NULL, 'PACKAGED FUNCTION' AS REMARKS, 2 AS PROCEDURE_TYPE,  NULL AS SPECIFIC_NAME "
                + "FROM DB_ARGUMENTS K WHERE K.ARGUMENT_NAME IS NULL AND K.IN_OUT = 'OUT' AND   K.DATA_LEVEL = 0 "
                + "AND K.PACKAGE_NAME LIKE ?  AND K.OWNER LIKE ? AND K.OBJECT_NAME LIKE ? ORDER BY PROCEDURE_SCHEM, "
                + "PROCEDURE_NAME";
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            for (int i = 1; i <= 9; i++) {
                if (i % 3 == 1) {
                    ps.setString(i, catalog);
                } else if (i % 3 == 2) {
                    ps.setString(i, schemaName);
                } else {
                    ps.setString(i, procedure);
                }
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        String initSql = "SELECT PACKAGE_NAME AS PROCEDURE_CAT,OWNER AS PROCEDURE_SCHEM, OBJECT_NAME AS "
                + "PROCEDURE_NAME,ARGUMENT_NAME AS COLUMN_NAME, DECODE(IN_OUT, 'IN', 1, 'IN OUT', 2, 'OUT', 4, 0) "
                + "AS COLUMN_TYPE,DECODE (DATA_TYPE, 'BINARY_INTEGER', 4,'BINARY_DOUBLE', 8, 'BINARY_UINT32',4, "
                + "'BINARY_BIGINT',-5,'NUMBER', 2, 'IMAGE',2004, 'CHAR', 1,'CLOB',2005,'VARCHAR',12,'BINARY',-2, "
                + " 'VARBINARY',-3,'BLOB',2004,'DATETIME',91,'DATE',91, 'TIMESTAMP',93,'TIMESTAMP_TZ',2014, "
                + "'TIMESTAMP_LTZ',93,'BOOLEAN',16,'INTERVAL YEAR TO MONTH',1111,'INTERVAL DAY TO SECOND',1111, "
                + "'BINARY_INTEGER[]',2003,'BINARY_DOUBLE[]', 2003, 'BINARY_UINT32[]',2003,'BINARY_BIGINT[]',"
                + "2003,'NUMBER[]',2003, 'CHAR[]',2003,'VARCHAR[]',2003,'DATE[]',2003,'TIMESTAMP[]',2003,'BOOLEAN[]', "
                + "2003,'TIMESTAMP_TZ[]',2003,'TIMESTAMP_LTZ[]',2003, 1111) AS DATA_TYPE, DATA_TYPE AS TYPE_NAME, "
                + "DATA_PRECISION AS PRECISION,DATA_LENGTH AS LENGTH,DATA_SCALE AS SCALE, 10 AS RADIX,1 AS NULLABLE, "
                + " NULL AS REMARKS, DEFAULT_VALUE AS COLUMN_DEF, NULL AS SQL_DATA_TYPE,  NULL AS SQL_DATETIME_SUB, "
                + "DECODE(DATA_TYPE,'CHAR', 8000,'VARCHAR',8000,'RAW', 8000,'BINARY', 8000, 'VARBINARY', 8000,NULL) "
                + "AS CHAR_OCTET_LENGTH, (SEQUENCE - 1) AS ORDINAL_POSITION,'YES' AS IS_NULLABLE, NULL AS "
                + "SPECIFIC_NAME FROM DB_ARGUMENTS WHERE OWNER LIKE ? ESCAPE '/' AND OBJECT_NAME LIKE ? ESCAPE '/' "
                + "AND DATA_LEVEL = 0 ";
        StringBuilder sql = new StringBuilder();
        sql.append(initSql);
        if (catalog != null) {
            if (catalog.isEmpty()) {
                sql.append(" AND PACKAGE_NAME IS NULL ");
            } else {
                sql.append(" AND PACKAGE_NAME LIKE ? ESCAPE '/' ");
            }
        }

        if (columnNamePattern != null && !columnNamePattern.equals("%")) {
            sql.append(" AND ARGUMENT_NAME LIKE ? ESCAPE '/'");
        } else {
            sql.append(" AND (ARGUMENT_NAME LIKE ? ESCAPE '/' OR (ARGUMENT_NAME IS NULL AND DATA_TYPE IS NOT NULL))");
        }
        sql.append(" ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME");
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql.toString());
            if (schemaPattern == null) {
                ps.setString(1, "%");
            } else if (schemaPattern.isEmpty()) {
                ps.setString(1, this.getUserName());
            } else {
                ps.setString(1, schemaPattern);
            }
            if (procedureNamePattern == null) {
                ps.setString(2, "%");
            } else {
                ps.setString(2, procedureNamePattern);
            }

            String column = columnNamePattern;
            if (column == null) {
                column = "%";
            }
            if (catalog != null && !catalog.isEmpty()) {
                ps.setString(3, catalog);
                ps.setString(4, column);
            } else {
                ps.setString(3, column);
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT NULL AS TABLE_CAT,k.OWNER AS TABLE_SCHEM,k.OBJECT_NAME AS TABLE_NAME,k.OBJECT_TYPE AS "
                + "TABLE_TYPE,'' AS REMARKS,NULL AS TYPE_CAT,NULL AS TYPE_SCHEM,NULL AS TYPE_NAME,NULL AS "
                + "SELF_REFERENCING_COL_NAME,NULL AS REF_GENERATION FROM DB_OBJECTS k WHERE k.OWNER LIKE ? "
                + "ESCAPE '/' AND k.OBJECT_NAME LIKE ? ESCAPE '/' AND k.OBJECT_TYPE IN (");
        String[] tables = (types == null || types.length == 0) ? DEFAULT_TABLE : types;
        for (int i = 0; i < tables.length; i++) {
            if (i == 0) {
                sql.append('?');
            } else {
                sql.append(",?");
            }
            if (i == tables.length - 1) {
                sql.append(")");
            }
        }

        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql.toString());
            if (schemaPattern != null) {
                ps.setString(1, schemaPattern);
            } else {
                ps.setString(1, "%");
            }

            if (tableNamePattern != null) {
                ps.setString(2, tableNamePattern);
            } else {
                ps.setString(2, "%");
            }
            int parameterIndex = 3;
            int index = 0;
            for (String type : tables) {
                ps.setString(parameterIndex + index, type);
                index++;
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        String sql = "SELECT USERNAME AS TABLE_SCHEM FROM DB_USERS ORDER BY TABLE_SCHEM";
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        String sql = "SELECT NULL AS TABLE_CAT FROM SYS_DUMMY WHERE FALSE";
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            return ps.executeQuery();
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        PreparedStatement ps = null;
        String sql = "SELECT 'TABLE' AS TABLE_TYPE FROM SYS_DUMMY UNION SELECT 'VIEW' AS TABLE_TYPE FROM "
                + "SYS_DUMMY UNION SELECT 'DYNAMIC VIEW' AS TABLE_TYPE from SYS_DUMMY UNION SELECT 'RECYCLED TABLE' "
                + "AS TABLE_TYPE from SYS_DUMMY UNION SELECT 'RECYCLED INDEX' AS TABLE_TYPE from SYS_DUMMY ";
        try {
            ps = this.connection.prepareStatement(sql);
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        String sql = "SELECT NULL AS TABLE_CAT, k.OWNER AS TABLE_SCHEM,  k.TABLE_NAME AS TABLE_NAME,  k.COLUMN_NAME "
                + "AS COLUMN_NAME, DATA_TYPE,  k.DATA_TYPE AS TYPE_NAME,  DECODE(k.DATA_SCALE, NULL, k.DATA_LENGTH, "
                + "k.DATA_PRECISION) AS COLUMN_SIZE,  0 AS BUFFER_LENGTH,  k.DATA_SCALE AS DECIMAL_DIGITS,  10 AS "
                + "NUM_PREC_RADIX ,  CASE k.NULLABLE WHEN 'Y' THEN 1 ELSE 0 END AS NULLABLE ,  C.COMMENTS AS REMARKS , "
                + "'' AS COLUMN_DEF ,  0 AS SQL_DATA_TYPE ,  0 AS SQL_DATETIME_SUB,  0 AS CHAR_OCTET_LENGTH ,  "
                + "k.COLUMN_ID + 1 AS ORDINAL_POSITION ,  '' AS IS_NULLABLE ,  '' AS SCOPE_CATLOG ,  '' AS "
                + "SCOPE_SCHEMA , '' AS SCOPE_TABLE ,  NULL AS SOURCE_DATA_TYPE ,  '' AS IS_AUTOINCREMENT  FROM "
                + "DB_TAB_COLUMNS k, DB_COL_COMMENTS C  WHERE k.OWNER LIKE ? ESCAPE '/' AND k.TABLE_NAME LIKE ? "
                + "ESCAPE '/'  AND k.COLUMN_NAME LIKE ? ESCAPE '/' AND k.OWNER=C.OWNER (+) AND k.TABLE_NAME="
                + "C.TABLE_NAME (+) AND k.COLUMN_NAME=C.COLUMN_NAME (+) ORDER BY TABLE_SCHEM, TABLE_NAME, "
                + "ORDINAL_POSITION";
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            if (schemaPattern != null) {
                ps.setString(1, schemaPattern);
            } else {
                ps.setString(1, "%");
            }

            if (tableNamePattern != null) {
                ps.setString(2, tableNamePattern);
            } else {
                ps.setString(2, "%");
            }
            if (columnNamePattern != null) {
                ps.setString(3, columnNamePattern);
            } else {
                ps.setString(3, "%");
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getColumnPrivileges(String,String,String,String)");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getTablePrivileges(String,String,String)");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        throw Driver.notImplemented(this.getClass(),
                "getBestRowIdentifier(String,String,String,int,boolean)");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getVersionColumns(String,String,String)");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        PreparedStatement ps = null;
        String sql = "SELECT NULL AS TABLE_CAT, C.OWNER AS TABLE_SCHEM, C.TABLE_NAME, C.COLUMN_NAME, C.POSITION "
                + "AS KEY_SEQ, C.CONSTRAINT_NAME AS PK_NAME FROM DB_CONS_COLUMNS C, DB_CONSTRAINTS K WHERE "
                + "K.CONSTRAINT_TYPE = 'P' AND K.TABLE_NAME = ? AND K.OWNER LIKE ? ESCAPE '/' AND K.CONSTRAINT_NAME = "
                + "C.CONSTRAINT_NAME AND K.TABLE_NAME = C.TABLE_NAME AND K.OWNER = C.OWNER ORDER BY COLUMN_NAME";
        try {
            ps = this.connection.prepareStatement(sql);
            ps.setString(1, table);
            if (schema == null) {
                ps.setString(2, "%");
            } else {
                ps.setString(2, schema);
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        String s1 = "SELECT NULL AS PKTABLE_CAT, P.OWNER AS PKTABLE_SCHEM,P.TABLE_NAME AS "
                + "PKTABLE_NAME, PC.COLUMN_NAME AS PKCOLUMN_NAME,NULL AS FKTABLE_CAT,F.OWNER AS "
                + "FKTABLE_SCHEM, F.TABLE_NAME AS FKTABLE_NAME,FC.COLUMN_NAME AS FKCOLUMN_NAME, "
                + "FC.POSITION AS KEY_SEQ,NULL AS UPDATE_RULE,DECODE(F.DELETE_RULE,'DELETE CASCADE',0,'SET NULL',2,1) "
                + "AS DELETE_RULE, F.CONSTRAINT_NAME AS FK_NAME,P.CONSTRAINT_NAME AS PK_NAME, DECODE(F.DEFERRABLE, "
                + "'DEFERRABLE',5,'NOT DEFERRABLE',7, 'DEFERRED', 6 ) DEFERRABILITY  FROM DB_CONS_COLUMNS PC, "
                + "DB_CONSTRAINTS P, DB_CONS_COLUMNS FC, DB_CONSTRAINTS F WHERE 1=1 ";

        String s2 = " AND F.CONSTRAINT_TYPE = 'R' AND P.OWNER = F.R_OWNER AND P.CONSTRAINT_NAME = "
                + "F.R_CONSTRAINT_NAME AND P.CONSTRAINT_TYPE = 'P' AND PC.OWNER = P.OWNER AND PC.CONSTRAINT_NAME = "
                + "P.CONSTRAINT_NAME AND PC.TABLE_NAME = P.TABLE_NAME AND FC.OWNER = F.OWNER AND "
                + "FC.CONSTRAINT_NAME = F.CONSTRAINT_NAME AND FC.TABLE_NAME = F.TABLE_NAME AND FC.POSITION = "
                + "PC.POSITION ORDER BY PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";
        PreparedStatement ps = null;
        try {
            StringBuilder importedKeySql = new StringBuilder();
            importedKeySql.append(s1);
            int tableIndex = 0;
            int p = 1;
            if (table != null) {
                tableIndex = p++;
                importedKeySql.append(" AND F.TABLE_NAME = ? ");
            }

            int ownerIndex = 0;
            if (schema != null && !schema.isEmpty()) {
                ownerIndex = p++;
                importedKeySql.append(" AND F.OWNER = ? ");
            }

            importedKeySql.append(s2);
            ps = this.connection.prepareStatement(importedKeySql.toString());
            if (table != null) {
                ps.setString(tableIndex, table);
            }

            if (schema != null && !schema.isEmpty()) {
                ps.setString(ownerIndex, schema);
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        String s1 = "SELECT NULL AS PKTABLE_CAT, P.OWNER AS PKTABLE_SCHEM, P.TABLE_NAME AS "
                + "PKTABLE_NAME, PC.COLUMN_NAME AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, F.OWNER AS "
                + "FKTABLE_SCHEM, F.TABLE_NAME AS FKTABLE_NAME, FC.COLUMN_NAME AS FKCOLUMN_NAME,FC.POSITION "
                + "AS KEY_SEQ,  NULL AS UPDATE_RULE,DECODE(F.DELETE_RULE, 'DELETE CASCADE', 0, 'SET NULL', 2, 1) "
                + "AS DELETE_RULE, F.CONSTRAINT_NAME AS FK_NAME, P.CONSTRAINT_NAME AS PK_NAME, DECODE(F.DEFERRABLE, "
                + "'DEFERRABLE',5,'NOT DEFERRABLE',7, 'DEFERRED', 6 ) DEFERRABILITY FROM DB_CONS_COLUMNS PC, "
                + "DB_CONSTRAINTS P,DB_CONS_COLUMNS FC, DB_CONSTRAINTS F WHERE 1=1 ";

        String s2 = " AND F.CONSTRAINT_TYPE = 'R' AND P.OWNER = F.R_OWNER AND "
                + "P.CONSTRAINT_NAME = F.R_CONSTRAINT_NAME AND P.CONSTRAINT_TYPE = 'P' AND PC.OWNER = P.OWNER AND "
                + "PC.CONSTRAINT_NAME = P.CONSTRAINT_NAME AND PC.TABLE_NAME = P.TABLE_NAME AND "
                + "FC.OWNER = F.OWNER  AND FC.CONSTRAINT_NAME = F.CONSTRAINT_NAME AND FC.TABLE_NAME = F.TABLE_NAME AND "
                + "FC.POSITION = PC.POSITION ORDER BY FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        PreparedStatement ps = null;
        try {
            StringBuilder exportedKeysSql = new StringBuilder();
            exportedKeysSql.append(s1);

            int tableIndex = 0;
            int p = 1;
            if (table != null) {
                tableIndex = p++;
                exportedKeysSql.append(" AND P.TABLE_NAME = ? ");
            }

            int ownerIndex = 0;
            if (schema != null && !schema.isEmpty()) {
                ownerIndex = p++;
                exportedKeysSql.append(" AND P.OWNER=? ");
            }

            exportedKeysSql.append(s2);
            ps = this.connection.prepareStatement(exportedKeysSql.toString());
            if (table != null) {
                ps.setString(tableIndex, table);
            }

            if (schema != null && !schema.isEmpty()) {
                ps.setString(ownerIndex, schema);
            }

            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                       String parentTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable) throws SQLException {
        throw Driver.notImplemented(this.getClass(),
                "getCrossReference(String,String,String,String,String,String)");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        String sql = "select 'INTEGER' as TYPE_NAME, 4 as DATA_TYPE, 10 as PRECISION, '' as LITERAL_PREFIX, '' as "
                + "LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, "
                + "true UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, true as AUTO_INCREMENT, 'INTEGER' as "
                + "LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB,"
                + "10 as NUM_PREC_RADIX from SYS_DUMMY union select 'BIGINT' as TYPE_NAME, -5 as DATA_TYPE, 19 as "
                + "PRECISION, '' as LITERAL_PREFIX, '' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, "
                + "false as CASE_SENSITIVE, 3 as SEARCHABLE, true UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, true "
                + "as AUTO_INCREMENT, 'BIGINT' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as "
                + "SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'REAL' as "
                + "TYPE_NAME, 8 as DATA_TYPE, 17 as PRECISION, '' as LITERAL_PREFIX, '' as LITERAL_SUFFIX, null as "
                + "CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, true UNSIGNED_ATTRIBUTE, "
                + "false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'REAL' as LOCAL_TYPE_NAME, -308 as "
                + "MINIMUM_SCALE,308 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX "
                + "from SYS_DUMMY union select 'NUMERIC' as TYPE_NAME, 2 as DATA_TYPE, 65 as PRECISION, '' as "
                + "LITERAL_PREFIX, '' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, false as "
                + "CASE_SENSITIVE, 3 as SEARCHABLE, true UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as "
                + "AUTO_INCREMENT, 'NUMERIC' as LOCAL_TYPE_NAME, -308 as MINIMUM_SCALE, 308 as MAXIMUM_SCALE, 0 as "
                + "SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'DATE' as "
                + "TYPE_NAME, 93 as DATA_TYPE, 0 as PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null "
                + "as CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, false "
                + "UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'DATE' as LOCAL_TYPE_NAME, "
                + "0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as "
                + "NUM_PREC_RADIX from SYS_DUMMY union select 'TIMESTAMP' as TYPE_NAME, 93 as DATA_TYPE, 0 as "
                + "PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, "
                + "false as CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, "
                + "false as AUTO_INCREMENT, 'TIMESTAMP' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, "
                + "0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select "
                + "'VARCHAR' as TYPE_NAME, 12 as DATA_TYPE, 65535 as PRECISION, '''' as LITERAL_PREFIX, '''' as "
                + "LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, "
                + "false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'VARCHAR' as "
                + "LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as "
                + "SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'BINARY' as TYPE_NAME, -2 as "
                + "DATA_TYPE, 255 as PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as "
                + "CREATE_PARAMS, 1 as NULLABLE, true as CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, "
                + "false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'BINARY' as LOCAL_TYPE_NAME, 0 as "
                + "MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX "
                + "from SYS_DUMMY union select 'VARBINARY' as TYPE_NAME, -3 as DATA_TYPE, 65535 as PRECISION, "
                + "'''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, true as "
                + "CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as "
                + "AUTO_INCREMENT, 'VARBINARY' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as "
                + "SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'BLOB' as "
                + "TYPE_NAME, 2004 as DATA_TYPE, 2147483647 as PRECISION, '''' as LITERAL_PREFIX, '''' as "
                + "LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, true as CASE_SENSITIVE, 3 as SEARCHABLE, "
                + "false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'BLOB' as "
                + "LOCAL_TYPE_NAME,0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, "
                + "10 as NUM_PREC_RADIX from SYS_DUMMY union select 'CLOB' as TYPE_NAME, 2005 as DATA_TYPE, "
                + "2147483647 as PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as CREATE_PARAMS, "
                + "1 as NULLABLE, true as CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, false as "
                + "FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'CLOB' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as "
                + "MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY "
                + "union select 'BOOL' as TYPE_NAME, 16 as DATA_TYPE, 4 as PRECISION, '' as LITERAL_PREFIX, '' as "
                + "LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, true as CASE_SENSITIVE, 3 as SEARCHABLE, "
                + "false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'BOOL' as "
                + "LOCAL_TYPE_NAME,0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, "
                + "10 as NUM_PREC_RADIX from SYS_DUMMY union select 'FLOAT' as TYPE_NAME, 8 as DATA_TYPE, 17 as "
                + "PRECISION, '' as LITERAL_PREFIX, '' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, "
                + "false as CASE_SENSITIVE, 3 as SEARCHABLE, true UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, "
                + "false as AUTO_INCREMENT, 'FLOAT' as LOCAL_TYPE_NAME, -308 as MINIMUM_SCALE, 308 as MAXIMUM_SCALE, "
                + "0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'CHAR' "
                + "as TYPE_NAME, 1 as DATA_TYPE, 8000 as PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, "
                + "null as CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, false "
                + "UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'CHAR' as LOCAL_TYPE_NAME, "
                + "0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as "
                + "NUM_PREC_RADIX from SYS_DUMMY union select 'NCHAR' as TYPE_NAME, -15 as DATA_TYPE, 8000 as "
                + "PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, "
                + "false as CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, "
                + "false as AUTO_INCREMENT, 'NCHAR' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 "
                + "as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select "
                + "'NVARCHAR' as TYPE_NAME, -9 as DATA_TYPE, 8000 as PRECISION, '''' as LITERAL_PREFIX, '''' as "
                + "LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, "
                + "false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'NVARCHAR' as "
                + "LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as "
                + "SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'DECIMAL' as TYPE_NAME, 3 as "
                + "DATA_TYPE, 65 as PRECISION, '' as LITERAL_PREFIX, '' as LITERAL_SUFFIX, null as CREATE_PARAMS, "
                + "1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, true UNSIGNED_ATTRIBUTE, false as "
                + "FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'DECIMAL' as LOCAL_TYPE_NAME, -308 as MINIMUM_SCALE, "
                + "308 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from "
                + "SYS_DUMMY union select 'DOUBLE' as TYPE_NAME, 8 as DATA_TYPE, 17 as PRECISION, '' as "
                + "LITERAL_PREFIX, '' as LITERAL_SUFFIX, null as CREATE_PARAMS,1 as NULLABLE, false as CASE_SENSITIVE, "
                + "3 as SEARCHABLE, true UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, "
                + "'DOUBLE' as LOCAL_TYPE_NAME, -308 as MINIMUM_SCALE, 308 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, "
                + "0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'TIME' as TYPE_NAME, "
                + "92 as DATA_TYPE, 0 as PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as "
                + "CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, "
                + "false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'TIME' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, "
                + "0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY "
                + "union select 'TIMESTAMP' as TYPE_NAME, 93 as DATA_TYPE, 0 as PRECISION, '''' as LITERAL_PREFIX, "
                + "'''' as LITERAL_SUFFIX, null as CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as "
                + "SEARCHABLE, false UNSIGNED_ATTRIBUTE, false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, "
                + "'TIMESTAMP' as LOCAL_TYPE_NAME, 0 as MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as "
                + "SQL_DATETIME_SUB, 10 as NUM_PREC_RADIX from SYS_DUMMY union select 'TIMESTAMP_LTZ' as TYPE_NAME, "
                + "93 as DATA_TYPE, 0 as PRECISION, '''' as LITERAL_PREFIX, '''' as LITERAL_SUFFIX, null as "
                + "CREATE_PARAMS, 1 as NULLABLE, false as CASE_SENSITIVE, 3 as SEARCHABLE, false UNSIGNED_ATTRIBUTE, "
                + "false as FIXED_PREC_SCALE, false as AUTO_INCREMENT, 'TIMESTAMP_LTZ' as LOCAL_TYPE_NAME, 0 as "
                + "MINIMUM_SCALE, 0 as MAXIMUM_SCALE, 0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, 10 as "
                + "NUM_PREC_RADIX from SYS_DUMMY";

        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        String sql = getIndexInfoSql(schema, unique);
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(sql);
            ps.setString(1, table);

            if (schema != null && !schema.isEmpty()) {
                ps.setString(2, schema);
            }
            ResultSet result = ps.executeQuery();
            if (result instanceof ORResultSet) {
                ((ORResultSet) result).getDataRows();
            }
            return result;
        } finally {
            if (ps instanceof ORStatement) {
                ((ORStatement) ps).closeStmt();
            }
        }
    }

    private String getIndexInfoSql(String schema, boolean unique) {
        String s1 = "SELECT NULL AS TABLE_CAT,i.OWNER AS TABLE_SCHEM,i.TABLE_NAME,CASE i.IS_PRIMARY "
                + " || i.IS_UNIQUE WHEN 'NN' THEN 1 ELSE 0 END AS NON_UNIQUE,NULL AS INDEX_QUALIFIER, "
                + " i.INDEX_NAME AS INDEX_NAME, 1 AS TYPE, c.COLUMN_POSITION AS ORDINAL_POSITION, "
                + " c.COLUMN_NAME,NULL AS ASC_OR_DESC,0 AS CARDINALITY,i.PAGES AS PAGES,NULL AS "
                + " FILTER_CONDITION FROM DB_INDEXES i,DB_IND_COLUMNS c WHERE i.TABLE_NAME = ? ";

        String s2 = " AND i.INDEX_NAME = c.INDEX_NAME AND i.OWNER = c.TABLE_OWNER AND i.TABLE_NAME = c.TABLE_NAME AND "
                + "i.OWNER = c.INDEX_OWNER ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION ";
        StringBuilder infoSql = new StringBuilder();
        infoSql.append(s1);

        if (unique) {
            String s3 = " AND (i.IS_UNIQUE = 'Y' or i.IS_PRIMARY = 'Y') ";
            infoSql.append(s3);
        }

        if (schema != null && !schema.isEmpty()) {
            infoSql.append(" AND i.OWNER = ? ");
        }
        infoSql.append(s2);
        return infoSql.toString();
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return true;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return true;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return true;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getUDTs(String,String,String,int[])");
    }

    @Override
    public boolean supportsSavepoints() {
        return true;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 7;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 7;
    }

    @Override
    public boolean supportsNamedParameters() {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getSuperTypes(String,String,String)");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getSuperTables(String,String,String)");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getAttributes(String,String,String,String)");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == 1;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public int getJDBCMajorVersion() {
        return JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() {
        return JDBC_MINOR_VERSION;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getRowIdLifetime()");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getSchemas(String,String)");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return true;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getClientInfoProperties()");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getFunctions(String,String,String)");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getFunctionColumns(String,String,String,String)");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getPseudoColumns(String,String,String,String)");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "unwrap(Class<T>)");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "isWrapperFor(Class<T>)");
    }
}
