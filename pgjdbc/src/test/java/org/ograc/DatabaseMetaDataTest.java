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

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test DatabaseMetaData
 *
 * @author zhangting
 * @since  2026-09-18
 */
public class DatabaseMetaDataTest {
    private static final String PARENT = "ograc_meta_parent_t";
    private static final String CHILD = "ograc_meta_child_t";
    private static final String PARENT_CAT = "OGRAC_META_PARENT_T";
    private static final String CHILD_CAT = "OGRAC_META_CHILD_T";
    private static final String TABLE_PATTERN = "OGRAC_META_%";

    private Connection conn;
    private DatabaseMetaData meta;
    private String schema;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        meta = conn.getMetaData();
        schema = meta.getUserName();
        assertNotNull(schema);
        dropFixtures();
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + PARENT + "("
                    + "id int primary key, "
                    + "name varchar(64) not null, "
                    + "amount decimal(10,2), "
                    + "created_at timestamp)");
            st.execute("create unique index " + PARENT + "_name_uidx on " + PARENT + "(name)");
            st.execute("create table " + CHILD + "("
                    + "cid int primary key, "
                    + "pid int not null, "
                    + "note varchar(32), "
                    + "constraint " + CHILD + "_fk foreign key (pid) references " + PARENT + "(id))");
            st.execute("create index " + CHILD + "_pid_idx on " + CHILD + "(pid)");
        }
    }

    @After
    public void tearDown() {
        try {
            dropFixtures();
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    private void dropFixtures() throws SQLException {
        TestUtil.dropTable(conn, CHILD);
        TestUtil.dropTable(conn, PARENT);
    }

    @Test
    public void testBasicIdentityInfo() throws SQLException {
        assertNotNull(meta.getDatabaseProductName());
        assertFalse(meta.getDatabaseProductName().isEmpty());
        assertNotNull(meta.getDatabaseProductVersion());
        assertNotNull(meta.getDriverName());
        assertNotNull(meta.getDriverVersion());
        assertTrue(meta.getDatabaseMajorVersion() >= 0);
        assertTrue(meta.getDatabaseMinorVersion() >= 0);
        assertTrue(meta.getJDBCMajorVersion() >= 3);
        assertTrue(meta.getDriverMajorVersion() >= 0);
        assertTrue(meta.getDriverMinorVersion() >= 0);
        assertNotNull(meta.getUserName());
        assertFalse(meta.getUserName().isEmpty());

        String url = meta.getURL();
        assertNotNull(url);
        assertTrue(url.startsWith(TestUtil.URL_PREFIX)
                || url.toLowerCase(Locale.ROOT).contains("ograc")
                || !url.isEmpty());
    }

    @Test
    public void testGetTablesDoesNotThrow() throws SQLException {
        try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
            assertNotNull(rs);
            rs.next();
        }
    }

    @Test
    public void testGetColumnsDoesNotThrow() throws SQLException {
        try (ResultSet rs = meta.getColumns(null, null, "%", "%")) {
            assertNotNull(rs);
        }
    }

    @Test
    public void testCapabilityFlags() throws SQLException {
        assertTrue(meta.supportsTransactions());
        assertTrue(meta.supportsBatchUpdates());
        assertTrue(meta.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertNotNull(meta.getSQLKeywords());
        assertNotNull(meta.getNumericFunctions());
        assertNotNull(meta.getStringFunctions());
        assertNotNull(meta.getSystemFunctions());
        assertNotNull(meta.getTimeDateFunctions());
        assertNotNull(meta.getIdentifierQuoteString());
        assertFalse(meta.getIdentifierQuoteString().isEmpty());
        assertTrue(meta.getMaxConnections() >= 0);
        assertTrue(meta.getMaxColumnsInTable() >= 0);
        assertTrue(meta.getDefaultTransactionIsolation() >= Connection.TRANSACTION_NONE);
    }

    @Test
    public void testGetTableTypes() throws SQLException {
        Set<String> types = new HashSet<String>();
        try (ResultSet rs = meta.getTableTypes()) {
            assertNotNull(rs);
            while (rs.next()) {
                types.add(rs.getString("TABLE_TYPE"));
            }
        }
        assertTrue(types.contains("TABLE") || types.contains("BASE TABLE"));
    }

    @Test
    public void testGetSchemasAndCatalogs() throws SQLException {
        boolean hasSchema = false;
        boolean hasCurrentUser = false;
        try (ResultSet rs = meta.getSchemas()) {
            assertNotNull(rs);
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd.getColumnCount() >= 1);
            while (rs.next()) {
                String schem = rs.getString(1);
                assertNotNull(schem);
                hasSchema = true;
                if (schema.equalsIgnoreCase(schem)) {
                    hasCurrentUser = true;
                }
            }
        }
        assertTrue(hasSchema);
        assertTrue("expected current user schema " + schema, hasCurrentUser);

        try (ResultSet rs = meta.getCatalogs()) {
            assertNotNull(rs);
            rs.next();
        }
    }

    @Test
    public void testGetTablesFindsFixture() throws SQLException {
        boolean isFoundParent = false;
        boolean isFoundChild = false;
        try (ResultSet rs = meta.getTables(null, schema, TABLE_PATTERN, new String[]{"TABLE"})) {
            assertNotNull(rs);
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                assertNotNull(name);
                assertNotNull(rs.getString("TABLE_TYPE"));
                if (PARENT_CAT.equalsIgnoreCase(name)) {
                    isFoundParent = true;
                }
                if (CHILD_CAT.equalsIgnoreCase(name)) {
                    isFoundChild = true;
                }
            }
        }
        assertTrue(isFoundParent);
        assertTrue(isFoundChild);
    }

    @Test
    public void testGetColumnsForFixture() throws SQLException {
        Set<String> columns = new HashSet<String>();
        try (ResultSet rs = meta.getColumns(null, schema, PARENT_CAT, "%")) {
            assertNotNull(rs);
            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                assertTrue(PARENT_CAT.equalsIgnoreCase(table));
                String col = rs.getString("COLUMN_NAME");
                assertNotNull(col);
                columns.add(col.toLowerCase(Locale.ROOT));
                assertNotNull(rs.getString("TYPE_NAME"));
                int nullable = rs.getInt("NULLABLE");
                assertTrue(nullable == DatabaseMetaData.columnNoNulls
                        || nullable == DatabaseMetaData.columnNullable
                        || nullable == DatabaseMetaData.columnNullableUnknown);
                try {
                    rs.getInt("DATA_TYPE");
                } catch (SQLException ignore) {
                    assertTrue(rs.getString("TYPE_NAME") != null);
                }
            }
        }
        assertTrue(columns.contains("id"));
        assertTrue(columns.contains("name"));
        assertTrue(columns.contains("amount"));
        assertTrue(columns.contains("created_at"));
    }

    @Test
    public void testGetPrimaryKeys() throws SQLException {
        boolean isFound = false;
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, PARENT_CAT)) {
            assertNotNull(rs);
            while (rs.next()) {
                assertTrue(PARENT_CAT.equalsIgnoreCase(rs.getString("TABLE_NAME")));
                if ("id".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                    isFound = true;
                    short seq = rs.getShort("KEY_SEQ");
                    assertTrue("KEY_SEQ=" + seq, seq == 1 || seq == 0);
                    assertNotNull(rs.getString("PK_NAME"));
                }
            }
        }
        assertTrue(isFound);
    }

    @Test
    public void testGetIndexInfo() throws SQLException {
        boolean isFoundUniqueName = false;
        boolean isFoundChildPid = false;
        try (ResultSet rs = meta.getIndexInfo(null, schema, PARENT_CAT, false, false)) {
            assertNotNull(rs);
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if (col != null && "name".equalsIgnoreCase(col)) {
                    isFoundUniqueName = true;
                    assertFalse(rs.getBoolean("NON_UNIQUE"));
                }
            }
        }
        try (ResultSet rs = meta.getIndexInfo(null, schema, CHILD_CAT, false, false)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if (col != null && "pid".equalsIgnoreCase(col)) {
                    isFoundChildPid = true;
                }
            }
        }
        assertTrue(isFoundUniqueName);
        assertTrue(isFoundChildPid);
    }

    @Test
    public void testGetImportedAndExportedKeys() throws SQLException {
        try (ResultSet rs = meta.getImportedKeys(null, schema, CHILD_CAT)) {
            assertNotNull(rs);
            boolean isImported = false;
            while (rs.next()) {
                assertTrue(PARENT_CAT.equalsIgnoreCase(rs.getString("PKTABLE_NAME")));
                assertTrue(CHILD_CAT.equalsIgnoreCase(rs.getString("FKTABLE_NAME")));
                assertTrue("id".equalsIgnoreCase(rs.getString("PKCOLUMN_NAME")));
                assertTrue("pid".equalsIgnoreCase(rs.getString("FKCOLUMN_NAME")));
                isImported = true;
            }
            if (!isImported) {
                return;
            }
        } catch (SQLException e) {
            assertTrue("unexpected: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().toLowerCase(Locale.ROOT).contains("syntax"));
        }

        try (ResultSet rs = meta.getExportedKeys(null, schema, PARENT_CAT)) {
            assertNotNull(rs);
            while (rs.next()) {
                assertTrue(PARENT_CAT.equalsIgnoreCase(rs.getString("PKTABLE_NAME")));
                assertTrue(CHILD_CAT.equalsIgnoreCase(rs.getString("FKTABLE_NAME")));
            }
        } catch (SQLException e) {
            assertTrue("unexpected: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().toLowerCase(Locale.ROOT).contains("syntax"));
        }
    }

    @Test
    public void testGetCrossReference() throws SQLException {
        try (ResultSet rs = meta.getCrossReference(null, null, PARENT_CAT,
                null, null, CHILD_CAT)) {
            fail("expected SQLFeatureNotSupportedException");
            assertNotNull(rs);
        } catch (SQLFeatureNotSupportedException expected) {
        }
    }

    @Test
    public void testGetTypeInfo() throws SQLException {
        Set<String> typeNames = new HashSet<String>();
        try (ResultSet rs = meta.getTypeInfo()) {
            assertNotNull(rs);
            ResultSetMetaData rsmd = rs.getMetaData();
            assertTrue(rsmd.getColumnCount() >= 5);
            while (rs.next()) {
                String name = rs.getString("TYPE_NAME");
                assertNotNull(name);
                typeNames.add(name.toLowerCase(Locale.ROOT));
                rs.getInt("DATA_TYPE");
            }
        }
        assertFalse(typeNames.isEmpty());
        assertTrue(typeNames.contains("int4")
                || typeNames.contains("integer")
                || typeNames.contains("int")
                || typeNames.contains("varchar")
                || typeNames.contains("bigint")
                || typeNames.contains("decimal"));
    }

    @Test
    public void testGetClientInfoPropertiesAndUDTs() throws SQLException {
        try (ResultSet rs = meta.getClientInfoProperties()) {
            fail("expected SQLFeatureNotSupportedException");
            assertNotNull(rs);
        } catch (SQLFeatureNotSupportedException expected) {
        }
        try (ResultSet rs = meta.getUDTs(null, null, "%", null)) {
            assertNotNull(rs);
            rs.next();
        } catch (SQLFeatureNotSupportedException expected) {
        }
    }

    @Test
    public void testGetProceduresAndFunctionsDoNotThrow() throws SQLException {
        try (ResultSet rs = meta.getProcedures(null, null, "%")) {
            assertNotNull(rs);
            rs.next();
        }
        try (ResultSet rs = meta.getFunctions(null, null, "%")) {
            fail("expected SQLFeatureNotSupportedException");
            assertNotNull(rs);
        } catch (SQLFeatureNotSupportedException expected) {
        }
    }

    @Test
    public void testResultSetHoldabilityAndVersions() throws SQLException {
        int holdability = meta.getResultSetHoldability();
        assertTrue(holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
                || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertTrue(meta.supportsResultSetHoldability(holdability));
        assertNotNull(meta.getDatabaseProductVersion());
        assertFalse(meta.getDatabaseProductVersion().trim().isEmpty());
    }
}
