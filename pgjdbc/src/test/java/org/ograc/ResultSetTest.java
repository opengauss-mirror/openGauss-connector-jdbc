/*
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

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test ResultSet
 *
 * @author zhangting
 * @since  2026-09-23
 */
public class ResultSetTest {
    private static final String TABLE = "ograc_rs_getxxx_t";
    private static final String POS_TABLE = "ograc_rspos_t";
    private static final String POS_SQL = "select id from " + POS_TABLE + " order by id";
    private static final Date DATE = Date.valueOf("2025-06-29");
    private static final Timestamp TS = Timestamp.valueOf("2025-06-29 12:34:56");
    private static final BigDecimal DECIMAL = new BigDecimal("123456.7890");
    private static final byte[] BYTES = new byte[]{0x01, 0x02, (byte) 0xFE, (byte) 0xFF};
    private static final String TEXT = "hello-ograc !@#";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        TestUtil.dropTable(conn, POS_TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_bool boolean, "
                    + "c_smallint smallint, "
                    + "c_int int, "
                    + "c_bigint bigint, "
                    + "c_real real, "
                    + "c_double double precision, "
                    + "c_decimal decimal(12,4), "
                    + "c_varchar varchar(128), "
                    + "c_text text, "
                    + "c_bytea bytea, "
                    + "c_date date, "
                    + "c_ts timestamp)");
            st.execute("create table " + POS_TABLE + "(id int primary key)");
            for (int i = 1; i <= 5; i++) {
                st.executeUpdate("insert into " + POS_TABLE + " values(" + i + ")");
            }
        }
        insertSampleRow(1);
    }

    @After
    public void tearDown() {
        try {
            if (conn != null && !conn.isClosed()) {
                TestUtil.dropTable(conn, TABLE);
                TestUtil.dropTable(conn, POS_TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    private Statement scrollableStatement() throws SQLException {
        return conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test
    public void testGetBooleanByIndexAndLabel() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(2));
            assertTrue(rs.getBoolean("c_bool"));
            assertFalse(rs.wasNull());
        }
    }

    @Test
    public void testGetByteShortIntLong() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(12, rs.getByte(3));
            assertEquals(12, rs.getByte("c_smallint"));
            assertEquals(12, rs.getShort(3));
            assertEquals(12, rs.getShort("c_smallint"));
            assertEquals(42, rs.getInt(4));
            assertEquals(42, rs.getInt("c_int"));
            assertEquals(4200000000L, rs.getLong(5));
            assertEquals(4200000000L, rs.getLong("c_bigint"));
        }
    }

    @Test
    public void testGetFloatAndDouble() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(1.25f, rs.getFloat(6), 0.0001f);
            assertEquals(1.25f, rs.getFloat("c_real"), 0.0001f);
            assertEquals(3.1415926535d, rs.getDouble(7), 0.0000001d);
            assertEquals(3.1415926535d, rs.getDouble("c_double"), 0.0000001d);
        }
    }

    @Test
    public void testGetBigDecimal() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(0, DECIMAL.compareTo(rs.getBigDecimal(8)));
            assertEquals(0, DECIMAL.compareTo(rs.getBigDecimal("c_decimal")));
        }
    }

    @Test
    public void testGetString() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(TEXT, rs.getString(9));
            assertEquals(TEXT, rs.getString("c_varchar"));
            assertEquals(TEXT, rs.getString(10));
            assertEquals(TEXT, rs.getString("c_text"));
        }
    }

    @Test
    public void testGetBytes() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertArrayEquals(BYTES, rs.getBytes(11));
            assertArrayEquals(BYTES, rs.getBytes("c_bytea"));
        }
    }

    @Test
    public void testGetDateTimeTimestamp() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(DATE, rs.getDate(12));
            assertEquals(DATE, rs.getDate("c_date"));
            assertEquals(TS, rs.getTimestamp(13));
            assertEquals(TS, rs.getTimestamp("c_ts"));
        }
    }

    @Test
    public void testGetObjectParsesToExpectedJavaTypes() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(Boolean.TRUE, rs.getObject("c_bool"));
            Object intObj = rs.getObject("c_int");
            assertNotNull(intObj);
            assertTrue(intObj instanceof Number);
            assertEquals(42, ((Number) intObj).intValue());
            Object bigintObj = rs.getObject("c_bigint");
            assertNotNull(bigintObj);
            assertTrue(bigintObj instanceof Number);
            assertEquals(4200000000L, ((Number) bigintObj).longValue());

            Object decimalObj = rs.getObject("c_decimal");
            assertNotNull(decimalObj);
            if (decimalObj instanceof BigDecimal) {
                assertEquals(0, DECIMAL.compareTo((BigDecimal) decimalObj));
            } else {
                assertEquals(0, DECIMAL.compareTo(new BigDecimal(decimalObj.toString())));
            }

            assertEquals(TEXT, rs.getObject("c_varchar"));

            Object bytesObj = rs.getObject("c_bytea");
            assertNotNull(bytesObj);
            if (bytesObj instanceof byte[]) {
                assertArrayEquals(BYTES, (byte[]) bytesObj);
            } else {
                assertTrue(bytesObj instanceof Blob);
                Blob blob = (Blob) bytesObj;
                assertArrayEquals(BYTES, blob.getBytes(1, (int) blob.length()));
            }

            Object dateObj = rs.getObject("c_date");
            assertNotNull(dateObj);
            if (dateObj instanceof Date) {
                assertEquals(DATE, dateObj);
            } else {
                assertTrue(dateObj instanceof Timestamp);
                assertEquals(DATE, new Date(((Timestamp) dateObj).getTime()));
            }

            assertEquals(TS, rs.getObject("c_ts"));
        }
    }

    @Test
    public void testGetNullAndWasNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_bool, c_int, c_varchar, c_bytea, "
                        + "c_date, c_ts) values (?,?,?,?,?,?,?)")) {
            ps.setInt(1, 2);
            ps.setNull(2, Types.BOOLEAN);
            ps.setNull(3, Types.INTEGER);
            ps.setNull(4, Types.VARCHAR);
            ps.setNull(5, Types.BINARY);
            ps.setNull(6, Types.DATE);
            ps.setNull(7, Types.TIMESTAMP);
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "select c_bool, c_int, c_varchar, c_bytea, c_date, c_ts from " + TABLE + " where id = ?")) {
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
                assertTrue(rs.wasNull());
                assertEquals(0, rs.getInt(2));
                assertTrue(rs.wasNull());
                assertNull(rs.getString(3));
                assertTrue(rs.wasNull());
                assertNull(rs.getBytes(4));
                assertTrue(rs.wasNull());
                assertNull(rs.getDate(5));
                assertTrue(rs.wasNull());
                assertNull(rs.getTimestamp(6));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject("c_varchar"));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testNumericCrossTypeGetters() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            assertEquals(42L, rs.getLong("c_int"));
            assertEquals("42", rs.getString("c_int"));
            assertEquals(42.0d, rs.getLong("c_int"), 0.0d);
            assertEquals(0, new BigDecimal("42").compareTo(BigDecimal.valueOf(rs.getLong("c_int"))));
            assertTrue(rs.getBoolean("c_bool"));
            Object boolObj = rs.getObject("c_bool");
            assertEquals(Boolean.TRUE, boolObj);
            String boolStr = rs.getString("c_bool");
            assertNotNull(boolStr);
            assertTrue(boolStr.equalsIgnoreCase("t") || boolStr.equalsIgnoreCase("true")
                    || boolStr.equals("1"));
        }
    }

    @Test
    public void testGetBinaryStream() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            try (InputStream in = rs.getBinaryStream("c_bytea")) {
                assertNotNull(in);
                byte[] buf = new byte[BYTES.length];
                assertEquals(BYTES.length, in.read(buf));
                assertArrayEquals(BYTES, buf);
            }
        }
    }

    @Test
    public void testGetCharacterStream() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(selectByIdSql(1))) {
            assertTrue(rs.next());
            try (Reader reader = rs.getCharacterStream("c_text")) {
                assertNotNull(reader);
                char[] buf = new char[TEXT.length()];
                assertEquals(TEXT.length(), reader.read(buf));
                assertEquals(TEXT, new String(buf));
            }
        }
    }

    @Test
    public void testGetByLabelIsCaseInsensitive() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_int as MyInt, c_varchar as MyStr from " + TABLE + " where id = 1")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("myint"));
            assertEquals(42, rs.getInt("MYINT"));
            assertEquals(TEXT, rs.getString("mystr"));
            assertEquals(TEXT, rs.getString("MyStr"));
        }
    }

    @Test
    public void testNextAndGettersByIndex() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_int, c_varchar, c_double) values(10, 1, 'one', 1.5)");
            st.executeUpdate("insert into " + TABLE + "(id, c_int, c_varchar, c_double) values(20, 2, 'two', 2.5)");
            st.executeUpdate("insert into " + TABLE + "(id, c_int, c_varchar, c_double) values(30, 3, 'three', 3.5)");
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int, c_varchar, c_double from " + TABLE
                     + " where id in (10, 20, 30) order by c_int")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("one", rs.getString(2));
            assertEquals(1.5, rs.getDouble(3), 0.0001);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGettersByLabel() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int, c_varchar from " + TABLE + " where id = 1")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("c_int"));
            assertEquals(TEXT, rs.getString("c_varchar"));
        }
    }

    @Test
    public void testGetObject() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int, c_varchar from " + TABLE + " where id = 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getObject(1));
            assertNotNull(rs.getObject(2));
        }
    }

    @Test
    public void testMetaData() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int, c_varchar, c_double from " + TABLE + " where id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertNotNull(md);
            assertEquals(3, md.getColumnCount());
            assertTrue(rs.next());
        }
    }

    @Test
    public void testEmptyResultSet() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select * from " + TABLE + " where id = -1")) {
            assertFalse(rs.next());
        }
    }

    @Test
    public void testWasNull() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_varchar, c_double) values(99, null, null)");
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_varchar, c_double from " + TABLE + " where id = 99")) {
            assertTrue(rs.next());
            rs.getString(1);
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testIsBeforeFirstAndIsAfterLastOnInitialRow() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(0, rs.getRow());
        }
    }

    @Test
    public void testIsFirstAndIsLastWhileIterating() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.next());
            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertTrue(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(1, rs.getInt(1));

            for (int i = 2; i <= 4; i++) {
                assertTrue(rs.next());
                assertFalse(rs.isFirst());
                assertFalse(rs.isLast());
                assertEquals(i, rs.getInt(1));
            }

            assertTrue(rs.next());
            assertFalse(rs.isFirst());
            assertTrue(rs.isLast());
            assertEquals(5, rs.getInt(1));

            assertFalse(rs.next());
            assertFalse(rs.isBeforeFirst());
            assertTrue(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
        }
    }

    @Test
    public void testAfterLast() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            rs.afterLast();
            assertFalse(rs.isBeforeFirst());
            assertTrue(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(6, rs.getRow());
            assertFalse(rs.next());
        }
    }

    @Test
    public void testLast() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.last());
            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertTrue(rs.isLast());
            assertEquals(5, rs.getRow());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    public void testAbsolutePositiveIndex() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.absolute(1));
            assertTrue(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(1, rs.getInt(1));

            assertTrue(rs.absolute(3));
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
            assertEquals(3, rs.getInt(1));

            assertTrue(rs.absolute(5));
            assertFalse(rs.isFirst());
            assertTrue(rs.isLast());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    public void testAbsoluteNegativeIndex() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.absolute(-1));
            assertTrue(rs.isLast());
            assertEquals(5, rs.getInt(1));

            assertTrue(rs.absolute(-3));
            assertEquals(3, rs.getInt(1));
            assertTrue(rs.absolute(-5));
            assertTrue(rs.isFirst());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testAbsoluteZeroAndOutOfRange() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            assertFalse(rs.absolute(0));
            assertTrue(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());

            assertFalse(rs.absolute(10));
            assertTrue(rs.isAfterLast());
            assertFalse(rs.isBeforeFirst());
            assertFalse(rs.absolute(-10));
            assertTrue(rs.isBeforeFirst());
            assertFalse(rs.isAfterLast());
        }
    }

    @Test
    public void testRelative() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.absolute(3));
            assertEquals(3, rs.getInt(1));

            assertTrue(rs.relative(1));
            assertEquals(4, rs.getInt(1));
            assertFalse(rs.isLast());
            assertTrue(rs.relative(-2));
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.isFirst());
            assertFalse(rs.relative(10));
            assertTrue(rs.isAfterLast());
            assertTrue(rs.relative(-1));
            assertTrue(rs.isLast());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    public void testSingleRowPositionFlags() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("delete from " + POS_TABLE);
            st.executeUpdate("insert into " + POS_TABLE + " values(42)");
        }
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.isBeforeFirst());
            assertTrue(rs.next());
            assertTrue(rs.isFirst());
            assertTrue(rs.isLast());
            assertEquals(42, rs.getInt(1));
            assertFalse(rs.next());
            assertTrue(rs.isAfterLast());
        }
    }

    @Test
    public void testEmptyResultSetPositionFlags() throws SQLException {
        try (Statement st = scrollableStatement();
             ResultSet rs = st.executeQuery("select id from " + POS_TABLE + " where id < 0")) {
            assertFalse(rs.next());
            assertFalse(rs.last());
            assertFalse(rs.absolute(1));
            assertFalse(rs.relative(1));
            assertEquals(0, rs.getRow());
        }
    }

    @Test
    public void testScrollablePreparedStatement() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(POS_SQL,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.absolute(2));
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.relative(2));
            assertEquals(4, rs.getInt(1));
            rs.afterLast();
            assertTrue(rs.isAfterLast());
        }
    }

    @Test
    public void testForwardOnlyScrollMethodsThrow() throws SQLException {
        try (Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertThrowsSql(() -> rs.afterLast());
            assertThrowsSql(() -> rs.absolute(1));
            assertThrowsSql(() -> rs.last());
            assertThrowsSql(() -> rs.relative(1));
            assertThrowsSql(() -> rs.beforeFirst());
        }
    }

    @Test
    public void testForwardOnlyPositionFlagsWithoutScrolling() throws SQLException {
        try (Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = st.executeQuery(POS_SQL)) {
            assertTrue(rs.isBeforeFirst());
            assertTrue(rs.next());
            assertTrue(rs.isFirst());
            assertFalse(rs.isLast());
            while (rs.next()) {
                // advance to last row
            }
            assertTrue(rs.isAfterLast());
            assertFalse(rs.isFirst());
            assertFalse(rs.isLast());
        }
    }

    private void insertSampleRow(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE
                        + "(id, c_bool, c_smallint, c_int, c_bigint, c_real, c_double, c_decimal,"
                        + " c_varchar, c_text, c_bytea, c_date, c_ts)"
                        + " values (?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            ps.setInt(1, id);
            ps.setBoolean(2, true);
            ps.setShort(3, (short) 12);
            ps.setInt(4, 42);
            ps.setLong(5, 4200000000L);
            ps.setFloat(6, 1.25f);
            ps.setDouble(7, 3.1415926535d);
            ps.setBigDecimal(8, DECIMAL);
            ps.setString(9, TEXT);
            ps.setString(10, TEXT);
            ps.setBytes(11, BYTES);
            ps.setDate(12, DATE);
            ps.setTimestamp(13, TS);
            ps.executeUpdate();
        }
    }

    private String selectByIdSql(int id) {
        return "select id, c_bool, c_smallint, c_int, c_bigint, c_real, c_double, c_decimal,"
                + " c_varchar, c_text, c_bytea, c_date, c_ts from " + TABLE
                + " where id = " + id;
    }

    private static void assertThrowsSql(SqlRunnable action) {
        try {
            action.run();
            fail("expected SQLException");
        } catch (SQLException expected) {
        }
    }

    @FunctionalInterface
    private interface SqlRunnable {
        /**
         * Runs a JDBC operation that is expected to throw {@link SQLException}.
         *
         * @throws SQLException if the JDBC operation fails
         */
        void run() throws SQLException;
    }
}
