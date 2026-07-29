/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc4;

import org.postgresql.PGProperty;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

/**
 * Tests character stream parameter binding.
 *
 * @since 2026-07-29
 */
public class CharacterStreamTest extends BaseTest4 {
  private static final String TEST_TABLE_NAME = "charstream";
  private static final String TEST_COLUMN_NAME = "cs";

  private static final String _delete;
  private static final String _insert;
  private static final String _select;

  static {
    _delete = String.format("DELETE FROM %s", TEST_TABLE_NAME);
    _insert = String.format("INSERT INTO %s (%s) VALUES (?)", TEST_TABLE_NAME, TEST_COLUMN_NAME);
    _select = String.format("SELECT %s FROM %s", TEST_COLUMN_NAME, TEST_TABLE_NAME);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.createTable(con, TEST_TABLE_NAME, "cs text");
  }

  @Override
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, TEST_TABLE_NAME);
    super.tearDown();
  }

  private void insertStreamKnownIntLength(String data) throws Exception {
    PreparedStatement insertPS = con.prepareStatement(_insert);
    try {
      Reader reader = (data != null) ? new StringReader(data) : null;
      int length = (data != null) ? data.length() : 0;
      insertPS.setCharacterStream(1, reader, length);
      insertPS.executeUpdate();
    } finally {
      TestUtil.closeQuietly(insertPS);
    }
  }

  private void insertStreamKnownLongLength(String data) throws Exception {
    PreparedStatement insertPS = con.prepareStatement(_insert);
    try {
      Reader reader = (data != null) ? new StringReader(data) : null;
      long length = (data != null) ? data.length() : 0;
      insertPS.setCharacterStream(1, reader, length);
      insertPS.executeUpdate();
    } finally {
      TestUtil.closeQuietly(insertPS);
    }
  }

  private void insertStreamUnknownLength(String data) throws Exception {
    PreparedStatement insertPS = con.prepareStatement(_insert);
    try {
      Reader reader = (data != null) ? new StringReader(data) : null;
      insertPS.setCharacterStream(1, reader);
      insertPS.executeUpdate();
    } finally {
      TestUtil.closeQuietly(insertPS);
    }
  }

    private void insertClobUnknownLength(String data) throws Exception {
        PreparedStatement insertPS = con.prepareStatement(_insert);
        try {
            Reader reader = (data != null) ? new StringReader(data) : null;
            insertPS.setClob(1, reader);
            insertPS.executeUpdate();
        } finally {
            TestUtil.closeQuietly(insertPS);
        }
    }

    private void insertClobKnownLength(String data) throws Exception {
        PreparedStatement insertPS = con.prepareStatement(_insert);
        try {
            Reader reader = (data != null) ? new StringReader(data) : null;
            long length = (data != null) ? data.length() : 0;
            insertPS.setClob(1, reader, length);
            insertPS.executeUpdate();
        } finally {
            TestUtil.closeQuietly(insertPS);
        }
    }

  private void validateContent(String data) throws Exception {
    PreparedStatement selectPS = con.prepareStatement(_select);
    try {
      ResultSet rs = selectPS.executeQuery();
      try {
        if (rs.next()) {
          String actualData = rs.getString(1);
          Assert.assertEquals("Sent and received data are not the same", data, actualData);
        } else {
          Assert.fail("query returned zero rows");
        }
      } finally {
        TestUtil.closeQuietly(rs);
      }
    } finally {
      TestUtil.closeQuietly(selectPS);
    }
  }

  private String getTestData(int size) {
    StringBuilder buf = new StringBuilder(size);
    String s = "This is a test string.\n";
    int slen = s.length();
    int len = 0;

    while ((len + slen) < size) {
      buf.append(s);
      len += slen;
    }

    while (len < size) {
      buf.append('.');
      len++;
    }

    return buf.toString();
  }

    private static Reader repeatingReader(final int length) {
        return new Reader() {
            private int remaining = length;

            @Override
            public int read(char[] cbuf, int off, int len) throws IOException {
                if (remaining == 0) {
                    return -1;
                }
                int count = Math.min(len, remaining);
                for (int i = 0; i < count; i++) {
                    cbuf[off + i] = 'x';
                }
                remaining -= count;
                return count;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    private void assertSimpleModeReaderRejected(ReaderAction action) throws Exception {
        Properties props = new Properties();
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        PGProperty.MAX_BUFFERED_STREAM_PARAMETER_CHARS.set(props, 1024);
        Connection simpleCon = TestUtil.openDB(props);
        try {
            PreparedStatement ps = simpleCon.prepareStatement("SELECT ?");
            try {
                action.set(ps);
                Assert.fail("large stream parameter should be rejected");
            } catch (SQLException e) {
                // expected
            } finally {
                TestUtil.closeQuietly(ps);
            }
        } finally {
            TestUtil.closeDB(simpleCon);
        }
    }

    private interface ReaderAction {
        /**
         * Sets a reader parameter on the prepared statement.
         *
         * @param ps prepared statement to bind.
         * @throws SQLException on binding failure.
         */
        void set(PreparedStatement ps) throws SQLException;
    }

  @Test
  public void testKnownIntLengthNull() throws Exception {
    String data = null;
    insertStreamKnownIntLength(data);
    validateContent(data);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testKnownLongLengthNull() throws Exception {
    String data = null;
    insertStreamKnownLongLength(data);
    validateContent(data);
  }

  @Test
  public void testUnknownLengthNull() throws Exception {
    String data = null;
    insertStreamUnknownLength(data);
    validateContent(data);
  }

  @Test
  public void testKnownIntLengthEmpty() throws Exception {
    String data = "";
    insertStreamKnownIntLength(data);
    if (data != "") {
      validateContent(data);
    }
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testKnownLongLengthEmpty() throws Exception {
    String data = "";
    insertStreamKnownLongLength(data);
    validateContent(data);
  }

  @Test
  public void testUnknownLengthEmpty() throws Exception {
    String data = "";
    insertStreamUnknownLength(data);
    validateContent(data);
  }

  @Test
  public void testKnownIntLength2Kb() throws Exception {
    String data = getTestData(2 * 1024);
    insertStreamKnownIntLength(data);
    validateContent(data);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testKnownLongLength2Kb() throws Exception {
    String data = getTestData(2 * 1024);
    insertStreamKnownLongLength(data);
    validateContent(data);
  }

  @Test
  public void testUnknownLength2Kb() throws Exception {
    String data = getTestData(2 * 1024);
    insertStreamUnknownLength(data);
    validateContent(data);
  }

  @Test
  public void testKnownIntLength10Kb() throws Exception {
    String data = getTestData(10 * 1024);
    insertStreamKnownIntLength(data);
    validateContent(data);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testKnownLongLength10Kb() throws Exception {
    String data = getTestData(10 * 1024);
    insertStreamKnownLongLength(data);
    validateContent(data);
  }

  @Test
  public void testUnknownLength10Kb() throws Exception {
    String data = getTestData(10 * 1024);
    insertStreamUnknownLength(data);
    validateContent(data);
  }

  @Test
  public void testKnownIntLength100Kb() throws Exception {
    String data = getTestData(100 * 1024);
    insertStreamKnownIntLength(data);
    validateContent(data);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testKnownLongLength100Kb() throws Exception {
    String data = getTestData(100 * 1024);
    insertStreamKnownLongLength(data);
    validateContent(data);
  }

  @Test
  public void testUnknownLength100Kb() throws Exception {
    String data = getTestData(100 * 1024);
    insertStreamUnknownLength(data);
    validateContent(data);
  }

  @Test
  public void testKnownIntLength200Kb() throws Exception {
    String data = getTestData(200 * 1024);
    insertStreamKnownIntLength(data);
    validateContent(data);
  }

  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testKnownLongLength200Kb() throws Exception {
    String data = getTestData(200 * 1024);
    insertStreamKnownLongLength(data);
    validateContent(data);
  }

  @Test
  public void testUnknownLength200Kb() throws Exception {
    String data = getTestData(200 * 1024);
    insertStreamUnknownLength(data);
    validateContent(data);
  }

    @Test
    public void testUnknownLength2Mb() throws Exception {
        String data = getTestData(2 * 1024 * 1024);
        insertStreamUnknownLength(data);
        validateContent(data);
    }

    @Test
    public void testClobUnknownLengthNull() throws Exception {
        String data = null;
        insertClobUnknownLength(data);
        validateContent(data);
    }

    @Test
    public void testClobUnknownLength2Kb() throws Exception {
        String data = getTestData(2 * 1024);
        insertClobUnknownLength(data);
        validateContent(data);
    }

    @Test
    public void testClobUnknownLength200Kb() throws Exception {
        String data = getTestData(200 * 1024);
        insertClobUnknownLength(data);
        validateContent(data);
    }

    @Test
    public void testClobUnknownLength2Mb() throws Exception {
        String data = getTestData(2 * 1024 * 1024);
        insertClobUnknownLength(data);
        validateContent(data);
    }

    @Test
    public void testClobKnownLength2Mb() throws Exception {
        String data = getTestData(2 * 1024 * 1024);
        insertClobKnownLength(data);
        validateContent(data);
    }

    @Test
    public void testSimpleModeUnknownLengthCharacterStreamTooLargeRejected() throws Exception {
        assertSimpleModeReaderRejected(new ReaderAction() {
            @Override
            public void set(PreparedStatement ps) throws SQLException {
                ps.setCharacterStream(1, repeatingReader(2 * 1024));
            }
        });
    }

    @Test
    public void testSimpleModeUnknownLengthClobTooLargeRejected() throws Exception {
        assertSimpleModeReaderRejected(new ReaderAction() {
            @Override
            public void set(PreparedStatement ps) throws SQLException {
                ps.setClob(1, repeatingReader(2 * 1024));
            }
        });
    }

    @Test
    public void testSimpleModeKnownLengthClobTooLargeRejected() throws Exception {
        assertSimpleModeReaderRejected(new ReaderAction() {
            @Override
            public void set(PreparedStatement ps) throws SQLException {
                ps.setClob(1, repeatingReader(2 * 1024), 2 * 1024);
            }
        });
    }
}
