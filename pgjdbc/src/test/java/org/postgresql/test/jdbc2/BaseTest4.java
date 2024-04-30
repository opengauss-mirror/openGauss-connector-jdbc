/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import com.vdurmont.semver4j.Semver;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.Version;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.test.TestUtil;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseTest4 {

  public enum BinaryMode {
    REGULAR, FORCE
  }

  public enum ReWriteBatchedInserts {
    YES, NO
  }

  public enum AutoCommit {
    YES, NO
  }

  public enum StringType {
    UNSPECIFIED, VARCHAR;
  }

  private Semver dbVersion = null;
  private String dbVendor = "";

  protected Connection con;
  private BinaryMode binaryMode;
  private ReWriteBatchedInserts reWriteBatchedInserts;
  protected PreferQueryMode preferQueryMode;
  private StringType stringType;

  protected void updateProperties(Properties props) {
    if (binaryMode == BinaryMode.FORCE) {
      forceBinary(props);
    }
    if (reWriteBatchedInserts == ReWriteBatchedInserts.YES) {
      PGProperty.REWRITE_BATCHED_INSERTS.set(props, true);
    }
    if (stringType != null) {
      PGProperty.STRING_TYPE.set(props, stringType.name().toLowerCase());
    }
  }

  protected void openDB(Properties props) throws Exception{
    con = TestUtil.openDB(props);
  }

  protected void forceBinary(Properties props) {
    PGProperty.PREPARE_THRESHOLD.set(props, -1);
  }

  public final void setBinaryMode(BinaryMode binaryMode) {
    this.binaryMode = binaryMode;
  }

  public StringType getStringType() {
    return stringType;
  }

  public void setStringType(StringType stringType) {
    this.stringType = stringType;
  }

  public void setReWriteBatchedInserts(
      ReWriteBatchedInserts reWriteBatchedInserts) {
    this.reWriteBatchedInserts = reWriteBatchedInserts;
  }

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    updateProperties(props);
    openDB(props);
    PGConnection pg = con.unwrap(PGConnection.class);
    preferQueryMode = pg == null ? PreferQueryMode.EXTENDED : pg.getPreferQueryMode();
    getDBVersion();
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.closeDB(con);
  }

  public void assumeByteaSupported() {
    Assume.assumeTrue("bytea is not supported in simple protocol execution mode",
        preferQueryMode.compareTo(PreferQueryMode.EXTENDED) >= 0);
  }

  public void assumeCallableStatementsSupported() {
    Assume.assumeTrue("callable statements are not fully supported in simple protocol execution mode",
        preferQueryMode.compareTo(PreferQueryMode.EXTENDED) >= 0);
  }

  public void assumeBinaryModeRegular() {
    Assume.assumeTrue(binaryMode == BinaryMode.REGULAR);
  }

  public void assumeBinaryModeForce() {
    Assume.assumeTrue(binaryMode == BinaryMode.FORCE);
    Assume.assumeTrue(preferQueryMode != PreferQueryMode.SIMPLE);
  }

  /**
   * Shorthand for {@code Assume.assumeTrue(TestUtil.haveMinimumServerVersion(conn, version)}.
   */
  public void assumeMinimumServerVersion(String message, Version version) throws SQLException {
    Assume.assumeTrue(message, TestUtil.haveMinimumServerVersion(con, version));
  }

  /**
   * Shorthand for {@code Assume.assumeTrue(TestUtil.haveMinimumServerVersion(conn, version)}.
   */
  public void assumeMinimumServerVersion(Version version) throws SQLException {
    Assume.assumeTrue(TestUtil.haveMinimumServerVersion(con, version));
  }

  /**
   * Randomly generate an 8-digit schema name.
   */
  public String getSchemaByRandom() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 8; i++) {
      int rand = new Random().nextInt(26);
      if (rand <= 0) {
        sb.append('a');
      } else {
        char start = (char) ('a' + rand);
        sb.append(start);
      }
    }
    return sb.toString();
  }
  public static boolean isEmpty(String value) {
    return value == null || value.isEmpty();
  }

  // Minimal opengauss that version
  public void assumeMiniOgVersion(String message, int major, int minor, int micro) throws SQLException {
    Assume.assumeTrue(message, isDBVendor("opengauss") && isVersionAtLeast(major,minor,micro));
  }

  public void getDBVersion() throws SQLException {
    String serverVersion = null;
    if (dbVersion == null) {
      PreparedStatement ps = con.prepareStatement("SELECT version()");
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        serverVersion = rs.getString(1);
      }
      if (isEmpty(serverVersion)) {
        return;
      }
      try {
        Matcher matcher = Pattern.compile("(openGauss|MogDB) ([0-9\\.]+)").matcher(serverVersion);
        if (matcher.find()) {
          dbVendor=matcher.group(1);
          String versionStr = matcher.group(2);
          if (!isEmpty(versionStr)) {
            dbVersion = new Semver(versionStr);
          }
        }
      } catch (Exception e) {
        dbVersion = new Semver("0.0.0");
      }
    }
  }
  public boolean isVersionLt(int major, int minor, int micro) {
    if (dbVersion == null) {
      return false;
    }
    if (dbVersion.getMajor() < major) {
      return true;
    }
    if (dbVersion.getMajor() == major) {
      if (dbVersion.getMinor() < minor) {
        return true;
      } else if (dbVersion.getMinor() == minor) {
        return dbVersion.getPatch() < micro;
      }
    }
    return false;
  }
  public boolean isVersionAtLeast(int major, int minor, int micro) {
    if (dbVersion == null) {
      return false;
    }
    if (dbVersion.getMajor() > major) {
      return true;
    }
    if (dbVersion.getMajor() == major) {
      if (dbVersion.getMinor() > minor) {
        return true;
      } else if (dbVersion.getMinor() == minor) {
        return dbVersion.getPatch() >= micro;
      }
    }
    return false;
  }
  public boolean isDBVendor(String s) {
    if (dbVendor == null) {
      return false;
    }
    return s.equalsIgnoreCase(dbVendor);
  }
}
