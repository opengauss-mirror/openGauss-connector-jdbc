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

package org.postgresql.test.ssl;

import org.junit.Ignore;
import org.postgresql.test.TestUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * This test-case is only for TLCP. 
 */
@Ignore
public class TlcpTest {
  private Connection con;
  private String sslrootcert;
  private String sslcert;
  private String sslkey;
  private String sslenccert;
  private String sslenckey;

  @Before
  public void setUp() throws Exception {
    TestUtil.initDriver();
    sslrootcert = System.getProperty("sslrootcert");
    sslcert = System.getProperty("sslcert");
    sslkey = System.getProperty("sslkey");
    sslenccert = System.getProperty("sslenccert");
    sslenckey = System.getProperty("sslenckey");
  }

  private void common_ssl(Connection con) throws Exception {
    TestUtil.createTable(con, "test_tlcp", "id int, name text");

    PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_tlcp VALUES (?, ?)");
    pstmt.setObject(1, 1);
    pstmt.setObject(2, "mike");
    pstmt.executeUpdate();

    pstmt.setObject(1, 15);
    pstmt.setObject(2, "john");
    pstmt.executeUpdate();

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id,name FROM test_tlcp");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("mike", rs.getString(2));

    assertTrue(rs.next());
    assertEquals(15, rs.getInt(1));
    assertEquals("john", rs.getString(2));

    TestUtil.dropTable(con, "test_tlcp");
  }

  /*
   * Test case 1:
   * Link parameter sslmode is set require.
   * At the same time, opengauss server sslciphers should be set ECC-SM4-SM3 or ECC-SM4-GCM-SM3
   */
  @Test
  public void testECC_case1() throws Exception {
    Properties props = new Properties();
    props.setProperty("tlcp", "true");
    props.setProperty("sslmode", "require");

    try {
      con = TestUtil.openDB(props);
      common_ssl(con);
    } catch (Exception ex) {
      Assert.fail("Test case testECC_case1 execute failed: " + ex.getMessage());
    } finally {
      if (con != null) {
        TestUtil.closeDB(con);
      }
    }
  }

  /*
   * Test case 2:
   * Link parameter sslmode is set verify-ca, and sslrootcert is set correctly.
   * At the same time, opengauss server sslciphers should be set ECC-SM4-SM3 or ECC-SM4-GCM-SM3
   */
  @Test
  public void testECC_case2() throws Exception {
    Properties props = new Properties();
    props.setProperty("tlcp", "true");
    props.setProperty("sslmode", "verify-ca");
    props.setProperty("sslrootcert", sslrootcert);
    try {
      con = TestUtil.openDB(props);
      common_ssl(con);
    } catch (Exception ex) {
      Assert.fail("Test case testECC_case2 execute failed: " + ex.getMessage());
    } finally {
      if (con != null) {
        TestUtil.closeDB(con);
      }
    }
  }

  /*
   * Test case 3:
   * Link parameter sslmode is set verify-ca, and sslrootcert/sslcert/sslkey is set correctly.
   * At the same time, opengauss server sslciphers should be set ECC-SM4-SM3 or ECC-SM4-GCM-SM3
   */
  @Test
  public void testECC_case3() throws Exception {
    Properties props = new Properties();
    props.setProperty("tlcp", "true");
    props.setProperty("sslmode", "verify-ca");
    props.setProperty("sslrootcert", sslrootcert);
    props.setProperty("sslcert", sslcert);
    props.setProperty("sslkey", sslkey);
    try {
      con = TestUtil.openDB(props);
      common_ssl(con);
    } catch (Exception ex) {
      Assert.fail("Test case testECC_case3 execute failed: " + ex.getMessage());
    } finally {
      if (con != null) {
        TestUtil.closeDB(con);
      }
    }
  }

  /*
   * Test case 4:
   * Link parameter sslmode is set verify-ca, and sslrootcert/sslenccert/sslenckey is set correctly.
   * At the same time, opengauss server sslciphers should be set ECC-SM4-SM3 or ECC-SM4-GCM-SM3
   */
  @Test
  public void testECC_case4() throws Exception {
    Properties props = new Properties();
    props.setProperty("tlcp", "true");
    props.setProperty("sslmode", "verify-ca");
    props.setProperty("sslrootcert", sslrootcert);
    props.setProperty("sslenccert", sslenccert);
    props.setProperty("sslenckey", sslenckey);
    try {
      con = TestUtil.openDB(props);
      common_ssl(con);
    } catch (Exception ex) {
      Assert.fail("Test case testECC_case4 execute failed: " + ex.getMessage());
    } finally {
      if (con != null) {
        TestUtil.closeDB(con);
      }
    }
  }

  /*
   * Test case 5:
   * Link parameter sslmode is set verify-ca, and sslrootcert/sslcert/sslkey/sslenccert/sslenckey/ is set correctly.
   * At the same time, opengauss server sslciphers should be set tlcp cipher suite.
   */
  @Test
  public void testTlcp_case1() throws Exception {
    Properties props = new Properties();
    props.setProperty("tlcp", "true");
    props.setProperty("sslmode", "verify-ca");
    props.setProperty("sslrootcert", sslrootcert);
    props.setProperty("sslcert", sslcert);
    props.setProperty("sslkey", sslkey);
    props.setProperty("sslenccert", sslenccert);
    props.setProperty("sslenckey", sslenckey);
    try {
      con = TestUtil.openDB(props);
      common_ssl(con);
    } catch (Exception ex) {
      Assert.fail("Test case testTlcp_case1 execute failed: " + ex.getMessage());
    } finally {
      if (con != null) {
        TestUtil.closeDB(con);
      }
    }
  }
}