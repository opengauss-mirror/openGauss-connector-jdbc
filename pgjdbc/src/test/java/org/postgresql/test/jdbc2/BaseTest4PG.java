/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.Version;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.test.TestUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

public class BaseTest4PG extends BaseTest4{
  protected void openDB(Properties props) throws Exception{
    con = TestUtil.openDBPG(props);
  }
}
