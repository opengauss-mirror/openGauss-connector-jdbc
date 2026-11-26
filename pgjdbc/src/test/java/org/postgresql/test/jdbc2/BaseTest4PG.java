/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
