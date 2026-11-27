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

import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PgDatabaseMetaData;

import org.junit.Test;

public class TestACL {

  @Test
  public void testParseACL() {
    PgConnection _a = null;
    PgDatabaseMetaData a = new PgDatabaseMetaData(_a) {
    };
    a.parseACL("{jurka=arwdRxt/jurka,permuser=rw*/jurka}", "jurka");
    a.parseACL("{jurka=a*r*w*d*R*x*t*/jurka,permuser=rw*/jurka}", "jurka");
    a.parseACL("{=,jurka=arwdRxt,permuser=rw}", "jurka");
    a.parseACL("{jurka=arwdRxt/jurka,permuser=rw*/jurka,grantuser=w/permuser}", "jurka");
    a.parseACL("{jurka=a*r*w*d*R*x*t*/jurka,permuser=rw*/jurka,grantuser=w/permuser}", "jurka");
    a.parseACL(
        "{jurka=arwdRxt/jurka,permuser=rw*/jurka,grantuser=w/permuser,\"group permgroup=a/jurka\"}",
        "jurka");
    a.parseACL(
        "{jurka=a*r*w*d*R*x*t*/jurka,permuser=rw*/jurka,grantuser=w/permuser,\"group permgroup=a/jurka\"}",
        "jurka");
  }
}
