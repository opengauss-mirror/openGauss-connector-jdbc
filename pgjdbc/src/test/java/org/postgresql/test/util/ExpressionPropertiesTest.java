/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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

package org.postgresql.test.util;

import org.postgresql.util.ExpressionProperties;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ExpressionPropertiesTest {
  @Test
  public void simpleReplace() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("server", "app1");
    p.put("file", "pgjdbc_${server}.txt");
    Assert.assertEquals("${server} should be replaced", "pgjdbc_app1.txt", p.getProperty("file"));
  }

  @Test
  public void replacementMissing() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("file", "pgjdbc_${server}.txt");
    Assert.assertEquals("${server} should be kept as is as there is no replacement",
        "pgjdbc_${server}.txt", p.getProperty("file"));
  }

  @Test
  public void multipleReplacements() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("server", "app1");
    p.put("file", "${server}${server}${server}${server}${server}");
    Assert.assertEquals("All the ${server} entries should be replaced",
        "app1app1app1app1app1", p.getProperty("file"));
  }

  @Test
  public void multipleParentProperties() {
    Properties p1 = new Properties();
    p1.setProperty("server", "app1_${app.type}");
    Properties p2 = new Properties();
    p2.setProperty("app.type", "production");

    ExpressionProperties p = new ExpressionProperties(p1, p2);
    p.put("file", "pgjdbc_${server}.txt");

    Assert.assertEquals("All the ${...} entries should be replaced",
        "pgjdbc_app1_production.txt", p.getProperty("file"));
  }

  @Test
  public void rawValue() {
    ExpressionProperties p = new ExpressionProperties();
    p.put("server", "app1");
    p.put("file", "${server}${server}${server}${server}${server}");
    Assert.assertEquals("No replacements in raw value expected",
        "${server}${server}${server}${server}${server}", p.getRawPropertyValue("file"));
  }
}
