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

package org.postgresql.util;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpressionProperties extends Properties {

  private static final Pattern EXPRESSION = Pattern.compile("\\$\\{([^}]+)\\}");

  private final Properties[] defaults;

  /**
   * Creates an empty property list with the specified defaults.
   *
   * @param defaults java.util.Properties
   */
  public ExpressionProperties(Properties ...defaults) {
    this.defaults = defaults;
  }

  /**
   * <p>Returns property value with all {@code ${propKey}} like references replaced with the value of
   * the relevant property with recursive resolution.</p>
   *
   * <p>The method returns <code>null</code> if the property is not found.</p>
   *
   * @param key the property key.
   *
   * @return the value in this property list with
   *         the specified key value.
   */
  @Override
  public String getProperty(String key) {
    String value = getRawPropertyValue(key);
    return replaceProperties(value);
  }

  @Override
  public String getProperty(String key, String defaultValue) {
    String value = getRawPropertyValue(key);
    if (value == null) {
      value = defaultValue;
    }
    return replaceProperties(value);
  }

  /**
   * Returns raw value of a property without any replacements.
   * @param key property name
   * @return raw property value
   */
  public String getRawPropertyValue(String key) {
    String value = super.getProperty(key);
    if (value != null) {
      return value;
    }
    for (Properties properties : defaults) {
      value = properties.getProperty(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private String replaceProperties(String value) {
    if (value == null) {
      return null;
    }
    Matcher matcher = EXPRESSION.matcher(value);
    StringBuffer sb = null;
    while (matcher.find()) {
      if (sb == null) {
        sb = new StringBuffer();
      }
      String propValue = getProperty(matcher.group(1));
      if (propValue == null) {
        // Use original content like ${propKey} if property is not found
        propValue = matcher.group();
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(propValue));
    }
    if (sb == null) {
      return value;
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
}
