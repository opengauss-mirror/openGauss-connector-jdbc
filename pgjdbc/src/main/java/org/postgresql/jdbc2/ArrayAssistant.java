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

package org.postgresql.jdbc2;

/**
 * Implement this interface and register the its instance to ArrayAssistantRegistry, to let Postgres
 * driver to support more array type.
 *
 * @author Minglei Tu
 */
public interface ArrayAssistant {
  /**
   * get array base type.
   *
   * @return array base type
   */
  Class<?> baseType();

  /**
   * build a array element from its binary bytes.
   *
   * @param bytes input bytes
   * @param pos position in input array
   * @param len length of the element
   * @return array element from its binary bytes
   */
  Object buildElement(byte[] bytes, int pos, int len);

  /**
   * build an array element from its literal string.
   *
   * @param literal string representation of array element
   * @return array element
   */
  Object buildElement(String literal);
}
