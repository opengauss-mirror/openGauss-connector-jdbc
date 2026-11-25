/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
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
