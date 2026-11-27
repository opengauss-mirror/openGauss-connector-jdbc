/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * <p>This class helps with URL encoding and decoding. UTF-8 encoding is used by default to make
 * encoding consistent across the driver, and encoding might be changed via {@code
 * postgresql.url.encoding} property</p>
 *
 * <p>Note: this should not be used outside of PostgreSQL source, this is not a public API of the
 * driver.</p>
 */
public final class URLCoder {
  private static final String ENCODING_FOR_URL =
      System.getProperty("postgresql.url.encoding", "UTF-8");

  /**
   * Decodes {@code x-www-form-urlencoded} string into Java string.
   *
   * @param encoded encoded value
   * @return decoded value
   * @see URLDecoder#decode(String, String)
   */
  public static String decode(String encoded) {
    try {
      return URLDecoder.decode(encoded, ENCODING_FOR_URL);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(
          "Unable to decode URL entry via " + ENCODING_FOR_URL + ". This should not happen", e);
    }
  }

  /**
   * Encodes Java string into {@code x-www-form-urlencoded} format
   *
   * @param plain input value
   * @return encoded value
   * @see URLEncoder#encode(String, String)
   */
  public static String encode(String plain) {
    try {
      return URLEncoder.encode(plain, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(
          "Unable to encode URL entry via " + ENCODING_FOR_URL + ". This should not happen", e);
    }
  }
}
