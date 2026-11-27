/*
 * Copyright (c) 2015, PostgreSQL Global Development Group
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

package org.postgresql.core;

import org.postgresql.util.CanEstimateSize;

import java.util.ArrayList;

/**
 * Stores information on the parsed JDBC query. It is used to cut parsing overhead when executing
 * the same query through {@link java.sql.Connection#prepareStatement(String)}.
 */
public class CachedQuery implements CanEstimateSize {
  /**
   * Cache key. {@link String} or {@code org.postgresql.util.CanEstimateSize}.
   */
  public final Object key;
  public final Query query;
  public final boolean isFunction;
  public final boolean isACompatibilityFunction;
  private int executeCount;
  // record queries after rewrite
  private final ArrayList<Query> rewriteQueries = new ArrayList<>();
  public CachedQuery(Object key, Query query, boolean isFunction, boolean isACompatibilityFunction) {
    assert key instanceof String || key instanceof CanEstimateSize
        : "CachedQuery.key should either be String or implement CanEstimateSize."
        + " Actual class is " + key.getClass();
    this.key = key;
    this.query = query;
    this.isFunction = isFunction;
    this.isACompatibilityFunction = isACompatibilityFunction;
    this.query.setIsFunction(isFunction);
  }

  public void increaseExecuteCount() {
    if (executeCount < Integer.MAX_VALUE) {
      executeCount++;
    }
  }

  public void increaseExecuteCount(int inc) {
    int newValue = executeCount + inc;
    if (newValue > 0) { // if overflows, just ignore the update
      executeCount = newValue;
    }
  }

  /**
   * Number of times this statement has been used.
   *
   * @return number of times this statement has been used
   */
  public int getExecuteCount() {
    return executeCount;
  }

  @Override
  public long getSize() {
    long queryLength;
    if (key instanceof String) {
      queryLength = ((String) key).length() * 2L; // 2 bytes per char, revise with Java 9's compact strings
    } else {
      queryLength = ((CanEstimateSize) key).getSize();
    }
    return queryLength * 2 /* original query and native sql */
        + 100L /* entry in hash map, CachedQuery wrapper, etc */;
  }

  /**
   * add rewrite query to list
   *
   * @param query the query to add
   */
  public void addRewriteQueries(Query query) {
    rewriteQueries.add(query);
  }

  /**
   * get rewrite queries list
   *
   * @return rewrite queries list
   */
  public ArrayList<Query> getRewriteQueries() {
    return rewriteQueries;
  }

  /**
   * check if rewrite queries list empty
   *
   * @return check result
   */
  public boolean isRewriteQueriesEmpty() {
    return rewriteQueries.isEmpty();
  }

  /**
   * clear rewrite queries
   */
  public void clearRewriteQueries() {
    if (rewriteQueries.isEmpty()) {
      return;
    }
    rewriteQueries.clear();
  }
  @Override
  public String toString() {
    return "CachedQuery{"
        + "executeCount=" + executeCount
        + ", query=" + query
        + ", isFunction=" + isFunction
        + '}';
  }
}
