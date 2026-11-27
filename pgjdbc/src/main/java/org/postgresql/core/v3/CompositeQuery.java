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
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core.v3;

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutionResult;
import org.postgresql.core.SqlCommand;

import java.util.Map;

/**
 * V3 Query implementation for queries that involve multiple statements. We split it up into one
 * SimpleQuery per statement, and wrap the corresponding per-statement SimpleParameterList objects
 * in a CompositeParameterList.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class CompositeQuery implements Query {
  CompositeQuery(SimpleQuery[] subqueries, int[] offsets) {
    this.subqueries = subqueries;
    this.offsets = offsets;
  }

  public ParameterList createParameterList() {
    SimpleParameterList[] subparams = new SimpleParameterList[subqueries.length];
    for (int i = 0; i < subqueries.length; ++i) {
      subparams[i] = (SimpleParameterList) subqueries[i].createParameterList();
    }
    return new CompositeParameterList(subparams, offsets);
  }

  public String toString(ParameterList parameters) {
    StringBuilder sbuf = new StringBuilder(subqueries[0].toString());
    for (int i = 1; i < subqueries.length; ++i) {
      sbuf.append(';');
      sbuf.append(subqueries[i]);
    }
    return sbuf.toString();
  }

  @Override
  public String getNativeSql() {
    StringBuilder sbuf = new StringBuilder(subqueries[0].getNativeSql());
    for (int i = 1; i < subqueries.length; ++i) {
      sbuf.append(';');
      sbuf.append(subqueries[i].getNativeSql());
    }
    return sbuf.toString();
  }

  @Override
  public void replaceNativeSqlForClientLogic(String sql) {
     // Empty Implementation for Composite Query
  }

  @Override
  public SqlCommand getSqlCommand() {
    return null;
  }

  public String toString() {
    return toString(null);
  }

  public void close() {
    for (SimpleQuery subquery : subqueries) {
      subquery.close();
    }
  }

  public Query[] getSubqueries() {
    return subqueries;
  }

  @Override
  public void setIsFunction(boolean isFunction) {

  }

  @Override
  public boolean getIsFunction() {
    return false;
  }

  public boolean isStatementDescribed() {
    for (SimpleQuery subquery : subqueries) {
      if (!subquery.isStatementDescribed()) {
        return false;
      }
    }
    return true;
  }

  public boolean isEmpty() {
    for (SimpleQuery subquery : subqueries) {
      if (!subquery.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public int getBatchSize() {
    return 0; // no-op, unsupported
  }

  @Override
  public Map<String, Integer> getResultSetColumnNameIndexMap() {
    return null; // unsupported
  }

  private final SimpleQuery[] subqueries;
  private final int[] offsets;

    @Override
    public void setQueryResult(QueryExecutionResult queryExecutionResult) {
        for (SimpleQuery subquery : subqueries) {
            subquery.setQueryResult(queryExecutionResult);
        }
    }

    @Override
    public QueryExecutionResult getQueryResult() {
        for (SimpleQuery subquery : subqueries) {
            return subquery.getQueryResult();
        }
        return null;
    }

    @Override
    public void setIsEnableATF(boolean isEnableATF) {
        for (int i = 0; i < subqueries.length; ++i) {
            subqueries[i].setIsEnableATF(isEnableATF);
        }
    }

    @Override
    public boolean getIsEnableATF() {
        for (int i = 0; i < subqueries.length; ++i) {
            if (subqueries[i].getIsEnableATF()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Query copy() {
        SimpleQuery[] simpleQueries = new SimpleQuery[subqueries.length];
        Query query = new CompositeQuery(simpleQueries, offsets);
        for (int i = 0; i < subqueries.length; ++i) {
            Query subquery = subqueries[i].copy();
            if (subquery instanceof SimpleQuery) {
                simpleQueries[i] = (SimpleQuery) subquery;
            }
        }
        return query;
    }

    @Override
    public void unprepare() {
        for (int i = 0; i < subqueries.length; ++i) {
            subqueries[i].unprepare();
        }
    }
}
