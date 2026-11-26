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

package org.postgresql.jdbc;

import java.sql.SQLWarning;

/**
 * Wrapper class for SQLWarnings that provides an optimisation to add
 * new warnings to the tail of the SQLWarning singly linked list, avoiding Θ(n) insertion time
 * of calling #setNextWarning on the head. By encapsulating this into a single object it allows
 * users(ie PgStatement) to atomically set and clear the warning chain.
 */
class PSQLWarningWrapper {

  private final SQLWarning firstWarning;
  private SQLWarning lastWarning;

  PSQLWarningWrapper(SQLWarning warning) {
    firstWarning = warning;
    lastWarning = warning;
  }

  void addWarning(SQLWarning sqlWarning) {
    lastWarning.setNextWarning(sqlWarning);
    lastWarning = sqlWarning;
  }

  SQLWarning getFirstWarning() {
    return firstWarning;
  }

}
