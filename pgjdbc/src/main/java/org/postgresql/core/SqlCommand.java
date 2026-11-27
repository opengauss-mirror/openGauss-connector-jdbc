/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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

import static org.postgresql.core.SqlCommandType.INSERT;
import static org.postgresql.core.SqlCommandType.SELECT;
import static org.postgresql.core.SqlCommandType.WITH;

/**
 * Data Modification Language inspection support.
 *
 * @author Jeremy Whiting jwhiting@redhat.com
 * @author Christopher Deckers (chrriis@gmail.com)
 *
 */
public class SqlCommand {
  public static final SqlCommand BLANK = SqlCommand.createStatementTypeInfo(SqlCommandType.BLANK);

  public boolean isBatchedReWriteCompatible() {
    return valuesBraceOpenPosition >= 0;
  }

  public int getBatchRewriteValuesBraceOpenPosition() {
    return valuesBraceOpenPosition;
  }

  public int getBatchRewriteValuesBraceClosePosition() {
    return valuesBraceClosePosition;
  }

  public SqlCommandType getType() {
    return commandType;
  }

  public boolean isReturningKeywordPresent() {
    return parsedSQLhasRETURNINGKeyword;
  }

  public boolean returnsRows() {
    return parsedSQLhasRETURNINGKeyword || commandType == SELECT || commandType == WITH;
  }

  public static SqlCommand createStatementTypeInfo(SqlCommandType type,
      boolean isBatchedReWritePropertyConfigured,
      int valuesBraceOpenPosition, int valuesBraceClosePosition, boolean isRETURNINGkeywordPresent,
      int priorQueryCount) {
    return new SqlCommand(type, isBatchedReWritePropertyConfigured,
        valuesBraceOpenPosition, valuesBraceClosePosition, isRETURNINGkeywordPresent,
        priorQueryCount);
  }

  public static SqlCommand createStatementTypeInfo(SqlCommandType type) {
    return new SqlCommand(type, false, -1, -1, false, 0);
  }

  public static SqlCommand createStatementTypeInfo(SqlCommandType type,
      boolean isRETURNINGkeywordPresent) {
    return new SqlCommand(type, false, -1, -1, isRETURNINGkeywordPresent, 0);
  }

  private SqlCommand(SqlCommandType type, boolean isBatchedReWriteConfigured,
      int valuesBraceOpenPosition, int valuesBraceClosePosition, boolean isPresent,
      int priorQueryCount) {
    commandType = type;
    parsedSQLhasRETURNINGKeyword = isPresent;
    boolean batchedReWriteCompatible = (type == INSERT) && isBatchedReWriteConfigured
        && valuesBraceOpenPosition >= 0 && valuesBraceClosePosition > valuesBraceOpenPosition
        && !isPresent && priorQueryCount == 0;
    this.valuesBraceOpenPosition = batchedReWriteCompatible ? valuesBraceOpenPosition : -1;
    this.valuesBraceClosePosition = batchedReWriteCompatible ? valuesBraceClosePosition : -1;
  }

  private final SqlCommandType commandType;
  private final boolean parsedSQLhasRETURNINGKeyword;
  private final int valuesBraceOpenPosition;
  private final int valuesBraceClosePosition;

}
