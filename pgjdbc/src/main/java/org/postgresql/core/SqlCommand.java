/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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
