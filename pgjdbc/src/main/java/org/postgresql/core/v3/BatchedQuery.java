/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3;

import org.postgresql.core.NativeQuery;
import org.postgresql.core.ParameterList;


/**
 * Purpose of this object is to support batched query re write behaviour. Responsibility for
 * tracking the batch size and implement the clean up of the query fragments after the batch execute
 * is complete. Intended to be used to wrap a Query that is present in the batchStatements
 * collection.
 *
 * @author Jeremy Whiting jwhiting@redhat.com
 * @author Christopher Deckers (chrriis@gmail.com)
 *
 */
public class BatchedQuery extends SimpleQuery {

  private String sql;
  private final int valuesBraceOpenPosition;
  private final int valuesBraceClosePosition;
  private final int batchSize;
  private BatchedQuery[] blocks;

  public BatchedQuery(NativeQuery query, TypeTransferModeRegistry transferModeRegistry,
      int valuesBraceOpenPosition,
      int valuesBraceClosePosition, boolean sanitiserDisabled) {
    super(query, transferModeRegistry, sanitiserDisabled);
    this.valuesBraceOpenPosition = valuesBraceOpenPosition;
    this.valuesBraceClosePosition = valuesBraceClosePosition;
    this.batchSize = 1;
  }

  private BatchedQuery(BatchedQuery src, int batchSize) {
    super(src);
    this.valuesBraceOpenPosition = src.valuesBraceOpenPosition;
    this.valuesBraceClosePosition = src.valuesBraceClosePosition;
    this.batchSize = batchSize;
  }

  public BatchedQuery deriveForMultiBatch(int valueBlock) {
    if (getBatchSize() != 1) {
      throw new IllegalStateException("Only the original decorator can be derived.");
    }
    if (valueBlock == 1) {
      return this;
    }
    int index = Integer.numberOfTrailingZeros(valueBlock) - 1;
    if (valueBlock > 128 || valueBlock != (1 << (index + 1))) {
      throw new IllegalArgumentException(
          "Expected value block should be a power of 2 smaller or equal to 128. Actual block is "
              + valueBlock);
    }
    if (blocks == null) {
      blocks = new BatchedQuery[7];
    }
    BatchedQuery bq = blocks[index];
    if (bq == null) {
      bq = new BatchedQuery(this, valueBlock);
      blocks[index] = bq;
    }
    return bq;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * Method to return the sql based on number of batches. Skipping the initial
   * batch.
   */
  @Override
  public String getNativeSql() {
    if (sql != null) {
      return sql;
    }
    sql = buildNativeSql(null);
    return sql;
  }

  private String buildNativeSql(ParameterList params) {
    String buildSql = null;
    // dynamically build sql with parameters for batches
    String nativeSql = super.getNativeSql();
    int batch = getBatchSize();
    if (batch < 2) {
      buildSql = nativeSql;
      return buildSql;
    }
    if (nativeSql == null) {
      buildSql = "";
      return buildSql;
    }
    int valuesBlockCharCount = 0;
    // Split the values section around every dynamic parameter.
    int[] bindPositions = getNativeQuery().bindPositions;
    int[] chunkStart = new int[1 + bindPositions.length];
    int[] chunkEnd = new int[1 + bindPositions.length];
    chunkStart[0] = valuesBraceOpenPosition;
    if (bindPositions.length == 0) {
      valuesBlockCharCount = valuesBraceClosePosition - valuesBraceOpenPosition + 1;
      chunkEnd[0] = valuesBraceClosePosition + 1;
    } else {
      chunkEnd[0] = bindPositions[0];
      valuesBlockCharCount += chunkEnd[0] - chunkStart[0];
      for (int i = 0; i < bindPositions.length; i++) {
        int startIndex = bindPositions[i] + 2;
        int endIndex =
            i < bindPositions.length - 1 ? bindPositions[i + 1] : valuesBraceClosePosition + 1;
        for (; startIndex < endIndex; startIndex++) {
          if (!Character.isDigit(nativeSql.charAt(startIndex))) {
            break;
          }
        }
        chunkStart[i + 1] = startIndex;
        chunkEnd[i + 1] = endIndex;
        valuesBlockCharCount += chunkEnd[i + 1] - chunkStart[i + 1];
      }
    }
    int length = nativeSql.length();
    //valuesBraceOpenPosition + valuesBlockCharCount;
    length += NativeQuery.calculateBindLength(bindPositions.length * batch);
    length -= NativeQuery.calculateBindLength(bindPositions.length);
    length += (valuesBlockCharCount + 1 /*comma*/) * (batch - 1 /* initial sql */);

    StringBuilder s = new StringBuilder(length);
    // Add query until end of values parameter block.
    int pos;
    if (bindPositions.length > 0 && params == null) {
      // Add the first values (...) clause, it would be values($1,..., $n), and it matches with
      // the values clause of a simple non-rewritten SQL
      s.append(nativeSql, 0, valuesBraceClosePosition + 1);
      pos = bindPositions.length + 1;
    } else {
      pos = 1;
      batch++; // do not use super.toString(params) as it does not work if query ends with --
      // We need to carefully add (...),(...), and we do not want to get (...) --, (...)
      s.append(nativeSql, 0, valuesBraceOpenPosition);
    }
    for (int i = 2; i <= batch; i++) {
      if (i > 2 || pos != 1) {
        // For "has binds" the first valuds
        s.append(',');
      }
      s.append(nativeSql, chunkStart[0], chunkEnd[0]);
      for (int j = 1; j < chunkStart.length; j++) {
        if (params == null) {
          NativeQuery.appendBindName(s, pos++);
        } else {
          s.append(params.toString(pos++, true));
        }
        s.append(nativeSql, chunkStart[j], chunkEnd[j]);
      }
    }
    // Add trailing content: final query is like original with multi values.
    // This could contain "--" comments, so it is important to add them at end.
    s.append(nativeSql, valuesBraceClosePosition + 1, nativeSql.length());
    buildSql = s.toString();
    // Predict length only when building sql with $1, $2, ... (that is no specific params given)
    assert params != null || s.length() == length
        : "Predicted length != actual: " + length + " !=" + s.length();
    return buildSql;
  }

  @Override
  public String toString(ParameterList params) {
    if (getBatchSize() < 2) {
      return super.toString(params);
    }
    return buildNativeSql(params);
  }

}
