/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
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

package org.postgresql.core.v3;

import org.postgresql.copy.CopyOut;

import java.sql.SQLException;

/**
 * <p>Anticipated flow of a COPY TO STDOUT operation:</p>
 *
 * <p>CopyManager.copyOut() -&gt;QueryExecutor.startCopy() - sends given query to server
 * -&gt;processCopyResults(): - receives CopyOutResponse from Server - creates new CopyOutImpl
 * -&gt;initCopy(): - receives copy metadata from server -&gt;CopyOutImpl.init() -&gt;lock()
 * connection for this operation - if query fails an exception is thrown - if query returns wrong
 * CopyOperation, copyOut() cancels it before throwing exception &lt;-returned: new CopyOutImpl
 * holding lock on connection repeat CopyOut.readFromCopy() until null
 * -&gt;CopyOutImpl.readFromCopy() -&gt;QueryExecutorImpl.readFromCopy() -&gt;processCopyResults() -
 * on copydata row from server -&gt;CopyOutImpl.handleCopydata() stores reference to byte array - on
 * CopyDone, CommandComplete, ReadyForQuery -&gt;unlock() connection for use by other operations
 * &lt;-returned: byte array of data received from server or null at end.</p>
 */
public class CopyOutImpl extends CopyOperationImpl implements CopyOut {
  private byte[] currentDataRow;

  public byte[] readFromCopy() throws SQLException {
    return readFromCopy(true);
  }

  @Override
  public byte[] readFromCopy(boolean block) throws SQLException {
    currentDataRow = null;
    queryExecutor.readFromCopy(this, block);
    return currentDataRow;
  }

  protected void handleCopydata(byte[] data) {
    currentDataRow = data;
  }
}
