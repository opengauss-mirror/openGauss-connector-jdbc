/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
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
