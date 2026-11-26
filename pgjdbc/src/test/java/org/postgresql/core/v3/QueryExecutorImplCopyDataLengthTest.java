/*
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

import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the CopyData length guard directly. This keeps the regression test focused on the security
 * boundary that must run before {@code PGStream.receive(...)} can allocate the CopyData payload.
 *
 * @since 2026-07-28
 */
public class QueryExecutorImplCopyDataLengthTest {
    private static final int MAX_COPY_DATA_RECEIVE_BYTES =
            Integer.parseInt(PGProperty.MAX_COPY_DATA_RECEIVE_BYTES.getDefaultValue());

    @Test
    public void acceptsNormalCopyDataLength() throws PSQLException {
        // A regular CopyData frame should keep the original payload length behavior.
        Assert.assertEquals(16,
                QueryExecutorImpl.validateCopyDataLength(20, MAX_COPY_DATA_RECEIVE_BYTES));
    }

    @Test
    public void rejectsNegativeCopyDataMessageLength() {
        // Negative protocol lengths must fail before integer subtraction can become an allocation size.
        assertProtocolViolation(Integer.MIN_VALUE);
    }

    @Test
    public void rejectsMalformedCopyDataMessageLength() {
        // The v3 message length includes its own 4-byte length field, so values below 4 are malformed.
        assertProtocolViolation(3);
    }

    @Test
    public void rejectsOversizedCopyDataMessageLength() {
        // Oversized frames are rejected before the payload is read into a byte array.
        assertProtocolViolation(MAX_COPY_DATA_RECEIVE_BYTES + 5);
    }

    private static void assertProtocolViolation(int messageLength) {
        try {
            QueryExecutorImpl.validateCopyDataLength(messageLength, MAX_COPY_DATA_RECEIVE_BYTES);
            Assert.fail("Expected protocol violation for CopyData message length " + messageLength);
        } catch (PSQLException e) {
            Assert.assertEquals(PSQLState.PROTOCOL_VIOLATION.getState(), e.getSQLState());
        }
    }
}
