/*
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
