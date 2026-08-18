/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.postgresql.core.v3;

import org.postgresql.core.Encoding;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests bounded ParameterStatus payload parsing.
 *
 * @since 2026-08-12
 */
public class QueryExecutorImplParameterStatusTest {
    private static final int PARAMETER_STATUS_LENGTH_FIELD_BYTES = 4;

    private static final int MAX_PARAMETER_STATUS_PAYLOAD_BYTES = 65536;

    @Test
    public void parseValidParameterStatusPayload() throws Exception {
        byte[] payload = "client_encoding\0UTF8\0".getBytes("UTF-8");
        String[] result = QueryExecutorImpl.parseParameterStatusPayload(
            Encoding.getDatabaseEncoding("UTF-8"), payload);
        Assert.assertEquals("client_encoding", result[0]);
        Assert.assertEquals("UTF8", result[1]);
    }

    @Test
    public void rejectOversizedParameterStatusPayloadLength() {
        assertProtocolViolation(PARAMETER_STATUS_LENGTH_FIELD_BYTES
            + MAX_PARAMETER_STATUS_PAYLOAD_BYTES + 1);
    }

    @Test
    public void rejectMalformedParameterStatusPayloadWithoutTerminators() throws Exception {
        byte[] payload = new byte[1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = 'A';
        }

        try {
            QueryExecutorImpl.parseParameterStatusPayload(Encoding.getDatabaseEncoding("UTF-8"), payload);
            Assert.fail("Expected malformed ParameterStatus payload to be rejected");
        } catch (PSQLException e) {
            Assert.assertEquals(PSQLState.PROTOCOL_VIOLATION.getState(), e.getSQLState());
        }
    }

    private static void assertProtocolViolation(int messageLength) {
        try {
            QueryExecutorImpl.validateParameterStatusMessageLength(messageLength);
            Assert.fail("Expected malformed ParameterStatus length to be rejected");
        } catch (PSQLException e) {
            Assert.assertEquals(PSQLState.PROTOCOL_VIOLATION.getState(), e.getSQLState());
        }
    }
}
