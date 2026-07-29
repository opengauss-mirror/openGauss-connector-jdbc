/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.postgresql.core.v3;

import org.postgresql.PGProperty;
import org.postgresql.copy.CopyOut;
import org.postgresql.core.PGStream;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.Properties;

import javax.net.SocketFactory;

/**
 * Exercises CopyData handling through {@link QueryExecutorImpl#processCopyResults}. The fake stream
 * feeds minimal backend protocol frames so the tests cover the real CopyData branch without opening
 * a database connection.
 *
 * @since 2026-07-28
 */
public class QueryExecutorImplCopyDataProcessTest {
    @Test
    public void processCopyResultsReadsNormalCopyData() throws Exception {
        // Normal CopyData should still reach the CopyOut operation with the original payload bytes.
        byte[] payload = new byte[]{9, 8, 7};
        QueryExecutorImpl executor = newExecutor(copyDataMessage(payload.length + 4, payload));
        TestCopyOut copyOut = new TestCopyOut();
        copyOut.init(executor, 0, new int[0]);

        executor.processCopyResults(copyOut, true);

        Assert.assertArrayEquals(payload, copyOut.data);
    }

    @Test
    public void processCopyResultsRejectsMalformedCopyDataLengthBeforeRead() throws Exception {
        // Length 3 is invalid because the v3 message length includes the 4-byte length field.
        assertProtocolViolation(copyDataMessage(3, new byte[0]));
    }

    @Test
    public void processCopyResultsRejectsOversizedCopyDataLengthBeforeRead() throws Exception {
        // The frame has no payload bytes; success here proves validation happens before allocation/read.
        assertProtocolViolation(copyDataMessage(maxCopyDataReceiveBytes() + 5, new byte[0]));
    }

    private static void assertProtocolViolation(byte[] copyDataMessage) throws Exception {
        QueryExecutorImpl executor = newExecutor(copyDataMessage);
        TestCopyOut copyOut = new TestCopyOut();
        copyOut.init(executor, 0, new int[0]);

        try {
            executor.processCopyResults(copyOut, true);
            Assert.fail("Expected protocol violation for malformed CopyData frame");
        } catch (PSQLException e) {
            Assert.assertEquals(PSQLState.PROTOCOL_VIOLATION.getState(), e.getSQLState());
        }
    }

    private static QueryExecutorImpl newExecutor(byte[] backendMessages)
            throws IOException, SQLException {
        PGStream stream = new PGStream(new TestSocketFactory(startupThen(backendMessages)),
                new HostSpec("localhost", 5432), 0);
        return new QueryExecutorImpl(stream, "user", "database", 0, new Properties());
    }

    private static int maxCopyDataReceiveBytes() {
        return Integer.parseInt(PGProperty.MAX_COPY_DATA_RECEIVE_BYTES.getDefaultValue());
    }

    private static byte[] startupThen(byte[] backendMessages) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write('Z');
        writeInt(out, 5);
        out.write('I');
        out.write(backendMessages);
        return out.toByteArray();
    }

    private static byte[] copyDataMessage(int messageLength, byte[] payload) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write('d');
        writeInt(out, messageLength);
        out.write(payload);
        return out.toByteArray();
    }

    private static void writeInt(ByteArrayOutputStream out, int value) {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static class TestCopyOut extends CopyOperationImpl implements CopyOut {
        private byte[] data;

        /**
         * Returns the payload captured by {@link #handleCopydata(byte[])}.
         *
         * @return captured payload
         */
        public byte[] readFromCopy() {
            return data;
        }

        /**
         * Returns the payload captured by {@link #handleCopydata(byte[])}.
         *
         * @param shouldBlock ignored block flag
         * @return captured payload
         */
        public byte[] readFromCopy(boolean shouldBlock) {
            return data;
        }

        /**
         * Stores CopyData delivered by {@link QueryExecutorImpl#processCopyResults}.
         *
         * @param data CopyData payload
         */
        protected void handleCopydata(byte[] data) {
            this.data = data;
        }
    }

    private static class TestSocketFactory extends SocketFactory {
        private final byte[] input;

        TestSocketFactory(byte[] input) {
            this.input = input;
        }

        /**
         * Creates a socket backed by the configured test input bytes.
         *
         * @return test socket
         */
        public Socket createSocket() {
            return new TestSocket(input);
        }

        /**
         * Creates a socket backed by the configured test input bytes.
         *
         * @param host ignored host
         * @param port ignored port
         * @return test socket
         */
        public Socket createSocket(String host, int port) {
            return createSocket();
        }

        /**
         * Creates a socket backed by the configured test input bytes.
         *
         * @param host ignored host
         * @param port ignored port
         * @param localHost ignored local host
         * @param localPort ignored local port
         * @return test socket
         */
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
            return createSocket();
        }

        /**
         * Creates a socket backed by the configured test input bytes.
         *
         * @param host ignored host
         * @param port ignored port
         * @return test socket
         */
        public Socket createSocket(InetAddress host, int port) {
            return createSocket();
        }

        /**
         * Creates a socket backed by the configured test input bytes.
         *
         * @param host ignored host
         * @param port ignored remote port
         * @param localHost ignored local host
         * @param localPort ignored local port
         * @return test socket
         */
        public Socket createSocket(InetAddress host, int port, InetAddress localHost,
                int localPort) {
            return createSocket();
        }
    }

    private static class TestSocket extends Socket {
        private final InputStream input;
        private final OutputStream output = new ByteArrayOutputStream();

        TestSocket(byte[] input) {
            this.input = new ByteArrayInputStream(input);
        }

        /**
         * Reports the test socket as already connected.
         *
         * @return always true
         */
        public boolean isConnected() {
            return true;
        }

        /**
         * Returns the configured in-memory input stream.
         *
         * @return input stream
         */
        public InputStream getInputStream() {
            return input;
        }

        /**
         * Returns a sink for data written by {@link PGStream}.
         *
         * @return output stream
         */
        public OutputStream getOutputStream() {
            return output;
        }

        /**
         * Ignores TCP_NODELAY changes for the in-memory test socket.
         *
         * @param isTcpDelayDisabled ignored TCP_NODELAY value
         */
        public void setTcpNoDelay(boolean isTcpDelayDisabled) {
        }
    }
}
