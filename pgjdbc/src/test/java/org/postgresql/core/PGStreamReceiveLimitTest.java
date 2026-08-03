/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.postgresql.core;

import org.postgresql.util.HostSpec;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.SocketFactory;

/**
 * Tests the bounded receive helper used by protocol handlers. The helper must preserve normal small
 * reads while rejecting invalid sizes before a byte array is allocated.
 *
 * @since 2026-07-28
 */
public class PGStreamReceiveLimitTest {
    @Test
    public void receiveWithLimitReadsNormalPayload() throws IOException {
        // Normal reads within the caller-supplied limit should behave like the original receive(int).
        byte[] input = new byte[]{1, 2, 3};
        PGStream stream =
                new PGStream(new TestSocketFactory(input), new HostSpec("localhost", 5432), 0);

        Assert.assertArrayEquals(input, stream.receive(input.length, input.length));
    }

    @Test
    public void rejectsNegativeReceiveSizeBeforeAllocation() {
        // Negative sizes used to reach byte[] allocation; the bounded helper rejects them first.
        assertInvalidReceiveSize(-1, 10);
    }

    @Test
    public void rejectsNegativeUnboundedReceiveSizeBeforeAllocation() {
        // The original receive(int) path keeps normal positive behavior, but negative sizes are invalid.
        try {
            PGStream.checkReceiveSize(-1);
            Assert.fail("Expected IOException for receive size -1");
        } catch (IOException e) {
            // expected
        }
    }

    @Test
    public void rejectsOversizedReceiveSizeBeforeAllocation() {
        // The limit is supplied by the protocol handler, so unrelated receive paths are not affected.
        assertInvalidReceiveSize(11, 10);
    }

    private static void assertInvalidReceiveSize(int size, int maxSize) {
        try {
            PGStream.checkReceiveSize(size, maxSize);
            Assert.fail("Expected IOException for receive size " + size);
        } catch (IOException e) {
            // expected
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
