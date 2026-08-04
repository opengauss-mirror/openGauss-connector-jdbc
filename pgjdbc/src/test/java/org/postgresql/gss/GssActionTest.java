/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.gss;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.ietf.jgss.GSSContext;
import org.postgresql.core.PGStream;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import javax.net.SocketFactory;

/**
 * Tests the hostname selection used when building the Kerberos service principal.
 * The cases stay below real Kerberos negotiation so they are deterministic.
 *
 * @since 2026-07-31
 */
public class GssActionTest {
    @Test
    public void usesExplicitKerberosServerHostname() {
        /*
         * Original behavior check:
         * a configured kerberosServerHostname is still used as the service
         * principal hostname instead of the socket endpoint host.
         */
        assertEquals("service.example.com",
                GssAction.getKerberosServerHostname("service.example.com", "db.example.com"));
    }

    @Test
    public void fallsBackToCurrentHostWhenKerberosServerHostnameIsNull() {
        /*
         * GssAction receives the value already resolved by ConnectionFactoryImpl.
         * A null explicit value means the service principal uses this host.
         */
        assertEquals("db.example.com",
                GssAction.getKerberosServerHostname(null, "db.example.com"));
    }

    @Test
    public void fallsBackToCurrentHostWhenKerberosServerHostnameIsEmpty() {
        /*
         * Empty-string handling check:
         * an empty override is treated the same as an unset override, matching the
         * historical fallback to the current connection host.
         */
        assertEquals("db.example.com",
                GssAction.getKerberosServerHostname("", "db.example.com"));
    }

    @Test
    public void exchangesContinuationTokenUntilContextEstablished() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PGStream pgStream = createStream(authenticationGssContinue(new byte[] {9, 8, 7}), output);
        FakeGssContext context = new FakeGssContext();
        GssAction action =
                new GssAction(pgStream, null, "db.example.com", "user", "postgres", false,
                        "service.example.com");

        GssAction.AuthenticationResult result =
                action.exchangeAuthenticationTokens(context.asGssContext());

        assertNull(result.getException());
        assertNull(result.getToken());
        assertTrue(context.shouldRequestMutualAuth);
        assertEquals(2, context.initCalls);
        assertArrayEquals(new byte[0], context.receivedTokens[0]);
        assertArrayEquals(new byte[] {9, 8, 7}, context.receivedTokens[1]);
        assertArrayEquals(
                new byte[] {'p', 0, 0, 0, 6, 1, 2, 'p', 0, 0, 0, 5, 3},
                output.toByteArray());
    }

    @Test
    public void reportsProtocolErrorDuringContinuation() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PGStream pgStream = createStream(new byte[] {'X'}, output);
        FakeGssContext context = new FakeGssContext();
        GssAction action =
                new GssAction(pgStream, null, "db.example.com", "user", "postgres", false,
                        "service.example.com");

        GssAction.AuthenticationResult result =
                action.exchangeAuthenticationTokens(context.asGssContext());

        assertTrue(result.getException() instanceof PSQLException);
        assertArrayEquals(new byte[] {'p', 0, 0, 0, 6, 1, 2}, output.toByteArray());
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual) {
        org.junit.Assert.assertArrayEquals(expected, actual);
    }

    private static PGStream createStream(byte[] input, ByteArrayOutputStream output)
            throws IOException {
        return new PGStream(new TestSocketFactory(input, output), new HostSpec("localhost", 5432),
                0);
    }

    private static byte[] authenticationGssContinue(byte[] token) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write('R');
        writeInt4(bytes, 8 + token.length);
        writeInt4(bytes, 8);
        bytes.write(token, 0, token.length);
        return bytes.toByteArray();
    }

    private static void writeInt4(ByteArrayOutputStream bytes, int value) {
        bytes.write((value >>> 24) & 0xFF);
        bytes.write((value >>> 16) & 0xFF);
        bytes.write((value >>> 8) & 0xFF);
        bytes.write(value & 0xFF);
    }

    private static class TestSocketFactory extends SocketFactory {
        private final byte[] input;
        private final ByteArrayOutputStream output;

        TestSocketFactory(byte[] input, ByteArrayOutputStream output) {
            this.input = input;
            this.output = output;
        }

        @Override
        public Socket createSocket() {
            return new TestSocket(input, output);
        }

        @Override
        public Socket createSocket(String host, int port) {
            return createSocket();
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
            return createSocket();
        }

        @Override
        public Socket createSocket(InetAddress host, int port) {
            return createSocket();
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress,
                int localPort) {
            return createSocket();
        }
    }

    private static class TestSocket extends Socket {
        private final ByteArrayInputStream input;
        private final ByteArrayOutputStream output;

        TestSocket(byte[] input, ByteArrayOutputStream output) {
            this.input = new ByteArrayInputStream(input);
            this.output = output;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void setTcpNoDelay(boolean shouldUseTcpDelayOption) {
        }

        @Override
        public InputStream getInputStream() {
            return input;
        }

        @Override
        public OutputStream getOutputStream() {
            return output;
        }

        @Override
        public InetSocketAddress getLocalSocketAddress() {
            return new InetSocketAddress("127.0.0.1", 10000);
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            return new InetSocketAddress("127.0.0.1", 5432);
        }
    }

    private static class FakeGssContext implements InvocationHandler {
        private final byte[][] receivedTokens = new byte[2][];
        private boolean shouldRequestMutualAuth;
        private boolean isEstablished;
        private int initCalls;

        GSSContext asGssContext() {
            Object context = Proxy.newProxyInstance(GSSContext.class.getClassLoader(),
                    new Class<?>[] {GSSContext.class}, this);
            if (context instanceof GSSContext) {
                return GSSContext.class.cast(context);
            }
            throw new IllegalStateException("Proxy did not implement GSSContext");
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            if ("requestMutualAuth".equals(methodName) && args[0] instanceof Boolean) {
                shouldRequestMutualAuth = Boolean.class.cast(args[0]).booleanValue();
                return null;
            }
            if ("isEstablished".equals(methodName)) {
                return Boolean.valueOf(isEstablished);
            }
            if ("initSecContext".equals(methodName) && args[0] instanceof byte[]
                    && args[1] instanceof Integer && args[2] instanceof Integer) {
                return initSecContext(byte[].class.cast(args[0]),
                        Integer.class.cast(args[1]).intValue(),
                        Integer.class.cast(args[2]).intValue());
            }
            Class<?> returnType = method.getReturnType();
            if (returnType == Boolean.TYPE) {
                return Boolean.FALSE;
            }
            if (returnType == Integer.TYPE) {
                return Integer.valueOf(0);
            }
            if (returnType == byte[].class) {
                return new byte[0];
            }
            return null;
        }

        private byte[] initSecContext(byte[] inputBuf, int offset, int len) {
            byte[] inputToken = new byte[len];
            System.arraycopy(inputBuf, offset, inputToken, 0, len);
            receivedTokens[initCalls] = inputToken;
            initCalls++;
            if (initCalls == 1) {
                return new byte[] {1, 2};
            }
            isEstablished = true;
            return new byte[] {3};
        }
    }
}
