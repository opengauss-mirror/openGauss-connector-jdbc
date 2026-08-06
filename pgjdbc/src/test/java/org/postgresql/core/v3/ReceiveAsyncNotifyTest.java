/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Copyright (c) 2026, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.net.SocketFactory;

import org.postgresql.PGNotification;
import org.postgresql.core.PGStream;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.junit.Test;

/**
 * Regression test for finding-3df9b4d797fab7567622adff3e1a1eb5: receiveAsyncNotify() must bound its
 * parsing by the declared frame length (and reject oversized frames) instead of scanning for NUL
 * via the unbounded receiveString().
 *
 * Drives {@link QueryExecutorImpl#processNotifies(int)} with crafted AsyncNotify frames over an
 * in-memory PGStream; no database is required.
 *
 * @since 2026-08-06
 */
public class ReceiveAsyncNotifyTest {
  /**
   * A normal AsyncNotify frame is parsed and delivered.
   *
   * @throws Exception if test setup or protocol processing fails
   */
  @Test
  public void normalNotifyIsAccepted() throws Exception {
    // 'A' + msglen=20 + pid=42 + "hello\0" + "world\0"
    QueryExecutorImpl executor = newExecutor(frame(Arrays.<Object>asList(
        (byte) 'A', i32(20), i32(42), bytes("hello"), nul(), bytes("world"), nul())));
    executor.processNotifies(0);
    PGNotification[] notifications = executor.getNotifications();
    assertEquals("expected one notification", 1, notifications.length);
    assertEquals("hello", notifications[0].getName());
    assertEquals("world", notifications[0].getParameter());
    assertEquals(42, notifications[0].getPID());
  }

  /**
   * An oversized declared msglen is rejected before any payload allocation.
   *
   * @throws Exception if test setup or protocol processing fails
   */
  @Test
  public void rejectsOversizedMessageLength() throws Exception {
    assertProtocolViolation(frame(Arrays.<Object>asList((byte) 'A', i32(0x2000000C), i32(1))));
  }

  /**
   * A msglen below the minimum (4 len + 4 pid + 1 + 1) is rejected.
   *
   * @throws Exception if test setup or protocol processing fails
   */
  @Test
  public void rejectsUndersizedMessageLength() throws Exception {
    assertProtocolViolation(frame(Arrays.<Object>asList((byte) 'A', i32(9), i32(1))));
  }

  /**
   * A channel name with no NUL terminator is rejected.
   *
   * @throws Exception if test setup or protocol processing fails
   */
  @Test
  public void rejectsMissingChannelTerminator() throws Exception {
    // msglen = 4 + 4(pid) + 4(no-NUL body) = 12
    assertProtocolViolation(frame(Arrays.<Object>asList(
        (byte) 'A', i32(12), i32(1), bytes("AAAA"))));
  }

  /**
   * Trailing bytes after the parameter are rejected.
   *
   * @throws Exception if test setup or protocol processing fails
   */
  @Test
  public void rejectsTrailingBytes() throws Exception {
    // msglen = 4 + 4 + 4("msg\0") + 4("par\0") + 1("X") = 17
    assertProtocolViolation(frame(Arrays.<Object>asList((byte) 'A', i32(17), i32(1),
        bytes("msg"), nul(), bytes("par"), nul(), bytes("X"))));
  }

  private static void assertProtocolViolation(byte[] backendMessages) throws Exception {
    try {
      newExecutor(backendMessages).processNotifies(0);
      fail("expected a protocol violation");
    } catch (PSQLException e) {
      assertTrue("expected a protocol error, got: " + e.getMessage(), mentionsProtocolError(e));
    } catch (SQLException e) {
      fail("unexpected: " + e);
    }
  }

  private static boolean mentionsProtocolError(Throwable exception) {
    for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.contains("Protocol error")) {
        return true;
      }
    }
    return false;
  }

  private static QueryExecutorImpl newExecutor(byte[] backendMessages)
      throws IOException, SQLException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write('Z');
    writeInt(out, 5);
    out.write('I'); // ReadyForQuery(idle) for readStartupMessages
    out.write(backendMessages);
    PGStream stream = new PGStream(
        new TestSocketFactory(new ByteArrayInputStream(out.toByteArray())),
        new HostSpec("localhost", 5432), 0);
    return new QueryExecutorImpl(stream, "user", "db", 0, new Properties());
  }

  // ---- v3 frame builder: each part is a single byte, a 4-byte int, or a raw byte[] ----

  private static byte[] frame(List<Object> parts) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Object part : parts) {
      if (part instanceof Byte) {
        out.write((Byte) part);
      } else if (part instanceof Integer) {
        writeInt(out, (Integer) part);
      } else if (part instanceof byte[]) {
        out.write((byte[]) part);
      } else {
        throw new IllegalArgumentException("bad frame part: " + part);
      }
    }
    return out.toByteArray();
  }

  private static Integer i32(int value) {
    return value;
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static Byte nul() {
    return (byte) 0;
  }

  private static void writeInt(ByteArrayOutputStream output, int value) {
    output.write((value >>> 24) & 0xFF);
    output.write((value >>> 16) & 0xFF);
    output.write((value >>> 8) & 0xFF);
    output.write(value & 0xFF);
  }

  // ---- in-memory socket ----

  static final class TestSocketFactory extends SocketFactory {
    private final InputStream input;

    TestSocketFactory(InputStream input) {
      this.input = input;
    }

    /**
     * Creates an in-memory socket for the test stream.
     *
     * @return an in-memory test socket
     */
    @Override
    public Socket createSocket() {
      return new TestSocket(input);
    }

    /**
     * Creates an in-memory socket without connecting to a host.
     *
     * @param host ignored remote host
     * @param port ignored remote port
     * @return an in-memory test socket
     */
    @Override
    public Socket createSocket(String host, int port) {
      return createSocket();
    }

    /**
     * Creates an in-memory socket without connecting to a host.
     *
     * @param host ignored remote host
     * @param port ignored remote port
     * @param localHost ignored local host
     * @param localPort ignored local port
     * @return an in-memory test socket
     */
    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
      return createSocket();
    }

    /**
     * Creates an in-memory socket without connecting to a host.
     *
     * @param host ignored remote host
     * @param port ignored remote port
     * @return an in-memory test socket
     */
    @Override
    public Socket createSocket(InetAddress host, int port) {
      return createSocket();
    }

    /**
     * Creates an in-memory socket without connecting to a host.
     *
     * @param host ignored remote host
     * @param port ignored remote port
     * @param localHost ignored local host
     * @param localPort ignored local port
     * @return an in-memory test socket
     */
    @Override
    public Socket createSocket(InetAddress host, int port, InetAddress localHost, int localPort) {
      return createSocket();
    }
  }

  static final class TestSocket extends Socket {
    private final InputStream input;
    private final OutputStream output = new ByteArrayOutputStream();

    TestSocket(InputStream input) {
      this.input = input;
    }

    /**
     * Reports that this test socket is connected.
     *
     * @return always true for the in-memory socket
     */
    @Override
    public boolean isConnected() {
      return true;
    }

    /**
     * Returns the input stream supplied to this test socket.
     *
     * @return the test input stream
     */
    @Override
    public InputStream getInputStream() {
      return input;
    }

    /**
     * Returns the in-memory output stream.
     *
     * @return the test output stream
     */
    @Override
    public OutputStream getOutputStream() {
      return output;
    }

    /**
     * Accepts the TCP no-delay setting without changing the in-memory socket.
     *
     * @param isEnabled ignored TCP no-delay setting
     */
    @Override
    public void setTcpNoDelay(boolean isEnabled) {
    }

    /**
     * Returns a stable local address for the in-memory socket.
     *
     * @return the local test address
     */
    @Override
    public InetSocketAddress getLocalSocketAddress() {
      return new InetSocketAddress("localhost", 0);
    }

    /**
     * Returns a stable remote address for the in-memory socket.
     *
     * @return the remote test address
     */
    @Override
    public InetSocketAddress getRemoteSocketAddress() {
      return new InetSocketAddress("localhost", 5432);
    }
  }
}
