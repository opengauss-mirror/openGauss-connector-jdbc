/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

package org.postgresql.core;

import org.postgresql.PGProperty;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Properties;
import java.io.Closeable;
import java.io.Flushable;
import java.io.OutputStream;
import java.io.Writer;
import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.EOFException;

/**
 * connection stream info
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORStream implements Closeable, Flushable {
    private static Log LOGGER = Logger.getLogger(ORStream.class.getName());
    private static final int BUFFER_SIZE = 8192;
    private static final int MAX_PARAMS_NUM = 65535;

    private SocketAddress socketAddress;
    private String localAddress;
    private Charset charset;
    private int requestCount;
    private Socket socket;
    private int serverVersion;
    private int requestFlag;
    private int capacity;

    private int bufferSize;
    private byte[] int4buf;
    private byte[] int2buf;
    private boolean isBigEndian;
    private VisibleBufferedInputStream visibleStream;
    private OutputStream outputStream;
    private Writer encodingWriter;
    private Encoding encoding;

    /**
     * input/output stream constructor
     *
     * @param hostSpec host address
     * @param bufferSize bufferSize
     */
    public ORStream(HostSpec hostSpec, int bufferSize) {
        this.charset = Charset.forName("UTF-8");
        this.bufferSize = bufferSize;
        this.socketAddress = new InetSocketAddress(hostSpec.getHost(), hostSpec.getPort());
        int2buf = new byte[2];
        int4buf = new byte[4];
    }

    /**
     * get server version
     *
     * @return server version
     */
    public int getServerVersion() {
        return serverVersion;
    }

    /**
     * set isBigEndian
     *
     * @param isBigEndian isBigEndian
     */
    public void setBigEndian(boolean isBigEndian) {
        this.isBigEndian = isBigEndian;
    }

    /**
     * get localAddress
     *
     * @return localAddress
     */
    public String getLocalAddress() {
        return localAddress;
    }

    /**
     * set server version
     *
     * @param serverVersion serverVersion
     */
    public void setServerVersion(int serverVersion) {
        this.serverVersion = serverVersion;
    }

    /**
     * set server capacity
     *
     * @param capacity server capacity
     */
    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * get requestFlag
     *
     * @return requestFlag
     */
    public int getRequestFlag() {
        return requestFlag;
    }

    /**
     * set requestFlag
     *
     * @param requestFlag requestFlag
     */
    public void setRequestFlag(int requestFlag) {
        this.requestFlag = requestFlag;
    }

    /**
     * get encoding
     *
     * @return encoding
     */
    public Encoding getEncoding() {
        return encoding;
    }

    /**
     * set charset
     *
     * @param charset charset
     */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    /**
     * get Charset
     *
     * @return Charset
     */
    public Charset getCharset() {
        return this.charset;
    }

    /**
     * Connect to the CT back end.
     *
     * @param properties properties
     * @param socketFactory socketFactory
     * @throws IOException if an I/O error occurs
     * @throws PSQLException if a database access error occurs
     */
    public void connect(Properties properties, SocketFactory socketFactory) throws IOException, PSQLException {
        Socket socketConn = socketFactory.createSocket();
        if (!socketConn.isConnected()) {
            socketConn.connect(this.socketAddress, getTimeout(properties));
            this.localAddress = socketConn.getLocalAddress().toString();
            socketConn.setKeepAlive(true);
            this.socket = socketConn;
            setSocketTimeout(properties);
            setReceiveBufferSize(properties);
            setSendBufferSize(properties);
            socketConn.setTcpNoDelay(true);
            visibleStream = new VisibleBufferedInputStream(socketConn.getInputStream(), BUFFER_SIZE);
            outputStream = new BufferedOutputStream(socketConn.getOutputStream(), BUFFER_SIZE);
            setEncoding(Encoding.getJVMEncoding("UTF-8"));
        }
    }

    /**
     * set encoding
     *
     * @param encoding encoding
     * @throws IOException if an I/O error occurs
     */
    public void setEncoding(Encoding encoding) throws IOException {
        if (this.encoding != null && this.encoding.name().equals(encoding.name())) {
            return;
        }
        // Close down any old writer.
        if (encodingWriter != null) {
            encodingWriter.close();
        }
        this.encoding = encoding;

        // Intercept flush() downcalls from the writer; our caller
        // will call PGStream.flush() as needed.
        OutputStream interceptor = new FilterOutputStream(outputStream) {
            /**
             * Flushes this output stream and forces any buffered output
             * bytes to be written out to the stream.
             *
             * @throws IOException if an I/O error occurs
             */
            public void flush() throws IOException {
            }

            /**
             * Closes this output stream and releases any system resources associated
             * with the stream.
             *
             * @throws IOException if an I/O error occurs
             */
            public void close() throws IOException {
                super.flush();
            }
        };

        encodingWriter = encoding.getEncodingWriter(interceptor);
    }

    private int getTimeout(Properties props) throws PSQLException {
        int loginTimeout = PGProperty.LOGIN_TIMEOUT.getInt(props);
        if (loginTimeout > Integer.MAX_VALUE / 1000) {
            LOGGER.debug("integer connectTimeout is too large, it will occur error after multiply by 1000.");
        }
        return loginTimeout;
    }

    private void setSocketTimeout(Properties props) throws PSQLException, IOException {
        int socketTimeout = PGProperty.SOCKET_TIMEOUT.getInt(props);
        if (socketTimeout > Integer.MAX_VALUE / 1000) {
            LOGGER.debug("integer socketTimeout is too large, it will occur error after multiply by 1000.");
        }
        if (socketTimeout >= 0) {
            this.socket.setSoTimeout(socketTimeout * 1000);
        }
    }

    private void setReceiveBufferSize(Properties props) throws PSQLException, IOException {
        int receiveBufferSize = PGProperty.RECEIVE_BUFFER_SIZE.getInt(props);
        if (receiveBufferSize >= 0) {
            this.socket.setReceiveBufferSize(receiveBufferSize);
        } else if (this.bufferSize > 0) {
            this.socket.setReceiveBufferSize(this.bufferSize);
        } else {
            LOGGER.warn("Ignore invalid value for receiveBufferSize: " + receiveBufferSize);
        }
    }

    private void setSendBufferSize(Properties props) throws PSQLException, IOException {
        int sendBufferSize = PGProperty.SEND_BUFFER_SIZE.getInt(props);
        if (sendBufferSize >= 0) {
            this.socket.setReceiveBufferSize(sendBufferSize);
        } else if (this.bufferSize > 0) {
            this.socket.setReceiveBufferSize(this.bufferSize);
        } else {
            LOGGER.warn("Ignore invalid value for sendBufferSize: " + sendBufferSize);
        }
    }

    /**
     * close outputStream/inputStream
     *
     * @throws IOException if an I/O error occurs
     */
    public void close() throws IOException {
        if (encodingWriter != null) {
            encodingWriter.close();
        }

        outputStream.close();
        visibleStream.close();
        socket.close();
    }

    /**
     * get request count
     *
     * @return request count
     */
    public int getRequestCount() {
        return requestCount;
    }

    /**
     * accumulate the number of requests
     *
     * @return request count
     */
    public int addRequestCount() {
        requestCount += 1;
        return requestCount;
    }

    /**
     * Flushes this stream by writing any buffered output to the underlying stream.
     *
     * @throws IOException if an I/O error occurs
     */
    public void flush() throws IOException {
        if (encodingWriter != null) {
            encodingWriter.flush();
        }
        outputStream.flush();
    }

    /**
     * Receives a fixed-size byte[] from the backend.
     *
     * @param size byte size
     * @return fixed-size byte[]
     * @throws IOException if an I/O error occurs
     */
    public byte[] receive(int size) throws IOException {
        byte[] answer = new byte[size];
        receive(answer, 0, size);
        return answer;
    }

    /**
     * Reads in a given number of bytes from the backend.
     *
     * @param buf buffer to store result
     * @param off offset in buffer
     * @param size number of bytes to read
     * @throws IOException if a data I/O error occurs
     */
    public void receive(byte[] buf, int off, int size) throws IOException {
        if (!visibleStream.ensureBytes(size)) {
            throw new EOFException("EOF Exception");
        }
        int s = 0;
        while (s < size) {
            int w = visibleStream.read(buf, off + s, size - s);
            if (w < 0) {
                throw new EOFException("EOF Exception");
            }
            s += w;
        }
    }

    /**
     * Receives a Long from the backend.
     *
     * @return the 64bit integer received from the backend
     * @throws IOException if an I/O error occurs
     */
    public long receiveLong() throws IOException {
        byte[] bs = receive(8);
        long value = 0L;
        if (isBigEndian) {
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (bs[i] & 0xFF);
            }
        } else {
            for (int i = 7; i >= 0; i--) {
                value = (value << 8) | (bs[i] & 0xFF);
            }
        }
        return value;
    }

    /**
     * Receives a single character from the backend.
     *
     * @return the character received
     * @throws IOException if an I/O Error occurs
     */
    public int receiveChar() throws IOException {
        int c = visibleStream.read();
        if (c < 0) {
            throw new EOFException("EOF Exception");
        }
        return c;
    }

    /**
     * Receives a two byte integer from the backend.
     *
     * @return the integer received from the backend
     * @throws IOException  if an I/O error occurs
     */
    public int receiveInteger2() throws IOException {
        if (!visibleStream.ensureBytes(2)) {
            throw new EOFException("EOF Exception");
        }
        if (visibleStream.read(int2buf) != 2) {
            throw new EOFException("EOF Exception");
        }
        if (isBigEndian) {
            return ((int2buf[0] << 8) | (int2buf[1] & 0xff));
        }
        return ((int2buf[1] << 8) | (int2buf[0] & 0xff));
    }

    /**
     * Receives a four byte integer from the backend.
     *
     * @return the integer received from the backend
     * @throws IOException if an I/O error occurs
     */
    public int receiveInteger4() throws IOException {
        if (!visibleStream.ensureBytes(4)) {
            throw new EOFException("EOF Exception");
        }
        if (visibleStream.read(int4buf) != 4) {
            throw new EOFException("EOF Exception");
        }
        if (isBigEndian) {
            return (int4buf[0] & 0xFF) << 24 | (int4buf[1] & 0xFF) << 16 | (int4buf[2] & 0xFF) << 8
                    | int4buf[3] & 0xFF;
        }

        return (int4buf[3] & 0xFF) << 24 | (int4buf[2] & 0xFF) << 16 | (int4buf[1] & 0xFF) << 8
                | int4buf[0] & 0xFF;
    }

    /**
     * Sends a 4-byte integer to the back end.
     *
     * @param val the integer to be sent
     * @throws IOException if an I/O error occurs
     */
    public void sendInteger4(int val) throws IOException {
        if (isBigEndian) {
            int4buf[0] = (byte) (val >>> 24);
            int4buf[1] = (byte) (val >>> 16);
            int4buf[2] = (byte) (val >>> 8);
            int4buf[3] = (byte) (val);
        } else {
            int4buf[0] = (byte) (val);
            int4buf[1] = (byte) (val >>> 8);
            int4buf[2] = (byte) (val >>> 16);
            int4buf[3] = (byte) (val >>> 24);
        }
        outputStream.write(int4buf);
    }

    /**
     * Convert int type value to byte[4]
     *
     * @param val int type value
     * @return byte[4]
     */
    public byte[] getInteger4Bytes(int val) {
        byte[] bytes = new byte[4];
        if (isBigEndian) {
            bytes[0] = (byte) (val >>> 24);
            bytes[1] = (byte) (val >>> 16);
            bytes[2] = (byte) (val >>> 8);
            bytes[3] = (byte) (val);
        } else {
            bytes[0] = (byte) (val);
            bytes[1] = (byte) (val >>> 8);
            bytes[2] = (byte) (val >>> 16);
            bytes[3] = (byte) (val >>> 24);
        }
        return bytes;
    }

    /**
     * Convert byte[2] to int type
     *
     * @param bytes byte[2]
     * @return int type value
     */
    public int bytesToShort(byte[] bytes) {
        if (bytes == null || bytes.length < 2) {
            throw new IllegalArgumentException("Byte array must have at least 2 elements");
        }
        if (isBigEndian) {
            return ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
        }
        return ((bytes[1] & 0xFF) << 8) | (bytes[0] & 0xFF);
    }

    /**
     * Convert byte[2] to int type value
     *
     * @param bytes byte[2]
     * @param start start position
     * @return int type value
     */
    public int bytesToShort(byte[] bytes, int start) {
        if (bytes == null || bytes.length < 2) {
            throw new IllegalArgumentException("Byte array must have at least 2 elements");
        }
        if (isBigEndian) {
            return ((bytes[0 + start] & 0xFF) << 8) | (bytes[1 + start] & 0xFF);
        }
        return ((bytes[1 + start] & 0xFF) << 8) | (bytes[0 + start] & 0xFF);
    }

    /**
     * Convert byte[4] to int type value
     *
     * @param bytes byte[4]
     * @return int type value
     */
    public int bytesToInt(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            throw new IllegalArgumentException("Byte array must have at least 4 elements");
        }
        if (isBigEndian) {
            return (bytes[0] & 0xFF) << 24
                    | (bytes[1] & 0xFF) << 16
                    | (bytes[2] & 0xFF) << 8
                    | (bytes[3] & 0xFF);
        }
        return (bytes[3] & 0xFF) << 24
                | (bytes[2] & 0xFF) << 16
                | (bytes[1] & 0xFF) << 8
                | (bytes[0] & 0xFF);
    }

    /**
     * Convert byte[8] to long type value
     *
     * @param bytes byte[8]
     * @return long type value
     */
    public long bytesToLong(byte[] bytes) {
        if (bytes == null || bytes.length < 8) {
            throw new IllegalArgumentException("Byte array must have at least 8 elements");
        }
        long value = 0L;
        if (isBigEndian) {
            for (int i = 0; i < 8; i++) {
                value |= (bytes[i] & 0xFFL) << ((7 - i) * 8);
            }
        } else {
            for (int i = 0; i < bytes.length; i++) {
                value |= ((long) (bytes[i] & 0xFF)) << (i * 8);
            }
        }
        return value;
    }

    /**
     * Convert long type value to byte[8]
     *
     * @param val long type value
     * @return byte[8]
     */
    public byte[] getInteger8Bytes(long val) {
        byte[] bytes = new byte[8];
        if (isBigEndian) {
            for (int i = 7; i >= 0; i--) {
                bytes[7 - i] = (byte) (val >>> (i * 8));
            }
        } else {
            for (int i = 0; i < 8; i++) {
                bytes[i] = (byte) (val >>> (i * 8));
            }
        }
        return bytes;
    }

    /**
     * Sends a 2-byte integer (short) to the back end.
     *
     * @param val the integer to be sent
     * @throws IOException if an I/O error occurs or {@code val} cannot be encoded in 2 bytes
     */
    public void sendInteger2(int val) throws IOException {
        if (isBigEndian) {
            int2buf[0] = (byte) (val >>> 8);
            int2buf[1] = (byte) val;
        } else {
            int2buf[0] = (byte) val;
            int2buf[1] = (byte) (val >>> 8);
        }
        outputStream.write(int2buf);
    }

    /**
     * Convert int type value to byte[2]
     *
     * @param val int type value
     * @return byte[2]
     */
    public byte[] getInteger2Bytes(int val) {
        if (val < 0 || val > MAX_PARAMS_NUM) {
            throw new IllegalArgumentException("Tried to send an out-of-range "
                    + "integer as a 2-byte unsigned int value: " + val);
        }
        byte[] bytes = new byte[2];
        if (isBigEndian) {
            bytes[0] = (byte) (val >>> 8);
            bytes[1] = (byte) val;
        } else {
            bytes[0] = (byte) val;
            bytes[1] = (byte) (val >>> 8);
        }
        return bytes;
    }

    /**
     * Sends a single character to the back end.
     *
     * @param val the character to be sent
     * @throws IOException if an I/O error occurs
     */
    public void sendChar(int val) throws IOException {
        outputStream.write(val);
    }

    /**
     * Send an array of bytes to the backend.
     *
     * @param buf The array of bytes to be sent
     * @throws IOException if an I/O error occurs
     */
    public void send(byte[] buf) throws IOException {
        outputStream.write(buf);
    }

    /**
     * get Socket
     *
     * @return Socket
     */
    public Socket getSocket() {
        return socket;
    }
}