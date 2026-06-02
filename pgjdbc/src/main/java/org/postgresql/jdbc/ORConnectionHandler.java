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

package org.postgresql.jdbc;

import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORStream;
import org.postgresql.ssl.ORStreamSSL;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.ORPackageHead;
import org.postgresql.util.MD5Digest;
import org.postgresql.util.ORRequestCommand;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.List;

/**
 * Establishes and initializes a new connection.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORConnectionHandler {
    private static final int AGENT = 256;
    private static final int PACKAGE_HEAD_SIZE = 16;
    private static final int SSL_SERVER = 512;
    private static final int NO_SSL_SERVER = -513;
    private static final byte[] OR_MARK = new byte[]{(byte) 0xfe, (byte) 0xdc, (byte) 0xba, (byte) 0x98};

    private ORStream orStream;
    private ORBaseConnection connection;
    private byte[] clientKey;
    private byte[] scramble;
    private int iteration;
    private Charset charset;
    private byte[] sha256Key;
    private ORPackageHead packageHead;

    /**
     * connection handler constructor
     *
     * @param connection connection
     * @param orStream output/input stream
     */
    public ORConnectionHandler(ORBaseConnection connection, ORStream orStream) {
        this.connection = connection;
        charset = orStream.getCharset();
        this.orStream = orStream;
        this.packageHead = orStream.getPackageHead();
    }

    /**
     * set sha256Key
     *
     * @param sha256Key sha256Key
     */
    public void setSha256Key(byte[] sha256Key) {
        this.sha256Key = sha256Key;
    }

    /**
     * try to connect with db. handleshake, auth and login
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    public void loginDB() throws IOException, SQLException {
        orStream.getLock().lock();
        try {
            handleshake();
            doLogin();
        } finally {
            if (orStream.getLock().isHeldByCurrentThread()) {
                orStream.getLock().unlock();
            }
        }
    }

    /**
     * tcp handleshake with server
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    private void handleshake() throws IOException, SQLException {
        orStream.send(OR_MARK);
        orStream.flush();
        int endian = orStream.receiveChar();
        boolean isBigEndian = endian == 1;
        orStream.setBigEndian(isBigEndian);

        int version = orStream.receiveChar();
        orStream.setVersion(version);
        int requestFlag = orStream.receiveInteger2();
        int capacity = 0;
        if ((requestFlag & SSL_SERVER) != 0) {
            capacity = capacity | SSL_SERVER;
        }
        orStream.setCapacity(capacity);
        if (orStream.isSsl() && (capacity & SSL_SERVER) == 0) {
            requestFlag = requestFlag & NO_SSL_SERVER;
            orStream.setSsl(false);
        }

        this.clientKey = new byte[32];
        new SecureRandom().nextBytes(this.clientKey);
        requestFlag = orStream.isSsl() ? (requestFlag | SSL_SERVER) : (requestFlag & NO_SSL_SERVER);
        requestFlag = requestFlag | 1;
        orStream.setRequestFlag(requestFlag);
        try {
            sendHandshakeQuery();
            if (orStream.isSsl()) {
                orStream.receive(4);
                ORStreamSSL.createSSLSocket(orStream);
            }
            processResults(false);
            sendAuthQuery();
            processResults(false);
        } catch (IOException | SQLException e) {
            throw new PSQLException("handshake and authentication failed.",
                    PSQLState.CONNECTION_UNABLE_TO_CONNECT, e);
        }
    }

    private void doLogin() throws SQLException {
        packageHead.init(orStream.getServerVersion());
        packageHead.setExecCmd((byte) ORRequestCommand.LOGIN);
        try {
            sendLoginQuery();
            processResults(true);
        } catch (SQLException | IOException e) {
            throw new PSQLException("login database failed.", PSQLState.CONNECTION_UNABLE_TO_CONNECT, e);
        }
    }

    private void sendLoginQuery() throws SQLException, IOException {
        packageHead.setRequestCount(this.orStream.addRequestCount());
        List<byte[]> sendData = new ArrayList<>();
        byte[] data = getHeadBytes();
        sendData.add(data);
        int msgLen = PACKAGE_HEAD_SIZE;
        msgLen = loginEncode(sendData, msgLen);
        msgLen = flagEncode(sendData, msgLen, "user_flag");
        msgLen = flagEncode(sendData, msgLen, "jdbc_flag");

        byte[] shortByte = orStream.getInteger2Bytes(0);
        sendData.add(shortByte);
        sendData.add(shortByte);
        int tzPosition = TimeZone.getDefault().getRawOffset();
        byte[] tzByte = orStream.getInteger2Bytes(tzPosition / (60 * 1000));
        sendData.add(tzByte);
        sendData.add(shortByte);

        byte[] shortByte2 = orStream.getInteger2Bytes(2);
        sendData.add(shortByte2);
        sendData.add(shortByte);
        int readWriteSplit = 0;
        byte[] rwSplitByte = orStream.getInteger2Bytes(readWriteSplit);
        sendData.add(rwSplitByte);
        msgLen += PACKAGE_HEAD_SIZE;
        sendData.add(shortByte);
        msgLen = labelEncode(sendData, msgLen);
        orStream.sendInteger4(msgLen);
        for (byte[] bs : sendData) {
            orStream.send(bs);
        }
        orStream.flush();
    }

    private int loginEncode(List<byte[]> sendData, int msgLen) throws SQLException {
        int len = msgLen;
        String userName = this.connection.getClientInfo("user");
        String password = this.connection.getClientInfo("password");
        if (userName == null || password == null) {
            throw new PSQLException("user or password is null.", PSQLState.CONNECTION_UNABLE_TO_CONNECT);
        }
        byte[] userByte = userName.getBytes(charset);
        len += userByte.length;
        byte[] userLenByte = orStream.getInteger4Bytes(userByte.length);
        len += userLenByte.length;
        byte[] userLeftBytes = fillBytes(userByte.length);
        len += userLeftBytes.length;
        sendData.add(userLenByte);
        sendData.add(userByte);
        sendData.add(userLeftBytes);

        byte[] passWordByte = MD5Digest.sha256encode(password, this.scramble, this.iteration, this);
        len += passWordByte.length;
        byte[] passWordLenByte = orStream.getInteger4Bytes(passWordByte.length);
        len += passWordLenByte.length;
        byte[] passWordLeftBytes = fillBytes(passWordByte.length);
        len += passWordLeftBytes.length;
        sendData.add(passWordLenByte);
        sendData.add(passWordByte);
        sendData.add(passWordLeftBytes);

        String address = orStream.getLocalAddress().toString();
        byte[] addressByte = address.getBytes(charset);
        len += addressByte.length;
        byte[] addressByteLenByte = orStream.getInteger4Bytes(addressByte.length);
        len += addressByteLenByte.length;
        byte[] addressLeftBytes = fillBytes(addressByte.length);
        len += addressLeftBytes.length;
        sendData.add(addressByteLenByte);
        sendData.add(addressByte);
        sendData.add(addressLeftBytes);
        return len;
    }

    private int flagEncode(List<byte[]> sendData, int msgLen, String flag) {
        int len = msgLen;
        byte[] flagByte = flag.getBytes(charset);
        len += flagByte.length;
        byte[] flagLenByte = orStream.getInteger4Bytes(flagByte.length);
        len += flagLenByte.length;
        byte[] flagLeftBytes = fillBytes(flagByte.length);
        len += flagLeftBytes.length;
        sendData.add(flagLenByte);
        sendData.add(flagByte);
        sendData.add(flagLeftBytes);
        return len;
    }

    private int labelEncode(List<byte[]> sendData, int msgLen) throws SQLException {
        int len = msgLen;
        byte[] intByte = new byte[0];
        byte[] userLabelByte = new byte[0];
        byte[] userLabelByteLen = new byte[0];
        byte[] userLabelLeftBytes = new byte[0];
        String userLabel = this.connection.getClientInfo("tenantName");
        if (userLabel == null) {
            intByte = orStream.getInteger4Bytes(0);
            len += 4;
        } else {
            userLabelByte = userLabel.getBytes(charset);
            userLabelByteLen = orStream.getInteger4Bytes(userLabelByte.length);
            userLabelLeftBytes = fillBytes(userLabelByte.length);
            len = len + userLabelByte.length + userLabelByteLen.length + userLabelLeftBytes.length;
        }
        sendData.add(intByte);
        sendData.add(userLabelByteLen);
        sendData.add(userLabelByte);
        sendData.add(userLabelLeftBytes);
        return len;
    }

    private byte[] fillBytes(int dataLen) {
        if (dataLen % 4 == 0) {
            return new byte[0];
        }
        return new byte[4 - dataLen % 4];
    }

    private void sendAuthQuery() throws SQLException, IOException {
        packageHead.init(orStream.getServerVersion());
        packageHead.setRequestCount(this.orStream.addRequestCount());
        int msgLen = PACKAGE_HEAD_SIZE;
        byte[] userNameByte = connection.getClientInfo("user").getBytes(charset);
        msgLen += userNameByte.length;
        byte[] userNameLenyte = orStream.getInteger4Bytes(userNameByte.length);
        msgLen += userNameLenyte.length;
        byte[] userNameLeftBytes = fillBytes(userNameByte.length);
        msgLen += userNameLeftBytes.length;

        msgLen += this.clientKey.length;
        byte[] clientKeyLenByte = orStream.getInteger4Bytes(this.clientKey.length);
        msgLen += clientKeyLenByte.length;
        byte[] clientKeyLeftBytes = fillBytes(this.clientKey.length);
        msgLen += clientKeyLeftBytes.length;

        byte[] bs = new byte[0];
        byte[] userLabelByte = new byte[0];
        byte[] userLabelByteLen = new byte[0];
        byte[] userLabelLeftBytes = new byte[0];
        if (orStream.getServerVersion() >= 18) {
            String userLabel = connection.getClientInfo("tenantName");
            if (userLabel == null) {
                bs = orStream.getInteger4Bytes(0);
                msgLen += 4;
            } else {
                userLabelByte = userLabel.getBytes(orStream.getCharset());
                userLabelByteLen = orStream.getInteger4Bytes(userLabelByte.length);
                userLabelLeftBytes = fillBytes(userLabelByte.length);
                msgLen = msgLen + userLabelByteLen.length + userLabelByte.length + userLabelLeftBytes.length;
            }
        }

        packageHead.setExecCmd((byte) ORRequestCommand.AUTH_INIT);
        byte[] headBytes = getHeadBytes();
        orStream.sendInteger4(msgLen);
        orStream.send(headBytes);
        orStream.send(userNameLenyte);
        orStream.send(userNameByte);
        orStream.send(userNameLeftBytes);
        orStream.send(clientKeyLenByte);
        orStream.send(this.clientKey);
        orStream.send(clientKeyLeftBytes);
        orStream.send(bs);
        orStream.send(userLabelByteLen);
        orStream.send(userLabelByte);
        orStream.send(userLabelLeftBytes);
        orStream.flush();
    }

    private byte[] getHeadBytes() {
        byte[] flagByte = orStream.getInteger2Bytes(packageHead.getFlags());
        byte[] data = new byte[12];
        int index = 0;
        data[index++] = packageHead.getExecCmd();
        data[index++] = packageHead.getExecResult();
        for (int i = 0; i < flagByte.length; i++) {
            data[index++] = flagByte[i];
        }

        data[index++] = packageHead.getVersion();
        data[index++] = packageHead.getVersion1();
        data[index++] = packageHead.getVersion2();
        data[index++] = 0;
        byte[] serialNumberByte = orStream.getInteger4Bytes(packageHead.getRequestCount());
        for (int i = 0; i < serialNumberByte.length; i++) {
            data[index++] = serialNumberByte[i];
        }
        return data;
    }

    private void sendHandshakeQuery() throws IOException {
        packageHead.init(orStream.getServerVersion());
        packageHead.setExecCmd((byte) ORRequestCommand.HANDLE_SHAKE);
        packageHead.setRequestCount(this.orStream.addRequestCount());
        int sendMsgLen = PACKAGE_HEAD_SIZE + 4;
        orStream.sendInteger4(sendMsgLen);
        orStream.sendChar(packageHead.getExecCmd());
        orStream.sendChar(packageHead.getExecResult());
        orStream.sendInteger2(packageHead.getFlags());
        orStream.sendChar(packageHead.getVersion());
        orStream.sendChar(packageHead.getVersion1());
        orStream.sendChar(packageHead.getVersion2());
        orStream.sendChar(0);
        orStream.sendInteger4(packageHead.getRequestCount());
        orStream.sendInteger4(orStream.getRequestFlag());
        orStream.flush();
    }

    private void processResults(boolean isLogin) throws SQLException, IOException {
        boolean hasResult = true;
        while (hasResult) {
            packageHead.setSize(orStream.receiveInteger4());
            packageHead.setExecCmd((byte) orStream.receiveChar());
            packageHead.setExecResult((byte) orStream.receiveChar());
            packageHead.setFlags((short) orStream.receiveInteger2());
            packageHead.setVersion((byte) orStream.receiveChar());
            packageHead.setVersion2((byte) orStream.receiveChar());
            packageHead.setVersion2((byte) orStream.receiveChar());
            orStream.receiveChar();
            packageHead.setRequestCount(orStream.receiveInteger4());
            hasResult = (packageHead.getFlags() & AGENT) != 0;
            int remainLen = packageHead.getSize() - PACKAGE_HEAD_SIZE;
            int requestCount = orStream.getRequestCount();
            if (packageHead.getRequestCount() == requestCount && remainLen > 0) {
                if (packageHead.getExecResult() != 0) {
                    handleError(remainLen);
                } else {
                    handleReponse(isLogin, remainLen);
                }
            }
        }
    }

    private void handleReponse(boolean isLogin, int remainLen)
            throws SQLException, IOException {
        if (isLogin) {
            getLoginMsg();
        } else {
            getHandshakeMsg(remainLen);
        }
    }

    private void handleError(int remainLen) throws IOException, SQLException {
        int offset = 0;
        orStream.receiveInteger4();
        offset += 4;
        orStream.receiveInteger2();
        offset += 2;
        orStream.receiveInteger2();
        offset += 2;
        orStream.receiveInteger2();
        offset += 2;
        orStream.receiveInteger2();
        offset += 2;
        byte[] errBytes;
        int protocolVersion = orStream.getServerVersion();
        if (orStream.isHandshake()) {
            protocolVersion = orStream.getVersion();
        }
        int msgLen = 0;
        int maxLen = remainLen - offset;
        if (protocolVersion < 23) {
            errBytes = orStream.receive(maxLen);
            for (int i = 0; i < errBytes.length; i++) {
                if (errBytes[i] == 0) {
                    msgLen = i;
                    break;
                }
            }
        } else {
            msgLen = orStream.receiveInteger4();
            maxLen -= 4;
            int msgByteLen = msgLen % 4 == 0 ? msgLen : msgLen + (4 - msgLen % 4);
            if (msgByteLen > maxLen) {
                throw new SQLException("message length error.");
            }
            errBytes = orStream.receive(msgByteLen);
            maxLen -= msgByteLen;
            orStream.receive(maxLen);
        }

        String err = new String(errBytes, 0, msgLen, charset);
        throw new PSQLException(err, PSQLState.CONNECTION_FAILURE);
    }

    private void getLoginMsg() throws SQLException, IOException {
        int sessionId = orStream.receiveInteger4();
        orStream.setSessionId(sessionId);
        int sessionNumber = orStream.receiveInteger4();
        orStream.setSessionNumber(sessionNumber);
        orStream.receiveInteger4();
        int charsetFlag = orStream.receiveInteger4();
        if (charsetFlag == 1) {
            charset = Charset.forName("GBK");
        } else if (charsetFlag == 0) {
            charset = Charset.forName("UTF-8");
        } else {
            throw new SQLException("server charset error, the charset can only be UTF-8 and GBK.");
        }
        orStream.setCharset(charset);

        int contentLen = orStream.receiveInteger4();
        byte[] signingKey = orStream.receive(contentLen);
        int moreBytesLen = (contentLen % 4 == 0 ? contentLen : contentLen + (4 - contentLen % 4)) - contentLen;
        orStream.receive(moreBytesLen);

        MD5Digest.verifyKey(this.sha256Key, scramble, signingKey);
        orStream.receiveInteger4();
        orStream.receiveInteger4();
    }

    private void getHandshakeMsg(int remainLen) throws IOException, SQLException {
        int rem = remainLen;
        if (packageHead.getExecCmd() != ORRequestCommand.HANDLE_SHAKE) {
            orStream.setCapacity(orStream.receiveInteger4());
            int serverVersion = orStream.receiveInteger4();
            orStream.setServerVersion(serverVersion);
            orStream.setHandshake(true);
            rem -= 8;
            int contentLen = orStream.receiveInteger4();
            this.scramble = orStream.receive(contentLen);
            int moreBytesLen = (contentLen % 4 == 0 ? contentLen : contentLen + (4 - contentLen % 4)) - contentLen;
            orStream.receive(moreBytesLen);
            rem = rem - 4 - this.scramble.length - moreBytesLen;
            if (rem > 0) {
                this.iteration = orStream.receiveInteger4();
            } else {
                this.iteration = 1000000;
            }
            for (int i = 0; i < this.clientKey.length; i++) {
                if (this.scramble[i] != this.clientKey[i]) {
                    throw new PSQLException("client key error, handshake failed.", PSQLState.CONNECTION_FAILURE);
                }
            }
        }
    }
}
