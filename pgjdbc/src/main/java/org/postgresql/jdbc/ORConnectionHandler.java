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
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.GT;
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
    private static ORStream orStream;

    private ORBaseConnection connection;
    private int sendMsgLen;
    private byte[] clientKey;
    private byte[] scramble;
    private int iteration;
    private Charset charset;
    private byte[] sha256Key;

    /**
     * connection handler constructor
     *
     * @param connection connection
     * @param orStream output/input stream
     */
    public ORConnectionHandler(ORBaseConnection connection, ORStream orStream) {
        this.connection = connection;
        charset = connection.getORStream().getCharset();
        this.orStream = orStream;
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
     * try to connect with CT. handleshake, auth and login
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    public synchronized void tryORConnect() throws IOException, SQLException {
        try {
            handleshake();
            doLogin();
        } catch (SQLException | IOException e) {
            orStream.close();
            throw new PSQLException(GT.tr("The database connection attempt failed."),
                    PSQLState.CONNECTION_UNABLE_TO_CONNECT, e);
        }
    }

    /**
     * tcp handleshake with server
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    private void handleshake() throws IOException, SQLException {
        byte[] orMark = new byte[4];
        orMark[0] = (byte) 0xfe;
        orMark[1] = (byte) 0xdc;
        orMark[2] = (byte) 0xba;
        orMark[3] = (byte) 0x98;
        orStream.send(orMark);
        orStream.flush();
        int endian = orStream.receiveChar();
        boolean isBigEndian = endian == 1;
        orStream.setBigEndian(isBigEndian);

        int serverVersion = orStream.receiveChar();
        orStream.setServerVersion(serverVersion);
        int reponseFlag = orStream.receiveInteger2();
        orStream.setRequestFlag(reponseFlag);

        this.clientKey = new byte[32];
        new SecureRandom().nextBytes(this.clientKey);
        int capacity = 0;
        connection.getORStream().setCapacity(capacity);
        int requestFlag = this.connection.getORStream().getRequestFlag();
        this.connection.getORStream().setRequestFlag(requestFlag);
        ORPackageHead handshakePackageHead = new ORPackageHead();
        ORPackageHead authPackageHead = new ORPackageHead();
        try {
            sendHandshakeQuery(handshakePackageHead);
            processResults(handshakePackageHead, false);
            sendAuthQuery(authPackageHead);
            processResults(authPackageHead, false);
        } catch (IOException | SQLException e) {
            throw new PSQLException(GT.tr("handshake and authentication failed."),
                    PSQLState.CONNECTION_UNABLE_TO_CONNECT, e);
        }
    }

    private void doLogin() throws SQLException {
        ORPackageHead loginPackageHead = new ORPackageHead();
        loginPackageHead.setExecCmd((byte) ORRequestCommand.LOGIN);
        try {
            sendLoginQuery(loginPackageHead);
            processResults(loginPackageHead, true);
        } catch (SQLException | IOException e) {
            throw new PSQLException(GT.tr("login database failed."),
                    PSQLState.CONNECTION_UNABLE_TO_CONNECT, e);
        }
    }

    private void sendLoginQuery(ORPackageHead loginPackageHead) throws SQLException, IOException {
        loginPackageHead.setRequestCount(this.orStream.addRequestCount());
        List<byte[]> sendData = new ArrayList<>();
        byte[] data = getHeadBytes(loginPackageHead);
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
            throw new PSQLException(GT.tr("user or password is null."),
                    PSQLState.CONNECTION_UNABLE_TO_CONNECT);
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

        String address = this.connection.getORStream().getLocalAddress().toString();
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

    private void sendAuthQuery(ORPackageHead authPackageHead) throws SQLException, IOException {
        authPackageHead.setRequestCount(this.orStream.addRequestCount());
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

        authPackageHead.setExecCmd((byte) ORRequestCommand.AUTH_INIT);
        byte[] headBytes = getHeadBytes(authPackageHead);
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

    private byte[] getHeadBytes(ORPackageHead packageHead) {
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

    private void sendHandshakeQuery(ORPackageHead handshakePackageHead) throws IOException {
        handshakePackageHead.setExecCmd((byte) ORRequestCommand.HANDLE_SHAKE);
        handshakePackageHead.setRequestCount(this.orStream.addRequestCount());
        sendMsgLen = PACKAGE_HEAD_SIZE + 4;
        orStream.sendInteger4(sendMsgLen);
        orStream.sendChar(handshakePackageHead.getExecCmd());
        orStream.sendChar(handshakePackageHead.getExecResult());
        orStream.sendInteger2(handshakePackageHead.getFlags());
        orStream.sendChar(handshakePackageHead.getVersion());
        orStream.sendChar(handshakePackageHead.getVersion1());
        orStream.sendChar(handshakePackageHead.getVersion2());
        orStream.sendChar(0);
        orStream.sendInteger4(handshakePackageHead.getRequestCount());
        orStream.sendInteger4(this.connection.getORStream().getRequestFlag());
        orStream.flush();
    }

    private void processResults(ORPackageHead packageHead, boolean isLogin) throws SQLException, IOException {
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
                    handleReponse(packageHead, isLogin, remainLen);
                }
            }
        }
    }

    private void handleReponse(ORPackageHead packageHead, boolean isLogin, int remainLen)
            throws SQLException, IOException {
        if (isLogin) {
            getLoginMsg();
        } else {
            getHandshakeMsg(packageHead, remainLen);
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
        byte[] errBytes = orStream.receive(remainLen - offset);
        int msgLen = 0;
        for (int i = 0; i < errBytes.length; i++) {
            if (errBytes[i] == 0) {
                msgLen = i;
                break;
            }
        }
        String err = new String(errBytes, 0, msgLen, charset);
        throw new PSQLException(GT.tr(err), PSQLState.CONNECTION_FAILURE);
    }

    private void getLoginMsg() throws SQLException, IOException {
        orStream.receiveInteger4();
        orStream.receiveInteger4();
        orStream.receiveInteger4();
        int charsetId = orStream.receiveInteger4();
        if (charsetId == 0) {
            orStream.setCharset(Charset.forName("UTF-8"));
        } else if (charsetId == 1) {
            orStream.setCharset(Charset.forName("GBK"));
        } else {
            throw new SQLException("server charset error, the charset can only be UTF-8 and GBK.");
        }

        int contentLen = orStream.receiveInteger4();
        byte[] signingKey = orStream.receive(contentLen);
        int moreBytesLen = (contentLen % 4 == 0 ? contentLen : contentLen + (4 - contentLen % 4)) - contentLen;
        orStream.receive(moreBytesLen);

        MD5Digest.verifyKey(this.sha256Key, scramble, signingKey);
        orStream.receiveInteger4();
        orStream.receiveInteger4();
    }

    private void getHandshakeMsg(ORPackageHead packageHead, int remainLen) throws IOException, SQLException {
        int rem = remainLen;
        if (packageHead.getExecCmd() != ORRequestCommand.HANDLE_SHAKE) {
            orStream.setCapacity(orStream.receiveInteger4());
            int serverVersion = orStream.receiveInteger4();
            orStream.setServerVersion(serverVersion);
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
                    throw new PSQLException(GT.tr("client key error, handshake failed."), PSQLState.CONNECTION_FAILURE);
                }
            }
        }
    }
}
