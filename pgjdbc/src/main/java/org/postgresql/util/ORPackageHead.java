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

package org.postgresql.util;

/**
 * package head.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORPackageHead {
    private int size;
    private byte execCmd;
    private byte execResult;
    private short flags;
    private byte version;
    private byte version1;
    private byte version2;
    private int requestCount;

    /**
     * package head constructor
     */
    public ORPackageHead() {
        this.version = 20;
    }

    /**
     * get message size
     *
     * @return message size
     */
    public int getSize() {
        return size;
    }

    /**
     * set message size
     *
     * @param size message size
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * get command type
     *
     * @return command type
     */
    public byte getExecCmd() {
        return execCmd;
    }

    /**
     * set command type
     *
     * @param execCmd command type
     */
    public void setExecCmd(byte execCmd) {
        this.execCmd = execCmd;
    }

    /**
     * get execute result
     *
     * @return execute result
     */
    public byte getExecResult() {
        return execResult;
    }

    /**
     * set execute result
     *
     * @param execResult execute result
     */
    public void setExecResult(byte execResult) {
        this.execResult = execResult;
    }

    /**
     * get flags
     *
     * @return flags
     */
    public short getFlags() {
        return flags;
    }

    /**
     * set flags
     *
     * @param flags flags
     */
    public void setFlags(short flags) {
        this.flags = flags;
    }

    /**
     * get version
     *
     * @return version
     */
    public byte getVersion() {
        return version;
    }

    /**
     * set version
     *
     * @param version version
     */
    public void setVersion(byte version) {
        this.version = version;
    }

    /**
     * get version1
     *
     * @return version1
     */
    public byte getVersion1() {
        return version1;
    }

    /**
     * set version1
     *
     * @param version1 version1
     */
    public void setVersion1(byte version1) {
        this.version1 = version1;
    }

    /**
     * get version2
     *
     * @return version2
     */
    public byte getVersion2() {
        return version2;
    }

    /**
     * set version2
     *
     * @param version2 version2
     */
    public void setVersion2(byte version2) {
        this.version2 = version2;
    }

    /**
     * get requestCount
     *
     * @return requestCount
     */
    public int getRequestCount() {
        return requestCount;
    }

    /**
     * set requestCount
     *
     * @param requestCount requestCount
     */
    public void setRequestCount(int requestCount) {
        this.requestCount = requestCount;
    }
}
