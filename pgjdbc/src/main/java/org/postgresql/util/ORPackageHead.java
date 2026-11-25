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
        this.version = 36;
    }

    /**
     * init package head
     *
     * @param serverVersion server version
     */
    public void init(int serverVersion) {
        this.size = 0;
        this.execCmd = 0;
        this.execResult = 0;
        this.flags = 0;
        this.version = (byte) serverVersion;
        this.version1 = 0;
        this.version2 = 0;
        this.requestCount = 0;
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
