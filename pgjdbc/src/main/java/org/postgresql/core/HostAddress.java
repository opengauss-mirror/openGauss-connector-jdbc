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

package org.postgresql.core;

/**
 * HostAddress
 *
 * @author zhangting
 * @since  2026-06-02
 */
public class HostAddress {
    private String hostAddress;
    private int port;

    /**
     * hostAddress constructor
     *
     * @param hostAddress hostAddress
     * @param port port
     */
    public HostAddress(String hostAddress, int port) {
        this.hostAddress = hostAddress;
        this.port = port;
    }

    /**
     * get hostAddress
     *
     * @return hostAddress
     */
    public String getAddress() {
        return hostAddress;
    }

    /**
     * get port
     *
     * @return port
     */
    public int getPort() {
        return port;
    }
}
