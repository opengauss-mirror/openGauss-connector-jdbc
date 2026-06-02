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
