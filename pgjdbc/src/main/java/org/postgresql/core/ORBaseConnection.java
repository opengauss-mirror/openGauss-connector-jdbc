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

import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.HostSpec;

import java.sql.Connection;

/**
 * connection interface
 *
 * @author zhangting
 * @since  2025-06-29
 */
public interface ORBaseConnection extends Connection {
    /**
     * get query executor
     *
     * @return query executor
     */
    ORQueryExecutor getQueryExecutor();

    /**
     * set query executor
     *
     * @param queryExecutor query executor
     */
    void setQueryExecutor(ORQueryExecutor queryExecutor);

    /**
     * get connection stream info
     *
     * @return data stream processor
     */
    ORStream getORStream();

    /**
     * set connection stream info
     *
     * @param orStream data stream processor
     */
    void setOrStream(ORStream orStream);

    /**
     * get timestamp utils
     *
     * @return TimestampUtils
     */
    TimestampUtils getTimestampUtils();

    /**
     * use ssl
     *
     * @return isSsl
     */
    boolean isSsl();

    /**
     * get host info
     *
     * @return host info
     */
    HostSpec getHostSpec();

    /**
     * set host info
     *
     * @param hostSpec host info
     */
    void setHostSpec(HostSpec hostSpec);

    /**
     * get url
     *
     * @return url
     */
    String getUrl();
}