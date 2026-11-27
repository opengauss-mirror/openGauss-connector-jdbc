/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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