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

import org.postgresql.PGConnection;
import org.postgresql.jdbc.TimestampUtils;

import java.sql.Connection;

/**
 * connection interface
 *
 * @author zhangting
 * @since  2025-06-29
 */
public interface ORBaseConnection extends PGConnection, Connection {
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
     * get buffer size
     *
     * @return buffer size
     */
    int getBufferSize();

    /**
     * get timestamp utils
     *
     * @return TimestampUtils
     */
    TimestampUtils getTimestampUtils();
}