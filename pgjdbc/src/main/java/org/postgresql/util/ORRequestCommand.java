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
 * request command type
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORRequestCommand {
    /**
     * login
     */
    public static final int LOGIN = 1;

    /**
     * freeStmt
     */
    public static final int FREE_STMT = 2;

    /**
     * prepare
     */
    public static final int PREPARE = 3;

    /**
     * execute
     */
    public static final int EXECUTE = 5;

    /**
     * fetch
     */
    public static final int FETCH = 6;

    /**
     * commit
     */
    public static final int COMMIT = 7;

    /**
     * rollback
     */
    public static final int ROLLBACK = 8;

    /**
     * logout
     */
    public static final int LOGOUT = 9;

    /**
     * cancel
     */
    public static final int CANCEL = 10;

    /**
     * query
     */
    public static final int QUERY = 11;

    /**
     * prepAndExec
     */
    public static final int PREP_AND_EXECUTE = 12;

    /**
     * handleshake
     */
    public static final int HANDLE_SHAKE = 19;

    /**
     * AuthInit
     */
    public static final int AUTH_INIT = 21;
}
