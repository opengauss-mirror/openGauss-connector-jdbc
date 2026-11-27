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
     * fetch lob data
     */
    public static final int READ_LOB_DATA = 13;

    /**
     * handleshake
     */
    public static final int HANDLE_SHAKE = 19;

    /**
     * AuthInit
     */
    public static final int AUTH_INIT = 21;
}
