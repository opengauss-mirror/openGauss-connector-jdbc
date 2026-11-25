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
