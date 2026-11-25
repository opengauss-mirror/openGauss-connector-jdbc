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

package org.postgresql.log;


public interface Log {
    boolean isDebugEnabled();

    boolean isWarnEnabled();

    boolean isTraceEnabled();

    boolean isInfoEnabled();

    boolean isErrorEnabled();

    boolean isFatalEnabled();

    void debug(Object msg);

    void debug(Object msg, Throwable throwable);

    void warn(Object msg);

    void warn(Object msg, Throwable throwable);

    void trace(Object msg);

    void trace(Object msg, Throwable throwable);

    void info(Object msg);

    void info(Object msg, Throwable throwable);

    void fatal(Object msg);

    void fatal(Object msg, Throwable throwable);

    void error(Object msg);

    void error(Object msg, Throwable throwable);
}
