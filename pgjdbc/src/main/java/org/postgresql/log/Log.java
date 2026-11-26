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
