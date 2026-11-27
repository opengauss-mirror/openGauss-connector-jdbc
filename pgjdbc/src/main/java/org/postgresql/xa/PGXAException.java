/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
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

package org.postgresql.xa;

import javax.transaction.xa.XAException;

/**
 * A convenience subclass of <code>XAException</code> which makes it easy to create an instance of
 * <code>XAException</code> with a human-readable message, a <code>Throwable</code> cause, and an XA
 * error code.
 *
 * @author Michael S. Allman
 */
public class PGXAException extends XAException {
  PGXAException(String message, int errorCode) {
    super(message);

    this.errorCode = errorCode;
  }

  PGXAException(String message, Throwable cause, int errorCode) {
    super(message);

    initCause(cause);
    this.errorCode = errorCode;
  }

  PGXAException(Throwable cause, int errorCode) {
    super(errorCode);

    initCause(cause);
  }
}
