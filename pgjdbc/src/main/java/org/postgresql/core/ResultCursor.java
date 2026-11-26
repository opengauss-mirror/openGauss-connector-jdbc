/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core;

/**
 * Abstraction of a cursor over a returned resultset. This is an opaque interface that only provides
 * a way to close the cursor; all other operations are done by passing a ResultCursor to
 * QueryExecutor methods.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public interface ResultCursor {
  /**
   * Close this cursor. This may not immediately free underlying resources but may make it happen
   * more promptly. Closed cursors should not be passed to QueryExecutor methods.
   */
  void close();
}
