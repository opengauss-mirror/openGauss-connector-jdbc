/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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

package org.postgresql;

/**
 * This interface defines the public PostgreSQL extension for Notifications.
 */
public interface PGNotification {
  /**
   * Returns name of this notification.
   *
   * @return name of this notification
   * @since 7.3
   */
  String getName();

  /**
   * Returns the process id of the backend process making this notification.
   *
   * @return process id of the backend process making this notification
   * @since 7.3
   */
  int getPID();

  /**
   * Returns additional information from the notifying process. This feature has only been
   * implemented in server versions 9.0 and later, so previous versions will always return an empty
   * String.
   *
   * @return additional information from the notifying process
   * @since 8.0
   */
  String getParameter();
}
