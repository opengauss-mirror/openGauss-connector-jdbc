/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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

package org.postgresql.core;

import org.postgresql.PGNotification;

public class Notification implements PGNotification {
  public Notification(String p_name, int p_pid) {
    this(p_name, p_pid, "");
  }

  public Notification(String p_name, int p_pid, String p_parameter) {
    m_name = p_name;
    m_pid = p_pid;
    m_parameter = p_parameter;
  }

  /*
   * Returns name of this notification
   */
  public String getName() {
    return m_name;
  }

  /*
   * Returns the process id of the backend process making this notification
   */
  public int getPID() {
    return m_pid;
  }

  public String getParameter() {
    return m_parameter;
  }

  private String m_name;
  private String m_parameter;
  private int m_pid;

}

