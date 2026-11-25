/*
 * Copyright (c) 2014, PostgreSQL Global Development Group
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

package org.postgresql.hostchooser;

/**
 * Describes the required server type.
 */
public enum HostRequirement {
  any {
    public boolean allowConnectingTo(HostStatus status) {
      return status != HostStatus.ConnectFail;
    }
  },
  master {
    public boolean allowConnectingTo(HostStatus status) {
      return status == HostStatus.Master || status == HostStatus.ConnectOK;
    }
  },
  secondary {
    public boolean allowConnectingTo(HostStatus status) {
      return status == HostStatus.Secondary || status == HostStatus.ConnectOK;
    }
  },
  preferSecondary {
    public boolean allowConnectingTo(HostStatus status) {
      return status != HostStatus.ConnectFail;
    }
  };

  public abstract boolean allowConnectingTo(HostStatus status);

  /**
   * <p>The postgreSQL project has decided not to use the term slave to refer to alternate servers.
   * secondary or standby is preferred. We have arbitrarily chosen secondary.
   * As of Jan 2018 in order not to break existint code we are going to accept both slave or
   * secondary for names of alternate servers.</p>
   *
   * <p>The current policy is to keep accepting this silently but not document slave, or slave preferSlave</p>
   *
   * @param targetServerType the value of {@code targetServerType} connection property
   * @return HostRequirement
   */

  public static HostRequirement getTargetServerType(String targetServerType) {
    String allowSlave = targetServerType.replace("lave", "econdary");
    return valueOf(allowSlave);
  }

}
