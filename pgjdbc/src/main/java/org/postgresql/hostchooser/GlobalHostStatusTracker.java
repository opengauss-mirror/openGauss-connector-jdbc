/*
 * Copyright (c) 2014, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.hostchooser;

import static java.lang.System.currentTimeMillis;

import org.postgresql.GlobalConnectionTracker;
import org.postgresql.PGProperty;
import org.postgresql.util.HostSpec;

import java.util.*;

/**
 * Keeps track of HostSpec targets in a global map.
 */
public class GlobalHostStatusTracker {
  private static final Map<HostSpec, HostSpecStatus> hostStatusMap =
          new HashMap<HostSpec, HostSpecStatus>();

  /**
   * Store the actual observed host status.
   *
   * @param hostSpec   The host whose status is known.
   * @param hostStatus Latest known status for the host.
   * @param prop       Extra properties controlling the connection.
   */
  public static void reportHostStatus(HostSpec hostSpec, HostStatus hostStatus, Properties prop) {
    HostStatus originalHostStatus;
    long now = currentTimeMillis();
    synchronized (hostStatusMap) {
      HostSpecStatus hostSpecStatus = hostStatusMap.get(hostSpec);
      if (hostSpecStatus == null) {
        hostSpecStatus = new HostSpecStatus(hostSpec);
        hostStatusMap.put(hostSpec, hostSpecStatus);
        originalHostStatus = hostStatus;
      } else {
        originalHostStatus = hostSpecStatus.status;
      }
      hostSpecStatus.status = hostStatus;
      hostSpecStatus.lastUpdated = now;

      observationState(prop, originalHostStatus, hostStatus, hostSpec);
    }
  }

  /**
   * To observe whether the status changes from standby to master
   *
   * @param prop Extra properties controlling the connection.
   * @param originalHostStatus The original state of the current host.
   * @param currentHostStatus  The current status of the current host.
   * @param hostSpec The host whose status is known.
   * @return
   */
  public static void observationState(Properties prop, HostStatus originalHostStatus, HostStatus currentHostStatus, HostSpec hostSpec) {
    if (originalHostStatus == HostStatus.Secondary && currentHostStatus == HostStatus.Master) {
      GlobalConnectionTracker.closeOldConnection(hostSpec.toString(), prop);
    }
  }

  /**
   * Returns a list of candidate hosts that have the required targetServerType.
   *
   * @param hostSpecs         The potential list of hosts.
   * @param targetServerType  The required target server type.
   * @param hostRecheckMillis How stale information is allowed.
   * @return candidate hosts to connect to.
   */
  static List<HostSpec> getCandidateHosts(HostSpec[] hostSpecs,
                                          HostRequirement targetServerType, long hostRecheckMillis) {
    List<HostSpec> candidates = new ArrayList<HostSpec>(hostSpecs.length);
    long latestAllowedUpdate = currentTimeMillis() - hostRecheckMillis;
    synchronized (hostStatusMap) {
      for (HostSpec hostSpec : hostSpecs) {
        HostSpecStatus hostInfo = hostStatusMap.get(hostSpec);
        // candidates are nodes we do not know about and the nodes with correct type
        if (hostInfo == null
                || hostInfo.lastUpdated < latestAllowedUpdate
                || targetServerType.allowConnectingTo(hostInfo.status)) {
          candidates.add(hostSpec);
        }
      }
    }
    return candidates;
  }

  static class HostSpecStatus {
    final HostSpec host;
    HostStatus status;
    long lastUpdated;

    HostSpecStatus(HostSpec host) {
      this.host = host;
    }

    @Override
    public String toString() {
      return host.toString() + '=' + status;
    }
  }
}
