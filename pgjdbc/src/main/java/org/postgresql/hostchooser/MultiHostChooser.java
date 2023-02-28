/*
 * Copyright (c) 2014, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.hostchooser;

import static java.util.Collections.shuffle;

import org.postgresql.Driver;
import org.postgresql.PGProperty;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.QueryCNListUtils;
import org.postgresql.quickautobalance.ConnectionManager;
import org.postgresql.quickautobalance.Cluster;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import java.util.*;


/**
 * HostChooser that keeps track of known host statuses.
 */
public class MultiHostChooser implements HostChooser {
  private HostSpec[] hostSpecs;
  private final HostRequirement targetServerType;
  private int hostRecheckTime;
  private boolean loadBalance;
  private LoadBalanceType loadBalanceType;
  private String URLIdentifier;
  private Properties info;
  private static Log LOGGER = Logger.getLogger(MultiHostChooser.class.getName());

  private static final int MAX_CONNECT_NUM = 1 << 30;

  private enum LoadBalanceType {
    Shuffle, RoundRobin, PriorityRoundRobin, LeastConn, NONE
  }

  private static Map<String, Integer> roundRobinCounter = new HashMap<>();


  MultiHostChooser(HostSpec[] hostSpecs, HostRequirement targetServerType,
      Properties info) {
    this.hostSpecs = hostSpecs;
    this.targetServerType = targetServerType;
    this.loadBalanceType = initLoadBalanceType(info);
    this.URLIdentifier = QueryCNListUtils.keyFromURL(info);
    this.info = info;
    try {
      hostRecheckTime = PGProperty.HOST_RECHECK_SECONDS.getInt(info) * 1000;
    } catch (PSQLException e) {
      throw new RuntimeException(e);
    }
  }

  // Select a load balancing algorithm based on the value of autoBalance.
  // In addition, the original loadbalancehosts = true is compatible.
  private LoadBalanceType initLoadBalanceType(Properties info) {
    String autoBalance = info.getProperty("autoBalance", "false");
    if (autoBalance.equals("roundrobin") || autoBalance.equals("true") || autoBalance.equals("balance"))
      return LoadBalanceType.RoundRobin;
    if (autoBalance.contains("priority"))
      return LoadBalanceType.PriorityRoundRobin;
    if (autoBalance.equals("leastconn"))
      return LoadBalanceType.LeastConn;
    if (PGProperty.LOAD_BALANCE_HOSTS.getBoolean(info) || autoBalance.equals("shuffle"))
      return LoadBalanceType.Shuffle;
    return LoadBalanceType.NONE;
  }

  // Load balancing algorithms are executed based on the value of loadBalanceType.
  private List<HostSpec> loadBalance(List<HostSpec> allHosts) {
    Boolean isOutPutLog = true;
    if (allHosts.size() <= 1) {
      return allHosts;
    }
    switch (loadBalanceType) {
      case Shuffle:
        allHosts = new ArrayList<HostSpec>(allHosts);
        shuffle(allHosts);
        break;
      case RoundRobin:
        allHosts = roundRobin(allHosts);
        break;
      case PriorityRoundRobin:
        allHosts = priorityRoundRobin(allHosts);
        break;
      case LeastConn:
        allHosts = leastConn(allHosts);
        break;
      default:
        isOutPutLog = false;
        break;
    }
    if(isOutPutLog){
      LOGGER.info("[AUTOBALANCE] The load balancing result of the cluster is:" +
              " | Cluster: " + URLIdentifier  +
              " | LoadBalanceResult: " + allHosts
      );
    }
    return allHosts;
  }
  
  // Returns a counter and increments it by one.
  // Because it is possible to use it in multiple instances,  use synchronized (MultiHostChooser.class).
  private int getRRIndex() {
    synchronized (roundRobinCounter) {
      int value = roundRobinCounter.getOrDefault(URLIdentifier, 0);
      value = (value + 1) % MAX_CONNECT_NUM;
      roundRobinCounter.put(URLIdentifier, value);
      return value;
    }
  }

  /*
   * Use for RR algorithm. In case of first CN is not been connected, jdbc will
   * try to connect the second one. So shuffering all CN except of the first one
   * will keep balance.
   */
  private List<HostSpec> roundRobin(List<HostSpec> hostSpecs) {
    if (hostSpecs.size() <= 1) {
      return hostSpecs;
    }
    int index = getRRIndex() % hostSpecs.size();
    List<HostSpec> result = new ArrayList<HostSpec>(hostSpecs.size());
    for (int i = 0; i < hostSpecs.size(); i++) {
      int primitiveIndex = (index + i) % hostSpecs.size();
      result.add(hostSpecs.get(primitiveIndex));
    }
    Collections.shuffle(result.subList(1, result.size()));
    return result;
  }
  
  private List<HostSpec> leastConn(List<HostSpec> hostSpecs) {
    if (hostSpecs.size() <= 1) {
      return hostSpecs;
    }
    Cluster cluster = ConnectionManager.getInstance().getCluster(URLIdentifier);
    if (cluster == null) {
      return hostSpecs;
    }
    return cluster.sortDnsByLeastConn(hostSpecs);
  }

  /*
   * Use for RR algorithm. In case of first CN is not been connected, jdbc will
   * try to connect the second one. So shuffering all CN except of the first one
   * will keep balance.
   * CN configured on url has higher priority to be connected.
   */
  private List<HostSpec> priorityRoundRobin(List<HostSpec> hostSpecs) {
    // Obtains the URL CN Host List.
    List<HostSpec> urlHostSpecs;
    int priorityCNNumber = Integer.parseInt(info.getProperty("autoBalance").substring("priority".length()));
    if(PGProperty.PRIORITY_SERVERS.get(info) != null){
      urlHostSpecs = getUrlHostSpecs(hostSpecs);
      if(priorityCNNumber > urlHostSpecs.size()){
        priorityCNNumber = urlHostSpecs.size();
      }
    }else{
      urlHostSpecs = Arrays.asList(Driver.getURLHostSpecs(info));
    }
    // Obtain the currently active CN node that is in the priority state.
    List<HostSpec> priorityURLHostSpecs = getSurvivalPriorityURLHostSpecs(hostSpecs, urlHostSpecs, priorityCNNumber);
    List<HostSpec> nonPriorityHostSpecs = getNonPriorityHostSpecs(hostSpecs, priorityURLHostSpecs);
    if (priorityURLHostSpecs.size() > 0) {
      List<HostSpec> resultHostSpecs = roundRobin(priorityURLHostSpecs);
      shuffle(nonPriorityHostSpecs);
      resultHostSpecs.addAll(nonPriorityHostSpecs);
      return resultHostSpecs;
    } else {
      return roundRobin(hostSpecs);
    }
  }

  // Get the URL string of the current cluster when using disaster recovery switching
  private List<HostSpec> getUrlHostSpecs(List<HostSpec> hostSpecs){
    HostSpec[] urlHostSpecs = Driver.getURLHostSpecs(info);
    Integer index = Integer.valueOf(PGProperty.PRIORITY_SERVERS.get(info));
    HostSpec[] imaginaryMasterHostSpec = Arrays.copyOfRange(urlHostSpecs, 0, index);
    HostSpec[] imaginarySlaveHostSpec = Arrays.copyOfRange(urlHostSpecs, index, urlHostSpecs.length);
    HostSpec[] currentHostSpecs = hostSpecs.toArray(new HostSpec[0]);
    if(Arrays.toString(currentHostSpecs).contains(imaginaryMasterHostSpec[0].toString())){
      return Arrays.asList(imaginaryMasterHostSpec);
    }else{
      return Arrays.asList(imaginarySlaveHostSpec);
    }
  }

  // Returns the alive PriorityURL.
  private List<HostSpec> getSurvivalPriorityURLHostSpecs(List<HostSpec> hostSpecs, List<HostSpec> urlHostSpecs, int priorityCNNumber) {
    List<HostSpec> priorityURLHostSpecs = new ArrayList<>();
    for (int i = 0; i < priorityCNNumber; i++) {
      HostSpec urlHostSpec = urlHostSpecs.get(i);
      for (HostSpec hostSpec : hostSpecs) {
        if (urlHostSpec.equals(hostSpec)) {
          priorityURLHostSpecs.add(urlHostSpec);
          break;
        }
      }
    }
    return priorityURLHostSpecs;
  }

  // Returns hostSpecs except alive PriorityURL.
  private List<HostSpec> getNonPriorityHostSpecs(List<HostSpec> hostSpecs, List<HostSpec> priorityURLHostSpecs) {
    List<HostSpec> nonPriorityHostSpecs = new ArrayList<>();
    for (HostSpec hostSpec : hostSpecs) {
      if (!priorityURLHostSpecs.contains(hostSpec)) {
        nonPriorityHostSpecs.add(hostSpec);
      }
    }
    return nonPriorityHostSpecs;
  }


  /**
   * Determine whether configuring the priority load balancing is valid;
   * if using priority load balancing, "autoBalance" should be start with priority and end with number
   * and the number of CNs with priority should be less than the number of CNs on the URL ;
   * otherwise, return false.
   *
   * testIsVaildPriorityLoadBalance Overwrite the function test.
   *
   * @param props : Connection properties
   */
  public static boolean isVaildPriorityLoadBalance(Properties props) {
    String autoBalance = props.getProperty("autoBalance", "false");
    if (!autoBalance.contains("priority")) {
      return true;
    }
    String priorityLoadBalance = "priority\\d+";
    if (!autoBalance.matches(priorityLoadBalance)) {
      LOGGER.warn("\"autoBalance\" is invaild. When configuring priority load balancing, \"autoBalance\" should be start with priority and end with number.");
      return false;
    }
    String urlPriorityCNNumber = autoBalance.substring("priority".length());
    try {
      int priorityCNNumber = Integer.parseInt(urlPriorityCNNumber);
      int lengthPGPORTURL = props.getProperty("PGPORTURL").split(",").length;
      if (lengthPGPORTURL <= priorityCNNumber) {
        LOGGER.warn("When configuring priority load balancing, the number of CNs with priority should be less than the number of CNs on the URL.");
        return false;
      }
    } catch (NumberFormatException e) {
      LOGGER.warn("When configuring priority load balancing, \"autoBalance\" should be end with number.");
      return false;
    }
    return true;
  }

  public static boolean isUsingAutoLoadBalance(Properties props) {
    String autoBalance = props.getProperty("autoBalance", "false");
    if (autoBalance.equals("shuffle") || autoBalance.equals("roundrobin") || autoBalance.contains("priority") ||
            autoBalance.equals("leastconn") || autoBalance.equals("true") || autoBalance.equals("balance")) {
      return true;
    }
    return false;
  }

  @Override
  public Iterator<CandidateHost> iterator() {
    Iterator<CandidateHost> res = candidateIterator();
    if (!res.hasNext()) {
      // In case all the candidate hosts are unavailable or do not match, try all the hosts just in case
      List<HostSpec> allHosts = Arrays.asList(hostSpecs);
      allHosts = loadBalance(allHosts);
      res = withReqStatus(targetServerType, allHosts).iterator();
    }
    return res;
  }

  private Iterator<CandidateHost> candidateIterator() {
    if (targetServerType != HostRequirement.preferSecondary) {
      return getCandidateHosts(targetServerType).iterator();
    }

    // preferSecondary tries to find secondary hosts first
    // Note: sort does not work here since there are "unknown" hosts,
    // and that "unknown" might turn out to be master, so we should discard that
    // if other secondaries exist
    List<CandidateHost> secondaries = getCandidateHosts(HostRequirement.secondary);
    List<CandidateHost> any = getCandidateHosts(HostRequirement.any);

    if (secondaries.isEmpty()) {
      return any.iterator();
    }

    if (any.isEmpty()) {
      return secondaries.iterator();
    }

    if (secondaries.get(secondaries.size() - 1).equals(any.get(0))) {
      // When the last secondary's hostspec is the same as the first in "any" list, there's no need
      // to attempt to connect it as "secondary"
      // Note: this is only an optimization
      secondaries = rtrim(1, secondaries);
    }
    return append(secondaries, any).iterator();
  }

  private List<CandidateHost> getCandidateHosts(HostRequirement hostRequirement) {
    List<HostSpec> candidates =
        GlobalHostStatusTracker.getCandidateHosts(hostSpecs, hostRequirement, hostRecheckTime);
    candidates = loadBalance(candidates);
    return withReqStatus(hostRequirement, candidates);
  }

  private List<CandidateHost> withReqStatus(final HostRequirement requirement, final List<HostSpec> hosts) {
    return new AbstractList<CandidateHost>() {
      @Override
      public CandidateHost get(int index) {
        return new CandidateHost(hosts.get(index), requirement);
      }

      @Override
      public int size() {
        return hosts.size();
      }
    };
  }

  private <T> List<T> append(final List<T> a, final List<T> b) {
    return new AbstractList<T>() {
      @Override
      public T get(int index) {
        return index < a.size() ? a.get(index) : b.get(index - a.size());
      }

      @Override
      public int size() {
        return a.size() + b.size();
      }
    };
  }

  private <T> List<T> rtrim(final int size, final List<T> a) {
    return new AbstractList<T>() {
      @Override
      public T get(int index) {
        return a.get(index);
      }

      @Override
      public int size() {
        return Math.max(0, a.size() - size);
      }
    };
  }

}
