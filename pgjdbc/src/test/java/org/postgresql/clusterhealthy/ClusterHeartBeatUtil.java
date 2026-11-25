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

package org.postgresql.clusterhealthy;

import org.postgresql.util.HostSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.joining;

public class ClusterHeartBeatUtil {
    public static List<HostSpec> getHostSpecs() {
        List<HostSpec> hostSpecList = new ArrayList<>();
        hostSpecList.add(new HostSpec(System.getProperty("server"),
                Integer.parseInt(System.getProperty("port"))));
        hostSpecList.add(new HostSpec(System.getProperty("secondaryServer"),
                Integer.parseInt(System.getProperty("secondaryPort"))));
        hostSpecList.add(new HostSpec(System.getProperty("secondaryServer2"),
                Integer.parseInt(System.getProperty("secondaryServerPort2"))));
        return hostSpecList;
    }
    public static String getUrl() {
        List<String> list = new ArrayList<>();
        list.add(System.getProperty("server") + ":" + System.getProperty("port"));
        list.add(System.getProperty("secondaryServer") + ":" + System.getProperty("secondaryPort"));
        list.add(System.getProperty("secondaryServer2") + ":" + System.getProperty("secondaryServerPort2"));
        String serverAndPort =  list.stream()
                .collect(joining(","));
        String database = getDatabase();
        return String.format("jdbc:postgresql://%s/%s", serverAndPort, database);
    }

    public static String getDatabase() {
        return System.getProperty("database");
    }

    public static String getUsername () {
        return System.getProperty("username");
    }

    public static String getPassword() {
        return System.getProperty("password");
    }

    public static Properties getProperties(List<HostSpec> hostSpecs) {

        Properties properties = new Properties();
        properties.put("user", getUsername());
        properties.put("password", getPassword());
        Properties info = new Properties(properties);
        info.put("PGDBNAME", getDatabase());
        info.put("PGPORT", hostSpecs.stream()
                .map(o -> String.valueOf(o.getPort()))
                .collect(joining(",")));
        info.put("PGHOSTURL", hostSpecs.stream()
                .map(HostSpec::getHost)
                .collect(joining(",")));
        info.put("PGHOST", hostSpecs.stream()
                .map(HostSpec::getHost)
                .collect(joining(",")));
        info.put("PGPORTURL", hostSpecs.stream()
                .map(o -> String.valueOf(o.getPort()))
                .collect(joining(",")));
        return info;
    }
}
