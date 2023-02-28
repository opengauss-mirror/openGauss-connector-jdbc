package org.postgresql.clusterhealthy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static org.postgresql.clusterhealthy.ClusterNodeCache.checkHostSpecs;

public class ClusterNodeCacheTest {

    private List<HostSpec> getHostSpecs() {
        List<HostSpec> hostSpecList = new ArrayList<>();
        hostSpecList.add(new HostSpec(System.getProperty("server"),
                Integer.parseInt(System.getProperty("port"))));
        hostSpecList.add(new HostSpec(System.getProperty("secondaryServer"),
                Integer.parseInt(System.getProperty("secondaryPort"))));
        hostSpecList.add(new HostSpec(System.getProperty("secondaryServer2"),
                Integer.parseInt(System.getProperty("secondaryServerPort2"))));
        return hostSpecList;
    }
    private static String getUrl() {
        List<String> list = new ArrayList<>();
        list.add(System.getProperty("server") + ":" + System.getProperty("port"));
        list.add(System.getProperty("secondaryServer") + ":" + System.getProperty("secondaryPort"));
        list.add(System.getProperty("secondaryServer2") + ":" + System.getProperty("secondaryServerPort2"));
        String serverAndPort =  list.stream()
                .collect(joining(","));
        String database = getDatabase();
        return String.format("jdbc:postgresql://%s/%s", serverAndPort, database);
    }

    private static String getDatabase() {
        return System.getProperty("database");
    }

    private static String getUsername () {
        return System.getProperty("username");
    }

    private static String getPassword() {
        return System.getProperty("password");
    }

    private static Properties getProperties(List<HostSpec> hostSpecs) {

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

    @Before
    public void initDirver() throws Exception {
        TestUtil.initDriver();
    }
    @Test
    public void getClusterConnection() throws SQLException {
//        Class.forName("org.postgresql.Driver");

        DriverManager.getConnection(getUrl(), getUsername(), getPassword());

    }

    @Test
    public void testPeriodTime() throws Exception {
        String url = getUrl() + "?targetServerType=master";
        DriverManager.getConnection(getUrl(), getUsername(), getPassword());
        String newUrl = url + "&heartbeatPeriod=3000";
        DriverManager.getConnection(newUrl, getUsername(), getPassword());
        Thread.sleep(10000L);
    }

    @Test
    public void testUnavailablePassword() throws ClassNotFoundException, SQLException, InterruptedException {
//        Class.forName("org.postgresql.Driver");
        String url = getUrl() + "?targetServerType=master";
        DriverManager.getConnection(url, getUsername(), getPassword());
        HostSpec master = new HostSpec(System.getProperty("server"),
                Integer.parseInt(System.getProperty("port")));
        ClusterHeartBeatMaster instance = ClusterHeartBeatMaster.getInstance();
        Set<Properties> properties = instance.getProperties(master);
        int before = properties.size();
        // Change password
        Thread.sleep(10000L);

        Set<Properties> afterProperties = instance.getProperties(master);
        int after = afterProperties.size();

        Assert.assertNotEquals(after, before);

        Thread.sleep(10000L);
    }

    @Test
    public void testCheckReplacement() {
        HostSpec master = new HostSpec(System.getProperty("server"),
                Integer.parseInt(System.getProperty("port")));
        Map<HostSpec, HostSpec> failureMap = ClusterHeartBeatFailureMaster.failureMap;
        HostSpec node = new HostSpec("10.10.0.1", 2525);
        failureMap.put(master, node);
        List<HostSpec> hostSpecList = getHostSpecs();
        HostSpec[] hostSpecs = hostSpecList.toArray(new HostSpec[0]);
        ClusterNodeCache.checkReplacement(hostSpecs);
        boolean containsMaster = false;
        boolean containsNode = false;
        for (HostSpec hostSpec : hostSpecs) {
            if (hostSpec.equals(master)) {
                containsMaster = true;
            }
            if (hostSpec.equals(node)) {
                containsNode = true;
            }
        }
        Assert.assertTrue(containsNode);
        Assert.assertFalse(containsMaster);
    }

    @Test
    public void testpushHostSpecs() throws Exception {

        List<HostSpec> hostSpecs = getHostSpecs();
        Properties info = getProperties(hostSpecs);
        checkHostSpecs(hostSpecs.toArray(new HostSpec[0]), info);
        System.out.println(ClusterNodeCache.isOpen());
        Thread.sleep(30000);
    }



}
