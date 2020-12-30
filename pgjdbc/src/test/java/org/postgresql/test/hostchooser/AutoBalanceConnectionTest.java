package org.postgresql.test.hostchooser;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.polling.CNListPollingThread;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static java.lang.Integer.parseInt;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.postgresql.Driver.parseURL;
import static org.postgresql.test.TestUtil.closeDB;
import static org.postgresql.test.hostchooser.HostChooserTest.userAndPassword;

public class AutoBalanceConnectionTest {


    @BeforeClass
    public static void setUpClass() {
    }

    private static final String user = TestUtil.getUser();
    private static final String password = TestUtil.getPassword();
    private static final ArrayList<HostSpec> CNListInSetting = getCNListInSettings();
    private static final HashMap<String,String> ipToHostSpec = new HashMap<>();
    private Map<HostSpec, Integer> connectCount = new HashMap<>();
    private Connection con;

    private Map<String, HostSpec[]> coordinationNodeListMapCache;
    private String key;

    @Before
    public void setUp() throws Exception {
        Field field = CNListPollingThread.class.getDeclaredField("coordinationNodeListMapCache");
        field.setAccessible(true);
        coordinationNodeListMapCache = (Map<String, HostSpec[]>) field.get(null);

        if(CNListInSetting.size() < 3) throw new Exception("The number of CNs must be greater than 3.");

        for(HostSpec hostSpec: CNListInSetting){
            TestUtil.initDriver();
            Properties props = userAndPassword();
            con = DriverManager.getConnection(TestUtil.getURL(hostSpec.getHost(),hostSpec.getPort()), props);
            ipToHostSpec.put(getRemoteHostSpec(),hostSpec.toString());
            closeDB(con);
        }
    }

    private static HostSpec hostSpec(String host) {
        int split = host.indexOf(':');
        return new HostSpec(host.substring(0, split), parseInt(host.substring(split + 1)));
    }

    private static ArrayList<HostSpec> getCNListInSettings() {
        ArrayList<HostSpec> cnListInSettings = new ArrayList<>();
        String CNList = System.getProperty("CNList", "");
        String[] addresses = CNList.split(",");
        for (String address : addresses) {
            cnListInSettings.add(hostSpec(address));
        }
        return cnListInSettings;
    }

    protected Connection getConnection(String autoBalnceMode, String... targets) throws SQLException {

        closeDB(con);

        key = null;
        con = null;
        int refreshCNIpListTime = 1;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("autoBalance", autoBalnceMode);
        props.setProperty("refreshCNIpListTime", String.valueOf(refreshCNIpListTime));
        StringBuilder sb = new StringBuilder();

        sb.append("jdbc:postgresql://");
        if (targets != null) {
            for (String target : targets) {
                sb.append(target).append(',');
            }
            sb.setLength(sb.length() - 1);
        }
        sb.append("/");
        sb.append(TestUtil.getDatabase());
        key = CNListPollingThread.keyFromURL(parseURL(sb.toString(), props));
        con = DriverManager.getConnection(sb.toString(), props);
        try {
            sleep(refreshCNIpListTime * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return con;
    }

    boolean isContainAllCoordinationNode(HostSpec[] hostSpecs) {
        if (hostSpecs == null) return false;
        else {
            return CNListInSetting.containsAll(Arrays.asList(hostSpecs));
        }
    }

    // 1.1.1.1/32:8080
    private String getRemoteHostSpec() throws SQLException {
        ResultSet rs = con.createStatement()
                .executeQuery("select inet_server_addr() || ':' || inet_server_port()");
        rs.next();
        return rs.getString(1);
    }

    // 1.1.1.1:8080
    private HostSpec getRegularRemoteHostSpec() throws SQLException {
        return hostSpec(ipToHostSpec.get(getRemoteHostSpec()));
    }


    private void assertCoordinationNodeList() throws SQLException {
        synchronized (coordinationNodeListMapCache) {
            HostSpec[] hostSpecs = coordinationNodeListMapCache.get(key);
            assertEquals(isContainAllCoordinationNode(hostSpecs), Boolean.TRUE);
        }
    }

    private void assertConnectCountEquls(int num) throws SQLException {
        boolean flag = true;
        for(HostSpec hostSpec : CNListInSetting){
            int count = connectCount.getOrDefault(hostSpec,0);
            if(count != num){
                flag = false;
                break;
            }
        }
        assertEquals(flag,Boolean.TRUE);
    }


    private void assertConnectCountGT(int num) throws SQLException {
        boolean flag = true;
        for(HostSpec hostSpec : CNListInSetting){
            int count = connectCount.getOrDefault(hostSpec,0);
            if(count < num){
                flag = false;
                break;
            }
        }
        assertEquals(flag,Boolean.TRUE);
    }

    private void assertConnectCountEquls(int num, int N) throws SQLException {
        boolean flag = true;
        for (int i = 0; i < N; i++) {
            HostSpec hostSpec = CNListInSetting.get(i);
            int count = connectCount.getOrDefault(hostSpec, 0);
            if (count != num) {
                flag = false;
                break;
            }
        }
        assertEquals(flag, Boolean.TRUE);
    }

    @Test
    public void testRefreshCNlist() throws Exception {
        getConnection("shuffle", CNListInSetting.get(0).toString());
        assertCoordinationNodeList();
        getConnection("shuffle", CNListInSetting.get(1).toString());
        assertCoordinationNodeList();
        getConnection("shuffle", CNListInSetting.get(2).toString());
        assertCoordinationNodeList();
        getConnection("shuffle", CNListInSetting.get(3).toString());
        assertCoordinationNodeList();
    }

    @Test
    public void testShuffleLoadBalance() throws Exception {
        connectCount.clear();
        for (int i = 0; i < CNListInSetting.size() * 10; i++) {
            getConnection("shuffle", CNListInSetting.get(0).toString());
            assertCoordinationNodeList();
            HostSpec remote = getRegularRemoteHostSpec();
            connectCount.put(remote, connectCount.getOrDefault(remote, 0) + 1);
        }
        assertConnectCountGT(1);
    }

    @Test
    public void testRoundRobinLoadBalance() throws Exception {
        connectCount.clear();
        for (int i = 0; i < CNListInSetting.size(); i++) {
            getConnection("roundrobin", CNListInSetting.get(0).toString());
            assertCoordinationNodeList();
            HostSpec remote = getRegularRemoteHostSpec();
            connectCount.put(remote, connectCount.getOrDefault(remote, 0) + 1);
        }
        assertConnectCountEquls(1);
    }

    @Test
    public void testRoundRobinPriorityLoadBalance() throws Exception {
        connectCount.clear();
        for (int i = 0; i < 4; i++) {
            getConnection("priority2", CNListInSetting.get(0).toString(),CNListInSetting.get(1).toString(),CNListInSetting.get(2).toString());
            assertCoordinationNodeList();
            HostSpec remote = getRegularRemoteHostSpec();
            connectCount.put(remote, connectCount.getOrDefault(remote, 0) + 1);
        }
        assertConnectCountEquls(2,2);
    }

    @Test
    public void testIsVaildPriorityLoadBalance() throws SQLException {
        // priority3
        try {
            getConnection("priority3", CNListInSetting.get(0).toString(), CNListInSetting.get(1).toString(), CNListInSetting.get(2).toString());
        } catch (SQLException sqlException){
            assertEquals(sqlException.getMessage().contains("No suitable driver found for"),Boolean.TRUE);
        }
        assertNull(con);
        // priorityddd
        try {
            getConnection("priorityddd", CNListInSetting.get(0).toString(), CNListInSetting.get(1).toString(), CNListInSetting.get(2).toString());
        } catch (SQLException sqlException){
            assertEquals(sqlException.getMessage().contains("No suitable driver found for"),Boolean.TRUE);
        }
        assertNull(con);
        // priority-1
        try {
            getConnection("priority-1", CNListInSetting.get(0).toString(), CNListInSetting.get(1).toString(), CNListInSetting.get(2).toString());
        } catch (SQLException sqlException){
            assertEquals(sqlException.getMessage().contains("No suitable driver found for"),Boolean.TRUE);
        }
        assertNull(con);
        // priority1.0
        try {
            getConnection("priority1.0", CNListInSetting.get(0).toString(), CNListInSetting.get(1).toString(), CNListInSetting.get(2).toString());
        } catch (SQLException sqlException){
            assertEquals(sqlException.getMessage().contains("No suitable driver found for"),Boolean.TRUE);
        }
        assertNull(con);

        // 3priority
        try {
            getConnection("3priority", CNListInSetting.get(0).toString(), CNListInSetting.get(1).toString(), CNListInSetting.get(2).toString());
        } catch (SQLException sqlException){
            assertEquals(sqlException.getMessage().contains("No suitable driver found for"),Boolean.TRUE);
        }
        assertNull(con);

        // priority1
        getConnection("priority000000000000000000000000000000000000000000000000000001", CNListInSetting.get(0).toString(), CNListInSetting.get(1).toString(), CNListInSetting.get(2).toString());
        assertNotNull(con);
    }

}
