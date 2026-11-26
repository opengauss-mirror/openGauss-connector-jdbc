/*
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

package org.postgresql.core.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Tests the per-connection kerberosServerHostname resolution before GSS authentication.
 * The cases avoid a live KDC and focus on the value passed into MakeGSS/GssAction.
 *
 * @since 2026-07-31
 */
public class ConnectionFactoryImplTest {
    private String oldKerberosServerHostname;

    @Before
    public void saveKerberosServerHostname() {
        oldKerberosServerHostname =
                System.getProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME);
        System.clearProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME);
    }

    @After
    public void restoreKerberosServerHostname() {
        if (oldKerberosServerHostname == null) {
            System.clearProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME);
        } else {
            System.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME,
                    oldKerberosServerHostname);
        }
    }

    @Test
    public void usesConnectionKerberosServerHostnameBeforeGlobalProperty() {
        Properties info = new Properties();
        info.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "service.example.com");
        System.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "global.example.com");

        /*
         * Original behavior check:
         * the connection-level kerberosServerHostname still takes precedence for
         * the current GSS authentication attempt.
         */
        assertEquals("service.example.com",
                ConnectionFactoryImpl.getKerberosServerHostname(info, true));
    }

    @Test
    public void doesNotWriteConnectionKerberosServerHostnameToGlobalProperty() {
        Properties info = new Properties();
        info.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "service.example.com");

        /*
         * Security regression check:
         * resolving the connection-level hostname must not publish it through the
         * JVM-wide property where a later connection could accidentally reuse it.
         */
        assertEquals("service.example.com",
                ConnectionFactoryImpl.getKerberosServerHostname(info, true));
        assertNull(System.getProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME));
    }

    @Test
    public void preservesLegacyGlobalKerberosServerHostnameFallback() {
        Properties info = new Properties();
        System.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "global.example.com");

        /*
         * Compatibility check:
         * applications that deliberately configured the legacy JVM property keep
         * that behavior when the connection itself does not supply the parameter.
         */
        assertEquals("global.example.com",
                ConnectionFactoryImpl.getKerberosServerHostname(info, true));
    }

    @Test
    public void usesLegacyGlobalKerberosServerHostnameWhenConnectionValueIsEmpty() {
        Properties info = new Properties();
        info.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "");
        System.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "global.example.com");

        /*
         * Compatibility check:
         * an empty connection-level value keeps the historical fallback path, so
         * callers that intentionally use the JVM property are not broken.
         */
        assertEquals("global.example.com",
                ConnectionFactoryImpl.getKerberosServerHostname(info, true));
    }

    @Test
    public void doesNotInventKerberosServerHostnameWhenUnsetEverywhere() {
        Properties info = new Properties();

        /*
         * Fallback check:
         * when neither the connection nor the JVM configured an override, null is
         * passed forward so GssAction can fall back to the current connection host.
         */
        assertNull(ConnectionFactoryImpl.getKerberosServerHostname(info, true));
    }

    @Test
    public void doesNotPolluteNextGssConnectionWithoutHostname() {
        Properties firstInfo = new Properties();
        firstInfo.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME,
                "service.example.com");
        Properties secondInfo = new Properties();

        /*
         * Reported vulnerability sequence:
         * connection A supplies kerberosServerHostname, then connection B omits it.
         * Because the driver no longer writes A's value into System properties, B
         * receives null and GssAction falls back to B's current host.
         */
        assertEquals("service.example.com",
                ConnectionFactoryImpl.getKerberosServerHostname(firstInfo, true));
        assertNull(ConnectionFactoryImpl.getKerberosServerHostname(secondInfo, true));
        assertNull(System.getProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME));
    }

    @Test
    public void keepsSspiFromUsingConnectionKerberosServerHostname() {
        Properties info = new Properties();
        info.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "service.example.com");

        /*
         * Compatibility check:
         * the original SSPI branch did not consume the connection-level property.
         * The refactor keeps that behavior while still passing an explicit value
         * into GssAction instead of letting it read global state during I/O.
         */
        assertNull(ConnectionFactoryImpl.getKerberosServerHostname(info, false));
    }

    @Test
    public void keepsSspiLegacyGlobalKerberosServerHostnameFallback() {
        Properties info = new Properties();
        info.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "service.example.com");
        System.setProperty(ConnectionFactoryImpl.KERBEROS_SERVER_HOSTNAME, "global.example.com");

        /*
         * SSPI compatibility check:
         * the refactor does not start honoring the connection-level GSS-only
         * property for SSPI, but it preserves the old JVM property fallback.
         */
        assertEquals("global.example.com",
                ConnectionFactoryImpl.getKerberosServerHostname(info, false));
    }
}
