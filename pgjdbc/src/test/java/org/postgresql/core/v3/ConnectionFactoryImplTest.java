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
