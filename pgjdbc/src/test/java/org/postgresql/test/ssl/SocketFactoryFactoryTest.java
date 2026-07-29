/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.test.ssl;

import static org.junit.Assert.assertTrue;

import org.postgresql.PGProperty;
import org.postgresql.core.SocketFactoryFactory;
import org.postgresql.ssl.NonValidatingFactory;
import org.postgresql.util.PSQLException;

import org.junit.Test;

import java.util.Properties;

import javax.net.ssl.SSLSocketFactory;

/**
 * Verifies NonValidatingFactory behavior across verifying and non-verifying SSL modes.
 *
 * @since 2026-07-28
 */
public class SocketFactoryFactoryTest {
    private static final String NON_VALIDATING_FACTORY =
            "org.postgresql.ssl.NonValidatingFactory";

    // sslmode=require keeps the existing non-validating factory behavior compatible.
    @Test
    public void allowsNonValidatingFactoryWithoutVerification()
            throws Exception {
        Properties info = new Properties();
        PGProperty.SSL_MODE.set(info, "require");
        PGProperty.SSL_FACTORY.set(info, NON_VALIDATING_FACTORY);

        SSLSocketFactory factory = SocketFactoryFactory.getSslSocketFactory(info);

        assertTrue(factory instanceof NonValidatingFactory);
    }

    // verify-ca must reject a factory that trusts all certificates.
    @Test
    public void rejectsNonValidatingFactoryWithVerifyCa() throws Exception {
        assertRejectsNonValidatingFactory("verify-ca");
    }

    // verify-full must reject the same certificate validation bypass.
    @Test
    public void rejectsNonValidatingFactoryWithVerifyFull() throws Exception {
        assertRejectsNonValidatingFactory("verify-full");
    }

    // In this driver, ssl=true maps to a verifying SSL mode; keep that path protected as well.
    @Test
    public void rejectsNonValidatingFactoryWhenSslPropertyRequestsVerification()
            throws Exception {
        Properties info = new Properties();
        PGProperty.SSL.set(info, true);
        PGProperty.SSL_FACTORY.set(info, NON_VALIDATING_FACTORY);

        assertRejectsNonValidatingFactory(info);
    }

    // Builds a minimal property set for the same factory selection path.
    private static void assertRejectsNonValidatingFactory(String sslMode) throws Exception {
        Properties info = new Properties();
        PGProperty.SSL_MODE.set(info, sslMode);
        PGProperty.SSL_FACTORY.set(info, NON_VALIDATING_FACTORY);

        assertRejectsNonValidatingFactory(info);
    }

    // The exact error text is less important than rejecting the unsafe combination before use.
    private static void assertRejectsNonValidatingFactory(Properties info) throws Exception {
        boolean isRejected = false;
        try {
            SocketFactoryFactory.getSslSocketFactory(info);
        } catch (PSQLException expected) {
            isRejected = true;
        }
        assertTrue("NonValidatingFactory must be rejected when certificate verification is requested",
                isRejected);
    }
}
