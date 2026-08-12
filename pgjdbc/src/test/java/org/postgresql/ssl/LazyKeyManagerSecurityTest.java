/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.postgresql.ssl;

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.security.Principal;
import java.security.PublicKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

/**
 * Tests SSL key loading guards that must reject oversized key files before allocation.
 *
 * @since 2026-08-12
 */
public class LazyKeyManagerSecurityTest {
    /**
     * Temporary directory for SSL key files created by the test.
     */
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void rejectOversizedSslKeyFileBeforeAllocation() throws Exception {
        File keyFile = tmp.newFile("huge.pk8");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(keyFile, "rw")) {
            randomAccessFile.setLength(11L * 1024L * 1024L);
        }

        LazyKeyManager keyManager = new LazyKeyManager(null, keyFile.getCanonicalPath(), null,
            false, null);
        Field certField = LazyKeyManager.class.getDeclaredField("cert");
        certField.setAccessible(true);
        certField.set(keyManager, new X509Certificate[]{new DummyX509Certificate()});

        Assert.assertNull(keyManager.getPrivateKey("user"));
        try {
            keyManager.throwKeyManagerException();
            Assert.fail("Expected oversized key file to be rejected");
        } catch (PSQLException e) {
            Assert.assertEquals(PSQLState.CONNECTION_FAILURE.getState(), e.getSQLState());
        }
    }

    private static final class DummyX509Certificate extends X509Certificate {
        private static final long serialVersionUID = 1L;

        private final Principal principal = new DummyPrincipal();

        @Override
        public PublicKey getPublicKey() {
            return new DummyPublicKey();
        }

        @Override
        public void checkValidity() {
        }

        @Override
        public void checkValidity(Date date) {
        }

        @Override
        public int getVersion() {
            return 3;
        }

        @Override
        public BigInteger getSerialNumber() {
            return BigInteger.ONE;
        }

        @Override
        public Principal getIssuerDN() {
            return principal;
        }

        @Override
        public Principal getSubjectDN() {
            return principal;
        }

        @Override
        public Date getNotBefore() {
            return new Date();
        }

        @Override
        public Date getNotAfter() {
            return new Date();
        }

        @Override
        public byte[] getTBSCertificate() throws CertificateEncodingException {
            return new byte[0];
        }

        @Override
        public byte[] getSignature() {
            return new byte[0];
        }

        @Override
        public String getSigAlgName() {
            return "NONE";
        }

        @Override
        public String getSigAlgOID() {
            return "0.0";
        }

        @Override
        public byte[] getSigAlgParams() {
            return new byte[0];
        }

        @Override
        public boolean[] getIssuerUniqueID() {
            return new boolean[0];
        }

        @Override
        public boolean[] getSubjectUniqueID() {
            return new boolean[0];
        }

        @Override
        public boolean[] getKeyUsage() {
            return new boolean[0];
        }

        @Override
        public int getBasicConstraints() {
            return -1;
        }

        @Override
        public byte[] getEncoded() {
            return new byte[0];
        }

        @Override
        public void verify(PublicKey key) {
        }

        @Override
        public void verify(PublicKey key, String sigProvider) {
        }

        @Override
        public String toString() {
            return "DummyX509Certificate";
        }

        @Override
        public boolean hasUnsupportedCriticalExtension() {
            return false;
        }

        @Override
        public Set<String> getCriticalExtensionOIDs() {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getNonCriticalExtensionOIDs() {
            return Collections.emptySet();
        }

        @Override
        public byte[] getExtensionValue(String oid) {
            return new byte[0];
        }
    }

    private static final class DummyPrincipal implements Principal {
        @Override
        public String getName() {
            return "dummy";
        }
    }

    private static final class DummyPublicKey implements PublicKey {
        private static final long serialVersionUID = 1L;

        @Override
        public String getAlgorithm() {
            return "EC";
        }

        @Override
        public String getFormat() {
            return "X.509";
        }

        @Override
        public byte[] getEncoded() {
            return new byte[0];
        }
    }
}
