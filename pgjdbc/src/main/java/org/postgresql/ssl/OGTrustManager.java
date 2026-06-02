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

package org.postgresql.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.TrustAnchor;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPath;
import java.security.cert.PKIXParameters;
import java.security.cert.CertPathValidatorResult;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.CertPathValidatorException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * OG TrustManager
 *
 * @author zhangting
 * @since  2026-06-02
 */
public class OGTrustManager implements X509TrustManager {
    private boolean isVerifyCert;
    private X509TrustManager trustManager;

    /**
     * OGTrustManager constructor
     */
    public OGTrustManager() {
    }

    /**
     * OGTrustManager constructor
     *
     * @param trustManager X509TrustManager
     * @param isVerifyCert isVerifyServerCert
     */
    public OGTrustManager(X509TrustManager trustManager, boolean isVerifyCert) {
        this.isVerifyCert = isVerifyCert;
        this.trustManager = trustManager;
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
        checkTrust();
        trustManager.checkServerTrusted(certs, authType);
        if (isVerifyCert) {
            try {
                List<X509Certificate> certificates = new ArrayList<>();
                for (X509Certificate certificate : certs) {
                    certificate.checkValidity();
                    certificates.add(certificate);
                }
                Set<TrustAnchor> trustAnchors = new HashSet<>();
                X509Certificate[] issuers = trustManager.getAcceptedIssuers();
                for (X509Certificate x509Certificate : issuers) {
                    TrustAnchor trustAnchor = new TrustAnchor(x509Certificate, null);
                    trustAnchors.add(trustAnchor);
                }
                CertificateFactory certFact = CertificateFactory.getInstance("X.509");
                CertPath path = certFact.generateCertPath(certificates);
                PKIXParameters params = new PKIXParameters(trustAnchors);
                params.setRevocationEnabled(false);
                CertPathValidator validator = CertPathValidator.getInstance("PKIX");
                CertPathValidatorResult validate = validator.validate(path, params);
                if (validate instanceof PKIXCertPathValidatorResult) {
                    PKIXCertPathValidatorResult cpvr = (PKIXCertPathValidatorResult) validate;
                    cpvr.getTrustAnchor().getTrustedCert().checkValidity();
                }
            } catch (CertPathValidatorException | InvalidAlgorithmParameterException | NoSuchAlgorithmException e) {
                throw new CertificateException(e);
            }
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] certs, String authType)
            throws CertificateException {
        checkTrust();
        trustManager.checkClientTrusted(certs, authType);
    }

    private void checkTrust() throws CertificateException {
        if (trustManager == null) {
            throw new CertificateException("TrustManager is null, create SSL socket failed.");
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManager == null ? new X509Certificate[0] : trustManager.getAcceptedIssuers();
    }
}
