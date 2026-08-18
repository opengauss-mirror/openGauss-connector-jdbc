/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.postgresql.xml;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Tests that the legacy XML factory alias no longer enables external entity expansion.
 *
 * @since 2026-08-12
 */
public class LegacyInsecurePGXmlFactoryFactorySecurityTest {
    /**
     * Temporary directory for XML external entity test files.
     */
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void legacyInsecureAliasDoesNotExpandExternalEntities() throws Exception {
        File secret = tmp.newFile("secret.txt");
        Files.write(secret.toPath(), "should-not-leak".getBytes(StandardCharsets.UTF_8));
        String xml = "<!DOCTYPE x [<!ENTITY leak SYSTEM \"" + secret.toURI().toString()
            + "\">]><root>&leak;</root>";

        try {
            LegacyInsecurePGXmlFactoryFactory.INSTANCE
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
            Assert.fail("Expected secure XML parser to reject or neutralize external entities");
        } catch (SAXException expected) {
            Assert.assertNotNull(expected);
        }
    }
}
