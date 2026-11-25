/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
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

package org.postgresql.xml;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;

public class LegacyInsecurePGXmlFactoryFactory implements PGXmlFactoryFactory {
    public static final LegacyInsecurePGXmlFactoryFactory INSTANCE = new LegacyInsecurePGXmlFactoryFactory();

    private LegacyInsecurePGXmlFactoryFactory() {
    }

    @Override
    public DocumentBuilder newDocumentBuilder() throws ParserConfigurationException {
        return DefaultPGXmlFactoryFactory.INSTANCE.newDocumentBuilder();
    }

    @Override
    public TransformerFactory newTransformerFactory() {
        return DefaultPGXmlFactoryFactory.INSTANCE.newTransformerFactory();
    }

    @Override
    public SAXTransformerFactory newSAXTransformerFactory() {
        return DefaultPGXmlFactoryFactory.INSTANCE.newSAXTransformerFactory();
    }

    @Override
    public XMLInputFactory newXMLInputFactory() {
        return DefaultPGXmlFactoryFactory.INSTANCE.newXMLInputFactory();
    }

    @Override
    public XMLOutputFactory newXMLOutputFactory() {
        return DefaultPGXmlFactoryFactory.INSTANCE.newXMLOutputFactory();
    }

    @Override
    public XMLReader createXMLReader() throws SAXException {
        return DefaultPGXmlFactoryFactory.INSTANCE.createXMLReader();
    }
}
