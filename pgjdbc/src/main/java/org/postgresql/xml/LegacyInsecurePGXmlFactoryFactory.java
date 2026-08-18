/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
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
