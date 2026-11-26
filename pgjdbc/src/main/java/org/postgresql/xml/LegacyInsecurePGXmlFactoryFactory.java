/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
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
