/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class implements the java.sql.SQLXML interface
 *
 * @author Horváth Attila
 */
class BQSQLXML implements java.sql.SQLXML {

  /** Reference for the XML document that will be parsed */
  Document document = null;
  /** Indicates wether the SQLXML is readable or not */
  Boolean Readable = true;

  Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Constructor that tries to parse the xmlString to an XML document
   *
   * @param xmlString
   * @throws SQLException if Document Parse error occurs
   */
  public BQSQLXML(String xmlString) throws SQLException {
    logger.debug("making BQSQLXML with the input: " + xmlString);
    try {
      DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
      this.document = docBuilder.parse(new InputSource(new StringReader(xmlString)));
    } catch (SAXException e) {
      throw new BQSQLException(e);
    } catch (IOException e) {
      throw new BQSQLException(e);
    } catch (ParserConfigurationException e) {
      throw new BQSQLException(e);
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Sets the reference to the xml document to null and sets the Readable boolean to false
   */
  @Override
  public void free() throws SQLException {
    this.document = null;
    this.Readable = false;
  }

  /** {@inheritDoc} */
  @Override
  public InputStream getBinaryStream() throws SQLException {
    if (this.Readable == false) {
      throw new BQSQLException("This SQLXML is not readable any more");
    }
    this.Readable = false;
    java.io.InputStream inptstrm;

    if (this.document == null) {
      throw new BQSQLException("This SQLXML is freed");
    } else {
      inptstrm = new java.io.ByteArrayInputStream(this.getS().getBytes());
      return inptstrm;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Reader getCharacterStream() throws SQLException {
    if (this.Readable == false) {
      throw new BQSQLException("This SQLXML is not readable any more");
    }
    this.Readable = false;
    if (this.document == null) {
      throw new BQSQLException("This SQLXML is freed");
    } else {
      Reader rdr = new StringReader(this.getS());
      return rdr;
    }
  }

  /**
   * Tries to parse the XML Document to a String
   *
   * @return String representation of the XML Document
   * @throws SQLException if the Document is freed or unable to get the String representation of the
   *     Document
   */
  private String getS() throws SQLException {

    if (this.document == null) {
      throw new BQSQLException("This SQLXML is freed");
    } else {
      // set up a transformer

      TransformerFactory transfac = TransformerFactory.newInstance();
      Transformer trans = null;
      try {
        trans = transfac.newTransformer();
      } catch (TransformerConfigurationException e) {
        throw new BQSQLException(e);
      }

      try {
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
      } catch (IllegalArgumentException e) {
        throw new BQSQLException(e);
      }

      // create string from xml tree
      StringWriter sw = new StringWriter();
      StreamResult result = new StreamResult(sw);
      DOMSource source = new DOMSource(this.document);
      try {
        trans.transform(source, result);
      } catch (TransformerException e) {
        throw new BQSQLException(e);
      }
      String xmlString = sw.toString();
      return xmlString;
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * This is a minimal implementation!
   */
  @Override
  public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
    if (this.Readable == false) {
      throw new BQSQLException("This SQLXML is not readable any more");
    }
    this.Readable = false;
    if (this.document == null) {
      throw new BQSQLException("This SQLXML is freed");
    } else if (sourceClass == null) {
      throw new BQSQLException("No default implementation");
    } else if (sourceClass == javax.xml.transform.dom.DOMSource.class) {
      javax.xml.transform.dom.DOMSource src = new javax.xml.transform.dom.DOMSource(this.document);
      return sourceClass.cast(src);
    } else if (sourceClass == javax.xml.transform.sax.SAXSource.class) {
      javax.xml.transform.sax.SAXSource src =
          new javax.xml.transform.sax.SAXSource(new InputSource(new StringReader(this.getS())));
      return sourceClass.cast(src);
    } else if (sourceClass == javax.xml.transform.stax.StAXSource.class) {
      XMLInputFactory inputFactory = XMLInputFactory.newInstance();
      XMLEventReader eventReaderXML;
      try {
        eventReaderXML =
            inputFactory.createXMLEventReader("Instance_01", this.getCharacterStream());
      } catch (XMLStreamException e1) {
        throw new BQSQLException(e1);
      }
      inputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", Boolean.TRUE);
      inputFactory.setProperty("javax.xml.stream.isNamespaceAware", Boolean.TRUE);
      inputFactory.setProperty("javax.xml.stream.isReplacingEntityReferences", Boolean.TRUE);
      javax.xml.transform.stax.StAXSource src;
      try {
        src = new javax.xml.transform.stax.StAXSource(eventReaderXML);
      } catch (XMLStreamException e) {
        throw new BQSQLException(e);
      }
      return sourceClass.cast(src);
    } else if (sourceClass == javax.xml.transform.stream.StreamSource.class) {
      javax.xml.transform.stream.StreamSource src =
          new javax.xml.transform.stream.StreamSource(this.getBinaryStream());
      return sourceClass.cast(src);
    } else {
      throw new BQSQLException("No implementation for this class");
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Calls the private function getS if the SQLXML is in Readable state
   */
  @Override
  public String getString() throws SQLException {
    if (this.Readable == false) {
      throw new BQSQLException("This SQLXML is not readable any more");
    }
    this.Readable = false;
    return this.getS();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public OutputStream setBinaryStream() throws SQLException {
    throw new BQSQLException("Not implemented." + "setBinaryStream()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Writer setCharacterStream() throws SQLException {
    throw new BQSQLException("Not implemented." + "setCharacterStream");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
    throw new BQSQLException("Not implemented." + "setResult(Class<T>)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setString(String xmlString) throws SQLException {
    throw new BQSQLException("Not implemented." + "setString(String)");
  }
}
