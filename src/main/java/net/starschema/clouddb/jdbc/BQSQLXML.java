package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

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

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


class BQSQLXML implements java.sql.SQLXML
{
	Document document = null;
	Boolean Readable = true;
	public BQSQLXML(String xmlString) throws SQLException
	{
		try  
		{  
			DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
	        DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
	        document = docBuilder.parse(new InputSource(new StringReader(xmlString))); 
		}
		catch (SAXException e) {
			throw new SQLException(e);
		}
		catch (IOException e) {
			throw new SQLException(e);
		}
		catch (ParserConfigurationException e){
			throw new SQLException(e);
		}
	}
	
	@Override
	public void free() throws SQLException {
		this.document = null;
	}

	@Override
	public InputStream getBinaryStream() throws SQLException {
		if(this.Readable == false)throw new SQLException("This SQLXML is not readable any more");
		this.Readable = false;
		java.io.InputStream inptstrm;
		
		if(this.document == null)
		{
			throw new SQLException("This SQLXML is freed");
		}
		else
		{
			inptstrm = new java.io.ByteArrayInputStream(this.getS().getBytes());
			return inptstrm;
		}
	}

	@Override
	public Reader getCharacterStream() throws SQLException {
		if(this.Readable == false)throw new SQLException("This SQLXML is not readable any more");
		this.Readable = false;
		if(this.document == null)
		{
			throw new SQLException("This SQLXML is freed");
		}
		else
		{
			Reader rdr = new StringReader(this.getS());
			return rdr;
		}
	}

	@Override
	public <T extends Source> T getSource(Class<T> sourceClass)
			throws SQLException {
		if(this.Readable == false)throw new SQLException("This SQLXML is not readable any more");
		this.Readable = false;
		if(this.document == null)
		{
			throw new SQLException("This SQLXML is freed");
		}
		else
		{
			if(sourceClass == null)
			{
				throw new SQLException("No default implementation");
			}
			else if(sourceClass == javax.xml.transform.dom.DOMSource.class)
			{
				javax.xml.transform.dom.DOMSource src = new javax.xml.transform.dom.DOMSource(document);
				return sourceClass.cast(src);
			}
			else if(sourceClass == javax.xml.transform.sax.SAXSource.class)
			{
				javax.xml.transform.sax.SAXSource src = new javax.xml.transform.sax.SAXSource(new InputSource(new StringReader(this.getS())));
				return sourceClass.cast(src);
			}
			else if(sourceClass == javax.xml.transform.stax.StAXSource.class)
			{
				XMLInputFactory inputFactory = XMLInputFactory.newInstance();
				XMLEventReader eventReaderXML;
				try {
					eventReaderXML = inputFactory.createXMLEventReader("Instance_01", this.getCharacterStream());
				} catch (XMLStreamException e1) {
					throw new SQLException(e1);
				}
				inputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", Boolean.TRUE);
			    inputFactory.setProperty("javax.xml.stream.isNamespaceAware", Boolean.TRUE);
			    inputFactory.setProperty("javax.xml.stream.isReplacingEntityReferences", Boolean.TRUE);
				javax.xml.transform.stax.StAXSource src;
				try {
					src = new javax.xml.transform.stax.StAXSource(eventReaderXML);
				} catch (XMLStreamException e){
					throw new SQLException(e);
				}
				return sourceClass.cast(src);
			}
			else if(sourceClass == javax.xml.transform.stream.StreamSource.class)
			{
				javax.xml.transform.stream.StreamSource src = new javax.xml.transform.stream.StreamSource(this.getBinaryStream());
				return sourceClass.cast(src);
			}
			else
			{
				throw new SQLException("No implementation for this class");
			}
		}
	}

	private String getS() throws SQLException {
		if(this.document == null)
		{
			throw new SQLException("This SQLXML is freed");
		}
		else
		{
			//set up a transformer
			
			TransformerFactory transfac = TransformerFactory.newInstance();
			Transformer trans = null;
			try {
				trans = transfac.newTransformer();
			} catch (TransformerConfigurationException e)  {
				throw new SQLException(e);
				}
			
			try {
				trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				trans.setOutputProperty(OutputKeys.INDENT, "yes");
			} catch (IllegalArgumentException e)  {
				throw new SQLException(e);
				}

			

			//create string from xml tree
			StringWriter sw = new StringWriter();
			StreamResult result = new StreamResult(sw);
			DOMSource source = new DOMSource(document);
			try {
				trans.transform(source, result);
			} catch (TransformerException e) {
				throw new SQLException(e);
				}
			String xmlString = sw.toString();
			return xmlString;
		}
		
	}
	
	@Override
	public String getString() throws SQLException {
		if(this.Readable == false)throw new SQLException("This SQLXML is not readable any more");
		this.Readable = false;
		return this.getS();
	}

	@Override
	public OutputStream setBinaryStream() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Writer setCharacterStream() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T extends Result> T setResult(Class<T> resultClass)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setString(String xmlString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
