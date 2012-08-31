/**
 *  Starschema Big Query JDBC Driver
 *  Copyright (C) 2012, Starschema Ltd.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * This class implements the java.sql.Connection interface
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.starschema.clouddb.cmdlineverification.Oauth2Bigquery;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.api.services.bigquery.Bigquery;

/**
 * The connection class which builds the connection between BigQuery and the
 * Driver
 * 
 * @author Gunics Balázs, Horváth Attila
 * 
 */
public class BQConnection implements Connection {
    /** Instance log4j.Logger */
    Logger logger;
    /**
     * The bigquery client to access the service.
     */
    private Bigquery bigquery = null;

    /**
     * The projectid which needed for the queries.
     */
    private String projectid = null;
    /** Boolean to determine if the Connection is closed */
    private boolean isclosed = false;

    /** List to contain sql warnings in */
    private List<SQLWarning> SQLWarningList = new ArrayList<SQLWarning>();

    /** String to contain the url except the url prefix */
    private String URLPART = null;

    /**
     * Extracts the JDBC URL then makes a connection to the Bigquery.
     * 
     * @param serverdata
     *            the JDBC connection URL
     * @param loginProp
     * 
     * @throws GeneralSecurityException
     * @throws IOException
     * @throws SQLException
     */
    public BQConnection(String url, Properties loginProp) throws SQLException {
	PropertyConfigurator.configure("log4j.properties");
	logger = Logger.getLogger("BQConnection");
	URLPART = url;
	this.isclosed = false;
	 
	if (url.contains("&user=") && url.contains("&password=")) {
	    int passwordindex = url.indexOf("&password=");
	    int userindex = url.indexOf("&user=");

	    String id;
	    String key;
	    try {
		id = URLDecoder.decode(url.substring(
			userindex + "&user=".length(), passwordindex), "UTF-8");
		key = URLDecoder.decode(
			url.substring(passwordindex + "&password=".length()),
			"UTF-8");
	    } catch (UnsupportedEncodingException e2) {
		throw new SQLException(e2);
	    }

	    String projectid;
	    try {
		projectid = URLDecoder.decode(
			url.substring(url.lastIndexOf(":") + 1), "UTF-8");
	    } catch (UnsupportedEncodingException e1) {
		throw new SQLException(e1);
	    }
	    if (url.contains("?withServiceAccount=true")) {
		this.projectid = projectid.substring(0, projectid.indexOf("?"));
		/*try {*/
		    throw new SQLException(id+"\n"+key+"\n"+this.projectid);
		    /*this.logger.error("2");
		    this.logger.error(id);
		    this.logger.error(key);
		    this.logger.error(this.projectid);
		    this.bigquery = Oauth2Bigquery.authorizeviaservice(id, key);
		} catch (GeneralSecurityException e) {
		    throw new SQLException(e);
		} catch (IOException e) {
		    throw new SQLException(e);
		}*/
	    } else if (url.contains("?withServiceAccount=false")) {
		this.projectid = projectid.substring(0, projectid.indexOf("?"));
		this.logger.info("3");
		this.logger.info(id);
		this.logger.info(key);
		this.logger.info(this.projectid);
		this.bigquery = Oauth2Bigquery.authorizeviainstalled(id, key);
	    } else {
		throw new SQLException(
			"The URL is not in the right format!, ?withServiceAccount must be set to true or false");
	    }
	} else {

	    String id = loginProp.getProperty("user");
	    String key = loginProp.getProperty("password");
	    String projectid;
	    try {
		projectid = URLDecoder.decode(
			url.substring(url.lastIndexOf(":") + 1), "UTF-8");
	    } catch (UnsupportedEncodingException e1) {
		throw new SQLException(e1);
	    }

	   
	   

	    if (url.contains("?withServiceAccount=true")) {
		this.projectid = projectid.substring(0, projectid.indexOf("?"));
		try {
		    this.logger.info("1");
		    this.logger.info(id);
		    this.logger.info(key);
		    this.logger.info(this.projectid);
		    this.bigquery = Oauth2Bigquery.authorizeviaservice(id, key);
		} catch (GeneralSecurityException e) {
		    throw new SQLException(e);
		} catch (IOException e) {
		    throw new SQLException(e);
		}
	    } else {
		if (url.contains("?withServiceAccount=false")) {
		    this.projectid = projectid.substring(0,
			    projectid.indexOf("?"));
		    
		} else {
		    this.projectid = projectid;
		}
		this.logger.info("4");
			this.logger.info(id);
		    this.logger.info(key);
		    this.logger.info(this.projectid);
		this.bigquery = Oauth2Bigquery.authorizeviainstalled(id,
			    key);
	    }
	}
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Uses SQLWarningList.clear() to clear all warnings
     * </p>
     */
    public void clearWarnings() throws SQLException {
	if (this.isclosed)
	    throw new SQLException("Connection is closed.");
	this.SQLWarningList.clear();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Sets bigquery to null and isclosed to true if the connection is not
     * already closed else no operation is performed
     * </p>
     */
    public void close() throws SQLException {
	if (!this.isclosed) {
	    this.bigquery = null;
	    this.isclosed = true;
	}
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws Exception
     * </p>
     * 
     * @throws SQLException
     *             <p>
     *             There is no Commit in Google BigQuery + Connection Status
     *             </p>
     */
    public void commit() throws SQLException {
	if (this.isclosed)
	    throw new SQLException(
		    "There's no commit in Google BigQuery.\nConnection Status: Closed.");
	else
	    throw new SQLException(
		    "There's no commit in Google BigQuery.\nConnection Status: Open.");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Array createArrayOf(String typeName, Object[] elements)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Blob createBlob() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Clob createClob() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public NClob createNClob() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public SQLXML createSQLXML() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Creates a new BQStatement object with the projectid in this Connection
     * </p>
     * 
     * @return a new BQStatement object with the projectid in this Connection
     * @throws SQLException
     *             if the Connection is closed
     */
    public Statement createStatement() throws SQLException {
	if (this.isclosed)
	    throw new SQLException("Connection is closed.");
	return new BQStatement(this.projectid, this);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Statement createStatement(int resultSetType,
	    int resultSetConcurrency, int resultSetHoldability)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Struct createStruct(String typeName, Object[] attributes)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean getAutoCommit() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * Getter method for the authorized bigquery client
     */
    public Bigquery getBigquery() {
	return this.bigquery;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Return projectid
     * </p>
     * 
     * @return projectid Contained in this Connection instance
     */
    public String getCatalog() throws SQLException {
	return this.projectid;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Properties getClientInfo() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public String getClientInfo(String name) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public int getHoldability() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Return a new BQDatabaseMetadata object constructed from this Connection
     * instance
     * </p>
     * 
     * @return a new BQDatabaseMetadata object constructed from this Connection
     *         instance
     */
    public DatabaseMetaData getMetaData() throws SQLException {
	BQDatabaseMetadata metadata = new BQDatabaseMetadata(this);
	return metadata;
    }

    /**
     * Getter method for projectid
     */
    public String getprojectid() {
	return this.projectid;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public int getTransactionIsolation() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * 
     * @return The URL which is in the JDBC drivers connection URL
     */
    public String getURLPART() {
	return this.URLPART;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * If SQLWarningList is empty returns null else it returns the first item
     * Contained inside <br>
     * Subsequent warnings will be chained to this SQLWarning.
     * </p>
     * 
     * @return SQLWarning (The First item Contained in SQLWarningList) + all
     *         others chained to it
     */
    public SQLWarning getWarnings() throws SQLException {
	if (this.isclosed)
	    throw new SQLException("Connection is closed.");
	if (this.SQLWarningList.isEmpty())
	    return null;

	SQLWarning forreturn = this.SQLWarningList.get(0);
	this.SQLWarningList.remove(0);
	if (!SQLWarningList.isEmpty())
	    for (SQLWarning warning : SQLWarningList) {
		forreturn.setNextWarning(warning);
	    }
	return forreturn;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns the status of isclosed boolean
     * </p>
     */
    public boolean isClosed() throws SQLException {
	return this.isclosed;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean isReadOnly() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Sends a query to BigQuery to get all the datasets contained in the
     * project accociated with this Connection object and checks if it's
     * succeeded
     * </p>
     * 
     * @throws SQLException
     */
    public boolean isValid(int timeout) throws SQLException {
	if (this.isclosed)
	    return false;
	if (timeout < 0)
	    throw new SQLException(
		    "Timeout value can't be negative. ie. it must be 0 or above");
	try {
	    this.bigquery.datasets().list(this.projectid).execute();
	} catch (IOException e) {
	    return false;
	}
	return true;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns false to everything
     * </p>
     * 
     * @return false
     */
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
	// TODO Implement
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public String nativeSQL(String sql) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
	    int resultSetConcurrency) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
	    int resultSetConcurrency, int resultSetHoldability)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
	    int resultSetConcurrency) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
	    int resultSetConcurrency, int resultSetHoldability)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
	    throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void rollback() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void rollback(Savepoint savepoint) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setCatalog(String catalog) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setClientInfo(Properties properties)
	    throws SQLClientInfoException {
	SQLClientInfoException e = new SQLClientInfoException();
	e.setNextException(new SQLFeatureNotSupportedException());
	throw e;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setClientInfo(String name, String value)
	    throws SQLClientInfoException {
	SQLClientInfoException e = new SQLClientInfoException();
	e.setNextException(new SQLFeatureNotSupportedException());
	throw e;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setHoldability(int holdability) throws SQLException {
	if (this.isclosed)
	    throw new SQLException("Connection is closed.");
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Savepoint setSavepoint() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public Savepoint setSavepoint(String name) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setTransactionIsolation(int level) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always throws SQLException
     * </p>
     * 
     * @return nothing
     * @throws SQLException
     *             Always
     */
    public <T> T unwrap(Class<T> arg0) throws SQLException {
	throw new SQLException("Not found");
    }

}
