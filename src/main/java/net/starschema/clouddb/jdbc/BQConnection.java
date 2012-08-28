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
 * The connection class which builds the connection between the local and the remote pc.
 * 
 * @author starschemaltd
 *
 */
public class BQConnection implements Connection
{
	Logger logger = Logger.getLogger("BQConnection");
	/**
	 * The bigquery client to access the service.
	 */
	private Bigquery bigquery = null;
	
	/**
	 * Getter method for the authorized bigquery client
	 */
	public Bigquery getBigquery(){
		return bigquery;
	}
	
	/**
	 * The projectid which needed for the queries.
	 */
	private String projectid = null;
	
	public String getprojectid()
	{
		return projectid;
	}
	
	private boolean isclosed = true;
	
	private List<SQLWarning> SQLWarningList = new ArrayList<SQLWarning>();
	
	private String URLPART = null;
	/**
	 * 
	 * @return the URL which is in the JDBC drivers connection URL
	 */
	public String getURLPART()
	{
		return URLPART;
	}
	/**
	 * Extracts the JDBC URL then makes a connection to the Bigquery.
	 * @param serverdata, the JDBC connection URL
	 * @throws GeneralSecurityException
	 * @throws IOException
	 * @throws SQLException 
	 */
	public BQConnection(String serverdata) throws SQLException {
		PropertyConfigurator.configure("log4j.properties");
		URLPART = serverdata;
		int indeoxofdot = serverdata.indexOf(":");
		int indeoxofdot2 = serverdata.indexOf(":", indeoxofdot+1);
		
		String id;
		String key;
		String projectid;
		try {
			id = URLDecoder.decode(serverdata.substring(0, indeoxofdot),"UTF-8");
			key = URLDecoder.decode(serverdata.substring(indeoxofdot+1, indeoxofdot2),"UTF-8");
			projectid = URLDecoder.decode(serverdata.substring(serverdata.lastIndexOf(":")+1),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new SQLException(e);
		}
		

		logger.debug(id);
		logger.debug(key);
		logger.debug(projectid);	
		this.isclosed = false;
	
		this.projectid = projectid;
		if(id.contains("serviceacc"))
			try {
				bigquery = Oauth2Bigquery.authorizeviaservice(id.substring(10),key);
			} catch (GeneralSecurityException e) {
				throw new SQLException(e);
			} catch (IOException e) {
				throw new SQLException(e);
			}
		else
		bigquery = Oauth2Bigquery.authorizeviainstalled(id,key);	
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Implement
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Implement
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		if(this.isclosed)throw new SQLException("Connection is closed.");
		SQLWarningList.clear();
	}

	@Override
	public void close() throws SQLException {
		if(!isclosed)
		{
			bigquery = null;
			isclosed = true;			
		}
		else throw new SQLException("Connection already in closed state.");
	}

	@Override
	public void commit() throws SQLException {
		if(this.isclosed)throw new SQLException("Connection is closed.");
		else throw new SQLException("There's no commit in google bigquery.");
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement() throws SQLException {
		if(this.isclosed)throw new SQLException("Connection is closed.");
		return new BQStatement(projectid, this);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCatalog() throws SQLException {
		return this.projectid;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		BQDatabaseMetadata metadata = new BQDatabaseMetadata(this);
		return metadata;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		if(this.isclosed)throw new SQLException("Connection is closed.");
		if(SQLWarningList.isEmpty()) return null;
		SQLWarning forreturn = SQLWarningList.get(0);
		SQLWarningList.remove(0);
		return forreturn;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isclosed;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		if(isclosed) return false;
		if(timeout < 0) throw new SQLException("Timeout value can't be negative. ie. it must be 0 or above");
		try{			 
			bigquery.datasets().list(projectid).execute();
		}
		catch (IOException e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		SQLClientInfoException e = new SQLClientInfoException();
		e.setNextException(new SQLFeatureNotSupportedException());
		throw e;
	}
	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		SQLClientInfoException e = new SQLClientInfoException();
		e.setNextException(new SQLFeatureNotSupportedException());
		throw e;
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		if(isclosed)throw new SQLException("Connection is closed.");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}







