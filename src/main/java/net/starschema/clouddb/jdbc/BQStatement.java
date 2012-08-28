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
 *  
 * This class implements the java.sql.Statement interface
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

import org.apache.log4j.Logger;

import com.google.api.services.bigquery.model.Job;


public class BQStatement implements java.sql.Statement
{
	private String ProjectId = null;
	private BQConnection connection;
	private int querytimeout = Integer.MAX_VALUE;
	Logger logger = Logger.getLogger(BQStatement.class);
	/**
	 * Constructor for BQStatement object just initializes local variables
	 * @param projectid
	 * @param bqConnection
	 */
	public BQStatement(String projectid, BQConnection bqConnection) {
		this.ProjectId = projectid;
		this.connection = bqConnection;
	}

	ResultSet resset = null;
	
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancel() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void close() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String arg0) throws SQLException {
		this.resset = executeQuery(arg0);
		
		if(resset != null)
		return true;
		else
			return false;
	}

	@Override
	public boolean execute(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet executeQuery(String querySql) throws SQLException {
		Job referencedJob;
		try
		{
		//Gets the Job reference of the completed job with give Query
		  referencedJob = BigQueryApi.startQuery(this.connection.getBigquery(), this.ProjectId, querySql);
		}
		catch(IOException e) {
			throw new SQLException(e);
		} catch (InterruptedException e) {
			throw new SQLException(e);
		}
		long starttime = System.currentTimeMillis();
		try {		
			do{
				if(BigQueryApi.getQueryState(referencedJob, this.connection.getBigquery(), this.ProjectId).equals("DONE")){
					return new BQResultSet(	BigQueryApi.GetQueryResults(
														this.connection.getBigquery(), 
														this.ProjectId, 
														referencedJob),this);	
				}
			    // Pause execution for half second before polling job status again, to
			    // reduce unnecessary calls to the BigQUery API and lower overall
			    // application bandwidth.
				Thread.sleep(500);
				logger.debug("slept for 500" + "ms, querytimeout is: "+ querytimeout +"s");
			}while(System.currentTimeMillis()-starttime<= (long)querytimeout*1000 );
					//it runs for a minimum of 1 time
		} catch (IOException e) {
			throw new SQLException(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//here we should kill/stop the running job, but bigquery doesn't support that :(
		throw new SQLException("Query run took more than the specified timeout");
	}

	@Override
	public int executeUpdate(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Connection getConnection() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRows() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getMoreResults(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		if(this.querytimeout == Integer.MAX_VALUE)	return 0;
		else return this.querytimeout;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		if (this.resset!= null)
				return resset;
		else
			return null;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCursorName(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	boolean EscapeProc = false;
	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException {
		EscapeProc = arg0;
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setMaxRows(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setPoolable(boolean arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		if(arg0==0) this.querytimeout = Integer.MAX_VALUE;
		else		this.querytimeout = arg0;
	}
	
}
