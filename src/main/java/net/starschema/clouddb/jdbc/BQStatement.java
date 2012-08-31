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

/**
 * This class implements java.sql.Statement
 * 
 * @author Horváth Attila
 * 
 */
public class BQStatement implements java.sql.Statement {

    /** String containing the context of the Project */
    private String ProjectId = null;
    /** Reference for the Connection that created this Statement object */
    private BQConnection connection;
    /** Variable that stores the set query timeout */
    private int querytimeout = Integer.MAX_VALUE;
    /** Instance of log4j.Logger */
    Logger logger = Logger.getLogger(BQStatement.class);

    /**
     * Variable stores the time an execute is made at first it stores when the
     * object is made
     */
    private long starttime = System.currentTimeMillis();

    /** Variable that stores the closed state of the statement */
    private boolean closed = false;

    /** Reference to store the ran Query run by Executequery or Execute */
    ResultSet resset = null;

    /** Variable to Store EscapeProc state */
    boolean EscapeProc = false;

    /**
     * Constructor for BQStatement object just initializes local variables
     * 
     * @param projectid
     * @param bqConnection
     */
    public BQStatement(String projectid, BQConnection bqConnection) {
	this.ProjectId = projectid;
	this.connection = bqConnection;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void addBatch(String arg0) throws SQLException {
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
    public void cancel() throws SQLException {
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
    public void clearBatch() throws SQLException {
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
    public void clearWarnings() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Only sets closed boolean to false
     * </p>
     */
    public void close() throws SQLException {
	this.closed = false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Executes the given SQL statement on BigQuery (note: it returns only 1
     * resultset). This function directly uses executeQuery function
     * </p>
     */
    public boolean execute(String arg0) throws SQLException {
	if (this.isClosed())
	    throw new SQLException("This Statement is Closed");
	this.resset = this.executeQuery(arg0);

	if (this.resset != null)
	    return true;
	else
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
    public boolean execute(String arg0, int arg1) throws SQLException {
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
    public boolean execute(String arg0, int[] arg1) throws SQLException {
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
    public boolean execute(String arg0, String[] arg1) throws SQLException {
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
    public int[] executeBatch() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public ResultSet executeQuery(String querySql) throws SQLException {
	if (this.isClosed())
	    throw new SQLException("This Statement is Closed");
	starttime = System.currentTimeMillis();
	Job referencedJob;
	try {
	    // Gets the Job reference of the completed job with give Query
	    referencedJob = BigQueryApi.startQuery(
		    this.connection.getBigquery(), this.ProjectId, querySql);
	} catch (IOException e) {
	    throw new SQLException(e);
	}
	try {
	    do {
		if (BigQueryApi.getQueryState(referencedJob,
			this.connection.getBigquery(), this.ProjectId).equals(
			"DONE"))
		    return new BQResultSet(BigQueryApi.GetQueryResults(
			    this.connection.getBigquery(), this.ProjectId,
			    referencedJob), this);
		// Pause execution for half second before polling job status
		// again, to
		// reduce unnecessary calls to the BigQUery API and lower
		// overall
		// application bandwidth.
		Thread.sleep(500);
		this.logger.debug("slept for 500" + "ms, querytimeout is: "
			+ this.querytimeout + "s");
	    } while (System.currentTimeMillis() - starttime <= (long) this.querytimeout * 1000);
	    // it runs for a minimum of 1 time
	} catch (IOException e) {
	    throw new SQLException(e);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
	// here we should kill/stop the running job, but bigquery doesn't
	// support that :(
	throw new SQLException("Query run took more than the specified timeout");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public int executeUpdate(String arg0) throws SQLException {
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
    public int executeUpdate(String arg0, int arg1) throws SQLException {
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
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
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
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
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
    public Connection getConnection() throws SQLException {
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
    public int getFetchDirection() throws SQLException {
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
    public int getFetchSize() throws SQLException {
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
    public ResultSet getGeneratedKeys() throws SQLException {
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
    public int getMaxFieldSize() throws SQLException {
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
    public int getMaxRows() throws SQLException {
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
    public boolean getMoreResults() throws SQLException {
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
    public boolean getMoreResults(int arg0) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public int getQueryTimeout() throws SQLException {
	if (this.isClosed())
	    throw new SQLException("This Statement is Closed");
	if (this.querytimeout == Integer.MAX_VALUE)
	    return 0;
	else {
	    if (System.currentTimeMillis() - starttime > querytimeout)
		throw new SQLException("Time is over");
	    return this.querytimeout;
	}
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Gives back reultset stored in resset
     * </p>
     */
    public ResultSet getResultSet() throws SQLException {
	if (this.isClosed())
	    throw new SQLException("This Statement is Closed");
	if (this.resset != null)
	    return this.resset;
	else
	    return null;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public int getResultSetConcurrency() throws SQLException {
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
    public int getResultSetHoldability() throws SQLException {
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
    public int getResultSetType() throws SQLException {
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
    public int getUpdateCount() throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns null
     * </p>
     * @return null
     */
    public SQLWarning getWarnings() throws SQLException {
	return null;
	//TODO Implement Warning Handling
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value of boolean closed
     * </p>
     */
    public boolean isClosed() throws SQLException {
	return closed;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean isPoolable() throws SQLException {
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
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
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
    public void setCursorName(String arg0) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Now Only setf this.EscapeProc to arg0
     * </p>
     */
    public void setEscapeProcessing(boolean arg0) throws SQLException {
	if (this.isClosed())
	    throw new SQLException("This Statement is Closed");
	this.EscapeProc = arg0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public void setFetchDirection(int arg0) throws SQLException {
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
    public void setFetchSize(int arg0) throws SQLException {
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
    public void setMaxFieldSize(int arg0) throws SQLException {
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
    public void setMaxRows(int arg0) throws SQLException {
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
    public void setPoolable(boolean arg0) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public void setQueryTimeout(int arg0) throws SQLException {
	if (this.isClosed())
	    throw new SQLException("This Statement is Closed");
	if (arg0 == 0)
	    this.querytimeout = Integer.MAX_VALUE;
	else
	    this.querytimeout = arg0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always throws SQLException
     * </p>
     * 
     * @throws SQLException
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
	throw new SQLException("not found");
    }

}
