/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * This class implements the java.sql.Statement interface
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.api.services.bigquery.model.Job;

/**
 * This class implements java.sql.Statement
 * 
 * @author Horváth Attila
 * @author Balazs Gunics
 * 
 */
public class BQStatement extends BQStatementRoot implements java.sql.Statement {
    
    /**
     * Constructor for BQStatement object just initializes local variables
     * 
     * @param projectid
     * @param bqConnection
     */
    public BQStatement(String projectid, BQConnection bqConnection) {
        this.ProjectId = projectid;
        this.connection = bqConnection;
        this.resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }
    
    /**
     * Constructor for BQStatement object just initializes local variables
     * 
     * @param projectid2
     * @param bqConnection
     * @param resultSetType
     * @param resultSetConcurrency
     * @throws BQSQLException
     */
    public BQStatement(String projectid, BQConnection bqConnection,
            int resultSetType, int resultSetConcurrency) throws BQSQLException {
        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
            throw new BQSQLException(
                    "The Resultset Concurrency can't be ResultSet.CONCUR_UPDATABLE");
        }
        
        this.ProjectId = projectid;
        this.connection = bqConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        
    }
    
    /** {@inheritDoc} */
    
    @Override
    public ResultSet executeQuery(String querySql) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.starttime = System.currentTimeMillis();
        Job referencedJob;
        try {
            // Gets the Job reference of the completed job with give Query
            referencedJob = BQSupportFuncts.startQuery(
                    this.connection.getBigquery(), this.ProjectId, querySql);
            this.logger.info("Executing Query: " + querySql);
        }
        catch (IOException e) {
            throw new BQSQLException(e);
        }
        try {
            do {
                if (BQSupportFuncts.getQueryState(referencedJob,
                        this.connection.getBigquery(), this.ProjectId).equals(
                        "DONE")) {
                    return new BQResultSet(BQSupportFuncts.GetQueryResults(
                            this.connection.getBigquery(), this.ProjectId,
                            referencedJob), this);
                }
                // Pause execution for half second before polling job status
                // again, to
                // reduce unnecessary calls to the BigQUery API and lower
                // overall
                // application bandwidth.
                Thread.sleep(500);
                this.logger.debug("slept for 500" + "ms, querytimeout is: "
                        + this.querytimeout + "s");
            }
            while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
            // it runs for a minimum of 1 time
        }
        catch (IOException e) {
            throw new BQSQLException(e);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        // here we should kill/stop the running job, but bigquery doesn't
        // support that :(
        throw new BQSQLException(
                "Query run took more than the specified timeout");
    }
    
}
