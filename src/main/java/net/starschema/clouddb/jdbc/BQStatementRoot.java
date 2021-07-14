/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This class is the parent of BQStatement and BQPreparedStatement
 */

package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class partially implements java.sql.Statement, and
 * java.sql.PreparedStatement
 *
 * @author Horváth Attila
 * @author Balazs Gunics
 *
 */
public abstract class BQStatementRoot {

    /** Reference to store the ran Query run by Executequery or Execute */
    ResultSet resset = null;

    /** String containing the context of the Project */
    String projectId = null;

    Logger logger = LoggerFactory.getLogger(BQStatementRoot.class);

    /** Variable that stores the closed state of the statement */
    boolean closed = false;

    /** Reference for the Connection that created this Statement object */
    BQConnection connection;

    /** Variable that stores the set query timeout */
    int querytimeout = Integer.MAX_VALUE / 1000 - 1;
    /** Instance of log4j.Logger */
    /**
     * Variable stores the time an execute is made
     */
    long starttime = 0;
    /**
     * Variable that stores the max row number which can be stored in the
     * resultset
     */
    int resultMaxRowCount = Integer.MAX_VALUE - 1;

    /** Variable to Store EscapeProc state */
    boolean EscapeProc = false;

    /**
     * These Variables contain information about the type of resultset this
     * statement creates
     */
    int resultSetType;
    int resultSetConcurrency;

    /**
     * to be used with setMaxFieldSize
     */
    private int maxFieldSize = 0;

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void addBatch(String arg0) throws SQLException {
        throw new BQSQLException("Not implemented." + "addBatch(string)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public void cancel() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("cancel()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void clearBatch() throws SQLException {
        throw new BQSQLException("Not implemented." + "clearBatch()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void clearWarnings() throws SQLException {
        throw new BQSQLException("Not implemented." + "clearWarnings()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Only sets closed boolean to true
     * </p>
     */
    public void close() throws SQLException {
        this.closed = true;
        if (this.resset != null) {
            this.resset.close();
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Wrapper for execute; calls execute(arg0, false). Does NOT bypass
     * maxBillingBytes.
     * </p>
     */

    public boolean execute(String sql) throws SQLException {
        return this.execute(sql, false);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Executes the given SQL statement on BigQuery (note: it returns only 1
     * resultset). This function directly uses executeQuery function.
     * It also allows bypassing maxBillingBytes for PDTs.
     * </p>
     */

    public boolean execute(String sql, boolean unlimitedBillingBytes) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.resset = this.executeQuery(sql, unlimitedBillingBytes);
        this.logger.info("Executing Query: " + sql);
        if (this.resset != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("execute(String, int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("execute(string,int[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("execute(string,string[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public int[] executeBatch() throws SQLException {
        throw new BQSQLException("Not implemented." + "executeBatch()");
    }

    /**
     * Execute DML (DELETE, INSERT, UPDATE). If you want to perform a SELECT or other DDL
     * look at {@link #execute(String)}
     */
    private int executeDML(String sql) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (connection.getUseLegacySql()) {
            throw new BQSQLFeatureNotSupportedException("Legacy SQL does not support DML");
        }
        this.starttime = System.currentTimeMillis();

        final Job referencedJob;

        try {
            QueryResponse qr = BQSupportFuncts.runSyncQuery(
                    this.connection.getBigquery(),
                    projectId,
                    sql,
                    connection.getDataSet(),
                    this.connection.getUseLegacySql(),
                    this.connection.getMaxBillingBytes(),
                    (long) querytimeout * 1000,
                    (long) getMaxRows(),
                    this.getAllLabels(),
                    this.connection.getUseQueryCache()
            );

            if (defaultValueIfNull(qr.getJobComplete(), false)) {
                // I hope they don't insert more than 2^32-1 :)
                return Math.toIntExact(defaultValueIfNull(qr.getNumDmlAffectedRows(), 0L));
            }

            referencedJob = this.connection.getBigquery().jobs().get(projectId, qr.getJobReference().getJobId()).execute();
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + sql, e);
        }

        try {
            do {
                if (BQSupportFuncts.getQueryState(referencedJob,
                        this.connection.getBigquery(), projectId).equals(
                        "DONE")) {
                    return Math.toIntExact(
                                BQSupportFuncts.getQueryResults(
                                        this.connection.getBigquery(), projectId,
                                        referencedJob).getNumDmlAffectedRows()
                            );
                }
                Thread.sleep(500);
                this.logger.debug("slept for 500" + "ms, querytimeout is: "
                        + this.querytimeout + "s");
            } while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
        } catch (IOException | InterruptedException e) {
            throw new BQSQLException("Something went wrong with the query: " + sql, e);
        }
        // here we should kill/stop the running job, but bigquery doesn't
        // support that :(
        throw new BQSQLException(
                "Query run took more than the specified timeout");
    }

    public ResultSet executeQuery(String querySql, boolean unlimitedBillingBytes) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.starttime = System.currentTimeMillis();
        Job referencedJob;

        Long billingBytes = !unlimitedBillingBytes ? this.connection.getMaxBillingBytes() : null;

        boolean jobAlreadyCompleted = false;

        try {
            QueryResponse qr = BQSupportFuncts.runSyncQuery(
                    this.connection.getBigquery(),
                    projectId,
                    querySql,
                    connection.getDataSet(),
                    this.connection.getUseLegacySql(),
                    billingBytes,
                    (long) querytimeout * 1000,
                    (long) getMaxRows(),
                    this.getAllLabels(),
                    this.connection.getUseQueryCache()
            );
            if (defaultValueIfNull(qr.getJobComplete(), false)) {
                List<TableRow> rows = defaultValueIfNull(qr.getRows(), new ArrayList<TableRow>());
                if (BigInteger.valueOf(rows.size()).equals(qr.getTotalRows())) {
                    Boolean cacheHit = defaultValueIfNull(qr.getCacheHit(), false);
                    Long totalBytesProcessed = defaultValueIfNull(qr.getTotalBytesProcessed(), 0L);
                    TableSchema schema = defaultValueIfNull(qr.getSchema(), new TableSchema());
                    return new BQScrollableResultSet(rows, this, schema, totalBytesProcessed, cacheHit);
                }
                jobAlreadyCompleted = true;
            }
            referencedJob = this.connection.getBigquery().jobs().get(projectId, qr.getJobReference().getJobId()).execute();

            this.logger.info("Executing Query: " + querySql);
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + querySql, e);
        }
        try {
            do {
                if (jobAlreadyCompleted || BQSupportFuncts.getQueryState(referencedJob,
                        this.connection.getBigquery(), projectId).equals(
                        "DONE")) {
                    if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE) {
                        return new BQScrollableResultSet(BQSupportFuncts.getQueryResults(
                                this.connection.getBigquery(), projectId,
                                referencedJob), this);
                    } else {
                        return new BQForwardOnlyResultSet(
                                this.connection.getBigquery(),
                                projectId,
                                referencedJob, this);
                    }
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
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + querySql, e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // here we should kill/stop the running job, but bigquery doesn't
        // support that :(
        throw new BQSQLException(
                "Query run took more than the specified timeout");
    }

    protected Map<String, String> getAllLabels() {
        return this.connection.getLabels();
    }

    private static <T> T defaultValueIfNull(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * Only supported for standard SQL (non-legacy)
     *
     * @throws BQSQLException
     */

    public int executeUpdate(String sql) throws SQLException {
        return executeDML(sql);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("executeUpdate(String,int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new BQSQLFeatureNotSupportedException(
                "executeUpdate(string,int[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new BQSQLFeatureNotSupportedException(
                "execute(update(string,string[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the connection that made the object.
     * </p>
     *
     * @returns connection
     */
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Fetch direction is unknown.
     * </p>
     *
     * @return FETCH_UNKNOWN
     */

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public int getFetchSize() throws SQLException {
        throw new BQSQLException("Not implemented." + "getFetchSize()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getGeneratedKeys()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * We store it in an array which indexed through, int
     * </p>
     * We could return Integer.MAX_VALUE too, but i don't think we could get
     * that much row.
     *
     * @return 0 -
     */

    public int getMaxRows() throws SQLException {
        return this.resultMaxRowCount;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet, since Bigquery Does not support precompiled sql
     * </p>
     *
     * @throws BQSQLException
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException(
                "getMetaData()"));
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Multiple result sets are not supported currently.
     * </p>
     *
     * @return false;
     */

    public boolean getMoreResults() throws SQLException {
        return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Multiple result sets are not supported currently. we check that the
     * result set is open, the parameter is acceptable, and close our current
     * resultset or throw a FeatureNotSupportedException
     * </p>
     *
     * @param current
     *            - one of the following Statement constants indicating what
     *            should happen to current ResultSet objects obtained using the
     *            method getResultSet: Statement.CLOSE_CURRENT_RESULT,
     *            Statement.KEEP_CURRENT_RESULT, or Statement.CLOSE_ALL_RESULTS
     * @throws BQSQLException
     */

    public boolean getMoreResults(int current) throws SQLException {
        if (this.closed) {
            throw new BQSQLException("Statement is closed.");
        }
        if (current == Statement.CLOSE_CURRENT_RESULT
                || current == Statement.KEEP_CURRENT_RESULT
                || current == Statement.CLOSE_ALL_RESULTS) {

            if (BQDatabaseMetadata.multipleOpenResultsSupported
                    && (current == Statement.KEEP_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS)) {
                throw new BQSQLFeatureNotSupportedException();
            }
            // Statement.CLOSE_CURRENT_RESULT
            this.close();
            return false;
        } else {
            throw new BQSQLException("Wrong parameter.");
        }
    }

    public int getQueryTimeout() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (this.starttime == 0) {
            return 0;
        }
        if (this.querytimeout == Integer.MAX_VALUE) {
            return 0;
        } else {
            if (System.currentTimeMillis() - this.starttime > this.querytimeout) {
                throw new BQSQLException("Time is over");
            }
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
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (this.resset != null) {
            return this.resset;
        } else {
            return null;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * The driver is read only currently.
     * </p>
     *
     * @return CONCUR_READ_ONLY
     */

    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Read only mode, no commit.
     * </p>
     *
     * @return CLOSE_CURSORS_AT_COMMIT
     */

    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        // TODO
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Updates and deletes not supported.
     * </p>
     *
     * @return TYPE_SCROLL_INSENSITIVE
     */

    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Result will be a ResultSet object.
     * </p>
     *
     * @return -1
     */

    public int getUpdateCount() throws SQLException {
        return -1;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns null
     * </p>
     *
     * @return null
     */

    public SQLWarning getWarnings() throws SQLException {
        return null;
        // TODO Implement Warning Handling
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value of boolean closed
     * </p>
     */
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public boolean isPoolable() throws SQLException {
        throw new BQSQLException("Not implemented." + "isPoolable()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @return false
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * FeatureNotSupportedExceptionjpg
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public void setCursorName(String arg0) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("setCursorName(string)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Now Only setf this.EscapeProc to arg0
     * </p>
     */

    public void setEscapeProcessing(boolean arg0) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.EscapeProc = arg0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void setFetchDirection(int arg0) throws SQLException {
        throw new BQSQLException("Not implemented." + "setFetchDirection(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Does nothing
     * </p>
     *
     * @throws BQSQLException
     */
    public void setFetchSize(int arg0) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("Statement closed");
        }
    }

    /**
     * <p>
     * Sets the limit for the maximum number of bytes in a ResultSet column storing
     * character or binary values to the given number of bytes. This limit applies only
     * to BINARY, VARBINARY, LONGVARBINARY, CHAR, VARCHAR, and LONGVARCHAR fields.
     * If the limit is exceeded, the excess data is silently discarded. For maximum
     * portability, use values greater than 256.
     * </p>
     *
     * @throws BQSQLException
     */
    public void setMaxFieldSize(int arg0) throws SQLException {
        this.maxFieldSize = arg0;
    }

    /**
     * NOTE: can pass 0 or negative to set to unlimited
     */
    public void setMaxRows(int newMax) {
        this.resultMaxRowCount = newMax <= 0 ? Integer.MAX_VALUE - 1 : newMax;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */
    public void setPoolable(boolean arg0) throws SQLException {
        throw new BQSQLException("Not implemented." + "setPoolable(bool)");
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (seconds == 0) {
            this.querytimeout = Integer.MAX_VALUE;
        } else {
            this.querytimeout = seconds;
        }
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
        throw new BQSQLException("not found");
    }
}
