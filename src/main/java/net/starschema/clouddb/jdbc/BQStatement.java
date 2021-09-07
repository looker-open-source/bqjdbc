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
 * This class implements the java.sql.Statement interface
 */

package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.model.BiEngineReason;
import com.google.api.services.bigquery.model.BiEngineStatistics;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.common.collect.ImmutableMap;
import com.google.api.services.bigquery.Bigquery;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements java.sql.Statement
 *
 * @author Horváth Attila
 * @author Balazs Gunics
 *
 */
public class BQStatement extends BQStatementRoot implements java.sql.Statement {

    private static final long MAX_LABELS = 64;
    public static final int MAX_IO_FAILURE_RETRIES = 3;
    private Job job;
    private AtomicReference<Thread> runningSyncThread = new AtomicReference<>();
    private AtomicReference<QueryResponse> syncResponseFromCurrentQuery = new AtomicReference<>();
    // Labels to be sent with the request
    // (in addition to the ones specified in the connection string).
    private ImmutableMap<String, String> statementLabels = ImmutableMap.of();

    /**
     * Enough time to give fast queries time to complete, but fast enough that if we want to cancel the query
     * (for which we have to wait at least this long), we don't have to wait too long. */
    public static final long SYNC_TIMEOUT_MILLIS = 5 * 1000;

    /**
     * Constructor for BQStatement object just initializes local variables
     *
     * @param projectId
     * @param bqConnection
     */
    public BQStatement(String projectId, BQConnection bqConnection) {
        logger.debug("Constructor of BQStatement is running projectId is: " + projectId);
        this.projectId = projectId;
        this.connection = bqConnection;
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * Constructor for BQStatement object just initializes local variables
     *
     * @param projectId
     * @param bqConnection
     * @param resultSetType
     * @param resultSetConcurrency
     * @throws BQSQLException
     */
    public BQStatement(String projectId, BQConnection bqConnection,
                       int resultSetType, int resultSetConcurrency) throws BQSQLException {
        logger.debug("Constructor of BQStatement is running projectId is: " + projectId +
                ",resultSetType is: " + resultSetType +
                ",resutSetConcurrency is: " + resultSetConcurrency);
        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
            throw new BQSQLException(
                    "The Resultset Concurrency can't be ResultSet.CONCUR_UPDATABLE");
        }

        this.projectId = projectId;
        this.connection = bqConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;

    }

    protected long getSyncTimeoutMillis() {
        return SYNC_TIMEOUT_MILLIS;
    }

    public void setLabels(Map<String, String> statementLabels) {
        this.statementLabels = ImmutableMap.copyOf(statementLabels);
    }

    @Override
    protected Map<String, String> getAllLabels() {
        return
            ImmutableMap.<String, String>builder()
            .putAll(
                    Stream.concat(
                        this.connection.getLabels()
                        .entrySet()
                        .stream()
                        .filter(entry -> !statementLabels.containsKey(entry.getKey())),
                        statementLabels
                        .entrySet()
                        .stream())
                    .limit(MAX_LABELS)
                    .collect(Collectors.toList()))
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet executeQuery(String querySql) throws SQLException {
        try {
            this.connection.addRunningStatement(this);
            return executeQueryHelper(querySql, false);
        }
        finally {
            this.job = null;
            this.syncResponseFromCurrentQuery.set(null);
            this.connection.removeRunningStatement(this);
        }
    }

    @Override
    public ResultSet executeQuery(String querySql, boolean unlimitedBillingBytes) throws SQLException {
        try {
            this.connection.addRunningStatement(this);
            return executeQueryHelper(querySql, unlimitedBillingBytes);
        }
        finally {
            this.job = null;
            this.connection.removeRunningStatement(this);
        }
    }

    private ResultSet executeQueryHelper(String querySql, boolean unlimitedBillingBytes) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }

        if (this.job != null) {
            throw new BQSQLException("Already running job");
        }

        this.starttime = System.currentTimeMillis();
        Job referencedJob;
        int retries = 0;
        boolean jobAlreadyCompleted = false;

        try {
            QueryResponse qr = runSyncQuery(querySql, unlimitedBillingBytes);
            boolean jobComplete = defaultValueIfNull(qr.getJobComplete(), false);
            boolean fetchedAll = jobComplete && qr.getTotalRows() != null &&
                    (qr.getTotalRows().equals(BigInteger.ZERO) ||
                            (qr.getRows() != null && qr.getTotalRows().equals(BigInteger.valueOf(qr.getRows().size()))));
            referencedJob = qr.getJobReference() == null ?
                            null :
                            this.connection.getBigquery()
                                    .jobs()
                                    .get(projectId, qr.getJobReference().getJobId())
                                    .setLocation(qr.getJobReference().getLocation())
                                    .execute();
            String biEngineMode;
            List<BiEngineReason> biEngineReasons;
            BiEngineStatistics biEngineStatistics = null;
            if (referencedJob != null){
                JobStatistics2 statistics2 = referencedJob.getStatistics().getQuery();
                if (statistics2 != null){
                    biEngineStatistics = statistics2.getBiEngineStatistics();
                }
            }
            
            if (biEngineStatistics != null){
                biEngineMode = biEngineStatistics.getBiEngineMode();
                biEngineReasons = biEngineStatistics.getBiEngineReasons();
            } else {
                biEngineMode = null;
                biEngineReasons = null;
            }

            if (jobComplete) {
                if (resultSetType != ResultSet.TYPE_SCROLL_INSENSITIVE) {
                    Boolean cacheHit = defaultValueIfNull(qr.getCacheHit(), false);
                    Long totalBytesProcessed = defaultValueIfNull(qr.getTotalBytesProcessed(), 0L);
                    List<TableRow> rows = defaultValueIfNull(qr.getRows(), new ArrayList<TableRow>());
                    TableSchema schema = defaultValueIfNull(qr.getSchema(), new TableSchema());
                    return new BQForwardOnlyResultSet(
                            this.connection.getBigquery(),
                            projectId,
                            referencedJob, this, rows, fetchedAll, schema, totalBytesProcessed, cacheHit, biEngineMode, biEngineReasons);
                } else if (fetchedAll) {
                    // We can only return scrollable result sets here if we have all the rows: otherwise we'll
                    // have to go get more below
                    Boolean cacheHit = defaultValueIfNull(qr.getCacheHit(), false);
                    Long totalBytesProcessed = defaultValueIfNull(qr.getTotalBytesProcessed(), 0L);
                    TableSchema schema = defaultValueIfNull(qr.getSchema(), new TableSchema());
                    return new BQScrollableResultSet(qr.getRows(), this, schema, totalBytesProcessed, cacheHit, biEngineMode, biEngineReasons, qr.getJobReference());
                }
                jobAlreadyCompleted = true;
            }
        } catch (IOException e) {
            // For the synchronous path, this is the place where the user will encounter errors in their SQL.
            throw new BQSQLException("Query execution failed: ", e);
        }

        try {
            do {
                if (this.connection.isClosed()) {
                    throw new BQSQLException("Connection is closed");
                }

                String status;
                if (jobAlreadyCompleted) {
                    status = "DONE";
                } else {
                    try {
                        status = BQSupportFuncts.getQueryState(referencedJob,
                                this.connection.getBigquery(),
                                projectId);
                    } catch (IOException e) {
                        if (retries++ < MAX_IO_FAILURE_RETRIES) {
                            continue;
                        } else {
                            throw new BQSQLException(
                                    "Something went wrong getting results for the job " + referencedJob.getId() + ", query: " + querySql,
                                    e);
                        }
                    }
                }

                if (status.equals("DONE")) {
                    if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE) {
                        return new BQScrollableResultSet(BQSupportFuncts.getQueryResults(
                                this.connection.getBigquery(),
                                projectId,
                                referencedJob), this);
                    } else {
                        logger.debug("HERE");
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
            throw new BQSQLException(
                    "Something went wrong getting results for the job " + referencedJob.getId() + ", query: " + querySql,
                    e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.cancel();
        throw new BQSQLException(
                "Query run took more than the specified timeout");
    }

    private static <T> T defaultValueIfNull(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    /**
     * Runs a query synchronously.
     *
     * Assumes will only be called once for a given statement. Runs the sync query in a thread and sets that thread
     * in [runningSyncThread], storing the result in [lastSyncResponse], so that cancel code can wait for a long
     * running job to time out on the sync response and then cancel it. */
    protected QueryResponse runSyncQuery(String querySql, boolean unlimitedBillingBytes)
            throws IOException, SQLException {
        final AtomicReference<Exception> diedWith = new AtomicReference<>();
        // Combine the connection's labels with `statementLabels`, the latter taking precedence.
        // However, if the total number of labels is greater than `MAX_LABELS`,
        // truncate the statement labels first, then the connection labels, if necessary.
        Runnable runSync = () -> {
            try {
                QueryResponse resp = BQSupportFuncts.runSyncQuery(
                        this.connection.getBigquery(),
                        projectId,
                        querySql,
                        connection.getDataSet(),
                        this.connection.getUseLegacySql(),
                        !unlimitedBillingBytes ? this.connection.getMaxBillingBytes() : null,
                        getSyncTimeoutMillis(), // we need this to respond fast enough to avoid any socket timeouts
                        (long) getMaxRows(),
                        this.getAllLabels(),
                        this.connection.getUseQueryCache());
                syncResponseFromCurrentQuery.set(resp);
                this.mostRecentJobReference.set(resp.getJobReference());
            } catch (Exception e) {
                diedWith.set(e);
            }
        };
        Thread syncThread = new Thread(runSync);
        runningSyncThread.set(syncThread);
        syncThread.start();
        try {
            syncThread.join();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        runningSyncThread.set(null);

        Exception e = diedWith.get();
        if (e != null) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else if (e instanceof SQLException) {
                throw (SQLException) e;
            } else {
                throw new RuntimeException(e);
            }
        }

        return syncResponseFromCurrentQuery.get();
    }

    public Job getJob() {
        return this.job;
    }

    @Override
    public void cancel() throws SQLException {
        Thread currentlyRunningSyncThread = runningSyncThread.get();
        QueryResponse maybeAlreadyComplete = syncResponseFromCurrentQuery.get();

        JobReference jobRefToCancel = null;
        // Check if we're in the synchronous path
        if (maybeAlreadyComplete != null && !maybeAlreadyComplete.getJobComplete()) {
            // The sync part of the query already completed (but the job is still running): we know which job we need to cancel
            jobRefToCancel = maybeAlreadyComplete.getJobReference();
        } else if (currentlyRunningSyncThread != null) {
            // The sync part of the query has not completed yet: wait for it so we can find the job to cancel
            try {
                currentlyRunningSyncThread.join(getSyncTimeoutMillis());
                QueryResponse resp = syncResponseFromCurrentQuery.get();
                if (resp != null && !resp.getJobComplete()) { // Don't bother cancel if the job is complete
                    jobRefToCancel = resp.getJobReference();
                }
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        if (jobRefToCancel == null && this.job != null) {
            // the async case
            jobRefToCancel = this.job.getJobReference();
        }

        if (jobRefToCancel == null) {
            this.logger.info("No currently running job to cancel.");
            return;
        }

        try {
            performQueryCancel(jobRefToCancel);
        } catch (IOException e) {
            throw new SQLException("Failed to kill query");
        }
    }

    /** Wrap [BQSupportFuncts.cancelQuery] purely for testability purposes. */
    protected void performQueryCancel(JobReference jobRefToCancel) throws IOException {
        BQSupportFuncts.cancelQuery(jobRefToCancel, this.connection.getBigquery(), projectId);
    }

    /** Use the BigQuery API to retrieve the labels sent along with the most recent query job.
     *
     * This is mainly useful for testing.
     * The returned label map may or may not be immutable.
     * Requires supplying an explicit connection.
     */
    public Map<String, String> getLabelsFromMostRecentQuery(BQConnection connection) throws SQLException {
        JobReference jobReference = this.mostRecentJobReference.get();
        if (jobReference != null) {
            try {
                return connection.getBigquery()
                    .jobs()
                    .get(projectId, jobReference.getJobId())
                    .setLocation(jobReference.getLocation())
                    .execute()
                    .getConfiguration()
                    .getLabels();
            } catch (IOException e) {
                throw new BQSQLException("Job query execution failed: ", e);
            }
        }
        return ImmutableMap.of();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new BQSQLException("Not implemented.");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new BQSQLException("Not implemented.");
    }

    @Override
    public void close() throws SQLException {
        if (!this.connection.isClosed()) {
            this.cancel();
        }
        super.close();
    }

}
