package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.model.DmlStatistics;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BQPolledStatement extends BQStatementRoot
    implements java.sql.Statement, LabelledStatement {

  private static final long MAX_LABELS = 64;

  static final int MAX_IO_FAILURES_RETRIES = 3;

  private final int pollInterval;

  private final Supplier<Long> nanoNow;

  private ActivePollableQuery activeQuery;

  private final AtomicBoolean running = new AtomicBoolean(true);

  private ImmutableMap<String, String> statementLabels = ImmutableMap.of();

  // statement labels

  public BQPolledStatement(
      final String projectId, final BQConnection connection, final int pollInterval) {
    this(
        projectId,
        connection,
        pollInterval,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
  }

  public BQPolledStatement(
      final String projectId,
      final BQConnection connection,
      final int pollInterval,
      final int resultSetType,
      final int resultSetConcurrency) {
    this(
        projectId, connection, pollInterval, resultSetType, resultSetConcurrency, System::nanoTime);
  }

  BQPolledStatement(
      final String projectId,
      final BQConnection connection,
      final int pollInterval,
      final int resultSetType,
      final int resultSetConcurrency,
      final Supplier<Long> nanoNow) {
    this.projectId = projectId;
    this.connection = connection;
    this.pollInterval = pollInterval;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.nanoNow = nanoNow;
  }

  /**
   * Set BigQuery labels for statement
   *
   * @param statementLabels the labels
   */
  public void setLabels(Map<String, String> statementLabels) {
    this.statementLabels = ImmutableMap.copyOf(statementLabels);
  }

  @Override
  public Map<String, String> getAllLabels() {
    return ImmutableMap.<String, String>builder()
        .putAll(
            Stream.concat(
                    this.connection.getLabels().entrySet().stream()
                        .filter(entry -> !statementLabels.containsKey(entry.getKey())),
                    statementLabels.entrySet().stream())
                .limit(MAX_LABELS)
                .collect(Collectors.toList()))
        .build();
  }

  public Map<String, String> getLabelsFromMostRecentQuery(BQConnection connection)
      throws BQSQLException {
    final JobReference jobReference = this.mostRecentJobReference.get();
    if (jobReference != null) {
      try {
        return connection
            .getBigquery()
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
  public int getQueryTimeout() {
    final Integer timeout = connection.getTimeoutMs();
    if (timeout != null) {
      return timeout;
    }
    return querytimeout * 1000;
  }

  private void pollForJobDone() throws SQLException {
    int retries = 0;
    final Long deadline = nanoNow.get() + (long) querytimeout * 1_000_000;
    do {
      synchronized (this) {
        final String queryJobState;
        try {
          queryJobState = activeQuery.getQueryJobState(connection);
        } catch (SQLException e) {
          if (retries++ < MAX_IO_FAILURES_RETRIES) {
            continue;
          }
          throw e;
        }

        if (queryJobState.equals("DONE")) {
          return;
        }
        try {
          wait(pollInterval);
        } catch (InterruptedException e) {
          throw activeQuery.jobExecutionException(e);
        }
      }
    } while (running.get() && nanoNow.get() < deadline);

    throw new BQSQLException("Query run took more than the specified timeout");
  }

  private ResultSet pollForResultSet() throws SQLException {
    int retries = 0;
    final Long deadline = nanoNow.get() + (long) querytimeout * 1_000_000;
    do {
      synchronized (this) {
        final String queryJobState;
        try {
          queryJobState = activeQuery.getQueryJobState(connection);
        } catch (SQLException e) {
          if (retries++ < MAX_IO_FAILURES_RETRIES) {
            continue;
          }
          throw e;
        }

        if (queryJobState.equals("DONE")) {
          if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE) {
            return new BQScrollableResultSet(activeQuery.getQueryResultsResponse(connection), this);
          }
          return new BQForwardOnlyResultSet(
              connection.getBigquery(),
              connection.getProjectId(),
              activeQuery.getJob(),
              null,
              this);
        }
        try {
          wait(pollInterval);
        } catch (InterruptedException e) {
          throw activeQuery.jobExecutionException(e);
        }
      }
    } while (running.get() && nanoNow.get() < deadline);

    throw new BQSQLException("Query run took more than the specified timeout");
  }

  /**
   * @param querySql an SQL Data Manipulation Language (DML) statement, such as <code>INSERT</code>,
   *     <code>UPDATE</code> or <code>DELETE</code>; or an SQL statement that returns nothing, such
   *     as a DDL statement.
   * @return Updated row count for Insert/Update/Delete and 0 otherwise
   */
  @Override
  public int executeUpdate(String querySql) throws SQLException {
    if (isClosed()) {
      throw new BQSQLException("This Statement is Closed");
    }

    connection.addRunningStatement(this);

    synchronized (this) {
      if (activeQuery != null) {
        activeQuery.throwAlreadyRunningException();
      }
      activeQuery = ActivePollableQuery.startQueryJob(connection, querySql);
      mostRecentJobReference.set(activeQuery.getJob().getJobReference());
    }
    Job queryJob = activeQuery.getJob();
    DmlStatistics stats = null;
    pollForJobDone();
    try {
      // Refresh Job Status from remote API
      queryJob =
          BQSupportFuncts.getPollJob(
              queryJob.getJobReference(), connection.getBigquery(), connection.getProjectId());
    } catch (Exception e) {
      throw new BQSQLException(
          "Something went wrong when getting Job info: " + e.getClass() + " " + e.getMessage());
    }
    if (queryJob == null) {
      throw new BQSQLException("Lost track of the query job.");
    }
    if (queryJob.getStatus() == null) {
      throw new BQSQLException("Lost track of the query job status.");
    }

    if (queryJob.getStatus().getErrors() != null && queryJob.getStatus().getErrors().size() > 0) {
      throw new BQSQLException("Query failed: " + queryJob.getStatus().getErrors());
    }
    if (queryJob.getStatistics() == null
        || queryJob.getStatistics().getQuery() == null
        || queryJob.getStatistics().getQuery().getDmlStats() == null) {
      return 0;
    }
    stats = queryJob.getStatistics().getQuery().getDmlStats();
    // Default return 0, if statement is Delete/Update/Insert, return the changed row count
    Long ret = 0L;
    ret = defaultValueIfNull(stats.getDeletedRowCount(), ret);
    ret = defaultValueIfNull(stats.getUpdatedRowCount(), ret);
    ret = defaultValueIfNull(stats.getInsertedRowCount(), ret);

    // This is what we do in the BQStatementRoot base class.
    return Math.toIntExact(ret);
  }

  @Override
  public ResultSet executeQuery(String querySql) throws SQLException {
    return executeQuery(querySql, false);
  }

  @Override
  public ResultSet executeQuery(final String querySql, boolean unlimitedBillingBytes)
      throws SQLException {
    if (isClosed()) {
      throw new BQSQLException("This Statement is Closed");
    }

    connection.addRunningStatement(this);

    synchronized (this) {
      if (activeQuery != null) {
        activeQuery.throwAlreadyRunningException();
      }
      activeQuery = ActivePollableQuery.startQueryJob(connection, querySql, unlimitedBillingBytes);
      mostRecentJobReference.set(activeQuery.getJob().getJobReference());
    }

    try {
      return pollForResultSet();
    } catch (Throwable t) {
      cancel();
      throw t;
    } finally {
      synchronized (this) {
        activeQuery = null;
      }

      connection.removeRunningStatement(this);
    }
  }

  public synchronized void cancel() throws SQLException {
    if (activeQuery == null) {
      return;
    }
    try {
      BQSupportFuncts.cancelQuery(
          activeQuery.getJob().getJobReference(), connection.getBigquery(), projectId);
    } catch (IOException e) {
      throw new SQLException("Failed to kill query", e);
    }

    synchronized (this) {
      notifyAll();
    }
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
    if (!connection.isClosed()) {
      cancel();
    }
    super.close();
  }

  private static <T> T defaultValueIfNull(T value, T defaultValue) {
    return value == null ? defaultValue : value;
  }
}
