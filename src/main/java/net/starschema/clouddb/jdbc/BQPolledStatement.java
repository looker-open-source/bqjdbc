package net.starschema.clouddb.jdbc;

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

public class BQPolledStatement
    extends BQStatementRoot
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
      final String projectId,
      final BQConnection connection,
      final int pollInterval
  ) {
    this(projectId,
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
      final int resultSetConcurrency
  )  {
    this(projectId,
        connection,
        pollInterval,
        resultSetType,
        resultSetConcurrency,
        System::nanoTime);
  }

  BQPolledStatement(
      final String projectId,
      final BQConnection connection,
      final int pollInterval,
      final int resultSetType,
      final int resultSetConcurrency,
      final Supplier<Long> nanoNow
  )
  {
    this.projectId = projectId;
    this.connection = connection;
    this.pollInterval = pollInterval;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.nanoNow = nanoNow;
  }

  /**
   * Set BigQuery labels for statement
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
              this
          );
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

  @Override
  public ResultSet executeQuery(final String querySql) throws SQLException {
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
          activeQuery.getJob().getJobReference(),
          connection.getBigquery(),
          projectId);
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
}
