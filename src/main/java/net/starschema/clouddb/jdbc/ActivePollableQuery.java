package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import java.io.IOException;
import java.sql.SQLException;

class ActivePollableQuery {
  private final String sql;
  private final Job job;

  static ActivePollableQuery startQueryJob(
      final BQConnection connection,
      final String sql
  ) throws SQLException {
    try {
      final Job job = BQSupportFuncts.startQuery(
          connection.getBigquery(),
          connection.getProjectId(),
          sql,
          connection.getDataSet(),
          connection.getUseLegacySql(),
          connection.getMaxBillingBytes(),
          connection.getKmsKeyName()
      );
      return new ActivePollableQuery(sql, job);
    } catch (IOException e) {
      throw new BQSQLException("Something went wrong with the query: " + sql, e);
    }
  }
  private ActivePollableQuery(final String sql, final Job job) {
    this.sql = sql;
    this.job = job;
  }

  void throwAlreadyRunningException() throws SQLException {
    throw new BQSQLException(
        String.format("statement already running query: %s (job ID %s)",
            sql,
            job.getId()));
  }

  BQSQLException jobExecutionException(final Throwable cause) {
    return new BQSQLException(
        "Something went wrong getting results for the job "
            + job.getId()
            + ", query: "
            + sql,
        cause);
  }

  String getQueryJobState(final BQConnection connection) throws SQLException {
    try {
      return BQSupportFuncts.getQueryState(
          job,
          connection.getBigquery(),
          connection.getProjectId());
    } catch (IOException e) {
      throw jobExecutionException(e);
    }
  }

  GetQueryResultsResponse getQueryResultsResponse(final BQConnection connection)
      throws BQSQLException {
    try {
      return BQSupportFuncts.getQueryResults(
          connection.getBigquery(),
          connection.getProjectId(),
          job
      );
    } catch (IOException e) {
      throw jobExecutionException(e);
    }
  }

  Job getJob() {
    return job;
  }
}
