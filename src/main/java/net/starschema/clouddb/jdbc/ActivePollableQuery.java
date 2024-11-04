package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import java.io.IOException;
import java.sql.SQLException;

class ActivePollableQuery {

  private final String sql;
  private final Job job;

  static ActivePollableQuery startQueryJob(final BQConnection connection, final String sql)
      throws SQLException {
    return startQueryJob(connection, sql, false, false);
  }

  static ActivePollableQuery startQueryJob(
      final BQConnection connection, final String sql, final boolean unlimitedBillingBytes)
      throws SQLException {
    return startQueryJob(connection, sql, unlimitedBillingBytes, false);
  }

  static ActivePollableQuery startQueryJob(
      final BQConnection connection,
      final String sql,
      final boolean unlimitedBillingBytes,
      final boolean omitKmsKey)
      throws SQLException {
    Long maxBillingBytes = unlimitedBillingBytes ? null : connection.getMaxBillingBytes();
    String kmsKeyName = omitKmsKey ? null : connection.getKmsKeyName();
    try {
      final Job job =
          BQSupportFuncts.startQuery(
              connection.getBigquery(),
              connection.getProjectId(),
              sql,
              connection.getDataSet(),
              connection.getUseLegacySql(),
              maxBillingBytes,
              kmsKeyName);
      if (!omitKmsKey && ddlCmekErrorJob(job)) {
        return startQueryJob(connection, sql, unlimitedBillingBytes, true);
      }
      return new ActivePollableQuery(sql, job);
    } catch (IOException e) {
      throw new BQSQLException("Something went wrong with the query: " + sql, e);
    }
  }

  private static boolean ddlCmekErrorJob(Job job) {
    final String ddlErrorMessage = "Cannot set kms key name in jobs with DDL";
    if (job.getStatus().getErrors() == null || job.getStatus().getErrors().size() == 0) {
      return false;
    }
    for (ErrorProto errorProto : job.getStatus().getErrors()) {
      if (errorProto.getMessage().contains(ddlErrorMessage)) {
        return true;
      }
    }
    return false;
  }

  private ActivePollableQuery(final String sql, final Job job) {
    this.sql = sql;
    this.job = job;
  }

  void throwAlreadyRunningException() throws SQLException {
    throw new BQSQLException(
        String.format("statement already running query: %s (job ID %s)", sql, job.getId()));
  }

  BQSQLException jobExecutionException(final Throwable cause) {
    return new BQSQLException(
        "Something went wrong getting results for the job " + job.getId() + ", query: " + sql,
        cause);
  }

  String getQueryJobState(final BQConnection connection) throws SQLException {
    try {
      return BQSupportFuncts.getQueryState(
          job, connection.getBigquery(), connection.getProjectId());
    } catch (IOException e) {
      throw jobExecutionException(e);
    }
  }

  GetQueryResultsResponse getQueryResultsResponse(final BQConnection connection)
      throws BQSQLException {
    try {
      return BQSupportFuncts.getQueryResults(
          connection.getBigquery(), connection.getProjectId(), job);
    } catch (IOException e) {
      throw jobExecutionException(e);
    }
  }

  Job getJob() {
    return job;
  }
}
