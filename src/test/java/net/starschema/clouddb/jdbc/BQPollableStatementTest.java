package net.starschema.clouddb.jdbc;

public class BQPollableStatementTest extends CommonTestsForStatements<BQPolledStatement> {

  @Override
  BQPolledStatement createStatement(BQConnection connection) {
    return new BQPolledStatement(connection.getProjectId(), connection, 250);
  }
}
