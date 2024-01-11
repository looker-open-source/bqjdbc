package net.starschema.clouddb.jdbc;

import java.sql.SQLException;

public class BQPollableStatementTest extends CommonTestsForStatements<BQPolledStatement> {

  @Override
  BQPolledStatement createStatement(BQConnection connection) {
    return new BQPolledStatement(connection.getProjectId(), connection, 250);
  }
}
