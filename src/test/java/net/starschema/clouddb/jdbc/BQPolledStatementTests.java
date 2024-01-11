package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BQPolledStatementTests {
  private BQConnection connection;

  @Before
  public void connect() throws SQLException, IOException {
    connection = ConnectionFromResources.connect("installedaccount1.properties", null);
  }

  @After
  public void disconnect() throws SQLException {
    connection.close();
  }

  @Test
  public void testGetAllLabels() {

  }
}
