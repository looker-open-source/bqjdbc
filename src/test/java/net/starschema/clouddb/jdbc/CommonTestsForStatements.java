package net.starschema.clouddb.jdbc;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class CommonTestsForStatements<T extends Statement & LabelledStatement> {
  protected static final String SMALL_SELECT = "SELECT 1";
  protected static final String BIG_SELECT =
      "SELECT * FROM "
          + "`bigquery-public-data.pypi.file_downloads`"
          + " WHERE datetime >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)";

  private BQConnection connection;
  private T statement;

  abstract T createStatement(final BQConnection connection);

  protected BQConnection connect(final String extraUrl) throws SQLException, IOException {
    return ConnectionFromResources.connect("installedaccount1.properties", extraUrl);
  }

  @Before
  public void setup() throws SQLException, IOException {
    connection = connect("&useLegacySql=false");
    statement = createStatement(connection);
  }

  @After
  public void teardown() throws SQLException {
    statement.close();
    connection.close();
  }

  @Test
  public void testGetAllLabels() throws SQLException, IOException {
    final String params =
        new QueryStringBuilder()
            .put("useLegacySql", "false")
            .put("labels", "label1=value,label2=other-value")
            .build();

    try (BQConnection labeledConnection = connect("&" + params);
        T statement = createStatement(labeledConnection)) {
      statement.setLabels(ImmutableMap.of("label2", "overridden-value", "label3", "statement"));
      final Map<String, String> allLabels = statement.getAllLabels();
      Assertions.assertThat(allLabels)
          .isEqualTo(
              ImmutableMap.of(
                  "label1", "value",
                  "label2", "overridden-value",
                  "label3", "statement"));
    }
  }

  @Test
  public void testSmallSelect() throws SQLException {
    final ResultSet results = statement.executeQuery(SMALL_SELECT);
    final String[][] rows = BQSupportMethods.GetQueryResult(results);
    Assertions.assertThat(rows).hasSize(1);
  }
}
