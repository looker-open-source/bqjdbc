/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.Assert;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Junit test tests functions in BQScrollableResultSet
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQScrollableResultSetFunctionTest extends CommonTestsForResultSets {

  private java.sql.Connection connection;
  private java.sql.ResultSet result;
  Logger logger = LoggerFactory.getLogger(BQScrollableResultSetFunctionTest.class);

  @Test
  public void ChainedCursorFunctionTest() {
    this.logger.info("ChainedFunctionTest");
    try {
      result.beforeFirst();
      Assert.assertTrue(result.next());
      Assert.assertEquals("you", result.getString(1));

    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    try {
      Assert.assertTrue(result.absolute(10));
      Assert.assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertFalse(result.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }

    try {
      Assert.assertTrue(result.first());
      Assert.assertEquals("you", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertTrue(result.isFirst());
      Assert.assertFalse(result.previous());
      result.afterLast();
      Assert.assertTrue(result.isAfterLast());
      Assert.assertTrue(result.absolute(-1));
      Assert.assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertTrue(result.relative(-5));
      Assert.assertEquals("without", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertFalse(result.relative(6));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertEquals("without", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
    this.logger.info("chainedfunctiontest end");
  }

  @Test
  public void databaseMetaDataGetTables() {
    // clouddb,ARTICLE_LOOKUP,starschema.net,[Ljava.lang.String;@9e8424
    ResultSet result = null;
    try {
      // Function call getColumns
      // catalog:null,
      // schemaPattern: starschema_net__clouddb,
      // tableNamePattern:OUTLET_LOOKUP, columnNamePattern: null
      // result = con.getMetaData().getTables("OUTLET_LOOKUP", null, "starschema_net__clouddb", null
      // );
      result =
          connection
              .getMetaData()
              .getColumns(null, "starschema_net__clouddb", "OUTLET_LOOKUP", null);
      // Function call getTables(catalog: ARTICLE_COLOR_LOOKUP, schemaPattern: null,
      // tableNamePattern: starschema_net__clouddb, types: TABLE , VIEW , SYSTEM TABLE , SYNONYM ,
      // ALIAS , )
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail();
    }
    try {
      Assert.assertTrue(result.first());
      while (!result.isAfterLast()) {
        String toprint = "";
        toprint += result.getString(1) + " , ";
        toprint += result.getString(2) + " , ";
        toprint += result.getString(3) + " , ";
        toprint += result.getString(4) + " , ";
        toprint += result.getString(5) + " , ";
        toprint += result.getString(6) + " , ";
        toprint += result.getString(7) + " , ";
        toprint += result.getString(8) + " , ";
        toprint += result.getString(9) + " , ";
        toprint += result.getString(10);
        System.err.println(toprint);
        result.next();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Compares two String[][]
   *
   * @param expected
   * @param reality
   * @return true if they are equal false if not
   */
  private boolean comparer(String[][] expected, String[][] reality) {
    for (int i = 0; i < expected.length; i++) {
      for (int j = 0; j < expected[i].length; j++) {
        if (expected[i][j].toString().equals(reality[i][j]) == false) {
          return false;
        }
      }
    }

    return true;
  }

  /** For testing isValid() , Close() , isClosed() */
  @Test
  public void isClosedValidtest() {
    try {
      Assert.assertEquals(true, connection.isValid(0));
    } catch (SQLException e) {
      Assert.fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      Assert.assertEquals(true, connection.isValid(10));
    } catch (SQLException e) {
      Assert.fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      connection.isValid(-10);
    } catch (SQLException e) {
      Assert.assertTrue(true);
      // e.printStackTrace();
    }

    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      Assert.assertTrue(connection.isClosed());
    } catch (SQLException e1) {
      e1.printStackTrace();
    }

    try {
      connection.isValid(0);
    } catch (SQLException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }

  private Connection connect(final String extraUrl) throws SQLException, IOException {
    return ConnectionFromResources.connect("installedaccount1.properties", extraUrl);
  }

  @Before
  public void setConnection() throws SQLException, IOException {
    connection = connect("&useLegacySql=true");
    QueryLoad();
  }

  @After
  public void closeConnection() throws SQLException {
    connection.close();
  }

  // Comprehensive Tests:

  public void QueryLoad() {
    final String sql =
        "SELECT TOP(word,10) AS word, COUNT(*) as count FROM publicdata:samples.shakespeare";
    final String description = "The top 10 word from shakespeare #TOP #COUNT";
    String[][] expectation =
        new String[][] {
          {"you", "yet", "would", "world", "without", "with", "will", "why", "whose", "whom"},
          {"42", "42", "42", "42", "42", "42", "42", "42", "42", "42"}
        };
    this.logger.info("Test number: 01");
    this.logger.info("Running query:" + sql);

    try {
      Statement stmt =
          connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(result);

    this.logger.debug(description);
    HelperFunctions.printer(expectation);

    try {
      Assert.assertTrue(
          "Comparing failed in the String[][] array",
          this.comparer(expectation, BQSupportMethods.GetQueryResult(result)));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail(e.toString());
    }
  }

  @Test
  public void ResultSetMetadata() {
    try {
      this.logger.debug(result.getMetaData().getSchemaName(1));
      this.logger.debug("{}", result.getMetaData().getScale(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
    }
    Assert.assertTrue(true);
  }

  @Test
  public void TestResultIndexOutofBound() {
    try {
      this.logger.debug("{}", result.getBoolean(99));
    } catch (SQLException e) {
      Assert.assertTrue(true);
      this.logger.error("SQLexception" + e.toString());
    }
  }

  @Test
  public void TestResultSetAbsolute() {
    try {
      Assert.assertTrue(result.absolute(1));
      Assert.assertEquals("you", result.getString(1));
      Assert.assertTrue(result.absolute(2));
      Assert.assertEquals("yet", result.getString(1));
      Assert.assertTrue(result.absolute(3));
      Assert.assertEquals("would", result.getString(1));
      Assert.assertTrue(result.absolute(4));
      Assert.assertEquals("world", result.getString(1));
      Assert.assertTrue(result.absolute(5));
      Assert.assertEquals("without", result.getString(1));
      Assert.assertTrue(result.absolute(6));
      Assert.assertEquals("with", result.getString(1));
      Assert.assertTrue(result.absolute(7));
      Assert.assertEquals("will", result.getString(1));
      Assert.assertTrue(result.absolute(8));
      Assert.assertEquals("why", result.getString(1));
      Assert.assertTrue(result.absolute(9));
      Assert.assertEquals("whose", result.getString(1));
      Assert.assertTrue(result.absolute(10));
      Assert.assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertFalse(result.absolute(0));
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }

    try {
      Assert.assertFalse(result.absolute(11));
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetAfterlast() {
    try {
      result.afterLast();
      Assert.assertTrue(result.previous());
      Assert.assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      result.afterLast();
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetBeforeFirst() {
    try {
      result.beforeFirst();
      Assert.assertTrue(result.next());
      Assert.assertEquals("you", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      result.beforeFirst();
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetFirst() {
    try {
      Assert.assertTrue(result.first());
      Assert.assertTrue(result.isFirst());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetBoolean() {
    try {
      Assert.assertTrue(result.absolute(1));
      Assert.assertEquals(Boolean.parseBoolean("42"), result.getBoolean(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetFloat() {
    try {
      Assert.assertTrue(result.absolute(1));
      Assert.assertEquals(new Float(42), result.getFloat(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetInteger() {
    try {
      Assert.assertTrue(result.absolute(1));
      Assert.assertEquals(42, result.getInt(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetRow() {

    try {
      Assert.assertTrue(result.absolute(1));
      Assert.assertEquals(1, result.getRow());
      Assert.assertTrue(result.absolute(10));
      Assert.assertEquals(10, result.getRow());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    try {
      result.beforeFirst();
      Assert.assertEquals(0, result.getRow());
      result.afterLast();
      Assert.assertEquals(0, result.getRow());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetString() {
    try {
      Assert.assertTrue(result.first());
      Assert.assertEquals("you", result.getString(1));
      Assert.assertTrue(result.last());
      Assert.assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetLast() {
    try {
      Assert.assertTrue(result.last());
      Assert.assertTrue(result.isLast());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetNext() {
    try {
      Assert.assertTrue(result.first());
      Assert.assertTrue(result.next());
      Assert.assertEquals("yet", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("would", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("world", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("without", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("with", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("will", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("why", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("whose", result.getString(1));
      Assert.assertTrue(result.next());
      Assert.assertEquals("whom", result.getString(1));
      Assert.assertFalse(result.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetPrevious() {
    try {
      Assert.assertTrue(result.last());
      Assert.assertTrue(result.previous());
      Assert.assertEquals("whose", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("why", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("will", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("with", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("without", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("world", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("would", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("yet", result.getString(1));
      Assert.assertTrue(result.previous());
      Assert.assertEquals("you", result.getString(1));
      Assert.assertFalse(result.previous());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    try {
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetRelative() {
    try {
      Assert.assertTrue(result.absolute(1));
      Assert.assertEquals("you", result.getString(1));
      Assert.assertTrue(result.relative(1));
      Assert.assertEquals("yet", result.getString(1));
      Assert.assertTrue(result.relative(2));
      Assert.assertEquals("world", result.getString(1));
      Assert.assertTrue(result.relative(5));
      Assert.assertEquals("whose", result.getString(1));
      Assert.assertTrue(result.relative(-5));
      Assert.assertEquals("world", result.getString(1));
      Assert.assertTrue(result.relative(-2));
      Assert.assertEquals("yet", result.getString(1));
      Assert.assertTrue(result.relative(-1));
      Assert.assertEquals("you", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    try {
      Assert.assertTrue(result.first());
      Assert.assertFalse(result.relative(-1));
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }

    try {
      Assert.assertTrue(result.last());
      Assert.assertFalse(result.relative(1));
      Assert.assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        Assert.assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetTotalBytesProcessedCacheHit() {
    Assert.assertTrue(result instanceof BQScrollableResultSet);
    BQScrollableResultSet results = (BQScrollableResultSet) result;
    final Boolean processedNoBytes = new Long(0L).equals(results.getTotalBytesProcessed());
    Assert.assertEquals(processedNoBytes, results.getCacheHit());
  }

  @Test
  public void testHandlesAllNullResponseFields() throws Exception {
    try {
      mockResponse("{}");
    } catch (BQSQLException e) {
      Assert.assertTrue(e.getMessage().contains("without a job reference"));
      return;
    }
    throw new AssertionError("Expected graceful failure due to lack of job reference");
  }

  @Test
  public void testHandlesSomeNullResponseFields() throws Exception {
    // Make sure we don't get any NPE's due to null values;
    mockResponse("{ \"jobComplete\": true, " + "\"totalRows\": \"0\", " + "\"rows\": [] }");
  }

  private void mockResponse(String jsonResponse) throws Exception {
    Properties properties =
        BQSupportFuncts.readFromPropFile(
            getClass().getResource("/installedaccount.properties").getFile());
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null);
    // Mock an empty response object.
    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(new MockLowLevelHttpResponse().setContent(jsonResponse))
            .build();
    BQConnection bq = new BQConnection(url + "&useLegacySql=false", properties, mockTransport);
    BQStatement stmt =
        new BQStatement(
            properties.getProperty("projectid"),
            bq,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
    String sqlStmt = "SELECT word from publicdata:samples.shakespeare LIMIT 100";

    BQScrollableResultSet results = ((BQScrollableResultSet) stmt.executeQuery(sqlStmt));

    results.getTotalBytesProcessed();
    results.getCacheHit();
    results.getJobId();
    results.getBiEngineMode();
    results.getBiEngineReasons();
  }

  @Test
  public void testStatelessQuery() throws SQLException, IOException {
    closeConnection();
    try (Connection statelessConnection =
        connect("&useLegacySql=true&jobcreationmode=JOB_CREATION_OPTIONAL")) {
      StatelessQuery.assumeStatelessQueriesEnabled(statelessConnection.getCatalog());
      try (Statement stmt =
          statelessConnection.createStatement(
              ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
        final ResultSet result = stmt.executeQuery(StatelessQuery.exampleQuery());
        final String[][] rows = BQSupportMethods.GetQueryResult(result);
        Assertions.assertThat(rows).isEqualTo(StatelessQuery.exampleValues());

        final BQScrollableResultSet bqResultSet = (BQScrollableResultSet) result;
        Assertions.assertThat(bqResultSet.getJobId()).isNull();
      }
    }
  }

  @Override
  protected Statement createStatementForCommonTests(Connection connection) throws SQLException {
    return connection.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  @Test
  @Ignore(
      "Contradiction between BQSCrollableResultSet and BQForwardOnlyResultSet "
          + "dates and times: b/317107706")
  public void testGetTimeByLabel() throws SQLException {}

  @Override
  @Test
  @Ignore(
      "Contradiction between BQSCrollableResultSet and BQForwardOnlyResultSet "
          + "dates and times: b/317107706")
  public void testGetDateByLabel() throws SQLException {}

  @Override
  @Test
  @Ignore(
      "Contradiction between BQSCrollableResultSet and BQForwardOnlyResultSet "
          + "dates and times: b/317107706")
  public void testGetTimestampByLabel() throws SQLException {}
}
