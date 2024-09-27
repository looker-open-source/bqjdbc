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

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Junit test tests functions in BQForwardOnlyResultSet
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQForwardOnlyResultSetFunctionTest extends CommonTestsForResultSets {

  private java.sql.Connection connection;
  private java.sql.Connection standardSqlConnection;
  private java.sql.ResultSet resultForTest = null;

  Logger logger = LoggerFactory.getLogger(BQForwardOnlyResultSetFunctionTest.class);
  private Integer maxRows = null;

  private Connection connect(final String extraUrl) throws SQLException, IOException {
    return ConnectionFromResources.connect("installedaccount1.properties", extraUrl);
  }

  @BeforeEach
  public void setConnection() throws SQLException, IOException {
    connection = connect("&useLegacySql=true");
  }

  @BeforeEach
  public void setStandardSqlConnection() throws SQLException, IOException {
    standardSqlConnection = connect("&useLegacySql=false");
  }

  @AfterEach
  public void closeConnection() throws SQLException {
    connection.close();
  }

  @AfterEach
  public void closeStandardSqlConnection() throws SQLException {
    standardSqlConnection.close();
  }

  private BQConnection bqConnection() {
    return (BQConnection) connection;
  }

  private BQConnection bqStandardSqlConnection() {
    return (BQConnection) standardSqlConnection;
  }

  @Test
  public void ChainedCursorFunctionTest() {
    this.QueryLoad();
    this.logger.info("ChainedFunctionTest");
    try {
      assertTrue(resultForTest.next());
      assertEquals("you", resultForTest.getString(1));

    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    try {
      resultForTest.absolute(10);
    } catch (SQLException e) {
      assertTrue(true);
    }

    try {
      for (int i = 0; i < 9; i++) {
        assertTrue(resultForTest.next());
      }
      assertFalse(resultForTest.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertEquals("", resultForTest.getString(1));
    } catch (SQLException e) {
      assertTrue(true);
    }

    QueryLoad();
    try {
      resultForTest.next();
      assertEquals("you", resultForTest.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    this.logger.info("chainedfunctiontest end");
  }

  @Test
  public void databaseMetaDataGetTables() {
    this.QueryLoad();
    ResultSet result = null;
    try {
      result =
          connection
              .getMetaData()
              .getColumns(null, "starschema_net__clouddb", "OUTLET_LOOKUP", null);
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
    try {
      assertTrue(result.first());
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
      fail();
    }
  }

  /** For testing isValid() , Close() , isClosed() */
  @Test
  public void isClosedValidtest() {
    this.QueryLoad();
    try {
      assertEquals(true, connection.isValid(0));
    } catch (SQLException e) {
      fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      assertEquals(true, connection.isValid(10));
    } catch (SQLException e) {
      fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      connection.isValid(-10);
    } catch (SQLException e) {
      assertTrue(true);
      // e.printStackTrace();
    }

    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      assertTrue(connection.isClosed());
    } catch (SQLException e1) {
      e1.printStackTrace();
    }

    try {
      connection.isValid(0);
    } catch (SQLException e) {
      assertTrue(true);
      e.printStackTrace();
    }
  }

  // Comprehensive Tests:

  public void QueryLoad() {
    final String sql =
        "SELECT TOP(word,10) AS word, COUNT(*) as count FROM publicdata:samples.shakespeare";
    this.logger.info("Test number: 01");
    this.logger.info("Running query:" + sql);

    try {
      // Statement stmt = BQResultSetFunctionTest.con.createStatement();
      Statement stmt =
          connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      if (this.maxRows != null) {
        stmt.setMaxRows(this.maxRows);
      }
      resultForTest = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(resultForTest);
  }

  @Test
  public void ResultSetMetadata() {
    QueryLoad();
    try {
      this.logger.debug(resultForTest.getMetaData().getSchemaName(1));
      this.logger.debug("{}", resultForTest.getMetaData().getScale(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
    }
    assertTrue(true);
  }

  @Test
  public void TestResultIndexOutofBound() {
    this.QueryLoad();
    try {
      this.logger.debug("{}", resultForTest.getBoolean(99));
    } catch (SQLException e) {
      assertTrue(true);
      this.logger.error("SQLexception" + e.toString());
    }
  }

  @Test
  public void TestResultSetFirst() {
    this.QueryLoad();
    try {
      //            Assert.assertTrue(resultForTest.first());
      resultForTest.next();
      assertTrue(resultForTest.isFirst());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetBoolean() {
    this.QueryLoad();
    try {
      assertTrue(resultForTest.next());
      assertEquals(Boolean.parseBoolean("42"), resultForTest.getBoolean(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetFloat() {
    this.QueryLoad();
    try {
      assertTrue(resultForTest.next());
      assertEquals(42f, resultForTest.getFloat(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetInteger() {
    this.QueryLoad();
    try {
      assertTrue(resultForTest.next());
      assertEquals(42, resultForTest.getInt(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetRow() {
    this.QueryLoad();
    try {
      assertTrue(resultForTest.next());
      resultForTest.getRow();
    } catch (SQLException e) {
      assertTrue(true);
    }
  }

  @Test
  public void TestResultSetgetString() {
    this.QueryLoad();
    try {
      assertTrue(resultForTest.next());
      assertEquals("you", resultForTest.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetNextWithLimitedRows() {
    this.maxRows = 2;
    TestResultSetNext();
  }

  @Test
  public void TestResultSetNext() {
    this.QueryLoad();
    try {
      //            Assert.assertTrue(resultForTest.first());
      assertTrue(resultForTest.next());
      assertTrue(resultForTest.next());
      assertEquals("yet", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("would", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("world", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("without", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("with", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("will", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("why", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("whose", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("whom", resultForTest.getString(1));
      assertFalse(resultForTest.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertEquals("", resultForTest.getString(1));
    } catch (SQLException e) {
      assertTrue(true);
    }
  }

  @Test
  public void TestResultSetNextWhenFetchSizeExceeded() throws SQLException {
    this.QueryLoad();
    resultForTest.setFetchSize(2);
    try {
      //            Assert.assertTrue(resultForTest.first());
      assertTrue(resultForTest.next());
      assertTrue(resultForTest.next());
      assertEquals("yet", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("would", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("world", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("without", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("with", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("will", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("why", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("whose", resultForTest.getString(1));
      assertTrue(resultForTest.next());
      assertEquals("whom", resultForTest.getString(1));
      assertFalse(resultForTest.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertEquals("", resultForTest.getString(1));
    } catch (SQLException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testResultSetTypesInGetString() throws SQLException {
    final String sql =
        "SELECT "
            + "STRUCT(1 as a, 'hello' as b), "
            + "['a', 'b', 'c'], "
            + "[STRUCT(1 as a, 'hello' as b), STRUCT(2 as a, 'goodbye' as b)], "
            + "STRUCT(1 as a, ['an', 'array'] as b),"
            + "TIMESTAMP('2012-01-01 00:00:03.0000') as t";

    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          standardSqlConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(result);
    assertTrue(result.next());

    HashMap<String, String> hello =
        new HashMap() {
          {
            put("a", "1");
            put("b", "hello");
          }
        };
    HashMap<String, String> goodbye =
        new HashMap() {
          {
            put("a", "2");
            put("b", "goodbye");
          }
        };

    assertEquals(
        hello,
        new Gson()
            .fromJson(result.getString(1), new TypeToken<Map<String, String>>() {}.getType()));

    assertEquals("[\"a\",\"b\",\"c\"]", result.getString(2));

    Map<String, String>[] arrayOfMapsActual =
        new Gson()
            .fromJson(result.getString(3), new TypeToken<Map<String, String>[]>() {}.getType());
    assertEquals(2, arrayOfMapsActual.length);
    assertEquals(hello, arrayOfMapsActual[0]);
    assertEquals(goodbye, arrayOfMapsActual[1]);

    Map<String, Object> mixedBagActual =
        new Gson().fromJson(result.getString(4), new TypeToken<Map<String, Object>>() {}.getType());
    assertEquals(2, mixedBagActual.size());
    assertEquals("1", mixedBagActual.get("a"));
    assertEquals(
        new Gson().toJson(new String[] {"an", "array"}),
        new Gson().toJson(mixedBagActual.get("b")));

    assertEquals("2012-01-01 00:00:03 UTC", result.getString(5));
  }

  @Test
  public void testResultSetDateTimeType() throws SQLException, ParseException {
    final String sql = "SELECT DATETIME('2012-01-01 01:02:03.04567')";
    final Calendar istCalendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Kolkata"));
    final DateFormat utcDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    utcDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

    Statement stmt =
        standardSqlConnection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setQueryTimeout(500);
    ResultSet result = stmt.executeQuery(sql);
    assertTrue(result.next());

    Timestamp resultTimestamp = result.getTimestamp(1);
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);

    // getObject() and getTimestamp() should behave the same for DATETIME.
    assertEquals(resultTimestamp, resultObject);

    // getTimestamp().toString() should be equivalent to getString(), with full microsecond support.
    assertEquals("2012-01-01 01:02:03.04567", resultTimestamp.toString());
    assertEquals("2012-01-01T01:02:03.045670", resultString);

    // If a different calendar is used, the string representation should be adjusted.
    Timestamp adjustedTimestamp = result.getTimestamp(1, istCalendar);
    // Render it from the perspective of UTC.
    // Since it was created for IST, it should be adjusted by -5:30.
    String adjustedString = utcDateFormatter.format(adjustedTimestamp);
    assertEquals("2011-12-31 19:32:03.045", adjustedString);
    // SimpleDateFormat does not support microseconds,
    // but they should be correct on the adjusted timestamp.
    assertEquals(45670000, adjustedTimestamp.getNanos());
  }

  @Test
  public void testResultSetTimestampType() throws SQLException, ParseException {
    final String sql = "SELECT TIMESTAMP('2012-01-01 01:02:03.04567')";

    Statement stmt =
        standardSqlConnection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setQueryTimeout(500);
    ResultSet result = stmt.executeQuery(sql);
    assertTrue(result.next());

    Timestamp resultTimestamp = result.getTimestamp(1);
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);

    // getObject() and getTimestamp() should behave the same for TIMESTAMP.
    assertEquals(resultTimestamp, resultObject);

    // getString() should be the string representation in UTC+0.
    assertEquals("2012-01-01 01:02:03.04567 UTC", resultString);

    // getTimestamp() should have the right number of milliseconds elapsed since epoch.
    // 1325379723045 milliseconds after epoch, the time is 2012-01-01 01:02:03.045 in UTC+0.
    assertEquals(1325379723045L, resultTimestamp.getTime());
    // The microseconds should also be correct, but nanoseconds are not supported by BigQuery.
    assertEquals(45670000, resultTimestamp.getNanos());
  }

  @Test
  public void testResultSetTypesInGetObject() throws SQLException, ParseException {
    final String sql =
        "SELECT "
            + "CAST('2312412432423423334.234234234' AS NUMERIC), "
            + "CAST('2011-04-03' AS DATE), "
            + "CAST('nan' AS FLOAT)";

    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(result);
    assertTrue(result.next());

    assertEquals(new BigDecimal("2312412432423423334.234234234"), result.getObject(1));

    SimpleDateFormat dateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date parsedDateDate = new java.sql.Date(dateDateFormat.parse("2011-04-03").getTime());
    assertEquals(parsedDateDate, result.getObject(2));

    assertEquals(Double.NaN, result.getObject(3));
  }

  @Test
  public void testResultSetArraysInGetObject() throws SQLException, ParseException {
    final String sql = "SELECT [1, 2, 3], [TIMESTAMP(\"2010-09-07 15:30:00 America/Los_Angeles\")]";

    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          standardSqlConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLException" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(result);
    assertTrue(result.next());

    // for arrays, getObject() and getString() should behave identically, allowing consumers
    // to interpret/convert types as needed
    String expected = "[\"1\",\"2\",\"3\"]";
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);
    assertEquals(expected, (String) resultObject);
    assertEquals(expected, resultString);

    // timestamp conversion should occur consumer-side for arrays
    String expectedTwo = "[\"1.2838986E9\"]";
    Object resultObjectTwo = result.getObject(2);
    String resultStringTwo = result.getString(2);
    assertEquals(expectedTwo, (String) resultObjectTwo);
    assertEquals(expectedTwo, resultStringTwo);
  }

  @Test
  public void testResultSetTimeType() throws SQLException, ParseException {
    final String sql = "select current_time(), CAST('00:00:02.12345' AS TIME)";

    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          standardSqlConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(result);
    assertTrue(result.next());

    java.sql.Time resultTime = result.getTime(1);
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);

    // getObject() and getTime() should behave the same for TIME.
    assertEquals(resultTime, resultObject);

    // getTime().toString() should be equivalent to getString() without milliseconds.
    assertTrue(resultString.startsWith(resultTime.toString()));

    // getTime() should have milliseconds, though. They're just not included in toString().
    // Get whole milliseconds (modulo whole seconds) from resultTime.
    long timeMillis = resultTime.getTime() % 1000;
    // Get whole milliseconds from resultString.
    int decimalPlace = resultString.lastIndexOf('.');
    long stringMillis = Long.parseLong(resultString.substring(decimalPlace + 1, decimalPlace + 4));
    assertEquals(timeMillis, stringMillis);

    // Check that explicit casts to TIME work as expected.
    Time fixedTime = result.getTime(2);
    assertEquals("00:00:02", fixedTime.toString());
    // The object should have milliseconds even though they're hidden by toString().
    // AFAICT [java.sql.Time] does not support microseconds.
    assertEquals(123, fixedTime.getTime() % 1000);
    // getString() should show microseconds.
    assertEquals("00:00:02.123450", result.getString(2));
  }

  @Test
  public void TestResultSetTotalBytesProcessedCacheHit() {
    QueryLoad();
    assertTrue(resultForTest instanceof BQForwardOnlyResultSet);
    BQForwardOnlyResultSet results = (BQForwardOnlyResultSet) resultForTest;
    final Boolean processedNoBytes = new Long(0L).equals(results.getTotalBytesProcessed());
    assertEquals(processedNoBytes, results.getCacheHit());
  }

  @Test
  public void testResultSetProcedures() throws SQLException, ParseException {
    final String sql =
        "CREATE PROCEDURE looker_test.procedure_test(target_id INT64)\n" + "BEGIN\n" + "END;";

    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          standardSqlConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    } finally {
      String cleanupSql = "DROP PROCEDURE looker_test.procedure_test;\n";
      Statement stmt =
          standardSqlConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      stmt.executeQuery(cleanupSql);
    }

    // System.out.println(result.toString());
  }

  @Test
  public void testResultSetProceduresAsync() throws SQLException {
    final String sql =
        "CREATE PROCEDURE looker_test.long_procedure(target_id INT64)\n" + "BEGIN\n" + "END;";

    try {
      BQConnection bq = bqStandardSqlConnection();
      BQStatement stmt =
          new BQStatement(
              bq.getProjectId(), bq, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) {
            @Override
            protected long getSyncTimeoutMillis() {
              return 0; // force async condition
            }
          };

      stmt.setQueryTimeout(500);
      stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    } finally {
      String cleanupSql = "DROP PROCEDURE looker_test.long_procedure;\n";
      Statement stmt =
          standardSqlConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      stmt.executeQuery(cleanupSql);
    }
  }

  @Test
  public void testBQForwardOnlyResultSetDoesntThrowNPE() throws Exception {
    BQConnection bq = bqConnection();
    BQStatement stmt = (BQStatement) bq.createStatement();
    QueryResponse qr = stmt.runSyncQuery("SELECT 1", false);
    Job ref =
        bq.getBigquery()
            .jobs()
            .get(bq.getProjectId(), qr.getJobReference().getJobId())
            .setLocation(qr.getJobReference().getLocation())
            .execute();
    // Under certain race conditions we could close the connection after the job is complete but
    // before the results have been fetched. This was throwing a NPE.
    bq.close();
    try {
      new BQForwardOnlyResultSet(bq.getBigquery(), bq.getProjectId(), ref, null, stmt);
      fail("Initalizing BQForwardOnlyResultSet should throw something other than a NPE.");
    } catch (SQLException e) {
      assertEquals(e.getMessage(), "Failed to fetch results. Connection is closed.");
    }
  }

  @Test
  public void testHandlesAllNullResponseFields() throws Exception {
    try {
      mockResponse("{}");
    } catch (BQSQLException e) {
      assertTrue(e.getMessage().contains("without a job reference"));
      return;
    }
    throw new AssertionError("Expected graceful failure due to lack of job reference");
  }

  @Test
  public void testHandlesNullTimeDateObjects() throws Exception {
    Statement stmt =
        standardSqlConnection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    final String date = "2011-11-11";
    final String time = "12:12:12";
    final String dateTime = date + " " + time;
    final String dateTimeWithT = date + "T" + time;
    // The number of milliseconds between epoch and 2011-11-11 12:12:12 UTC+0.
    final long millis = 1321013532000L;

    // spotless:off
    String sql = "SELECT " +
        "TIMESTAMP('" + dateTime + "') AS ts, " +
        "DATETIME('" + dateTime + "') AS dt, " +
        "DATE('" + date + "') AS d, " +
        "TIME(12, 12, 12) AS t\n" +
        "UNION ALL SELECT " +
        "CASE WHEN 1 = 0 THEN TIMESTAMP('" + dateTime + "') ELSE NULL END, " +
        "CASE WHEN 1 = 0 THEN DATETIME('" + dateTime + "') ELSE NULL END, " +
        "CASE WHEN 1 = 0 THEN DATE('" + date + "') ELSE NULL END, " +
        "CASE WHEN 1 = 0 THEN TIME(12, 12, 12) ELSE NULL END";
    // spotless:on

    ResultSet results = stmt.executeQuery(sql);

    // First row has all non-null objects.
    Assertions.assertThat(results.next()).isTrue();
    Assertions.assertThat(results.getObject("ts"))
        .isEqualTo(Timestamp.from(Instant.ofEpochMilli(millis)));
    Assertions.assertThat(results.getString("ts")).isEqualTo(dateTime + " UTC");
    Assertions.assertThat(results.getObject("dt")).isEqualTo(Timestamp.valueOf(dateTime));
    Assertions.assertThat(results.getString("dt")).isEqualTo(dateTimeWithT);
    Assertions.assertThat(results.getObject("d")).isEqualTo(java.sql.Date.valueOf(date));
    Assertions.assertThat(results.getString("d")).isEqualTo(date);
    Assertions.assertThat(results.getObject("t")).isEqualTo(java.sql.Time.valueOf(time));
    Assertions.assertThat(results.getString("t")).isEqualTo(time);

    // Second row is all null.
    Assertions.assertThat(results.next()).isTrue();
    Assertions.assertThat(results.getObject("ts")).isNull();
    Assertions.assertThat(results.getString("ts")).isNull();
    Assertions.assertThat(results.getObject("dt")).isNull();
    Assertions.assertThat(results.getString("dt")).isNull();
    Assertions.assertThat(results.getObject("d")).isNull();
    Assertions.assertThat(results.getString("d")).isNull();
    Assertions.assertThat(results.getObject("t")).isNull();
    Assertions.assertThat(results.getString("t")).isNull();

    // Only two rows.
    Assertions.assertThat(results.next()).isFalse();
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
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
    String sqlStmt = "SELECT word from publicdata:samples.shakespeare LIMIT 100";

    BQForwardOnlyResultSet results = ((BQForwardOnlyResultSet) stmt.executeQuery(sqlStmt));

    results.getTotalBytesProcessed();
    results.getCacheHit();
    results.getJobId();
    results.getBiEngineMode();
    results.getBiEngineReasons();
  }

  @Test
  public void testStatelessQuery() throws SQLException, IOException {
    try (Connection statelessConnection =
        connect("&useLegacySql=false&jobcreationmode=JOB_CREATION_OPTIONAL")) {
      StatelessQuery.assumeStatelessQueriesEnabled(statelessConnection.getCatalog());
      try (Statement stmt =
          statelessConnection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        final ResultSet result = stmt.executeQuery(StatelessQuery.exampleQuery());
        final String[][] rows = BQSupportMethods.GetQueryResult(result);
        Assertions.assertThat(rows).isEqualTo(StatelessQuery.exampleValues());

        final BQForwardOnlyResultSet bqResultSet = (BQForwardOnlyResultSet) result;
        Assertions.assertThat(bqResultSet.getJobId()).isNull();
        Assertions.assertThat(bqResultSet.getQueryId()).contains("!");
      }
    }
  }

  @Override
  protected Statement createStatementForCommonTests(final Connection connection)
      throws SQLException {
    return connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }
}
