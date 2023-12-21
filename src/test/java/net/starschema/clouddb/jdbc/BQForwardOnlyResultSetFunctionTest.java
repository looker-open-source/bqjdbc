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
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
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
import junit.framework.Assert;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Junit test tests functions in BQForwardOnlyResultSet
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQForwardOnlyResultSetFunctionTest extends CommonTestsForResultSets {

  private static java.sql.Connection con = null;
  private java.sql.ResultSet resultForTest = null;

  Logger logger = LoggerFactory.getLogger(BQForwardOnlyResultSetFunctionTest.class);
  private Integer maxRows = null;
  private String defaultProjectId = null;
  private BQConnection defaultConn = null;

  @Before
  public void setup() throws SQLException, IOException {
    Properties props =
        BQSupportFuncts.readFromPropFile(
            getClass().getResource("/installedaccount.properties").getFile());
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(props, true, null);
    url += "&useLegacySql=false";
    this.defaultProjectId = props.getProperty("projectid");
    this.defaultConn = new BQConnection(url, new Properties());
  }

  @After
  public void teardown() throws SQLException {
    if (defaultConn != null) {
      defaultConn.close();
      defaultConn = null;
    }
  }

  private BQConnection conn() throws SQLException, IOException {
    return this.defaultConn;
  }

  @Test
  public void ChainedCursorFunctionTest() {
    this.QueryLoad();
    this.logger.info("ChainedFunctionTest");
    try {
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("you", resultForTest.getString(1));

    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    try {
      resultForTest.absolute(10);
    } catch (SQLException e) {
      Assert.assertTrue(true);
    }

    try {
      for (int i = 0; i < 9; i++) {
        Assert.assertTrue(resultForTest.next());
      }
      Assert.assertFalse(resultForTest.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertEquals("", resultForTest.getString(1));
    } catch (SQLException e) {
      Assert.assertTrue(true);
    }

    QueryLoad();
    try {
      resultForTest.next();
      Assert.assertEquals("you", resultForTest.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    this.logger.info("chainedfunctiontest end");
  }

  @Test
  public void databaseMetaDataGetTables() {
    this.QueryLoad();
    ResultSet result = null;
    try {
      result = con.getMetaData().getColumns(null, "starschema_net__clouddb", "OUTLET_LOOKUP", null);
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

  /** For testing isValid() , Close() , isClosed() */
  @Test
  public void isClosedValidtest() {
    this.QueryLoad();
    try {
      Assert.assertEquals(true, BQForwardOnlyResultSetFunctionTest.con.isValid(0));
    } catch (SQLException e) {
      Assert.fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      Assert.assertEquals(true, BQForwardOnlyResultSetFunctionTest.con.isValid(10));
    } catch (SQLException e) {
      Assert.fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      BQForwardOnlyResultSetFunctionTest.con.isValid(-10);
    } catch (SQLException e) {
      Assert.assertTrue(true);
      // e.printStackTrace();
    }

    try {
      BQForwardOnlyResultSetFunctionTest.con.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.con.isClosed());
    } catch (SQLException e1) {
      e1.printStackTrace();
    }

    try {
      BQForwardOnlyResultSetFunctionTest.con.isValid(0);
    } catch (SQLException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }

  /**
   * Makes a new Bigquery Connection to URL in file and gives back the Connection to static con
   * member.
   */
  @Before
  public void NewConnection() {
    NewConnection("&useLegacySql=true");
  }

  void NewConnection(String extraUrl) {
    this.logger.info("Testing the JDBC driver");
    try {
      Class.forName("net.starschema.clouddb.jdbc.BQDriver");
      Properties props =
          BQSupportFuncts.readFromPropFile(
              getClass().getResource("/installedaccount1.properties").getFile());
      String jdcbUrl = BQSupportFuncts.constructUrlFromPropertiesFile(props);
      if (extraUrl != null) {
        jdcbUrl += extraUrl;
      }
      if (BQForwardOnlyResultSetFunctionTest.con != null) {
        BQForwardOnlyResultSetFunctionTest.con.close();
      }
      BQForwardOnlyResultSetFunctionTest.con =
          DriverManager.getConnection(
              jdcbUrl,
              BQSupportFuncts.readFromPropFile(
                  getClass().getResource("/installedaccount1.properties").getFile()));
    } catch (Exception e) {
      e.printStackTrace();
      this.logger.error("Error in connection" + e.toString());
      Assert.fail("General Exception:" + e.toString());
    }
    this.logger.info(((BQConnection) BQForwardOnlyResultSetFunctionTest.con).getURLPART());
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
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      if (this.maxRows != null) {
        stmt.setMaxRows(this.maxRows);
      }
      resultForTest = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(resultForTest);
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
    Assert.assertTrue(true);
  }

  @Test
  public void TestResultIndexOutofBound() {
    this.QueryLoad();
    try {
      this.logger.debug("{}", resultForTest.getBoolean(99));
    } catch (SQLException e) {
      Assert.assertTrue(true);
      this.logger.error("SQLexception" + e.toString());
    }
  }

  @Test
  public void TestResultSetFirst() {
    this.QueryLoad();
    try {
      //            Assert.assertTrue(resultForTest.first());
      resultForTest.next();
      Assert.assertTrue(resultForTest.isFirst());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetBoolean() {
    this.QueryLoad();
    try {
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals(Boolean.parseBoolean("42"), resultForTest.getBoolean(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetFloat() {
    this.QueryLoad();
    try {
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals(42f, resultForTest.getFloat(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetInteger() {
    this.QueryLoad();
    try {
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals(42, resultForTest.getInt(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetRow() {
    this.QueryLoad();
    try {
      Assert.assertTrue(resultForTest.next());
      resultForTest.getRow();
    } catch (SQLException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void TestResultSetgetString() {
    this.QueryLoad();
    try {
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("you", resultForTest.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
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
      Assert.assertTrue(resultForTest.next());
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("yet", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("would", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("world", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("without", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("with", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("will", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("why", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("whose", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("whom", resultForTest.getString(1));
      Assert.assertFalse(resultForTest.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertEquals("", resultForTest.getString(1));
    } catch (SQLException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void TestResultSetNextWhenFetchSizeExceeded() throws SQLException {
    this.QueryLoad();
    resultForTest.setFetchSize(2);
    try {
      //            Assert.assertTrue(resultForTest.first());
      Assert.assertTrue(resultForTest.next());
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("yet", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("would", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("world", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("without", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("with", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("will", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("why", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("whose", resultForTest.getString(1));
      Assert.assertTrue(resultForTest.next());
      Assert.assertEquals("whom", resultForTest.getString(1));
      Assert.assertFalse(resultForTest.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }

    try {
      Assert.assertEquals("", resultForTest.getString(1));
    } catch (SQLException e) {
      Assert.assertTrue(true);
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

    this.NewConnection("&useLegacySql=false");
    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(result);
    Assert.assertTrue(result.next());

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

    Assert.assertEquals(
        hello,
        new Gson()
            .fromJson(result.getString(1), new TypeToken<Map<String, String>>() {}.getType()));

    Assert.assertEquals("[\"a\",\"b\",\"c\"]", result.getString(2));

    Map<String, String>[] arrayOfMapsActual =
        new Gson()
            .fromJson(result.getString(3), new TypeToken<Map<String, String>[]>() {}.getType());
    Assert.assertEquals(2, arrayOfMapsActual.length);
    Assert.assertEquals(hello, arrayOfMapsActual[0]);
    Assert.assertEquals(goodbye, arrayOfMapsActual[1]);

    Map<String, Object> mixedBagActual =
        new Gson().fromJson(result.getString(4), new TypeToken<Map<String, Object>>() {}.getType());
    Assert.assertEquals(2, mixedBagActual.size());
    Assert.assertEquals("1", mixedBagActual.get("a"));
    Assert.assertEquals(
        new Gson().toJson(new String[] {"an", "array"}),
        new Gson().toJson(mixedBagActual.get("b")));

    Assert.assertEquals("2012-01-01 00:00:03 UTC", result.getString(5));
  }

  @Test
  public void testResultSetDateTimeType() throws SQLException, ParseException {
    final String sql = "SELECT DATETIME('2012-01-01 01:02:03.04567')";
    final Calendar istCalendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Kolkata"));
    final DateFormat utcDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    utcDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

    this.NewConnection("&useLegacySql=false");
    Statement stmt =
        BQForwardOnlyResultSetFunctionTest.con.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setQueryTimeout(500);
    ResultSet result = stmt.executeQuery(sql);
    Assert.assertTrue(result.next());

    Timestamp resultTimestamp = result.getTimestamp(1);
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);

    // getObject() and getTimestamp() should behave the same for DATETIME.
    Assert.assertEquals(resultTimestamp, resultObject);

    // getTimestamp().toString() should be equivalent to getString(), with full microsecond support.
    Assert.assertEquals("2012-01-01 01:02:03.04567", resultTimestamp.toString());
    Assert.assertEquals("2012-01-01T01:02:03.045670", resultString);

    // If a different calendar is used, the string representation should be adjusted.
    Timestamp adjustedTimestamp = result.getTimestamp(1, istCalendar);
    // Render it from the perspective of UTC.
    // Since it was created for IST, it should be adjusted by -5:30.
    String adjustedString = utcDateFormatter.format(adjustedTimestamp);
    Assert.assertEquals("2011-12-31 19:32:03.045", adjustedString);
    // SimpleDateFormat does not support microseconds,
    // but they should be correct on the adjusted timestamp.
    Assert.assertEquals(45670000, adjustedTimestamp.getNanos());
  }

  @Test
  public void testResultSetTimestampType() throws SQLException, ParseException {
    final String sql = "SELECT TIMESTAMP('2012-01-01 01:02:03.04567')";

    this.NewConnection("&useLegacySql=false");
    Statement stmt =
        BQForwardOnlyResultSetFunctionTest.con.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setQueryTimeout(500);
    ResultSet result = stmt.executeQuery(sql);
    Assert.assertTrue(result.next());

    Timestamp resultTimestamp = result.getTimestamp(1);
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);

    // getObject() and getTimestamp() should behave the same for TIMESTAMP.
    Assert.assertEquals(resultTimestamp, resultObject);

    // getString() should be the string representation in UTC+0.
    Assert.assertEquals("2012-01-01 01:02:03.04567 UTC", resultString);

    // getTimestamp() should have the right number of milliseconds elapsed since epoch.
    // 1325379723045 milliseconds after epoch, the time is 2012-01-01 01:02:03.045 in UTC+0.
    Assert.assertEquals(1325379723045L, resultTimestamp.getTime());
    // The microseconds should also be correct, but nanoseconds are not supported by BigQuery.
    Assert.assertEquals(45670000, resultTimestamp.getNanos());
  }

  @Test
  public void testResultSetTypesInGetObject() throws SQLException, ParseException {
    final String sql =
        "SELECT "
            + "CAST('2312412432423423334.234234234' AS NUMERIC), "
            + "CAST('2011-04-03' AS DATE), "
            + "CAST('nan' AS FLOAT)";

    this.NewConnection("&useLegacySql=true");
    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(result);
    Assert.assertTrue(result.next());

    Assert.assertEquals(new BigDecimal("2312412432423423334.234234234"), result.getObject(1));

    SimpleDateFormat dateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date parsedDateDate = new java.sql.Date(dateDateFormat.parse("2011-04-03").getTime());
    Assert.assertEquals(parsedDateDate, result.getObject(2));

    Assert.assertEquals(Double.NaN, result.getObject(3));
  }

  @Test
  public void testResultSetArraysInGetObject() throws SQLException, ParseException {
    final String sql = "SELECT [1, 2, 3], [TIMESTAMP(\"2010-09-07 15:30:00 America/Los_Angeles\")]";

    this.NewConnection("&useLegacySql=false");
    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLException" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(result);
    Assert.assertTrue(result.next());

    // for arrays, getObject() and getString() should behave identically, allowing consumers
    // to interpret/convert types as needed
    String expected = "[\"1\",\"2\",\"3\"]";
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);
    Assert.assertEquals(expected, (String) resultObject);
    Assert.assertEquals(expected, resultString);

    // timestamp conversion should occur consumer-side for arrays
    String expectedTwo = "[\"1.2838986E9\"]";
    Object resultObjectTwo = result.getObject(2);
    String resultStringTwo = result.getString(2);
    Assert.assertEquals(expectedTwo, (String) resultObjectTwo);
    Assert.assertEquals(expectedTwo, resultStringTwo);
  }

  @Test
  public void testResultSetTimeType() throws SQLException, ParseException {
    final String sql = "select current_time(), CAST('00:00:02.12345' AS TIME)";
    this.NewConnection("&useLegacySql=false");

    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(result);
    Assert.assertTrue(result.next());

    java.sql.Time resultTime = result.getTime(1);
    Object resultObject = result.getObject(1);
    String resultString = result.getString(1);

    // getObject() and getTime() should behave the same for TIME.
    Assert.assertEquals(resultTime, resultObject);

    // getTime().toString() should be equivalent to getString() without milliseconds.
    Assert.assertTrue(resultString.startsWith(resultTime.toString()));

    // getTime() should have milliseconds, though. They're just not included in toString().
    // Get whole milliseconds (modulo whole seconds) from resultTime.
    long timeMillis = resultTime.getTime() % 1000;
    // Get whole milliseconds from resultString.
    int decimalPlace = resultString.lastIndexOf('.');
    long stringMillis = Long.parseLong(resultString.substring(decimalPlace + 1, decimalPlace + 4));
    Assert.assertEquals(timeMillis, stringMillis);

    // Check that explicit casts to TIME work as expected.
    Time fixedTime = result.getTime(2);
    Assert.assertEquals("00:00:02", fixedTime.toString());
    // The object should have milliseconds even though they're hidden by toString().
    // AFAICT [java.sql.Time] does not support microseconds.
    Assert.assertEquals(123, fixedTime.getTime() % 1000);
    // getString() should show microseconds.
    Assert.assertEquals("00:00:02.123450", result.getString(2));
  }

  @Test
  public void TestResultSetTotalBytesProcessedCacheHit() {
    QueryLoad();
    Assert.assertTrue(resultForTest instanceof BQForwardOnlyResultSet);
    BQForwardOnlyResultSet results = (BQForwardOnlyResultSet) resultForTest;
    final Boolean processedNoBytes = new Long(0L).equals(results.getTotalBytesProcessed());
    Assert.assertEquals(processedNoBytes, results.getCacheHit());
  }

  @Test
  public void testResultSetProcedures() throws SQLException, ParseException {
    final String sql =
        "CREATE PROCEDURE looker_test.procedure_test(target_id INT64)\n" + "BEGIN\n" + "END;";

    this.NewConnection("&useLegacySql=false");
    java.sql.ResultSet result = null;
    try {
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    } finally {
      String cleanupSql = "DROP PROCEDURE looker_test.procedure_test;\n";
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      stmt.executeQuery(cleanupSql);
    }

    System.out.println(result.toString());
  }

  @Test
  public void testResultSetProceduresAsync() throws SQLException {
    final String sql =
        "CREATE PROCEDURE looker_test.long_procedure(target_id INT64)\n" + "BEGIN\n" + "END;";
    this.NewConnection("&useLegacySql=false");

    try {
      BQConnection bq = conn();
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
    } catch (SQLException | IOException e) {
      this.logger.error("SQLexception" + e.toString());
      Assert.fail("SQLException" + e.toString());
    } finally {
      String cleanupSql = "DROP PROCEDURE looker_test.long_procedure;\n";
      Statement stmt =
          BQForwardOnlyResultSetFunctionTest.con.createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      stmt.executeQuery(cleanupSql);
    }
  }

  @Test
  public void testBQForwardOnlyResultSetDoesntThrowNPE() throws Exception {
    BQConnection bq = conn();
    BQStatement stmt = (BQStatement) bq.createStatement();
    QueryResponse qr = stmt.runSyncQuery("SELECT 1", false);
    Job ref =
        bq.getBigquery()
            .jobs()
            .get(defaultProjectId, qr.getJobReference().getJobId())
            .setLocation(qr.getJobReference().getLocation())
            .execute();
    // Under certain race conditions we could close the connection after the job is complete but
    // before the results have been fetched. This was throwing a NPE.
    bq.close();
    try {
      new BQForwardOnlyResultSet(bq.getBigquery(), defaultProjectId, ref, null, stmt);
      Assert.fail("Initalizing BQForwardOnlyResultSet should throw something other than a NPE.");
    } catch (SQLException e) {
      Assert.assertEquals(e.getMessage(), "Failed to fetch results. Connection is closed.");
    }
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
  public void testHandlesNullTimeDateObjects() throws Exception {
    this.NewConnection("&useLegacySql=false");
    Statement stmt = BQForwardOnlyResultSetFunctionTest.con.createStatement(
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    final String date = "2011-11-11";
    final String time = "12:12:12";
    final String dateTime = date + " " + time;
    final String dateTimeWithT = date + "T" + time;
    // The number of milliseconds between epoch and 2011-11-11 12:12:12 UTC+0.
    final long millis = 1321013532000L;

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

    ResultSet results = stmt.executeQuery(sql);

    // First row has all non-null objects.
    Assertions.assertThat(results.next()).isTrue();
    Assertions.assertThat(results.getObject("ts")).isEqualTo(Timestamp.from(Instant.ofEpochMilli(millis)));
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
  public void testStatelessQuery() throws SQLException {
    NewConnection("&useLegacySql=false&jobcreationmode=JOB_CREATION_OPTIONAL");
    StatelessQuery.assumeStatelessQueriesEnabled(
        BQForwardOnlyResultSetFunctionTest.con.getCatalog());
    final Statement stmt =
        BQForwardOnlyResultSetFunctionTest.con.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    final ResultSet result = stmt.executeQuery(StatelessQuery.exampleQuery());
    final String[][] rows = BQSupportMethods.GetQueryResult(result);
    Assertions.assertThat(rows).isEqualTo(StatelessQuery.exampleValues());

    final BQForwardOnlyResultSet bqResultSet = (BQForwardOnlyResultSet) result;
    Assertions.assertThat(bqResultSet.getJobId()).isNull();
    Assertions.assertThat(bqResultSet.getQueryId()).contains("!");
  }

  @Override
  protected Statement createStatementForCommonTests(final Connection connection)
      throws SQLException {
    return connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }
}
