/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.IOException;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This Junit test tests functions in BQForwardOnlyResultSet
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQForwardOnlyResultSetFunctionTest {

    private static java.sql.Connection con = null;
    private java.sql.ResultSet resultForTest = null;

    Logger logger = LoggerFactory.getLogger(BQForwardOnlyResultSetFunctionTest.class);
    private Integer maxRows = null;

    private BQConnection conn()  throws SQLException, IOException {
        String url = BQSupportFuncts.constructUrlFromPropertiesFile(BQSupportFuncts
            .readFromPropFile(getClass().getResource("/installedaccount.properties").getFile()), true, null);
        url += "&useLegacySql=false";
        return new BQConnection(url, new Properties());
    }

    @Test
    public void ChainedCursorFunctionTest() {
        this.QueryLoad();
        this.logger.info("ChainedFunctionTest");
        try {
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("you",
                    resultForTest.getString(1));

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
            Assert.assertEquals("you",
                    resultForTest.getString(1));
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

    /**
     * For testing isValid() , Close() , isClosed()
     */
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
     * Makes a new Bigquery Connection to URL in file and gives back the
     * Connection to static con member.
     */
    @Before
    public void NewConnection() {
        NewConnection(true);
    }

    void NewConnection(boolean useLegacySql) {

         this.logger.info("Testing the JDBC driver");
         try {
             Class.forName("net.starschema.clouddb.jdbc.BQDriver");
             Properties props = BQSupportFuncts
                     .readFromPropFile(getClass().getResource("/installedaccount1.properties").getFile());
             props.setProperty("useLegacySql", String.valueOf(useLegacySql));
             BQForwardOnlyResultSetFunctionTest.con = DriverManager.getConnection(
                     BQSupportFuncts.constructUrlFromPropertiesFile(props),
                     BQSupportFuncts.readFromPropFile(getClass().getResource("/installedaccount1.properties").getFile()));
         } catch (Exception e) {
             e.printStackTrace();
             this.logger.error("Error in connection" + e.toString());
             Assert.fail("General Exception:" + e.toString());
         }
         this.logger.info(((BQConnection) BQForwardOnlyResultSetFunctionTest.con)
                 .getURLPART());
    }

    // Comprehensive Tests:


    public void QueryLoad() {
        final String sql = "SELECT TOP(word,10) AS word, COUNT(*) as count FROM publicdata:samples.shakespeare";
        this.logger.info("Test number: 01");
        this.logger.info("Running query:" + sql);

        try {
            //Statement stmt = BQResultSetFunctionTest.con.createStatement();
            Statement stmt = BQForwardOnlyResultSetFunctionTest.con
                    .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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
            this.logger.debug(resultForTest.getMetaData()
                    .getSchemaName(1));
            this.logger.debug("{}", resultForTest.getMetaData()
                    .getScale(1));
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
            Assert.assertEquals(Boolean.parseBoolean("42"),
                    resultForTest.getBoolean(2));
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
            Assert.assertEquals(42f,
                    resultForTest.getFloat(2));
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
            Assert.assertEquals("you",
                    resultForTest.getString(1));
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
            Assert.assertEquals("yet",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("would",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("world",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("without",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("with",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("will",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("why",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("whose",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("whom",
                    resultForTest.getString(1));
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
            Assert.assertEquals("yet",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("would",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("world",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("without",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("with",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("will",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("why",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("whose",
                    resultForTest.getString(1));
            Assert.assertTrue(resultForTest.next());
            Assert.assertEquals("whom",
                    resultForTest.getString(1));
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
        final String sql = "SELECT " +
                "STRUCT(1 as a, 'hello' as b), " +
                "['a', 'b', 'c'], " +
                "[STRUCT(1 as a, 'hello' as b), STRUCT(2 as a, 'goodbye' as b)], " +
                "STRUCT(1 as a, ['an', 'array'] as b)," +
                "TIMESTAMP('2012-01-01 00:00:03.032') as t"
        ;

        this.NewConnection(false);
        java.sql.ResultSet result = null;
        try {
            Statement stmt = BQForwardOnlyResultSetFunctionTest.con
                    .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(500);
            result = stmt.executeQuery(sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(result);
        Assert.assertTrue(result.next());

        HashMap<String, String> hello = new HashMap(){{put("a", "1"); put("b", "hello");}};
        HashMap<String, String> goodbye = new HashMap(){{put("a", "2"); put("b", "goodbye");}};

        Assert.assertEquals(hello,
                new Gson().fromJson(result.getString(1), new TypeToken<Map<String, String>>(){}.getType()));

        Assert.assertEquals("[\"a\",\"b\",\"c\"]", result.getString(2));

        Map<String, String>[] arrayOfMapsActual = new Gson().fromJson(result.getString(3), new TypeToken<Map<String, String>[]>(){}.getType());
        Assert.assertEquals(2, arrayOfMapsActual.length);
        Assert.assertEquals(hello, arrayOfMapsActual[0]);
        Assert.assertEquals(goodbye, arrayOfMapsActual[1]);

        Map<String, Object> mixedBagActual = new Gson().fromJson(result.getString(4), new TypeToken<Map<String, Object>>(){}.getType());
        Assert.assertEquals(2, mixedBagActual.size());
        Assert.assertEquals("1", mixedBagActual.get("a"));
        Assert.assertEquals(new Gson().toJson(new String[]{"an", "array"}), new Gson().toJson(mixedBagActual.get("b")));

        Assert.assertEquals("2012-01-01 00:00:03.032", result.getString(5));
    }

    @Test
    public void testResultSetTypesInGetObject() throws SQLException, ParseException {
        final String sql = "SELECT " +
                "DATETIME('2012-01-01 00:00:02'), " +
                "TIMESTAMP('2012-01-01 00:00:03'), " +
                "CAST('2312412432423423334.234234234' AS NUMERIC), " +
                "CAST('2011-04-03' AS DATE), " +
                "CAST('nan' AS FLOAT)"
                ;

        this.NewConnection(true);
        java.sql.ResultSet result = null;
        try {
            Statement stmt = BQForwardOnlyResultSetFunctionTest.con
                    .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(500);
            result = stmt.executeQuery(sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(result);
        Assert.assertTrue(result.next());
        Assert.assertEquals("2012-01-01T00:00:02", result.getObject(1));

        SimpleDateFormat timestampDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
        Date parsedDate = timestampDateFormat.parse("2012-01-01 00:00:03 UTC");
        Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
        Assert.assertEquals(timestamp, result.getObject(2));

        Assert.assertEquals(new BigDecimal("2312412432423423334.234234234"), result.getObject(3));
        SimpleDateFormat dateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date parsedDateDate = new java.sql.Date(dateDateFormat.parse("2011-04-03").getTime());
        Assert.assertEquals(parsedDateDate, result.getObject(4));

        Assert.assertEquals(Double.NaN, result.getObject(5));
    }

    @Test
    public void testResultSetArraysInGetObject() throws SQLException, ParseException {
        final String sql = "SELECT [1, 2, 3], [TIMESTAMP(\"2010-09-07 15:30:00 America/Los_Angeles\")]";

        this.NewConnection(false);
        java.sql.ResultSet result = null;
        try {
            Statement stmt = BQForwardOnlyResultSetFunctionTest.con
                .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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
        final String sql = "select current_time(), CAST('00:00:02.123455' AS TIME)";
        this.NewConnection(false);
        java.sql.ResultSet result = null;
        try {
            Statement stmt = BQForwardOnlyResultSetFunctionTest.con
                .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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

        // getObject() and getString() should behave the same for TIME
        Assert.assertEquals(resultString, (String) resultObject);

        // getTime() will return a 'time' without milliseconds
        Assert.assertTrue(resultString.startsWith(resultTime.toString()));

        // also check that explicit casts to TIME work as expected
        Assert.assertEquals(result.getTime(2).toString(), "00:00:02");
    }

    @Test
    public void TestResultSetTotalBytesProcessedCacheHit() {
        QueryLoad();
        Assert.assertTrue(resultForTest instanceof BQForwardOnlyResultSet);
        BQForwardOnlyResultSet results = (BQForwardOnlyResultSet)resultForTest;
        final Boolean processedNoBytes = new Long(0L).equals(results.getTotalBytesProcessed());
        Assert.assertEquals(processedNoBytes, results.getCacheHit());
    }

	@Test
	public void testResultSetProcedures() throws SQLException, ParseException {
		final String sql = "CREATE PROCEDURE looker_test.procedure_test(target_id INT64)\n"
			+ "BEGIN\n"
			+ "END;";

		this.NewConnection(false);
		java.sql.ResultSet result = null;
		try {
			Statement stmt = BQForwardOnlyResultSetFunctionTest.con
				.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setQueryTimeout(500);
			result = stmt.executeQuery(sql);
		} catch (SQLException e) {
			this.logger.error("SQLexception" + e.toString());
			Assert.fail("SQLException" + e.toString());
		} finally {
			String cleanupSql = "DROP PROCEDURE looker_test.procedure_test;\n";
			Statement stmt = BQForwardOnlyResultSetFunctionTest.con
				.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			stmt.setQueryTimeout(500);
			stmt.executeQuery(cleanupSql);
		}

		System.out.println(result.toString());
	}

    @Test
    public void testResultSetProceduresAsync() throws SQLException {
        final String sql = "CREATE PROCEDURE looker_test.long_procedure(target_id INT64)\n"
            + "BEGIN\n"
            + "END;";
        this.NewConnection(false);

        try {
            BQConnection bq = conn();
            BQStatement stmt = new BQStatement(bq.getProjectId(), bq, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) {
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
            Statement stmt = BQForwardOnlyResultSetFunctionTest.con
                .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(500);
            stmt.executeQuery(cleanupSql);
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
    public void testHandlesSomeNullResponseFields() throws Exception {
        // Make sure we don't get any NPE's due to null values;
        mockResponse(
            "{ \"jobComplete\": true, "
            + "\"totalRows\": \"0\", "
            + "\"rows\": [] }");
    }

    private void mockResponse(String jsonResponse) throws Exception {
        Properties properties =
            BQSupportFuncts.readFromPropFile(
                getClass().getResource("/installedaccount.properties").getFile());
        String url = BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null);
        // Mock an empty response object.
        MockHttpTransport mockTransport =
            new MockHttpTransport.Builder()
                .setLowLevelHttpResponse(
                    new MockLowLevelHttpResponse().setContent(jsonResponse))
                .build();
        BQConnection bq = new BQConnection(url + "&useLegacySql=false", properties, mockTransport);
        BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bq, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        String sqlStmt = "SELECT word from publicdata:samples.shakespeare LIMIT 100";

        BQForwardOnlyResultSet results = ((BQForwardOnlyResultSet)stmt.executeQuery(sqlStmt));

        results.getTotalBytesProcessed();
        results.getCacheHit();
    }
}
