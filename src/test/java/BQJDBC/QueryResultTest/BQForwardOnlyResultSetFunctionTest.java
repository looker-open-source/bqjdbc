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
package BQJDBC.QueryResultTest;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

/**
 * This Junit test tests functions in BQResultset
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQForwardOnlyResultSetFunctionTest {

    private static java.sql.Connection con = null;
    private static java.sql.ResultSet Result = null;

    Logger logger = Logger.getLogger(BQForwardOnlyResultSetFunctionTest.class.getName());

    @Test
    public void ChainedCursorFunctionTest() {
        this.logger.info("ChainedFunctionTest");
        try {
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("you",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));

        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            BQForwardOnlyResultSetFunctionTest.Result.absolute(10);
        } catch (SQLException e) {
            Assert.assertTrue(true);
        }

        try {
            for (int i = 0; i < 9; i++) {
                Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            }
            Assert.assertFalse(BQForwardOnlyResultSetFunctionTest.Result.next());
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertEquals("", BQForwardOnlyResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            Assert.assertTrue(true);
        }

        QueryLoad();
        try {
            Result.next();
            Assert.assertEquals("you",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        this.logger.info("chainedfunctiontest end");
    }

    @Test
    public void databaseMetaDataGetTables() {
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
         if (useLegacySql) {
            this.QueryLoad();
        }
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
            BQForwardOnlyResultSetFunctionTest.Result = stmt.executeQuery(sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(BQForwardOnlyResultSetFunctionTest.Result);
    }

    @Test
    public void ResultSetMetadata() {
        try {
            this.logger.debug(BQForwardOnlyResultSetFunctionTest.Result.getMetaData()
                    .getSchemaName(1));
            this.logger.debug(BQForwardOnlyResultSetFunctionTest.Result.getMetaData()
                    .getScale(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
        }
        Assert.assertTrue(true);
    }

    @Test
    public void TestResultIndexOutofBound() {
        try {
            this.logger.debug(BQForwardOnlyResultSetFunctionTest.Result.getBoolean(99));
        } catch (SQLException e) {
            Assert.assertTrue(true);
            this.logger.error("SQLexception" + e.toString());
        }
    }


    @Test
    public void TestResultSetFirst() {
        try {
//            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.first());
            Result.next();
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.isFirst());
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetgetBoolean() {
        try {
            Assert.assertTrue(Result.next());
            Assert.assertEquals(Boolean.parseBoolean("42"),
                    BQForwardOnlyResultSetFunctionTest.Result.getBoolean(2));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetgetFloat() {
        try {
            Assert.assertTrue(Result.next());
            Assert.assertEquals(new Float(42),
                    BQForwardOnlyResultSetFunctionTest.Result.getFloat(2));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetgetInteger() {
        try {
            Assert.assertTrue(Result.next());
            Assert.assertEquals(42, BQForwardOnlyResultSetFunctionTest.Result.getInt(2));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetgetRow() {

        try {
            Assert.assertTrue(Result.next());
            BQForwardOnlyResultSetFunctionTest.Result.getRow();
        } catch (SQLException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void TestResultSetgetString() {
        try {
            Assert.assertTrue(Result.next());
            Assert.assertEquals("you",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetNext() {
        try {
//            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.first());
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("yet",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("would",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("world",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("without",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("with",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("will",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("why",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("whose",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQForwardOnlyResultSetFunctionTest.Result.next());
            Assert.assertEquals("whom",
                    BQForwardOnlyResultSetFunctionTest.Result.getString(1));
            Assert.assertFalse(BQForwardOnlyResultSetFunctionTest.Result.next());
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertEquals("", BQForwardOnlyResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testResultSetTypesInGetString() throws SQLException, ParseException, java.io.IOException {
        final String sql = "SELECT " +
                "STRUCT(1 as a, 'hello' as b), " +
                "['a', 'b', 'c'], " +
                "[STRUCT(1 as a, 'hello' as b), STRUCT(2 as a, 'goodbye' as b)], " +
                "STRUCT(1 as a, ['an', 'array'] as b)," +
                "TIMESTAMP('2012-01-01 00:00:03') as t"
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

        HashMap hello = new HashMap(){{put("a", "1"); put("b", "hello");}};
        HashMap goodbye = new HashMap(){{put("a", "2"); put("b", "goodbye");}};

        Assert.assertEquals(hello, org.mortbay.util.ajax.JSON.parse(result.getString(1)));

        Assert.assertEquals("[\"a\",\"b\",\"c\"]", result.getString(2));

        Object[] arrayOfMapsActual = (Object []) org.mortbay.util.ajax.JSON.parse(result.getString(3));
        Assert.assertEquals(2, arrayOfMapsActual.length);
        Assert.assertEquals(hello, arrayOfMapsActual[0]);
        Assert.assertEquals(goodbye, arrayOfMapsActual[1]);

        HashMap<String, Object> mixedBagActual = (HashMap) org.mortbay.util.ajax.JSON.parse(result.getString(4));
        Assert.assertEquals(2, mixedBagActual.size());
        Assert.assertEquals("1", mixedBagActual.get("a"));
        Assert.assertEquals(org.mortbay.util.ajax.JSON.toString(new String[]{"an", "array"}), org.mortbay.util.ajax.JSON.toString(mixedBagActual.get("b")));

        Assert.assertEquals("2012-01-01 00:00:03", result.getString(5));
    }

    @Test
    public void testResultSetTypesInGetObject() throws SQLException, ParseException {
        final String sql = "SELECT " +
                "DATETIME('2012-01-01 00:00:02'), " +
                "TIMESTAMP('2012-01-01 00:00:03'), " +
                "CAST('2312412432423423334.234234234' AS NUMERIC), " +
                "CAST('2011-04-03' AS DATE)";

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
    }

}
