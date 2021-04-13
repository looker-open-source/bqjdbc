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

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This Junit test runs query with preparedstatement, while changing the number
 * of results
 *
 * @author Horváth Attila
 * @author Balazs Gunics
 *
 */
public class PreparedStatementTests {

    /** Static reference to the connection object */
    static Connection con = null;

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

    /**
     * Makes a new Bigquery Connection to Hardcoded URL and gives back the
     * Connection to static con member.
     */
    @Before
    public void Connect() throws Exception {
        try {
            Class.forName("net.starschema.clouddb.jdbc.BQDriver");
            PreparedStatementTests.con = DriverManager
                    .getConnection(
                            BQSupportFuncts
                                    .constructUrlFromPropertiesFile(BQSupportFuncts
                                            .readFromPropFile("src/test/resources/installedaccount1.properties")),
                            BQSupportFuncts
                                    .readFromPropFile("src/test/resources/installedaccount1.properties"));
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    /**
     * out of range test
     */
    @Test
    public void outOfRangeTest() {
        final String sql = "SELECT corpus, COUNT(word) as wc FROM publicdata:samples.shakespeare WHERE corpus = ? GROUP BY corpus ORDER BY wc DESC LIMIT ?";

        final String first = "othello";
        final String second = "macbeth";
        final int limit = 10;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setString(1, second);
            stm.setBigDecimal(2, BigDecimal.valueOf((double) limit));
            stm.setString(3, first);
        } catch (SQLException e) {
            Assert.assertTrue(true);
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    /**
     * This test run a Query without parameters as a preparedstatement
     */
    @Test
    public void ParameterlessTest() {
        final String sql = "SELECT TOP(word, 3), COUNT(*) FROM publicdata:samples.shakespeare";
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            java.sql.ResultSet theResult = stm.executeQuery();
            Assert.assertNotNull(theResult);
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    /**
     * This test ensures that getColumnType supports the following types: DOUBLE, BOOLEAN, BIGINT, VARCHAR, TIMESTAMP, DATE, DATETIME
     * Note: RECORD/STRUCT are not tested here because PreparedStatementsTests run queries on Legacy SQL
     */
    @Test
    public void ResultSetMetadataFunctionTestTypes() {
        final String[][] queries = new String[][] {
                {"SELECT 3.14", "3.14"},
                {"SELECT TRUE", "true"},
                {"SELECT 1", "1"},
                {"SELECT 'test'", "test"},
                {"SELECT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP)", null},
                {"SELECT CAST(CURRENT_DATE() AS DATE)", null},
                {"SELECT CAST('1' AS NUMERIC)", null},
                {"SELECT CAST('2021-04-09T20:24:39' AS DATETIME)", "2021-04-09T20:24:39"},
                {"SELECT CAST('1:23:45' AS TIME)", "01:23:45"},
                {"SELECT CAST('test' AS BYTES)", "dGVzdA=="},
                {"SELECT CAST('123' as BIGNUMERIC)", "123"}
        };

        final int[] expectedType = new int[]{
                java.sql.Types.DOUBLE,
                java.sql.Types.BOOLEAN,
                java.sql.Types.BIGINT,
                java.sql.Types.VARCHAR,
                java.sql.Types.TIMESTAMP,
                java.sql.Types.DATE,
                java.sql.Types.NUMERIC,
                java.sql.Types.TIMESTAMP,
                java.sql.Types.TIME,
                java.sql.Types.VARCHAR,
                java.sql.Types.NUMERIC
        };

        for (int i = 0; i < queries.length; i ++) {
            try {
                PreparedStatement stm = PreparedStatementTests.con
                        .prepareStatement(queries[i][0]);
                String expectedStringValue = queries[i][1];
                java.sql.ResultSet theResult = stm.executeQuery();

                Assert.assertNotNull(theResult);
                Assert.assertEquals("Expected type was not returned in metadata",
                        expectedType[i],
                        theResult.getMetaData().getColumnType(1));
                while(theResult.next()){
                    //should only be one row for each of these, but lets validate that we can read the object and string
                    Assert.assertNotNull(theResult.getObject(1));
                    if (expectedStringValue != null) {
                        Assert.assertEquals( expectedStringValue, theResult.getString(1));
                    } else {
                        theResult.getString(1);
                    }
                }
            } catch (SQLException e) {
                Assert.fail(e.toString());
            }
        }

        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    /**
     * setBigDecimal test
     */
    @Test
    public void setBigDecimalTest() {
        final String sql = "SELECT corpus, COUNT(word) as wc FROM publicdata:samples.shakespeare WHERE corpus = ? GROUP BY corpus ORDER BY wc DESC LIMIT ?";

        // final String first = "othello";
        final String second = "macbeth";
        final int limit = 10;
        int actual = 0;

        java.sql.ResultSet theResult = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setString(1, second);
            stm.setBigDecimal(2, BigDecimal.valueOf((double) limit));
            theResult = stm.executeQuery();
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }

            // Print out Column Values
            while (theResult.next()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    if (i == 0) {
                        Assert.assertEquals(second, theResult.getString(i + 1));
                    }
                    Line += String.format("%-32s", theResult.getString(i + 1));
                }
                actual++;
            }

        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertTrue(limit >= actual);

    }

    @Test
    public void SetByteTest() {
        final String sql = "SELECT TOP(word, ?), COUNT(*) FROM publicdata:samples.shakespeare";

        // SET HOW MANY RESULT YOU WISH
        Byte COUNT = new Byte("3");
        int Actual = 0;
        java.sql.ResultSet theResult = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setByte(1, COUNT);
            theResult = stm.executeQuery();
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            // Count Column Values
            while (theResult.next()) {
                Actual++;
            }
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;

        Byte Act = Byte.parseByte(Integer.toString(Actual));
        Assert.assertEquals(COUNT, Act);
    }

    @Test
    public void SetCharacterStreamTest() {
        String input = "lord";
        java.io.StringReader reader = new StringReader(input);
        final String sql = "SELECT corpus FROM publicdata:samples.shakespeare WHERE LOWER(word)=? GROUP BY corpus ORDER BY corpus DESC LIMIT 5;";
        String[][] expectation = new String[][]{{"winterstale", "various",
                "twogentlemenofverona", "twelfthnight", "troilusandcressida"}};
        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setCharacterStream(1, reader);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Assert.assertTrue(
                    "Comparing failed in the String[][] array",
                    this.comparer(expectation,
                            BQSupportMethods.GetQueryResult(Result)));
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    /**
     * setDate test
     */
    @Test
    public void setDateTest() {
        final String sql = "SELECT year, month, day, is_male, weight_pounds FROM publicdata:samples.natality"
                + " WHERE year = INTEGER(LEFT( ? ,4)) AND month = ? AND is_male = ? ORDER BY weight_pounds DESC LIMIT 1000";

        final String first = "1989-05-01";
        final boolean istrue = true;
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date date = null;
        try {
            date = formatter.parse(first);
        } catch (ParseException e2) {
            Assert.fail();
            e2.printStackTrace();
        }
        java.sql.ResultSet theResult = null;
        java.sql.Date firstdate = new java.sql.Date(date.getTime());
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setDate(1, firstdate);
            // stm.setDate(2, seconddate);
            stm.setInt(2, 5);
            stm.setBoolean(3, istrue);
            theResult = stm.executeQuery();
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }

            // Print out Column Values
            while (theResult.next()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    // if(i==0)
                    // Assert.assertEquals(String.valueOf(first),theResult.getString(i+1));
                    // if(i==1)
                    // Assert.assertEquals(String.valueOf(second),theResult.getString(i+1));
                    // if(i==3)
                    // Assert.assertEquals(String.valueOf(istrue),theResult.getString(i+1));
                    Line += String.format("%-15s", theResult.getString(i + 1));
                }
            }
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertTrue(true);

    }

    @Test
    public void SetDoubleTest() {
        final String sql = "SELECT corpus,COUNT(word)/? as countn FROM publicdata:samples.shakespeare Group by corpus having countn=?";
        double number = 1849.5;

        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setInt(1, 2);
            stm.setDouble(2, number);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Result.first();
        } catch (SQLException e) {
            try {
                Result.next();
            } catch (Exception ef) {

            }
        }
        try {
            Assert.assertTrue(
                    "The result was not as expected: " + Result.getString(1),
                    Result.getString(1).equals("tamingoftheshrew"));
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    @Test
    public void SetFloatTest() {
        final String sql = "SELECT corpus,COUNT(word)/? as countn FROM publicdata:samples.shakespeare Group by corpus having countn=?";
        float number = 1849.5f;

        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setInt(1, 2);
            stm.setFloat(2, number);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Result.first();
        } catch (SQLException e) {
            try {
                Result.next();
            } catch (SQLException e1) {
            }
        }
        try {
            Assert.assertTrue(
                    "The result was not as expected: " + Result.getString(1),
                    Result.getString(1).equals("tamingoftheshrew"));
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    /**
     * This test sets the first parameter of the preparedstatement to 3, which
     * mainly limits the rows we get back with the query
     */
    @Test
    public void setIntTest() {
        final String sql = "SELECT TOP(word, ?), COUNT(*) FROM publicdata:samples.shakespeare";

        // SET HOW MANY RESULT YOU WISH
        int COUNT = 3;
        int Actual = 0;
        java.sql.ResultSet theResult = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setInt(1, COUNT);
            theResult = stm.executeQuery();
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }

            // Print out Column Values
            while (theResult.next()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    Line += String.format("%-32s", theResult.getString(i + 1));
                }
                Actual++;
            }
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertEquals(COUNT, Actual);
    }

    @Test
    public void SetLongTest() {
        final String sql = "SELECT corpus,COUNT(word) as countn FROM publicdata:samples.shakespeare Group by corpus having countn>? limit 10";
        long number = 5200;

        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setLong(1, number);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        } catch (SQLException e) {
            // BIGQUERY Can only store long as a string
            Assert.assertTrue(e.toString().contains(
                    "Argument type mismatch in function GREATER"));
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }

    @Test
    public void SetObjectTest() {
        // TODO SetBooleanTest
    }

    @Test
    public void SetSQLXMLTest() {
        // TODO SetBooleanTest
    }

    /**
     * First parameter will be a string, second an int
     */
    @Test
    public void setStringTest() {
        final String sql = "SELECT corpus, COUNT(word) as wc FROM publicdata:samples.shakespeare WHERE corpus = ? GROUP BY corpus ORDER BY wc DESC LIMIT ?";

        final String first = "othello";
        final String second = "macbeth";
        final int limit = 10;
        int actual = 0;

        java.sql.ResultSet theResult = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setString(1, first);
            stm.setString(1, second);
            stm.setInt(2, limit);
            theResult = stm.executeQuery();
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }

            // Print out Column Values
            while (theResult.next()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    if (i == 0) {
                        Assert.assertEquals(second, theResult.getString(i + 1));
                    }
                    Line += String.format("%-32s", theResult.getString(i + 1));
                }
                actual++;
            }
        } catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertTrue(limit >= actual);

    }

    @Test
    public void SetTimeStampTest() {
        // TODO SetBooleanTest
    }

    @Test
    public void SetTimeTest() {
        // TODO SetBooleanTest
    }

    @Test
    public void SetUnicodeStreamTest() {
        // TODO SetBooleanTest
    }

    @Test
    public void SetURLTest() {
        // TODO SetBooleanTest
    }

}
