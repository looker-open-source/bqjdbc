/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * This Junit test runs query with preparedstatement after changing parameters
 * 
 * @author Horváth Attila
 */

package BQJDBC.QueryResultTest;

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

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.BQSupportMethods;

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
                                    .ConstructUrlFromPropertiesFile(BQSupportFuncts
                                            .ReadFromPropFile("installedaccount.properties")),
                            BQSupportFuncts
                                    .ReadFromPropFile("installedaccount.properties"));
        }
        catch (Exception e) {
            e.printStackTrace();
            
        }
    }
    
    /**
     * out of range test
     */
    @Test
    public void outOfRangeTest() {
        final String sql = "SELECT corpus, COUNT(word) as wc FROM publicdata:samples.shakespeare WHERE corpus = ? GROUP BY corpus ORDER BY wc DESC LIMIT ?";
        System.out.println("Test number: 01");
        System.out.println("Running query:" + sql);
        
        final String first = "othello";
        final String second = "macbeth";
        final int limit = 10;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setString(1, second);
            stm.setBigDecimal(2, BigDecimal.valueOf((double) limit));
            stm.setString(3, first);
        }
        catch (SQLException e) {
            Assert.assertTrue(true);
        }
        try {
            con.close();
        }
        catch (SQLException e) {
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
        System.out.println("Test number: 00");
        System.out.println("Running query:" + sql);
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            java.sql.ResultSet theResult = stm.executeQuery();
            Assert.assertNotNull(theResult);
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
    }
    
    /**
     * setBigDecimal test
     */
    @Test
    public void setBigDecimalTest() {
        final String sql = "SELECT corpus, COUNT(word) as wc FROM publicdata:samples.shakespeare WHERE corpus = ? GROUP BY corpus ORDER BY wc DESC LIMIT ?";
        System.out.println("Test number: 01");
        System.out.println("Running query:" + sql);
        
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
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            theResult.first();
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }
            System.out.println(Line + "\n");
            
            // Print out Column Values
            while (!theResult.isAfterLast()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    if (i == 0) {
                        Assert.assertEquals(second, theResult.getString(i + 1));
                    }
                    Line += String.format("%-32s", theResult.getString(i + 1));
                }
                System.out.println(Line);
                theResult.next();
                actual++;
            }
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertTrue(limit >= actual);
        
    }
    
    @Test
    public void SetByteTest() {
        final String sql = "SELECT TOP(word, ?), COUNT(*) FROM publicdata:samples.shakespeare";
        System.out.println("Test SetByteTest");
        System.out.println("Running query:" + sql);
        
        // SET HOW MANY RESULT YOU WISH
        Byte COUNT = new Byte("3");
        int Actual = 0;
        java.sql.ResultSet theResult = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setByte(1, COUNT);
            theResult = stm.executeQuery();
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            theResult.first();
            // Count Column Values
            while (!theResult.isAfterLast()) {
                theResult.next();
                Actual++;
            }
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
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
        String[][] expectation = new String[][] { { "winterstale", "various",
                "twogentlemenofverona", "twelfthnight", "troilusandcressida" } };
        System.out.println("Test SetCharacterStreamTest");
        System.out.println("Running query:" + sql);
        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setCharacterStream(1, reader);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Assert.assertTrue(
                    "Comparing failed in the String[][] array",
                    this.comparer(expectation,
                            BQSupportMethods.GetQueryResult(Result)));
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
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
        System.out.println("Test setDateTest");
        System.out.println("Running query:" + sql);
        
        final String first = "1989-05-01";
        final boolean istrue = true;
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date date = null;
        // System.out.println(String.valueOf(date.getTime()));
        try {
            date = formatter.parse(first);
        }
        catch (ParseException e2) {
            Assert.fail();
            e2.printStackTrace();
        }
        System.out.println(String.valueOf(date.getTime()));
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
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            theResult.first();
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }
            System.out.println(Line + "\n");
            
            // Print out Column Values
            while (!theResult.isAfterLast()) {
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
                System.out.println(Line);
                theResult.next();
            }
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertTrue(true);
        
    }
    
    @Test
    public void SetDoubleTest() {
        final String sql = "SELECT corpus,COUNT(word)/? as countn FROM publicdata:samples.shakespeare Group by corpus having countn=?";
        double number = 1849.5;
        
        System.out.println("Test SetDoubleTest");
        System.out.println("Running query:" + sql);
        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setInt(1, 2);
            stm.setDouble(2, number);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Result.first();
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Assert.assertTrue(
                    "The result was not as expected: " + Result.getString(1),
                    Result.getString(1).equals("tamingoftheshrew"));
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
    }
    
    @Test
    public void SetFloatTest() {
        final String sql = "SELECT corpus,COUNT(word)/? as countn FROM publicdata:samples.shakespeare Group by corpus having countn=?";
        float number = 1849.5f;
        
        System.out.println("Test SetFloatTest");
        System.out.println("Running query:" + sql);
        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setInt(1, 2);
            stm.setFloat(2, number);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Result.first();
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            Assert.assertTrue(
                    "The result was not as expected: " + Result.getString(1),
                    Result.getString(1).equals("tamingoftheshrew"));
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
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
        System.out.println("Test number: 01");
        System.out.println("Running query:" + sql);
        
        // SET HOW MANY RESULT YOU WISH
        int COUNT = 3;
        int Actual = 0;
        java.sql.ResultSet theResult = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setInt(1, COUNT);
            theResult = stm.executeQuery();
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            theResult.first();
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }
            System.out.println(Line + "\n");
            
            // Print out Column Values
            while (!theResult.isAfterLast()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    Line += String.format("%-32s", theResult.getString(i + 1));
                }
                System.out.println(Line);
                theResult.next();
                
                Actual++;
            }
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        con = null;
        Assert.assertEquals(COUNT, Actual);
    }
    
    @Test
    public void SetLongTest() {
        final String sql = "SELECT corpus,COUNT(word) as countn FROM publicdata:samples.shakespeare Group by corpus having countn>? limit 10";
        long number = 5200;
        
        System.out.println("Test SetLongTest");
        System.out.println("Running query:" + sql);
        java.sql.ResultSet Result = null;
        try {
            PreparedStatement stm = PreparedStatementTests.con
                    .prepareStatement(sql);
            stm.setLong(1, number);
            Result = stm.executeQuery();
            Assert.assertNotNull(Result);
        }
        catch (SQLException e) {
            // BIGQUERY Can only store long as a string
            Assert.assertTrue(e.toString().contains(
                    "Argument type mismatch in function GREATER"));
        }
        try {
            con.close();
        }
        catch (SQLException e) {
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
        System.out.println("Test number: 01");
        System.out.println("Running query:" + sql);
        
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
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            theResult.first();
            ResultSetMetaData metadata = theResult.getMetaData();
            int ColumnCount = metadata.getColumnCount();
            // Print Out Column Names
            String Line = "";
            for (int i = 0; i < ColumnCount; i++) {
                Line += String.format("%-32s", metadata.getColumnName(i + 1));
            }
            System.out.println(Line + "\n");
            
            // Print out Column Values
            while (!theResult.isAfterLast()) {
                Line = "";
                for (int i = 0; i < ColumnCount; i++) {
                    if (i == 0) {
                        Assert.assertEquals(second, theResult.getString(i + 1));
                    }
                    Line += String.format("%-32s", theResult.getString(i + 1));
                }
                System.out.println(Line);
                theResult.next();
                actual++;
            }
        }
        catch (SQLException e) {
            Assert.fail(e.toString());
        }
        try {
            con.close();
        }
        catch (SQLException e) {
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
