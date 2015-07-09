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
 */
package BQJDBC.QueryResultTest;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;
//import net.starschema.clouddb.bqjdbc.logging.Logger;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.BQSupportMethods;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * This Junit test tests functions in BQResultset
 * 
 * @author Horv�th Attila
 * @author Gunics Balazs
 */
public class BQScrollableResultSetFunctionTest {
    
    private static java.sql.Connection con = null;
    private static java.sql.ResultSet Result = null;
    
    private final Log logger = LogFactory.getLog(getClass());
    
    @Test
    public void ChainedCursorFunctionTest() {
        this.logger.info("ChainedFunctionTest");
        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(10));
            Assert.assertEquals("word",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.next());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
            
        }
        
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isFirst());
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.previous());
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isAfterLast());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(-1));
            Assert.assertEquals("word",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(-5));
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.relative(6));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
        this.logger.info("chainedfunctiontest end");
    }
    
    @Test
    public void databaseMetaDataGetTables()
    {
        //clouddb,ARTICLE_LOOKUP,starschema.net,[Ljava.lang.String;@9e8424
        ResultSet result = null;
        try {
            //Function call getColumns 
            //catalog:null, 
            //schemaPattern: starschema_net__clouddb, 
            //tableNamePattern:OUTLET_LOOKUP, columnNamePattern: null
            //result = con.getMetaData().getTables("OUTLET_LOOKUP", null, "starschema_net__clouddb", null );
            result = con.getMetaData().getColumns(null, "starschema_net__clouddb", "OUTLET_LOOKUP", null);
            //Function call getTables(catalog: ARTICLE_COLOR_LOOKUP, schemaPattern: null, tableNamePattern: starschema_net__clouddb, types: TABLE , VIEW , SYSTEM TABLE , SYNONYM , ALIAS , )            
        }
        catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            Assert.assertTrue(result.first());
            while(!result.isAfterLast()){
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
        }
        catch (SQLException e) {
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
    
    /**
     * For testing isValid() , Close() , isClosed()
     */
    @Test
    public void isClosedValidtest() {
        try {
            Assert.assertEquals(true, BQScrollableResultSetFunctionTest.con.isValid(0));
        }
        catch (SQLException e) {
            Assert.fail("Got an exception" + e.toString());
            e.printStackTrace();
        }
        try {
            Assert.assertEquals(true, BQScrollableResultSetFunctionTest.con.isValid(10));
        }
        catch (SQLException e) {
            Assert.fail("Got an exception" + e.toString());
            e.printStackTrace();
        }
        try {
            BQScrollableResultSetFunctionTest.con.isValid(-10);
        }
        catch (SQLException e) {
            Assert.assertTrue(true);
            // e.printStackTrace();
        }
        
        try {
            BQScrollableResultSetFunctionTest.con.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.con.isClosed());
        }
        catch (SQLException e1) {
            e1.printStackTrace();
        }
        
        try {
            BQScrollableResultSetFunctionTest.con.isValid(0);
        }
        catch (SQLException e) {
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
        
        try {
            if (BQScrollableResultSetFunctionTest.con == null
                    || !BQScrollableResultSetFunctionTest.con.isValid(0)) {
                this.logger.info("Testing the JDBC driver");
                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    BQScrollableResultSetFunctionTest.con = DriverManager.getConnection(
                            BQSupportFuncts.constructUrlFromPropertiesFile(BQSupportFuncts
                                    .readFromPropFile("installedaccount1.properties")),
                            BQSupportFuncts.readFromPropFile("installedaccount1.properties"));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    this.logger.error("Error in connection" + e.toString());
                    Assert.fail("General Exception:" + e.toString());
                }
                this.logger.info(((BQConnection) BQScrollableResultSetFunctionTest.con)
                        .getURLPART());
            }
        }
        catch (SQLException e) {
            logger.debug("Oops something went wrong",e);
        }
        this.QueryLoad();
    }
    
    // Comprehensive Tests:       
    
    public void QueryLoad() {
        final String sql = "SELECT TOP(word,10) AS word, COUNT(*) as count FROM publicdata:samples.shakespeare";
        final String description = "The top 10 word from shakespeare #TOP #COUNT";
        String[][] expectation = new String[][] {
                {"you", "yet", "would", "world", "without", "with", "your", "young",
                    "words", "word"},
                { "42", "42", "42", "42", "42", "42", "41", "41", "41", "41" } };
                /** somehow the result changed with time
                { "you", "yet", "would", "world", "without", "with", "will",
                        "why", "whose", "whom" },
                { "42", "42", "42", "42", "42", "42", "42", "42", "42", "42" } };
                 */        
        this.logger.info("Test number: 01");
        this.logger.info("Running query:" + sql);
        
        try {
            //Statement stmt = BQResultSetFunctionTest.con.createStatement();
            Statement stmt = BQScrollableResultSetFunctionTest.con
                    .createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(500);
            BQScrollableResultSetFunctionTest.Result = stmt.executeQuery(sql);
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(BQScrollableResultSetFunctionTest.Result);
        
        this.logger.debug(description);
        HelperFunctions.printer(expectation);
        
        
        try {
            Assert.assertTrue("Comparing failed in the String[][] array", this
                    .comparer(expectation, BQSupportMethods
                            .GetQueryResult(BQScrollableResultSetFunctionTest.Result)));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail(e.toString());
        }
    }
    
    @Test
    public void ResultSetMetadata() {
        try {
            this.logger.debug(BQScrollableResultSetFunctionTest.Result.getMetaData()
                    .getSchemaName(1));
            this.logger.debug(BQScrollableResultSetFunctionTest.Result.getMetaData()
                    .getScale(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
        }
        Assert.assertTrue(true);
    }
    
    @Test
    public void TestResultIndexOutofBound() {
        try {
            this.logger.debug(BQScrollableResultSetFunctionTest.Result.getBoolean(99));
        }
        catch (SQLException e) {
            Assert.assertTrue(true);
            this.logger.error("SQLexception" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetAbsolute() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(2));
            Assert.assertEquals("yet",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(3));
            Assert.assertEquals("would",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(4));
            Assert.assertEquals("world",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(5));
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(6));
            Assert.assertEquals("with",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(7));
            Assert.assertEquals("your",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(8));
            Assert.assertEquals("young",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(9));
            Assert.assertEquals("words",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(10));
            Assert.assertEquals("word",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.absolute(0));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
        
        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.absolute(11));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }
    
    @Test
    public void TestResultSetAfterlast() {
        try {
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("word",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }
    
    @Test
    public void TestResultSetBeforeFirst() {
        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }
    
    @Test
    public void TestResultSetFirst() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isFirst());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetgetBoolean() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals(Boolean.parseBoolean("42"),
                    BQScrollableResultSetFunctionTest.Result.getBoolean(2));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetgetFloat() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals(new Float(42),
                    BQScrollableResultSetFunctionTest.Result.getFloat(2));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetgetInteger() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals(42, BQScrollableResultSetFunctionTest.Result.getInt(2));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetgetRow() {
        
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals(1, BQScrollableResultSetFunctionTest.Result.getRow());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(10));
            Assert.assertEquals(10, BQScrollableResultSetFunctionTest.Result.getRow());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertEquals(0, BQScrollableResultSetFunctionTest.Result.getRow());
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertEquals(0, BQScrollableResultSetFunctionTest.Result.getRow());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetgetString() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertEquals("word",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetLast() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isLast());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }
    
    @Test
    public void TestResultSetNext() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("yet",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("would",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("world",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("with",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("your",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("young",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("words",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("word",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.next());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        
        try {
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }
    
    @Test
    public void TestResultSetPrevious() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("words",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("young",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("your",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("with",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("world",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("would",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("yet",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.previous());
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }
    
    @Test
    public void TestResultSetRelative() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(1));
            Assert.assertEquals("yet",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(2));
            Assert.assertEquals("world",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(5));
            Assert.assertEquals("words",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(-5));
            Assert.assertEquals("world",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(-2));
            Assert.assertEquals("yet",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(-1));
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.relative(-1));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
        
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.relative(1));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        }
        catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            }
            else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }
    
}
