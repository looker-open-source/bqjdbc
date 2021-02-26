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

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Junit test tests functions in BQResultset
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQScrollableResultSetFunctionTest {

    private static java.sql.Connection con = null;
    private static java.sql.ResultSet Result = null;

    Logger logger = LoggerFactory.getLogger(BQScrollableResultSetFunctionTest.class);

    @Test
    public void ChainedCursorFunctionTest() {
        this.logger.info("ChainedFunctionTest");
        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));

        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(10));
            Assert.assertEquals("whom",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.next());
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            } else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }

        }

        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isFirst());
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.previous());
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isAfterLast());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(-1));
            Assert.assertEquals("whom",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.relative(-5));
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.relative(6));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertEquals("without",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
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

    /**
     * For testing isValid() , Close() , isClosed()
     */
    @Test
    public void isClosedValidtest() {
        try {
            Assert.assertEquals(true, BQScrollableResultSetFunctionTest.con.isValid(0));
        } catch (SQLException e) {
            Assert.fail("Got an exception" + e.toString());
            e.printStackTrace();
        }
        try {
            Assert.assertEquals(true, BQScrollableResultSetFunctionTest.con.isValid(10));
        } catch (SQLException e) {
            Assert.fail("Got an exception" + e.toString());
            e.printStackTrace();
        }
        try {
            BQScrollableResultSetFunctionTest.con.isValid(-10);
        } catch (SQLException e) {
            Assert.assertTrue(true);
            // e.printStackTrace();
        }

        try {
            BQScrollableResultSetFunctionTest.con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.con.isClosed());
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        try {
            BQScrollableResultSetFunctionTest.con.isValid(0);
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

        try {
            if (BQScrollableResultSetFunctionTest.con == null
                    || !BQScrollableResultSetFunctionTest.con.isValid(0)) {
                this.logger.info("Testing the JDBC driver");
                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    String jdbcUrl = BQSupportFuncts
                            .constructUrlFromPropertiesFile(BQSupportFuncts
                                    .readFromPropFile(getClass().getResource("/installedaccount1.properties").getFile()));
                    jdbcUrl += "&useLegacySql=true";
                    BQScrollableResultSetFunctionTest.con = DriverManager
                            .getConnection(jdbcUrl,
                                    BQSupportFuncts
                                            .readFromPropFile(getClass().getResource("/installedaccount1.properties").getFile()));
                } catch (Exception e) {
                    e.printStackTrace();
                    this.logger.error("Error in connection" + e.toString());
                    Assert.fail("General Exception:" + e.toString());
                }
                this.logger.info(((BQConnection) BQScrollableResultSetFunctionTest.con)
                        .getURLPART());
            }
        } catch (SQLException e) {
            logger.debug("Oops something went wrong", e);
        }
        this.QueryLoad();
    }

    // Comprehensive Tests:

    public void QueryLoad() {
        final String sql = "SELECT TOP(word,10) AS word, COUNT(*) as count FROM publicdata:samples.shakespeare";
        final String description = "The top 10 word from shakespeare #TOP #COUNT";
        String[][] expectation = new String[][]{
                { "you", "yet", "would", "world", "without", "with", "will", "why", "whose", "whom" },
                { "42", "42", "42", "42", "42", "42", "42", "42", "42", "42" } };
        this.logger.info("Test number: 01");
        this.logger.info("Running query:" + sql);

        try {
            Statement stmt = BQScrollableResultSetFunctionTest.con
                    .createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            stmt.setQueryTimeout(500);
            BQScrollableResultSetFunctionTest.Result = stmt.executeQuery(sql);
        } catch (SQLException e) {
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
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail(e.toString());
        }
    }

    @Test
    public void ResultSetMetadata() {
        try {
            this.logger.debug(BQScrollableResultSetFunctionTest.Result.getMetaData()
                    .getSchemaName(1));
            this.logger.debug("{}", BQScrollableResultSetFunctionTest.Result.getMetaData()
                    .getScale(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
        }
        Assert.assertTrue(true);
    }

    @Test
    public void TestResultIndexOutofBound() {
        try {
            this.logger.debug("{}", BQScrollableResultSetFunctionTest.Result.getBoolean(99));
        } catch (SQLException e) {
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
            Assert.assertEquals("will",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(8));
            Assert.assertEquals("why",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(9));
            Assert.assertEquals("whose",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(10));
            Assert.assertEquals("whom",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.absolute(0));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            } else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }

        try {
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.absolute(11));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
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
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("whom",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
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
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("you",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
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
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isFirst());
        } catch (SQLException e) {
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
        } catch (SQLException e) {
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
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetgetInteger() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.absolute(1));
            Assert.assertEquals(42, BQScrollableResultSetFunctionTest.Result.getInt(2));
        } catch (SQLException e) {
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
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            BQScrollableResultSetFunctionTest.Result.beforeFirst();
            Assert.assertEquals(0, BQScrollableResultSetFunctionTest.Result.getRow());
            BQScrollableResultSetFunctionTest.Result.afterLast();
            Assert.assertEquals(0, BQScrollableResultSetFunctionTest.Result.getRow());
        } catch (SQLException e) {
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
            Assert.assertEquals("whom",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
    }

    @Test
    public void TestResultSetLast() {
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.isLast());
        } catch (SQLException e) {
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
            Assert.assertEquals("will",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("why",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("whose",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.next());
            Assert.assertEquals("whom",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.next());
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }

        try {
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
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
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("whose",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("why",
                    BQScrollableResultSetFunctionTest.Result.getString(1));
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.previous());
            Assert.assertEquals("will",
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
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
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
            Assert.assertEquals("whose",
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
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.first());
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.relative(-1));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            } else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }

        try {
            Assert.assertTrue(BQScrollableResultSetFunctionTest.Result.last());
            Assert.assertFalse(BQScrollableResultSetFunctionTest.Result.relative(1));
            Assert.assertEquals("", BQScrollableResultSetFunctionTest.Result.getString(1));
        } catch (SQLException e) {
            boolean ct = e.toString().contains(
                    "Cursor is not in a valid Position");
            if (ct == true) {
                Assert.assertTrue(ct);
            } else {
                this.logger.error("SQLexception" + e.toString());
                Assert.fail("SQLException" + e.toString());
            }
        }
    }

}
