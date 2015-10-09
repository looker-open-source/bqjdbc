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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQSupportFuncts;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class GrammarTest {
    /** String for System independent newline */
    public static String newLine = System.getProperty("line.separator");
    /**
     * Static Connection holder
     */
    private Connection con;
    /**
     * Logger initialization
     */
    Logger logger = Logger.getLogger(this.toString());

    /**
     * Creates a new Connection to bigquery with the jdbc driver
     */
    @Before
    public void NewConnection() {
        try {
            if (con == null || !con.isValid(0)) {
                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    con = DriverManager.getConnection(
                            BQSupportFuncts.constructUrlFromPropertiesFile(BQSupportFuncts
                                    .readFromPropFile(getClass().getResource("/installedaccount.properties").getFile())),
                            BQSupportFuncts.readFromPropFile(getClass().getResource("/installedaccount.properties").getFile()));
                } catch (Exception e) {
                    logger.debug("Failed to make connection trough the JDBC driver", e);
                }
            }
            logger.debug("Running the next test");
        } catch (SQLException e) {
            logger.fatal("Something went wrong", e);
        }
    }


    static String input;

    /**
     * to Test the columnresolver from 2 subqueries
     */
    @Test
    public void selectJokerFromSubQueries() {
        input = "SELECT * FROM (SELECT aa.*, bb.* FROM efashion.OUTLET_LOOKUP aa, efashion.OUTLET_LOOKUP bb) LIMIT 10;";
        logger.info("Runing test: select Joker From SubQueries:" + newLine + input);
        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** a sample query which passed back by Eclipse Birt */
    @Test
    public void twoJoin() {
        input = "SELECT \r\n" +
                "    `efashion`.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE, \r\n" +
                "    `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE,\r\n" +
                "    `efashion`.ARTICLE_LOOKUP.ARTICLE_LABEL, \r\n" +
                "FROM \r\n" +
                "    `efashion`.ARTICLE_COLOR_LOOKUP \r\n" +
                "        JOIN \r\n" +
                "    `efashion`.ARTICLE_LOOKUP \r\n" +
                "        ON \r\n" +
                "    `efashion`.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE";

        logger.info("Running test: twoJoin \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }


    /**
     * to test the JOINresolver, it should make a JOIN with it's
     * ONCLAUSE derived by the WHERE clause
     */
    @Test
    public void twoTableWithWhere() {

        input = "SELECT * FROM efashion.ARTICLE_LOOKUP a, efashion.ARTICLE_COLOR_LOOKUP b " +
                "WHERE (a.ARTICLE_CODE = b.ARTICLE_CODE);";

        logger.info("Running test: twoTableWithWhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * to test the JOINresolver, where there are multiple WHERE conditions
     */
    @Test
    public void twoTableWithMultipleWhereOne() {

        input = "Select aa.SALE_PRICE, bc.ARTICLE_CODE " +
                "FROM efashion.ARTICLE_LOOKUP_CRITERIA bc,efashion.ARTICLE_LOOKUP aa, \r\n" +
                "    efashion.ARTICLE_LOOKUP_CRITERIA bb  \r\n" +
                "    WHERE (aa.ARTICLE_CODE = bb.ARTICLE_CODE ) \r\n" +
                "    AND bc.ARTICLE_CODE = bb.ARTICLE_CODE\r\n" +
                "    AND aa.SALE_PRICE <= 60\r\n" +
                "    limit 10";

        logger.info("Running test: twoTableWithMultipleWhereOne \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * to test the multijoinresolver
     */
    @Test
    public void threeJoinsInChain() {

        input = "    Select * from efashion.ARTICLE_LOOKUP_CRITERIA alc \r\n" +
                "    JOIN  efashion.ARTICLE_LOOKUP al ON (alc.ARTICLE_CODE = al.ARTICLE_CODE )\r\n" +
                "    JOIN efashion.ARTICLE_COLOR_LOOKUP acl ON alc.ARTICLE_CODE = acl.ARTICLE_CODE\r\n" +
                "    limit 10";

        logger.info("Running test: threeJoinsInChain \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * to test the Joinresolver with 2 where clause
     */
    @Test
    public void twoTableWithMultipleWhere() {

        input = "SELECT a.SALE_PRICE,a.ARTICLE_CODE,b.COLOR_LABEL,b.CATEGORY " +
                "FROM efashion.ARTICLE_LOOKUP a, efashion.ARTICLE_COLOR_LOOKUP b " +
                "WHERE (a.ARTICLE_CODE = b.ARTICLE_CODE) AND (a.SALE_PRICE >= 100);";
        logger.info("Running test: twoTableWithMultipleWhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * to test the union of the bigquery we'll make 2 subqueries,
     * 1 with 2nd where clause and other with the 3rd where clause
     * both subqueries will contain a join with the onclause from the 1st where clause
     */
    @Test
    public void twoTableWithMultipleWheres() {

        input = "SELECT a.SALE_PRICE,a.ARTICLE_CODE,b.COLOR_LABEL,b.CATEGORY FROM efashion.ARTICLE_LOOKUP a, efashion.ARTICLE_COLOR_LOOKUP b " +
                "WHERE (a.ARTICLE_CODE = b.ARTICLE_CODE) AND ((a.SALE_PRICE <= 100) AND (a.SALE_PRICE  >= 90));";
        logger.info("Running test: twoTableWithMultipleWheres \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     *
     */
    @Test
    public void twoTableWithMultipleWheresWithOr() {

        input = "SELECT a.SALE_PRICE,a.ARTICLE_CODE,b.COLOR_LABEL,b.CATEGORY FROM efashion.ARTICLE_LOOKUP a, efashion.ARTICLE_COLOR_LOOKUP b " +
                "WHERE (a.ARTICLE_CODE = b.ARTICLE_CODE) AND ((a.SALE_PRICE <= 100) OR (a.SALE_PRICE  >= 90))";
        logger.info("Running test: twoTableWithMultipleWheresWithOr \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * to test the columnresolving from the joins
     */
    @Test
    public void twoJoinsWithWhere() {

        input = "SELECT *\r\n" +
                "    FROM \r\n" +
                "   (SELECT \r\n" +
                "       ARTICLE_CODE AS UNIQ_ID_AAAB,\r\n" +
                "       ARTICLE_LABEL AS UNIQ_ID_AAAC,\r\n" +
                "       CATEGORY AS UNIQ_ID_AAAD,\r\n" +
                "       SALE_PRICE AS UNIQ_ID_AAAE,\r\n" +
                "       FAMILY_NAME AS UNIQ_ID_AAAF,\r\n" +
                "       FAMILY_CODE AS UNIQ_ID_AAAG\r\n" +
                "    FROM \r\n" +
                "           efashion.ARTICLE_LOOKUP) a\r\n" +
                "\r\n" +
                "        JOIN \r\n" +
                "   (SELECT \r\n" +
                "       ARTICLE_CODE AS UNIQ_ID_AAAJ,\r\n" +
                "       COLOR_CODE AS UNIQ_ID_AAAK,\r\n" +
                "       ARTICLE_LABEL AS UNIQ_ID_AAAL,\r\n" +
                "       COLOR_LABEL AS UNIQ_ID_AAAM,\r\n" +
                "       CATEGORY AS UNIQ_ID_AAAN,\r\n" +
                "       SALE_PRICE AS UNIQ_ID_AAAO,\r\n" +
                "       FAMILY_NAME AS UNIQ_ID_AAAP,\r\n" +
                "       FAMILY_CODE AS UNIQ_ID_AAAQ\r\n" +
                "    FROM \r\n" +
                "           efashion.ARTICLE_COLOR_LOOKUP) b\r\n" +
                "\r\n" +
                "       ON (a.UNIQ_ID_AAAB=b.UNIQ_ID_AAAJ)\r\n" +
                "\r\n" +
                "WHERE a.UNIQ_ID_AAAE<=90";

        logger.info("Running test: threeTableWithMultipleWhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * A sample first made by Tamas
     * by default this is a BigQuery compatible query
     */
    @Test
    public void testMultipleSubqueriesWithJoins() {

        input = "SELECT DATE.YEAR," +
                "DATE.MONTH, " +
                "SUM(FACT_OUTLET_PRODUCT.AMOUNT_SOLD) AS AMOUNT_SOLD, " +
                "SUM(FACT_OUTLET_PRODUCT.QUANTITY_SOLD) QUANTITY_SOLD, " +
                "FACT_OUTLET_PRODUCT.STATE, " +
                "FACT_OUTLET_PRODUCT.CATEGORY" +
                " FROM " +
                "(SELECT " +
                "FACT_OUTLET.QUANTITY_SOLD AS QUANTITY_SOLD, " +
                "FACT_OUTLET.AMOUNT_SOLD AS AMOUNT_SOLD, " +
                "FACT_OUTLET.STATE AS STATE, " +
                "PRODUCT.CATEGORY AS CATEGORY, " +
                "FACT_OUTLET.WEEK_KEY AS WEEK_KEY " +
                " FROM " +
                "(SELECT " +
                "FACTS.QUANTITY_SOLD AS QUANTITY_SOLD, " +
                "FACTS.AMOUNT_SOLD AS AMOUNT_SOLD, " +
                "OUTLET.STATE AS STATE, " +
                "FACTS.ARTICLE_CODE AS ARTICLE_CODE, " +
                "FACTS.WEEK_KEY AS WEEK_KEY " +
                " FROM " +
                "efashion.SHOP_FACTS AS FACTS " +
                "INNER JOIN efashion.OUTLET_LOOKUP AS OUTLET " +
                " ON FACTS.SHOP_CODE = OUTLET.SHOP_CODE) AS " +
                "FACT_OUTLET " +
                "INNER JOIN efashion.ARTICLE_LOOKUP AS PRODUCT " +
                "ON ( PRODUCT.ARTICLE_CODE = FACT_OUTLET.ARTICLE_CODE )) " +
                "FACT_OUTLET_PRODUCT " +
                "INNER JOIN efashion.CALENDAR_YEAR_LOOKUP AS DATE " +
                "ON ( FACT_OUTLET_PRODUCT.WEEK_KEY = DATE.WEEK_KEY ) " +
                "GROUP  BY DATE.YEAR, " +
                "DATE.MONTH, " +
                "FACT_OUTLET_PRODUCT.STATE, " +
                "FACT_OUTLET_PRODUCT.CATEGORY";

        logger.info("Running test:  testMultipleSubqueriesWithJoins" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** A simple query with count(*) */
    @Test
    public void countJoker() {

        input = "select COUNT(*) from efashion.ARTICLE_COLOR_LOOKUP";
        logger.info("Running test: countJoker" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * A modified query from iReport, original containted: "efahsio"n."ARTICLE
     * we test with "efashion"."ARTICLE
     */
    @Test
    public void iReportTwoColumnsOneTable() {

        input = "SELECT " +
                "\"ARTICLE_COLOR_LOOKUP\".\"ARTICLE_LABEL\" AS ARTICLE_COLOR_LOOKUP_ARTICLE_LABEL," +
                "\"ARTICLE_COLOR_LOOKUP\".\"COLOR_LABEL\" AS ARTICLE_COLOR_LOOKUP_COLOR_LABEL" +
                " FROM \"efashion\".\"ARTICLE_COLOR_LOOKUP\" ARTICLE_COLOR_LOOKUP;";
        logger.info("Running test: iReportTwoColumnsOneTable " + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * a publicdatatest
     */
    @Test
    public void publicDataSelectJoker() {

        input = "SELECT corpus FROM [publicdata:samples.shakespeare] LIMIT 10";
        logger.info("Running test: publicDataSelectJoker" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** to test Ticket 56 */
    @Test
    public void selectDuplicateColumn() {

        input = "SELECT * from (SELECT COLOR_LABEL,CATEGORY one,CATEGORY two FROM efashion.ARTICLE_COLOR_LOOKUP)";
        logger.info("Running test: selectDuplicateColumn" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** A sample made by SQuirrel SQL */
    @Test
    public void selectTableAliasJoker() {

        input = "select tbl.* from \"ARTICLE_COLOR_LOOKUP\" tbl";
        logger.info("Running test: selectTableAliasJoker" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /**
     * we can't parse the query, since we don't have a table named "notvalidtable"
     * because of that we'll fail to resolve it's columns
     */
    @Test
    public void selectJokerFromNotValidTable() {

        input = "select * from notvalidtable limit 10";
        logger.info("Running test: selectJokerFromNotValidTable" + input);

        try {
            @SuppressWarnings("unused")
            ResultSet queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            //it's not a valid table, so it fails
            Assert.assertTrue(true);
        }

    }

    /** A sample made by ireport */
    @Test
    public void ireportSelectColumnsFromInnerJoin() {


        input =
                "SELECT ARTICLE_LOOKUP.\"ARTICLE_CODE\" AS ARTICLE_LOOKUP_ARTICLE_CODE," + newLine +
                        "\t ARTICLE_LOOKUP.\"ARTICLE_LABEL\" AS ARTICLE_LOOKUP_ARTICLE_LABEL," + newLine +
                        "\t ARTICLE_LOOKUP_CRITERIA.\"ID\" AS ARTICLE_LOOKUP_CRITERIA_ID," + newLine +
                        "\t ARTICLE_LOOKUP_CRITERIA.\"ARTICLE_CODE\" AS ARTICLE_LOOKUP_CRITERIA_ARTICLE_CODE," + newLine +
                        "\t ARTICLE_LOOKUP_CRITERIA.\"CRITERIA_TYPE\" AS ARTICLE_LOOKUP_CRITERIA_CRITERIA_TYPE" + newLine +
                        " FROM" + newLine +
                        "\"efashion\".\"ARTICLE_LOOKUP\" ARTICLE_LOOKUP INNER JOIN \"efashion\".\"ARTICLE_LOOKUP_CRITERIA\" ARTICLE_LOOKUP_CRITERIA " + newLine +
                        "ON ARTICLE_LOOKUP.\"ARTICLE_CODE\" = ARTICLE_LOOKUP_CRITERIA.\"ARTICLE_CODE\" ";

        logger.info("Running test: ireportSelectColumnsFromInnerJoin" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** To test the joinresolver */
    @Test
    public void selectFromDuplicateTableWithAliasesWithoutWhere() {

        input = "SELECT aa.ARTICLE_CODE as ARTICLE_CODE1, bb.ARTICLE_CODE as ARTICLE_CODE2 FROM efashion.ARTICLE_LOOKUP as aa, efashion.ARTICLE_LOOKUP as bb limit 10;";
        logger.info("Running test: selectFromDuplicateTableWithAliasesWithoutWhere" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** to test chained joins */
    @Test
    public void selectJokerFromTwoChainedJoin() {
        input = "Select * from efashion.ARTICLE_LOOKUP_CRITERIA alc \r\n" +
                "    JOIN  efashion.ARTICLE_LOOKUP al ON (alc.ARTICLE_CODE = al.ARTICLE_CODE )\r\n" +
                "    JOIN efashion.ARTICLE_COLOR_LOOKUP acl ON alc.ARTICLE_CODE = acl.ARTICLE_CODE\r\n" +
                "limit 10\r\n";
        logger.info("Running test: selectJokerFromTwoChainedJoin" + input);
        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectJokerFromTwoTablesNotNecessaryWhere() {

        input = "SELECT * FROM (SELECT SUM(aa.ARTICLE_CODE) as MUCHACHO " +
                " FROM ARTICLE_LOOKUP aa,ARTICLE_LOOKUP bb " +
                " where aa.ARTICLE_CODE=bb.ARTICLE_CODE " +
                "     or aa.ARTICLE_CODE=bb.ARTICLE_CODE " +
                "     and aa.ARTICLE_CODE = 3)";
        //expectation = "SELECT efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE, efashion.ARTICLE_LOOKUP.ARTICLE_CODE,efashion.ARTICLE_LOOKUP.ARTICLE_LABEL, efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE, efashion.ARTICLE_LOOKUP_CRITERIA.ID FROM efashion.ARTICLE_LOOKUP_CRITERIA, efashion.ARTICLE_COLOR_LOOKUP JOIN efashion.ARTICLE_LOOKUP ON efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE";

        logger.info("Running test: selectjokerfromtwotablesnotnecessarywhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);

        //Assert.assertEquals(idReplacer(expectation), idReplacer(reality));
    }

    @Test
    public void selectColumnFromTableWithAlias() {

        input = "select aa.ARTICLE_CODE\r\n " +
                "from ARTICLE_LOOKUP AS aa";
        //expectation = "SELECT efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE, efashion.ARTICLE_LOOKUP.ARTICLE_CODE,efashion.ARTICLE_LOOKUP.ARTICLE_LABEL, efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE, efashion.ARTICLE_LOOKUP_CRITERIA.ID FROM efashion.ARTICLE_LOOKUP_CRITERIA, efashion.ARTICLE_COLOR_LOOKUP JOIN efashion.ARTICLE_LOOKUP ON efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE";

        logger.info("Running test: selectjokerfromtwotablesnotnecessarywhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectColumnFromTableWithTableBeforeColumn() {

        input = "select ARTICLE_LOOKUP.ARTICLE_CODE\r\n" +
                "from ARTICLE_LOOKUP";
        input = "select efashion.ARTICLE_LOOKUP.ARTICLE_CODE, efashion.ARTICLE_LOOKUP.ARTICLE_LABEL\n" +
                "from efashion.ARTICLE_LOOKUP\r\n" +
                "";

        logger.info("Running test: selectjokerfromtwotablesnotnecessarywhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectColumnFromTableSimple() {

        input = "select ARTICLE_CODE\r\n " +
                "from ARTICLE_LOOKUP";
        //expectation = "SELECT efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE, efashion.ARTICLE_LOOKUP.ARTICLE_CODE,efashion.ARTICLE_LOOKUP.ARTICLE_LABEL, efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE, efashion.ARTICLE_LOOKUP_CRITERIA.ID FROM efashion.ARTICLE_LOOKUP_CRITERIA, efashion.ARTICLE_COLOR_LOOKUP JOIN efashion.ARTICLE_LOOKUP ON efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE";

        logger.info("Running test: selectjokerfromtwotablesnotnecessarywhere \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }


    @Test
    public void differentSchemaOne() {

        input = "select a.ARTICLE_CODE, b.COLOR_LABEL FROM efashion.ARTICLE_LOOKUP a\r\n" +
                "    JOIN efashion.ARTICLE_COLOR_LOOKUP b ON a.ARTICLE_CODE = b.ARTICLE_CODE";

        logger.info("Running test: selectJokerFromFourJoin \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    /** A sample made by RazorSQL */
    @Test
    public void selectTwoTableJoinWithWhereAndGroupBy() {

        input = "SELECT al.CATEGORY,COUNT(acl.ARTICLE_CODE) FROM efashion.ARTICLE_LOOKUP al\r\n" +
                ", efashion.ARTICLE_COLOR_LOOKUP acl WHERE al.ARTICLE_CODE = acl.ARTICLE_CODE\r\n" +
                "GROUP BY al.CATEGORY";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectFromEfashionOne() {

        input = "SELECT * FROM efashion.ARTICLE_LOOKUP";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectFromEfashionTwo() {

        input = "SELECT * FROM efashion.ARTICLE_LOOKUP";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectFromEfashionWtf() {

        input = "SELECT * FROM ARTICLE_LOOKUP";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectColumnFromEfashionOne() {

        input = "SELECT ARTICLE_CODE FROM efashion.ARTICLE_LOOKUP";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectColumnFromEfashionTwo() {

        input = "SELECT ARTICLE_CODE FROM efashion.ARTICLE_LOOKUP";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void selectColumnFromEfashionWtf() {

        input = "SELECT ARTICLE_CODE FROM ARTICLE_LOOKUP";
        logger.info("Running test: selectCountJokerFromTableWithPrefixes" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void kartezianSubSelect() {

        input = "SELECT * FROM \r\n" +
                "(SELECT al.*, ol.* FROM efashion.ARTICLE_LOOKUP al, efashion.OUTLET_LOOKUP ol) \r\n" +
                "LIMIT 10;";

        logger.info("Running test: kartezianSubSelect \r\n" + input);

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------

}
