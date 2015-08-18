package BQJDBC.QueryResultTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQSupportFuncts;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class GrammarTestsToBeFixed {
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
                                    .readFromPropFile("installedaccount.properties")),
                            BQSupportFuncts.readFromPropFile("installedaccount.properties"));
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

    @Test
    public void testStringLiteral() {
        input = "SELECT * FROM efashion.ARTICLE_LOOKUP al " +
                " WHERE al.ARTICLE_LABEL = \"Pastel Colored Viscose Scarf\"";
        NewConnection();
        try {
            Statement myStatement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet myResult = myStatement
                    .executeQuery(input);
            HelperFunctions.printer(myResult);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testStringLiteralOnLeft() {
        input = "SELECT * FROM efashion.ARTICLE_LOOKUP al " +
                " WHERE \"Pastel Colored Viscose Scarf\" = al.ARTICLE_LABEL";
        NewConnection();
        try {
            Statement myStatement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet myResult = myStatement
                    .executeQuery(input);
            HelperFunctions.printer(myResult);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testIntegerOnLeft() {
        input = "SELECT * FROM efashion.ARTICLE_LOOKUP al " +
                " WHERE ARTICLE_CODE = 115121";
        NewConnection();
        try {
            Statement myStatement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet myResult = myStatement
                    .executeQuery(input);
            HelperFunctions.printer(myResult);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        Assert.assertTrue(true);
    }

    /**
     * To reproduce the error in BiRT
     */
    @Test
    public void birtTableDotColumnFromTable() {

        input = "select ARTICLE_LOOKUP.ARTICLE_CODE \r\n" +
                " from ARTICLE_LOOKUP";
        input = "SELECT * FROM efashion.ARTICLE_LOOKUP_T a, efashion.SHOP_FACTS b " +
                "WHERE (a.ARTICLE_CODE = b.ARTICLE_CODE) ";
        logger.info("Running test: birtTableDotColumnFromTable " + input);

        ResultSet queryResult = null;
        try {
            PreparedStatement stm = con.prepareStatement(input);
            try {
                stm.getMetaData();
            } catch (Exception ex) {
            }
            queryResult = stm.executeQuery();
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void crystalReportsOne() {

        input = "SELECT DISTINCT \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA_LABEL\"," + "\n\r" +
                " \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA_TYPE\"," + "\n\r" +
                " \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA\"," + "\n\r" +
                " \"ARTICLE_LOOKUP_CRITERIA\".\"ID\"," + "\n\r" +
                " \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA_TYPE_LABEL\"," + "\n\r" +
                " \"ARTICLE_LOOKUP_CRITERIA\".\"ARTICLE_CODE\" " + "\n\r" +
                "FROM" + "\n\r" +
                "   \"starschema_net__clouddb\":\"efashion\".\"ARTICLE_LOOKUP_CRITERIA\" \"ARTICLE_LOOKUP_CRITERIA\" " + "\n\r" +
                "ORDER BY " + "\n\r" +
                "   \"ARTICLE_LOOKUP_CRITERIA\".\"ID\"," + "\n\r" +
                "   \"ARTICLE_LOOKUP_CRITERIA\".\"ARTICLE_CODE\"," + "\n\r" +
                "   \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA_TYPE\", " + "\n\r" +
                "   \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA\", " + "\n\r" +
                "   \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA_TYPE_LABEL\", " + "\n\r" +
                "   \"ARTICLE_LOOKUP_CRITERIA\".\"CRITERIA_LABEL\"";

        logger.info("Running test: crystalReportsOne " + input);

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
    public void crystalReportsTwo() {

        input = "SELECT \"ARTICLE_LOOKUP\".\"ARTICLE_CODE\", \"ARTICLE_LOOKUP\".\"ARTICLE_LABEL\", \"ARTICLE_COLOR_LOOKUP\".\"COLOR_CODE\", \"ARTICLE_COLOR_LOOKUP\".\"SALE_PRICE\"\r\n" +
                " FROM   (\"AGG_YR_QT_RN_ST_LN_CA_SR\" \"AGG_YR_QT_RN_ST_LN_CA_SR\" INNER JOIN \"ARTICLE_COLOR_LOOKUP\" \"ARTICLE_COLOR_LOOKUP\" ON \"AGG_YR_QT_RN_ST_LN_CA_SR\".\"CATEGORY\"=\"ARTICLE_COLOR_LOOKUP\".\"CATEGORY\") \r\n" +
                "        INNER JOIN \"ARTICLE_LOOKUP\" \"ARTICLE_LOOKUP\" ON \"AGG_YR_QT_RN_ST_LN_CA_SR\".\"CATEGORY\"=\"ARTICLE_LOOKUP\".\"CATEGORY\"";

        logger.info("Running test: crystalReportsTwo " + input);

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


    //@Test
    public void crystalReportsThree() {

        input = "SELECT \"ARTICLE_LOOKUP\".\"ARTICLE_CODE\", \r\n" +
                "    \"ARTICLE_LOOKUP\".\"ARTICLE_LABEL\", \r\n" +
                "    \"ARTICLE_COLOR_LOOKUP\".\"COLOR_LABEL\"\r\n" +
                "FROM   \"ARTICLE_LOOKUP\" \"ARTICLE_LOOKUP\" \r\n" +
                "    INNER JOIN \r\n" +
                "    \"ARTICLE_COLOR_LOOKUP\" \"ARTICLE_COLOR_LOOKUP\" \r\n" +
                "    ON \r\n" +
                "    (\"ARTICLE_LOOKUP\".\"ARTICLE_CODE\"=\"ARTICLE_COLOR_LOOKUP\".\"ARTICLE_CODE\") \r\n" +
                "    AND \r\n" +
                "    (\"ARTICLE_LOOKUP\".\"CATEGORY\"=\"ARTICLE_COLOR_LOOKUP\".\"CATEGORY\")";
        logger.info("Running test: crystalReportsThree " + input);

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
    public void squirrelOne() {

        input = "SELECT *\r\n" +
                "FROM efashion.ARTICLE_LOOKUP AS AL\r\n" +
                "JOIN\r\n" +
                "     efashion.ARTICLE_COLOR_LOOKUP\r\n" +
                "ON\r\n" +
                "    AL.ARTICLE_CODE = efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE";
        logger.info("Running test: squirrelOne " + input);

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
    public void squirrelTwo() {

        input = "SELECT ARTICLE_LOOKUP.ARTICLE_CODE, ARTICLE_LOOKUP.ARTICLE_LABEL, ARTICLE_LOOKUP.SALE_PRICE\r\n" +
                "FROM efashion.ARTICLE_LOOKUP";
        logger.info("Running test: squirrelTwo " + input);

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
    public void testMaxFieldSize() {
        input = "Select * FROM OUTLET_LOOKUP LIMIT 5";
        logger.info("Running test: testMaxFieldSize \r\n" + input);

        ResultSet queryResult = null;
        try {
            Statement stm = con.createStatement();
            stm.setMaxFieldSize(4);
            queryResult = stm.executeQuery(input);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void testMaxFieldSizePrepared() {
        input = "Select * FROM OUTLET_LOOKUP LIMIT 5";
        logger.info("Running test: testMaxFieldSizePrepared \r\n" + input);

        ResultSet queryResult = null;
        try {
            PreparedStatement stm = con.prepareStatement(input);
            stm.setMaxFieldSize(3);
            queryResult = stm.executeQuery();
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        HelperFunctions.printer(queryResult);
    }

    @Test
    public void testSquirrelForwardOnly() {
        input = "SELECT * SHOP_FACTS";
        NewConnection();
        try {
            Assert.assertTrue(con.getMetaData().supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
            Assert.assertTrue(con.getMetaData().supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
            Assert.assertFalse(con.getMetaData().supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
            Statement myStatement = con.createStatement();
            ResultSet myResult = myStatement
                    .executeQuery(input);
            HelperFunctions.printer(myResult);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        try {
            Statement myStatement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet myResult = myStatement
                    .executeQuery(input);
            HelperFunctions.printer(myResult);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testSquirrelForwardOnlyOneRow() {
        input = "SELECT count(*) FROM SHOP_FACTS";
        NewConnection();
        try {
            Statement myStatement = con.createStatement();
            ResultSet myResult = myStatement
                    .executeQuery(input);
            //HelperFunctions.printer(myResult);
            while (myResult.next()) {
                System.out.println(myResult.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        Assert.assertTrue(true);
    }

    @Test
    public void testSquirrelForwardOnlyJoinEach() {
        input = "    SELECT count(*) FROM " +
                "bank.CdrcallFact a JOIN EACH bank.CdrcallFact b ON a.IDRSSD = b.IDRSSD";
        /*input="    SELECT * FROM " +
                "bank.CdrcallFact a JOIN EACH bank.CdrcallFact b ON a.IDRSSD = b.IDRSSD";*/
        NewConnection();
        try {
            Statement myStatement = con.createStatement();
            //Statement myStatement = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet myResult = myStatement
                    .executeQuery(input);
            HelperFunctions.printer(myResult);
           /* while(myResult.next()) {
                System.out.println(myResult.getString(1));
            }*/
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        }
        Assert.assertTrue(true);
    }


    @Test
    public void testResultSetCaching() {
        input = "Select * FROM SHOP_FACTS LIMIT 1000";
        logger.info("Running test: testMaxFieldSizePrepared \r\n" + input);

        ResultSet queryResult = null;
        try {
            Statement stm = con.createStatement();
//            PreparedStatement stm = con.prepareStatement(input);
//            stm.setMaxFieldSize(3);
            stm.setFetchSize(10);
            queryResult = stm.executeQuery(input);
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
