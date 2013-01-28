package BQJDBC.QueryResultTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.BQSupportMethods;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class GrammarTestRunningForLong {
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
                }
                catch (Exception e) {
                    logger.debug("Failed to make connection trough the JDBC driver",e);
                }
            }
            logger.debug("Running the next test");
        }
        catch (SQLException e) {
            logger.fatal("Something went wrong",e);
        }
    }
    
    
    static String input;    
    /**
     * to Test the columnresolver from 2 subqueries
     */
    
    /**
     * to test the Grammars MultiJoin
     */
    @Test
    public void fourJoin() {
        
        input = "SELECT \r\n" + 
                "    `efashion`.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE, \r\n" + 
                "    `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE,\r\n" + 
                "    `efashion`.ARTICLE_LOOKUP.ARTICLE_LABEL, \r\n" + 
                "FROM \r\n" + 
                "    `efashion`.ARTICLE_COLOR_LOOKUP \r\n" + 
                "        JOIN \r\n" + 
                "    `efashion`.ARTICLE_LOOKUP \r\n" + 
                "        ON \r\n" + 
                "    `efashion`.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE" +
                "       JOIN \r\n" +
                "   efashion.ARTICLE_LOOKUP_CRITERIA" +
                "       ON" +
                "   efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE" +
                "       JOIN" +
                "   efashion.SHOP_FACTS" +
                "       ON" +
                "   efashion.SHOP_FACTS.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE" +
                " LIMIT 2";
        logger.info("Running test: fourJoin \r\n" + input );
        
        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        printer(queryResult);
    }    
        
    
    /** 
     * A simple query with count(*)
     * from Quartezian joined tables
     */
    @Test
    public void countJokerFromTwoTables() {
        
        input = "select COUNT(*) from efashion.ARTICLE_COLOR_LOOKUP, efashion.ARTICLE_LOOKUP";

        logger.info("Running test: test_countjoker" + input );
        
        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        printer(queryResult);
    }
    
    
    /** To test the joinresolver */
   @Test
    public void selectFromDuplicateTableWithoutAlias() {
        input = "SELECT efashion.ARTICLE_LOOKUP.ARTICLE_CODE as ARTICLE_CODE1, efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE as ARTICLE_CODE2 FROM efashion.ARTICLE_LOOKUP, efashion.ARTICLE_LOOKUP_CRITERIA limit 10";       
        logger.info("Running test: selectFromDuplicateTableWithoutAlias" + input );

        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        printer(queryResult);
    }
            
    @Test
    public void selectJokerFromFourJoin() {
        
        input = "SELECT * FROM \r\n" + 
        		"    efashion.ARTICLE_COLOR_LOOKUP\r\n" + 
        		"        JOIN \r\n" + 
        		"    efashion.ARTICLE_LOOKUP \r\n" + 
        		"        ON \r\n" + 
        		"    efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE\r\n" + 
        		"       JOIN\r\n" + 
        		"   efashion.ARTICLE_LOOKUP_CRITERIA\r\n" + 
        		"       ON\r\n" + 
        		"   efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE\r\n" + 
        		"       JOIN\r\n" + 
        		"   efashion.SHOP_FACTS\r\n" + 
        		"       ON\r\n" + 
        		"   efashion.SHOP_FACTS.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE\r\n" + 
        		"LIMIT 10";
        logger.info("Running test: selectJokerFromFourJoin \r\n" + input );
        
        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        printer(queryResult);
    }    
    
    @Test
    public void selectJokerFromFourJoinWithWhere() {
        
        input = "SELECT " +
        		"     efashion.ARTICLE_LOOKUP.ARTICLE_CODE, \r\n" +
                "     efashion.ARTICLE_LOOKUP.ARTICLE_LABEL, \r\n" +
                "     efashion.ARTICLE_LOOKUP.CATEGORY, \r\n" + 
                "     efashion.ARTICLE_COLOR_LOOKUP.COLOR_CODE, \r\n" + 
        		"     efashion.ARTICLE_COLOR_LOOKUP.COLOR_LABEL, \r\n" + 
                "     CRITERIA_TYPE, \r\n" + 
                "     CRITERIA, \r\n" + 
                "     CRITERIA_TYPE_LABEL, \r\n" + 
                "     CRITERIA_LABEL, \r\n" +
        		"     efashion.ARTICLE_LOOKUP.FAMILY_NAME, \r\n" + 
        		"     efashion.ARTICLE_LOOKUP.FAMILY_CODE, \r\n" + 
        		"     efashion.ARTICLE_LOOKUP.SALE_PRICE, \r\n" + 
                "     MARGIN, \r\n" + 
                "     QUANTITY_SOLD, " +
        		"     AMOUNT_SOLD, \r\n" + 
                "     WEEK_KEY \r\n" +
        		" FROM \r\n" + 
                "    efashion.ARTICLE_COLOR_LOOKUP\r\n" + 
                "        JOIN \r\n" + 
                "    efashion.ARTICLE_LOOKUP \r\n" + 
                "        ON \r\n" + 
                "    efashion.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE\r\n" + 
                "       JOIN\r\n" + 
                "   efashion.ARTICLE_LOOKUP_CRITERIA\r\n" + 
                "       ON\r\n" + 
                "   efashion.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE = efashion.ARTICLE_LOOKUP.ARTICLE_CODE\r\n" + 
                "       JOIN\r\n" + 
                "   efashion.SHOP_FACTS\r\n" + 
                "       ON\r\n" + 
                "   efashion.SHOP_FACTS.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE\r\n" +
                "WHERE" +
                " efashion.ARTICLE_LOOKUP.SALE_PRICE <= '100'" + 
                "LIMIT 10";
        logger.info("Running test: selectJokerFromFourJoin \r\n" + input );
        
        ResultSet queryResult = null;
        try {
            queryResult = con.createStatement().executeQuery(input);
        }
        catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(queryResult);
        printer(queryResult);
    }    
         
    @Test
    public void birtQueryBuilderOne() {
        
        input = "SELECT `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE, `efashion`.ARTICLE_LOOKUP.ARTICLE_LABEL,\r\n" + 
        		"  `efashion`.ARTICLE_LOOKUP.SALE_PRICE, `efashion`.ARTICLE_LOOKUP.FAMILY_NAME,\r\n" + 
        		"  `efashion`.PRODUCT_PROMOTION_FACTS.WEEK_KEY, `efashion`.PRODUCT_PROMOTION_FACTS.DURATION,\r\n" + 
        		"  `efashion`.PRODUCT_PROMOTION_FACTS.PROMOTION_KEY, `efashion`.CALENDAR_YEAR_LOOKUP.YEAR,\r\n" + 
        		"  `efashion`.CALENDAR_YEAR_LOOKUP.WEEK_IN_YEAR, `efashion`.PROMOTION_LOOKUP.PROMOTION,\r\n" + 
        		"  `efashion`.PROMOTION_LOOKUP.PRINT_FLAG, `efashion`.PROMOTION_LOOKUP.RADIO_FLAG,\r\n" + 
        		"  `efashion`.PROMOTION_LOOKUP.TELEVISION_FLAG, `efashion`.PROMOTION_LOOKUP.DIRECT_MAIL_FLAG,\r\n" + 
        		"  `efashion`.SHOP_FACTS.COLOR_CODE, `efashion`.SHOP_FACTS.SHOP_CODE,\r\n" + 
        		"  `efashion`.SHOP_FACTS.AMOUNT_SOLD, `efashion`.SHOP_FACTS.QUANTITY_SOLD\r\n" + 
        		"  FROM\r\n" + 
        		"       `efashion`.SHOP_FACTS " +
        		"     JOIN `efashion`.PRODUCT_PROMOTION_FACTS " +
        		"     JOIN `efashion`.ARTICLE_LOOKUP " +
        		"     ON `efashion`.PRODUCT_PROMOTION_FACTS.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE " +
        		"         JOIN `efashion`.CALENDAR_YEAR_LOOKUP " +
        		"         ON `efashion`.PRODUCT_PROMOTION_FACTS.WEEK_KEY = `efashion`.CALENDAR_YEAR_LOOKUP.WEEK_KEY " +
        		"             JOIN `efashion`.PROMOTION_LOOKUP " +
        		"             ON `efashion`.PRODUCT_PROMOTION_FACTS.PROMOTION_KEY = `efashion`.PROMOTION_LOOKUP.PROMOTION_KEY " +
        		"             ON `efashion`.SHOP_FACTS.ARTICLE_CODE = `efashion`.PRODUCT_PROMOTION_FACTS.ARTICLE_CODE\r\n" + 
        		"";
        
    logger.info("Running test: selectJokerFromFourJoin \r\n" + input );
        
    ResultSet queryResult = null;
    try {
        queryResult = con.createStatement().executeQuery(input);
    }
    catch (SQLException e) {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(queryResult);
    printer(queryResult);
}      
    @Test
    public void inetClearReports() {
        
        input = " SELECT PROMOTION_LOOKUP.DIRECT_MAIL_FLAG," +
        		"     PROMOTION_LOOKUP.PRINT_FLAG," +
        		"     PROMOTION_LOOKUP.PROMOTION," +
        		"     PROMOTION_LOOKUP.PROMOTION_KEY," +
        		"     PROMOTION_LOOKUP.RADIO_FLAG," +
        		"     PROMOTION_LOOKUP.TELEVISION_FLAG," +
        		"     TICLE_COLOR_LOOKUP.ARTICLE_CODE," +
        		"     TICLE_COLOR_LOOKUP.ARTICLE_LABEL," +
        		"     TICLE_COLOR_LOOKUP.CATEGORY," +
        		"     TICLE_COLOR_LOOKUP.COLOR_CODE," +
        		"     TICLE_COLOR_LOOKUP.COLOR_LABEL," +
        		"     TICLE_COLOR_LOOKUP.FAMILY_CODE," +
        		"     TICLE_COLOR_LOOKUP.FAMILY_NAME," +
        		"     TICLE_COLOR_LOOKUP.SALE_PRICE," +
        	    "      QT_RN_ST_LN_CA_SR.CATEGORY," +
        		"     QT_RN_ST_LN_CA_SR.ID," +
        		"     QT_RN_ST_LN_CA_SR.LINE," +
        		"     QT_RN_ST_LN_CA_SR.QUARTER," +
        		"     QT_RN_ST_LN_CA_SR.SALES_REVENUE," +
        		"     QT_RN_ST_LN_CA_SR.STATE," +
        		"     QT_RN_ST_LN_CA_SR.\"YEAR\"," +
        		"     CT_PROMOTION_FACTS.ARTICLE_CODE," +
        		"     CT_PROMOTION_FACTS.DURATION," +
        		"     CT_PROMOTION_FACTS.ID," +
        		"     CT_PROMOTION_FACTS.PROMOTION_COST," +
        		"     CT_PROMOTION_FACTS.PROMOTION_KEY," +
        		"     CT_PROMOTION_FACTS.WEEK_KEY," +
        		"     LE_LOOKUP_CRITERIA.ARTICLE_CODE," +
        		"     LE_LOOKUP_CRITERIA.CRITERIA," +
        		"     LE_LOOKUP_CRITERIA.CRITERIA_LABEL," +
        		"     LE_LOOKUP_CRITERIA.CRITERIA_TYPE," +
        		"     LE_LOOKUP_CRITERIA.CRITERIA_TYPE_LABEL," +
        		"     LE_LOOKUP_CRITERIA.ID " +
        		"FROM    efashion.ARTICLE_COLOR_LOOKUP TICLE_COLOR_LOOKUP " +
        		"INNER JOIN " +
        		"     efashion.AGG_YR_QT_RN_ST_LN_CA_SR QT_RN_ST_LN_CA_SR " +
        		"ON   " +
        		"     TICLE_COLOR_LOOKUP.CATEGORY=QT_RN_ST_LN_CA_SR.CATEGORY  " +
        		"     INNER JOIN " +
        		"         efashion.ARTICLE_LOOKUP_CRITERIA LE_LOOKUP_CRITERIA " +
        		"     ON TICLE_COLOR_LOOKUP.ARTICLE_CODE=LE_LOOKUP_CRITERIA.ARTICLE_CODE  " +
        		"         INNER JOIN " +
        		"             efashion.PRODUCT_PROMOTION_FACTS CT_PROMOTION_FACTS " +
        		"         ON TICLE_COLOR_LOOKUP.ARTICLE_CODE=CT_PROMOTION_FACTS.ARTICLE_CODE  " +
        		"             INNER JOIN efashion.PROMOTION_LOOKUP PROMOTION_LOOKUP " +
        		"                 ON CT_PROMOTION_FACTS.PROMOTION_KEY=PROMOTION_LOOKUP.PROMOTION_KEY " +
        		"WHERE 1=0";
        
    logger.info("Running test: inetClearReports \r\n" + input );
        
    ResultSet queryResult = null;
    try {
        queryResult = con.createStatement().executeQuery(input);
    }
    catch (SQLException e) {
        this.logger.error("SQLexception" + e.toString());
        Assert.fail("SQLException" + e.toString());
    }
    Assert.assertNotNull(queryResult);
    printer(queryResult);
}   
    
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    //---------------------------------------------------
    
        
    
    /**
     * Prints a String[][] QueryResult to Log
     * 
     * @param input
     */
    private void printer(ResultSet input) {
        String columnnames = "";
        try {
            for (int i = 1; i <= input.getMetaData().getColumnCount(); i++) {
                    columnnames += input.getMetaData().getColumnName(i)+"\t";
                }
            }
            catch (SQLException e) {
                logger.warn(e);
            }
        logger.debug(columnnames);
        String[][] inputArray = null;
        try {
            inputArray = BQSupportMethods.GetQueryResult(input);
        }
        catch (SQLException e) {
            logger.warn(e);
        }
        //limiting the output to 10 lines
        //for all the results use "s < input[0].length"s
        for (int s = 0; s < 10 && s < inputArray[0].length ; s++) {
            String Output = "";
            for (int i = 0; i < inputArray.length; i++) {
                if (i == inputArray.length - 1) {
                    Output += inputArray[i][s];
                }
                else {
                    Output += inputArray[i][s] + "\t";
                }
            }
            this.logger.debug(Output);
        }
    }
        
}
