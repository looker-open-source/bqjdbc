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
import java.sql.SQLException;

import net.starschema.clouddb.jdbc.BQSQLException;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.list.CallContainer;
import net.starschema.clouddb.jdbc.list.SQLCleaner;
import net.starschema.clouddb.jdbc.list.SelectStatement;
import net.starschema.clouddb.jdbc.list.TreeBuilder;

import org.antlr.runtime.RecognitionException;
import org.apache.log4j.BasicConfigurator;

public class GrammarTreeTest {

    //Not working for public datasets since bigquery api functions don't treat public datasets as the users own it doesn't list them with its getdatasets function
    //PUBLICDATARA NEM M�K�DIK, MERT A GETCOLUMNS FUNCI� NEM KERES A PUBLIC DATASETEKBEN, MERT AZ APIBAN L�V� H�V�SOK NEM VESZIK SAJ�T A PROJECTHEZ TARTOZ� T�BL�NAK
    static String string =
            //"select ARTICLE_LOOKUP.ARTICLE_CODE from ARTICLE_LOOKUP";
            //SELECT ARTICLE_CODE,  `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE,`efashion`.ARTICLE_LOOKUP.ARTICLE_LABEL, `efashion`.ARTICLE_LOOKUP_CRITERIA.ARTICLE_CODE, `efashion`.ARTICLE_LOOKUP_CRITERIA.ID FROM `efashion`.ARTICLE_LOOKUP_CRITERIA, `efashion`.ARTICLE_COLOR_LOOKUP JOIN `efashion`.ARTICLE_LOOKUP ON `efashion`.ARTICLE_COLOR_LOOKUP.ARTICLE_CODE = `efashion`.ARTICLE_LOOKUP.ARTICLE_CODE";
            "SELECT \"ARTICLE_LOOKUP\".\"ARTICLE_CODE\", \"ARTICLE_LOOKUP\".\"ARTICLE_LABEL\", \"ARTICLE_COLOR_LOOKUP\".\"COLOR_CODE\", \"ARTICLE_COLOR_LOOKUP\".\"SALE_PRICE\"\r\n" +
                    " FROM   (\"AGG_YR_QT_RN_ST_LN_CA_SR\" \"AGG_YR_QT_RN_ST_LN_CA_SR\" INNER JOIN \"ARTICLE_COLOR_LOOKUP\" \"ARTICLE_COLOR_LOOKUP\" ON \"AGG_YR_QT_RN_ST_LN_CA_SR\".\"CATEGORY\"=\"ARTICLE_COLOR_LOOKUP\".\"CATEGORY\") \r\n" +
                    "        INNER JOIN \"ARTICLE_LOOKUP\" \"ARTICLE_LOOKUP\" ON \"AGG_YR_QT_RN_ST_LN_CA_SR\".\"CATEGORY\"=\"ARTICLE_LOOKUP\".\"CATEGORY\"";

    /**
     * @param args
     * @throws BQSQLException
     * @throws RecognitionException
     */
    public static void main(String[] args) throws BQSQLException, RecognitionException {
        BasicConfigurator.configure();

        NewConnection();

        TreeBuilder builder = new TreeBuilder(string, con, new CallContainer());
        SelectStatement build = null;
        try {
            build = (SelectStatement) builder.build();
            SQLCleaner.Clean(build);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.err.println(build.toPrettyString(1));
    }

    /**
     * Static Connection holder
     */
    public static Connection con;

    /**
     * Creates a new Connection to bigquery with the jdbc driver
     */
    public static void NewConnection() {
        try {
            if (GrammarTreeTest.con == null || !GrammarTreeTest.con.isValid(0)) {

                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    GrammarTreeTest.con = DriverManager
                            .getConnection(
                                    BQSupportFuncts
                                            .constructUrlFromPropertiesFile(BQSupportFuncts
                                                    .readFromPropFile("installedaccount.properties")),
                                    BQSupportFuncts
                                            .readFromPropFile("installedaccount.properties"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
