package BQJDBC.QueryResultTest;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class HelperFunctions {
    static Logger logger = Logger.getLogger("HelperFunctions");
    
    /**
     * Prints a ResultSet QueryResult to Log
     * 
     * @param input
     */
    public static void printer(ResultSet input) {
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
        try {
            int columnCount = input.getMetaData().getColumnCount();
            int limit = 50;
            while (input.next() && limit > 0) {
                String Output = "";
                for (int i = 0; i < columnCount; i++) {
                    Output += input.getString(i+1) + "\t";
                }
                logger.debug(Output);
                limit--;
            }             
        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.warn("failed: ",e);
        }
    }
    
    /**
     * Prints a String[][] QueryResult to Log
     * 
     * @param input
     */
    public static void printer(String[][] input) {
        for (int s = 0; s < input[0].length; s++) {
            String Output = "";
            for (int i = 0; i < input.length; i++) {
                if (i == input.length - 1) {
                    Output += input[i][s];
                }
                else {
                    Output += input[i][s] + "\t";
                }
            }
            logger.debug(Output);
        }
    }
    
}
