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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class HelperFunctions {
    static Logger logger = LoggerFactory.getLogger(HelperFunctions.class);

    /**
     * Prints a ResultSet QueryResult to Log
     *
     * @param input
     */
    public static void printer(ResultSet input) {
        String columnnames = "";
        try {
            for (int i = 1; i <= input.getMetaData().getColumnCount(); i++) {
                columnnames += input.getMetaData().getColumnName(i) + "\t";
            }
        } catch (SQLException e) {
            logger.warn("", e);
        }
        logger.debug(columnnames);
        try {
            int columnCount = input.getMetaData().getColumnCount();
            int limit = 50;
            while (input.next() && limit > 0) {
                String Output = "";
                for (int i = 0; i < columnCount; i++) {
                    Output += input.getString(i + 1) + "\t";
                }
                logger.debug(Output);
                limit--;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.warn("failed: ", e);
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
                } else {
                    Output += input[i][s] + "\t";
                }
            }
            logger.debug(Output);
        }
    }

}
