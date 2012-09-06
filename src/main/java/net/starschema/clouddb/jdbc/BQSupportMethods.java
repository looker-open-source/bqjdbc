/**
 *  Starschema Big Query JDBC Driver
 *  Copyright (C) 2012, Starschema Ltd.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.starschema.clouddb.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This Class provides static function to cooperate with the objects of the JDBC
 * driver
 * 
 * @author Horváth Attila
 */
public class BQSupportMethods {
    /**
     * This method is to get containing data of a java.sql.ResultSet as a
     * List<List<String>>
     * 
     * @param result
     *            is a java.sql.ResultSet
     * @return a List {@code<List<String>>} containing data of the ResultSet
     * @throws SQLException
     *             if error occurs when obtaining data from the Resultset
     */
    public static String[][] GetQueryResult(java.sql.ResultSet result)
            throws SQLException {
        int ColumnCount = result.getMetaData().getColumnCount();
        List<List<String>> returned = new ArrayList<List<String>>();

        for (int i = 0; i < ColumnCount; i++) {
            returned.add(new ArrayList<String>());
            List<String> Column = returned.get(i);
            result.first();
            while (result.isAfterLast() != true) {
                Column.add(result.getString(i + 1));
                result.next();
            }
            result.beforeFirst();
        }
        String[][] returning = new String[returned.size()][];
        for (int i = 0; i < returned.size(); i++) {
            returning[i] = new String[returned.get(i).size()];
            for (int k = 0; k < returned.get(i).size(); k++)
                returning[i][k] = returned.get(i).get(k);
        }
        return returning;
    }
}
