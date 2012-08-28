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

public class BQJDBCAPI 
{	
	/**
	 * This method is to get containing data of a java.sql.ResultSet as a List<List<String>>
	 * @param Result is a java.sql.ResultSet
	 * @return a List<List<String>> containing data of the ResultSet
	 * @throws SQLException
	 */
	public static String[][] GetQueryResult(java.sql.ResultSet Result) throws SQLException
	{
		int ColumnCount = Result.getMetaData().getColumnCount();
		List<List<String>> returned = new ArrayList<List<String>>();

		for(int i = 0;i< ColumnCount; i++)
		{
			returned.add(new ArrayList<String>());
			List<String> Column = returned.get(i);
			Result.first();			
			while(Result.isAfterLast()!=true)
			{
				Column.add(Result.getString(i+1));
				Result.next();
			}
			Result.beforeFirst();
		}
		String[][] returning = new String[returned.size()][];
		for(int i=0; i< returned.size();i++)
		{
			returning[i] = new String[returned.get(i).size()];
			for(int k=0;k< returned.get(i).size();k++)
			{
				returning[i][k] = returned.get(i).get(k);
			}
		}	
		return returning;
	}
}
