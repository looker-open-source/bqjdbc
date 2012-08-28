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
 *  
 * This class implements the java.sql.Array interface
 */

package net.starschema.clouddb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import org.apache.log4j.Logger;

class BQArray implements java.sql.Array
{
	Object objects;
	Logger logger = Logger.getLogger(BQArray.class);
	
	public BQArray(Object objects2) {
		this.objects = objects2;
	}

	//Implemented Functions:
	
	@Override
	public void free() throws SQLException {
		objects = null;
	}

	@Override
	public Object getArray() throws SQLException {
		return objects;
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		Object[] Container = (Object[])this.getArray();
		if(index + count <= Container.length)
		{
			Object[] Temp = new Object[count];
			for(long i=index;i<=index+count;i++)
			{
				Temp[(int)(i-index)] = Container[(int)i];  
			}
			return Temp;
		}
		else
		return null;
	}

	
	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
	
	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	//Unimplemented Functions:
	@Override
	public int getBaseType() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet(long index, int count,
			Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
