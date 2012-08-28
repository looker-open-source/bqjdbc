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
 * This class implements the java.sql.ResultSet interface for Object[] and Object[][] types instead of
 * GetQueryResultsResponse.getrows() 
 */

package net.starschema.clouddb.jdbc;

import java.sql.SQLException;

/**
 * This class implements the java.sql.ResultSet interface for Object[] and Object[][] types instead of
 * GetQueryResultsResponse.getrows() for giving back Resultset Data from BQDatabaseMetadata
 * @author Horváth Attila
 *
 */
public class DMDResultSet extends ScrollableResultset<Object> implements java.sql.ResultSet
{
	public DMDResultSet (Object[] objects,String[] colnames){
		RowsofResult = objects;
		this.Colnames = colnames;
	}
	String[] Colnames = null; 
	
	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		for(int i=0;i<Colnames.length;i++)
		{
			if(Colnames[i].equals(columnLabel))
				return i+1;
		}
		throw new SQLException("No such column");
	};
	
	
	@Override
	public java.sql.ResultSetMetaData getMetaData() throws SQLException 
	{
		return new COLResultSetMetadata((String[][])this.RowsofResult, this.Colnames);
		
	};
	
		@Override
		public String getString(int columnIndex) throws SQLException {
			
			if(this.isClosed())throw new SQLException("This Resultset is Closed");
			ThrowCursorNotValidExeption();
			if(RowsofResult==null) throw new SQLException("No Valid Rows");
			
			if(columnIndex < 1)	{
				throw new SQLException("ColumnIndex is not valid");
			}
			else {
			try
				{
					Object[][] Containter = Object[][].class.cast(RowsofResult);
					if(columnIndex > Containter[Cursor].length)
						throw new SQLException("ColumnIndex is not valid");
					else
					{
						if(Containter[Cursor][columnIndex-1]==null)
							{
								wasnull = true;
								return null;
							}
						else
						{
							wasnull = false;
							return Containter[Cursor][columnIndex-1].toString();
						}
					}
				}
				catch (ClassCastException e) {
					if(columnIndex == 1)
					{
						if(RowsofResult[Cursor]==null)
							{
							wasnull = true;
							return null;
							}
						else
						{
							wasnull = false;
						return RowsofResult[Cursor].toString();
						}
					}
					else
					{
						throw new SQLException("ColumnIndex is not valid");
					}
				}
			}			
		}
		
		@Override
		public Object getObject(int columnIndex) throws SQLException {
			
			if(this.isClosed())throw new SQLException("This Resultset is Closed");
			ThrowCursorNotValidExeption();
			if(RowsofResult==null) throw new SQLException("No Valid Rows");
			
			if(columnIndex < 1)	{
				throw new SQLException("ColumnIndex is not valid");
			}
			else {
			try
				{
					Object[][] Containter = Object[][].class.cast(RowsofResult);
					if(columnIndex > Containter[Cursor].length)
						throw new SQLException("ColumnIndex is not valid");
					else
					{
						if(Containter[Cursor][columnIndex-1]== null)
							wasnull = true;
						else
							wasnull = false;
						return Containter[Cursor][columnIndex-1];
					}
				}
				catch (ClassCastException e) {
					if(columnIndex == 1)
					{
						if(RowsofResult[Cursor]== null)
							wasnull = true;
						else
							wasnull = false;
						return RowsofResult[Cursor];
					}
					else
					{
						throw new SQLException(e);
					}
				}
			}			
		}
	}

class COLResultSetMetadata implements java.sql.ResultSetMetaData
{
	String[][] data = null;
	String[] labels;
	public COLResultSetMetadata(String[][] data, String[] labels)
	{
		this.labels = labels;
		this.data = data;
	}
	/*
	String[] Col = new String[23];
	//TABLE_CAT String => table catalog (may be null)
	Col[0] = ProjectId+String.valueOf(index);
	//TABLE_SCHEM String => table schema (may be null)
	Col[1] = DatasetId;
	//TABLE_NAME String => table name
	Col[2] = TableId;
	//COLUMN_NAME String => column name
	Col[3] = Column.getName();
	//DATA_TYPE int => SQL type from java.sql.Types
	Col[4] =  String.valueOf(BigQueryApi.ParseToSqlFieldType(Column.getType()));
	//TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
	Col[5] =  Column.getType();
	//COLUMN_SIZE int => column size. (In Bigquery max colsize is 64kb and its not specifically determined for fields) 
	Col[6] =  String.valueOf(Column.size());
	//BUFFER_LENGTH is not used.
	Col[7] =  null;
	//DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
	Col[8] =  null;
	//NUM_PREC_RADIX int => Radix (typically either 10 or 2)
	Col[9] =  null;
	//NULLABLE int => is NULL allowed.
	Col[10] = Column.getMode();
	//REMARKS String => comment describing column (may be null) 
	Col[11] = null; 
	//COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
	Col[12] = "String";
	//SQL_DATA_TYPE int => unused
	Col[13] = Column.getType();
	//SQL_DATETIME_SUB int => unused
	Col[14] = null;
	//CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
	Col[15] = String.valueOf(64*1024);
	//ORDINAL_POSITION int => index of column in table (starting at 1) 
	Col[16] = String.valueOf(index);
	//IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
	Col[17] = "";
	//SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
	Col[18] = null;
	//SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
	Col[19] = null;
	//SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)
	Col[20] = null;
	//SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
	Col[21] = null;
	//IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
	*/
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return data[0][0];
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return java.lang.String.class.toString();
	}

	@Override
	public int getColumnCount() throws SQLException {
		// TODO Auto-generated method stub
		return this.labels.length;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		// TODO Auto-generated method stub
		return labels[column-1];
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return this.labels[column-1];
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		
		return java.sql.Types.VARCHAR;
		/*
switch (column) {
case 1:
	return java.sql.Types.VARCHAR;
case 2:
	return java.sql.Types.VARCHAR;	
case 3:
	return java.sql.Types.VARCHAR;		
case 4:
	return java.sql.Types.VARCHAR;		
case 5:
	return java.sql.Types.VARCHAR;		
case 6:
	return java.sql.Types.VARCHAR;	
case 7:
	return java.sql.Types.INTEGER;	
case 8:
	return java.sql.Types.INTEGER;	
case 9:
	return java.sql.Types.INTEGER;	
case 10:
	return java.sql.Types.INTEGER;	
case 11:
	return java.sql.Types.INTEGER;	
case 12:
	return java.sql.Types.VARCHAR;		
case 13:
	return java.sql.Types.VARCHAR;	
case 14:
	return java.sql.Types.INTEGER;	
case 15:
	return java.sql.Types.INTEGER;	
case 16:
	return java.sql.Types.INTEGER;	
case 17:
	return java.sql.Types.INTEGER;	
case 18:
	return java.sql.Types.VARCHAR;
case 19:
	return java.sql.Types.VARCHAR;	
case 20:
	return java.sql.Types.VARCHAR;		
case 21:
	return java.sql.Types.VARCHAR;	
case 22:
	return java.sql.Types.INTEGER;
		default:
			break;
		}
return java.sql.Types.VARCHAR;	*/
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return data[0][5];
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return data[0][1];
	}

	@Override
	public String getTableName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return data[0][2];
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
}