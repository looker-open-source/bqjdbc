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
 * this class implements the java.sql.ResultSetMetaData interface for usage in
 * Resultsets which was made from DatabaseMetadata
 * 
 * @author Horváth Attila
 * 
 */
class COLResultSetMetadata implements java.sql.ResultSetMetaData {
    String[][] data = null;
    String[] labels;

    /**
     * Constructor initializes variables
     * 
     * @param data
     *            The column informations
     * @param labels
     *            The name of columns
     */
    public COLResultSetMetadata(String[][] data, String[] labels) {
	this.labels = labels;
	this.data = data;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns this.data[0][0]
     * </p>
     */
    public String getCatalogName(int column) throws SQLException {
	return this.data[0][0];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns java.lang.String.class.toString()
     * </p>
     */
    public String getColumnClassName(int column) throws SQLException {
	return java.lang.String.class.toString();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.labels.length
     * </p>
     */
    public int getColumnCount() throws SQLException {
	return this.labels.length;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 64*1024
     * </p>
     */
    public int getColumnDisplaySize(int column) throws SQLException {
	return 64 * 1024;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.labels[column - 1]
     * </p>
     */
    public String getColumnLabel(int column) throws SQLException {
	return this.labels[column - 1];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.labels[column - 1]
     * </p>
     */
    public String getColumnName(int column) throws SQLException {
	return this.labels[column - 1];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns java.sql.Types.VARCHAR
     * </p>
     */
    public int getColumnType(int column) throws SQLException {

	return java.sql.Types.VARCHAR;
	/*
	 * switch (column) { case 1: return java.sql.Types.VARCHAR; case 2:
	 * return java.sql.Types.VARCHAR; case 3: return java.sql.Types.VARCHAR;
	 * case 4: return java.sql.Types.VARCHAR; case 5: return
	 * java.sql.Types.VARCHAR; case 6: return java.sql.Types.VARCHAR; case
	 * 7: return java.sql.Types.INTEGER; case 8: return
	 * java.sql.Types.INTEGER; case 9: return java.sql.Types.INTEGER; case
	 * 10: return java.sql.Types.INTEGER; case 11: return
	 * java.sql.Types.INTEGER; case 12: return java.sql.Types.VARCHAR; case
	 * 13: return java.sql.Types.VARCHAR; case 14: return
	 * java.sql.Types.INTEGER; case 15: return java.sql.Types.INTEGER; case
	 * 16: return java.sql.Types.INTEGER; case 17: return
	 * java.sql.Types.INTEGER; case 18: return java.sql.Types.VARCHAR; case
	 * 19: return java.sql.Types.VARCHAR; case 20: return
	 * java.sql.Types.VARCHAR; case 21: return java.sql.Types.VARCHAR; case
	 * 22: return java.sql.Types.INTEGER; default: break; } return
	 * java.sql.Types.VARCHAR;
	 */
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.data[0][5]
     * </p>
     */
    public String getColumnTypeName(int column) throws SQLException {
	return this.data[0][5];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 0
     * </p>
     */
    public int getPrecision(int column) throws SQLException {
	return 0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 0
     * </p>
     */
    public int getScale(int column) throws SQLException {
	return 0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.data[0][1]
     * </p>
     */
    public String getSchemaName(int column) throws SQLException {
	return this.data[0][1];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.data[0][2]
     * </p>
     */
    public String getTableName(int column) throws SQLException {
	return this.data[0][2];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isAutoIncrement(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isCaseSensitive(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isCurrency(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 1
     * </p>
     */
    public int isNullable(int column) throws SQLException {
	return 1;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns true
     * </p>
     */
    public boolean isReadOnly(int column) throws SQLException {
	return true;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isSearchable(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isSigned(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    public boolean isWritable(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always Throws SQLException
     * </p>
     * 
     * @throws SQLException
     *             always
     */
    public <T> T unwrap(Class<T> arg0) throws SQLException {
	throw new SQLException("not found");
    }
}

/**
 * This class implements the java.sql.ResultSet interface for Object[] and
 * 
 * Object[][] types instead of GetQueryResultsResponse.getrows() for giving back
 * Resultset Data from BQDatabaseMetadata
 * 
 * @author Horváth Attila
 * 
 */
public class DMDResultSet extends ScrollableResultset<Object> implements
	java.sql.ResultSet {
    /** Variable containing column names */
    String[] Colnames = null;

    /**
     * Constructor for initializing variables
     * 
     * @param objects
     *            data to contain
     * @param colnames
     *            names of columns
     */
    public DMDResultSet(Object[] objects, String[] colnames) {
	this.RowsofResult = objects;
	this.Colnames = colnames;
    }

    /** {@inheritDoc} */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
	for (int i = 0; i < this.Colnames.length; i++)
	    if (this.Colnames[i].equals(columnLabel))
		return i + 1;
	throw new SQLException("No such column");
    };

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns a COLResultSetMetadata
     * </p>
     */
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
	return new COLResultSetMetadata((String[][]) this.RowsofResult,
		this.Colnames);
    };

    /** {@inheritDoc} */
    public Object getObject(int columnIndex) throws SQLException {

	if (this.isClosed())
	    throw new SQLException("This Resultset is Closed");
	this.ThrowCursorNotValidExeption();
	if (this.RowsofResult == null)
	    throw new SQLException("No Valid Rows");

	if (columnIndex < 1)
	    throw new SQLException("ColumnIndex is not valid");
	else
	    try {
		Object[][] Containter = Object[][].class
			.cast(this.RowsofResult);
		if (columnIndex > Containter[this.Cursor].length)
		    throw new SQLException("ColumnIndex is not valid");
		else {
		    if (Containter[this.Cursor][columnIndex - 1] == null)
			this.wasnull = true;
		    else
			this.wasnull = false;
		    return Containter[this.Cursor][columnIndex - 1];
		}
	    } catch (ClassCastException e) {
		if (columnIndex == 1) {
		    if (this.RowsofResult[this.Cursor] == null)
			this.wasnull = true;
		    else
			this.wasnull = false;
		    return this.RowsofResult[this.Cursor];
		} else
		    throw new SQLException(e);
	    }
    }

    /** {@inheritDoc} */
    public String getString(int columnIndex) throws SQLException {

	if (this.isClosed())
	    throw new SQLException("This Resultset is Closed");
	this.ThrowCursorNotValidExeption();
	if (this.RowsofResult == null)
	    throw new SQLException("No Valid Rows");

	if (columnIndex < 1)
	    throw new SQLException("ColumnIndex is not valid");
	else
	    try {
		Object[][] Containter = Object[][].class
			.cast(this.RowsofResult);
		if (columnIndex > Containter[this.Cursor].length)
		    throw new SQLException("ColumnIndex is not valid");
		else if (Containter[this.Cursor][columnIndex - 1] == null) {
		    this.wasnull = true;
		    return null;
		} else {
		    this.wasnull = false;
		    return Containter[this.Cursor][columnIndex - 1].toString();
		}
	    } catch (ClassCastException e) {
		if (columnIndex == 1) {
		    if (this.RowsofResult[this.Cursor] == null) {
			this.wasnull = true;
			return null;
		    } else {
			this.wasnull = false;
			return this.RowsofResult[this.Cursor].toString();
		    }
		} else
		    throw new SQLException("ColumnIndex is not valid");
	    }
    }
}