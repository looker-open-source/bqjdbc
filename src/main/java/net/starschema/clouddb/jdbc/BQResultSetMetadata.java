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
 * This class implements the java.sql.ResultSetMetaData interface
 */

package net.starschema.clouddb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * This class implements the java.sql.ResultSetMetadata interface
 * 
 * @author Horváth Attila
 * 
 */
class BQResultsetMetaData implements ResultSetMetaData {

    /** Reference of the bigquery GetQueryResultsResponse object */
    GetQueryResultsResponse result = null;

    /**
     * Constructor that initializes variables
     * 
     * @param result
     *            the bigquery GetQueryResultsResponse object
     */
    public BQResultsetMetaData(GetQueryResultsResponse result) {

	this.result = result;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public String getCatalogName(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public String getColumnClassName(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public int getColumnCount() throws SQLException {
	TableSchema schema = this.result.getSchema();
	List<TableFieldSchema> schemafieldlist = null;
	if (schema != null)
	    schemafieldlist = schema.getFields();
	else
	    return 0;
	if (schemafieldlist != null)
	    return this.result.getSchema().getFields().size();
	else
	    return 0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public int getColumnDisplaySize(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    public String getColumnLabel(int column) throws SQLException {

	if (this.getColumnCount() == 0)
	    throw new IndexOutOfBoundsException();
	try {
	    return this.result.getSchema().getFields().get(column - 1)
		    .getName();
	} catch (IndexOutOfBoundsException e) {
	    throw new SQLException(e);
	}
    }

    /** {@inheritDoc} */
    public String getColumnName(int column) throws SQLException {
	if (this.getColumnCount() == 0)
	    throw new IndexOutOfBoundsException();
	try {
	    return this.result.getSchema().getFields().get(column - 1)
		    .getName();
	} catch (IndexOutOfBoundsException e) {
	    throw new SQLException(e);
	}
    }

    /**
     * {@inheritDoc} <br>
     * note: This Can only Return due to bigquery:<br>
     * java.sql.Types.FLOAT<br>
     * java.sql.Types.BOOLEAN<br>
     * java.sql.Types.INTEGER<br>
     * java.sql.Types.VARCHAR
     * */
    public int getColumnType(int column) throws SQLException {
	if (this.getColumnCount() == 0)
	    return 0;
	String Columntype = "";
	try {
	    Columntype = this.result.getSchema().getFields().get(column - 1)
		    .getType();
	} catch (IndexOutOfBoundsException e) {
	    throw new SQLException(e);
	}

	if (Columntype.equals("FLOAT"))
	    return java.sql.Types.FLOAT;
	else if (Columntype.equals("BOOLEAN"))
	    return java.sql.Types.BOOLEAN;
	else if (Columntype.equals("INTEGER"))
	    return java.sql.Types.INTEGER;
	else if (Columntype.equals("STRING"))
	    return java.sql.Types.VARCHAR;
	else
	    return 0;
    }

    /** {@inheritDoc} */
    public String getColumnTypeName(int column) throws SQLException {
	if (this.getColumnCount() == 0)
	    throw new IndexOutOfBoundsException();
	String Columntype = "";
	try {
	    Columntype = this.result.getSchema().getFields().get(column - 1)
		    .getType();
	} catch (IndexOutOfBoundsException e) {
	    throw new SQLException(e);
	}
	return Columntype;
    }

    /** {@inheritDoc} */
    public int getPrecision(int column) throws SQLException {
	if (this.getColumnCount() == 0)
	    return 0;
	String Columntype = "";
	try {
	    Columntype = this.result.getSchema().getFields().get(column - 1)
		    .getType();
	} catch (IndexOutOfBoundsException e) {
	    throw new SQLException(e);
	}

	if (Columntype.equals("FLOAT"))
	    return Float.MAX_EXPONENT;
	else if (Columntype.equals("BOOLEAN"))
	    return 1; // A boolean is 1 bit length, but it asks for byte, so 1
		      // byte.
	else if (Columntype.equals("INTEGER"))
	    return Integer.SIZE;
	else if (Columntype.equals("STRING"))
	    return 64 * 1024;
	else
	    return 0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public int getScale(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public String getSchemaName(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public String getTableName(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean isAutoIncrement(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns false
     * </p>
     * 
     * @return false
     */
    public boolean isCaseSensitive(int column) throws SQLException {
	// Google bigquery is case insensitive
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean isCurrency(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns false
     * </p>
     * 
     * @return false
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns ResultSetMetaData.columnNullable
     * </p>
     * 
     * @return ResultSetMetaData.columnNullable
     */
    public int isNullable(int column) throws SQLException {
	return ResultSetMetaData.columnNullable;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns true
     * </p>
     * 
     * @return true
     */
    public boolean isReadOnly(int column) throws SQLException {
	return true;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean isSearchable(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws SQLFeatureNotSupportedException
     * </p>
     * 
     * @throws SQLFeatureNotSupportedException
     */
    public boolean isSigned(int column) throws SQLException {
	throw new SQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns false
     * </p>
     * 
     * @return false
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns false
     * </p>
     * 
     * @return false
     */
    public boolean isWritable(int column) throws SQLException {
	return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always throws SQLExceptionL
     * </p>
     * 
     * @throws SQLException
     *             always
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
	throw new SQLException("Not found");
    }
}