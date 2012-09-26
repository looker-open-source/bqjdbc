/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * This class implements the java.sql.ResultSet interface for Object[] and
 * Object[][] types instead of
 * GetQueryResultsResponse.getrows()
 */

package net.starschema.clouddb.jdbc;

import java.sql.SQLException;

import net.starschema.clouddb.jdbc.DMDResultSet.DMDResultSetType;

import org.apache.log4j.Logger;

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
    
    /** Instance of logger */
    Logger logger;
    
    DMDResultSetType MetaDataType;
    
    /**
     * Constructor initializes variables
     * 
     * @param data
     *            The column informations
     * @param labels
     *            The name of columns
     */
    public COLResultSetMetadata(String[][] data, String[] labels,
            DMDResultSetType type) {
        this.labels = labels;
        this.data = data;
        this.MetaDataType = type;
        this.logger = Logger.getLogger(COLResultSetMetadata.class);
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns this.data[0][0]
     * </p>
     */
    @Override
    public String getCatalogName(int column) throws SQLException {
        this.logger.debug("Function Call getCatalogName parameter: " + column);
        if (this.MetaDataType == DMDResultSetType.getColumns) {
            return this.data[0][0];
        }
        if (this.MetaDataType == DMDResultSetType.getSchemas) {
            return this.data[column - 1][1];
        }
        else {
            return "";
        }
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns java.lang.String.class.toString()
     * </p>
     */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        this.logger.debug("Function Call getColumnClassName parameter: "
                + column);
        return java.lang.String.class.toString();
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.labels.length
     * </p>
     */
    @Override
    public int getColumnCount() throws SQLException {
        this.logger.debug("Function Call getColumnCount");
        return this.labels.length;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 64*1024
     * </p>
     */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        this.logger.debug("Function Call getColumnDisplaySize");
        return 64 * 1024;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.labels[column - 1]
     * </p>
     */
    @Override
    public String getColumnLabel(int column) throws SQLException {
        this.logger.debug("Function Call getColumnLabel parameter: " + column);
        return this.labels[column - 1];
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.labels[column - 1]
     * </p>
     */
    @Override
    public String getColumnName(int column) throws SQLException {
        this.logger.debug("Function Call getColumnName parameter: " + column);
        return this.labels[column - 1];
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns java.sql.Types.VARCHAR
     * </p>
     */
    @Override
    public int getColumnType(int column) throws SQLException {
        this.logger.debug("Function Call getColumnType parameter: " + column);
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
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        this.logger.debug("Function Call getColumnTypeName parameter: "
                + column);
        /*
         * if(this.MetaDataType == DMDResultSetType.getColumns) return
         * this.data[0][5]; else
         */
        return "VARCHAR";
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 0
     * </p>
     */
    @Override
    public int getPrecision(int column) throws SQLException {
        this.logger.debug("Function Call getPrecision parameter: " + column);
        return 0;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 0
     * </p>
     */
    @Override
    public int getScale(int column) throws SQLException {
        this.logger.debug("Function Call getScale parameter: " + column);
        return 0;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.data[0][1]
     * </p>
     */
    @Override
    public String getSchemaName(int column) throws SQLException {
        this.logger.debug("Function Call getSchemaName parameter: " + column);
        if (this.MetaDataType == DMDResultSetType.getColumns) {
            return this.data[0][1];
        }
        if (this.MetaDataType == DMDResultSetType.getSchemas) {
            return this.data[column - 1][0];
        }
        else {
            return "";
        }
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns this.data[0][2]
     * </p>
     */
    @Override
    public String getTableName(int column) throws SQLException {
        this.logger.debug("Function Call getTableName parameter: " + column);
        if (this.MetaDataType == DMDResultSetType.getColumns) {
            return this.data[0][2];
        }
        else {
            return "";
        }
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        this.logger.debug("Function Call isAutoIncrement parameter: " + column);
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        this.logger.debug("Function Call isCaseSensitive parameter: " + column);
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isCurrency(int column) throws SQLException {
        this.logger.debug("Function Call isCurrency parameter: " + column);
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        this.logger.debug("Function Call isDefinitelyWritable parameter: "
                + column);
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 1
     * </p>
     */
    @Override
    public int isNullable(int column) throws SQLException {
        this.logger.debug("Function Call getTableName parameter: " + column);
        return 1;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns true
     * </p>
     */
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        this.logger.debug("Function Call isReadOnly parameter: " + column);
        return true;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isSearchable(int column) throws SQLException {
        this.logger.debug("Function Call isSearchable parameter: " + column);
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isSigned(int column) throws SQLException {
        this.logger.debug("Function Call isSigned parameter: " + column);
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns false
     * </p>
     */
    @Override
    public boolean isWritable(int column) throws SQLException {
        this.logger.debug("Function Call isWritable parameter: " + column);
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
    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new BQSQLException("not found");
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
    
    /** enum declaration that manages function call identity */
    public static enum DMDResultSetType {
        getAttributes, getBestRowIdentifier, getCatalogs, getClientInfoProperties, getColumnPrivileges, getColumns, getCrossReference, getExportedKeys, getFunctionColumns, getFunctions, getImportedKeys, getIndexInfo, getPrimaryKeys, getProcedureColumns, getProcedures, getSchemas, getSuperTables, getSuperTypes, getTablePrivileges, getTables, getTableTypes, getTypeInfo, getUDTs, getVersionColumns
    }
    
    /** Instance of logger */
    Logger logger;
    
    /** Var to store the resultsettype */
    DMDResultSetType ResultsetType;
    
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
    public DMDResultSet(Object[] objects, String[] colnames,
            DMDResultSetType type) {
        this.RowsofResult = objects;
        this.Colnames = colnames;
        this.ResultsetType = type;
        this.logger = Logger.getLogger(DMDResultSet.class);
    }
    
    /** {@inheritDoc} */
    @Override
    public void close() throws SQLException {
        super.close();
        this.Colnames = null;
    };
    
    /** {@inheritDoc} */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < this.Colnames.length; i++) {
            if (this.Colnames[i].equals(columnLabel)) {
                return i + 1;
            }
        }
        throw new BQSQLException("No such column");
    };
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns a COLResultSetMetadata
     * </p>
     */
    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        
        try {
            try {
                Object[][] Containter = Object[][].class
                        .cast(this.RowsofResult);
                
                String[][] data = null;
                if (Containter.length == 0) {
                    data = new String[0][0];
                }
                else {
                    data = new String[Containter.length][Containter[0].length];
                }
                
                for (int i = 0; i < Containter.length; i++) {
                    for (int k = 0; k < Containter[i].length; k++) {
                        if (Containter[i][k] == null) {
                            data[i][k] = null;
                        }
                        else {
                            data[i][k] = Containter[i][k].toString();
                        }
                    }
                }
                
                return new COLResultSetMetadata(data, this.Colnames,
                        this.ResultsetType);
                
            }
            catch (ClassCastException e) {
                
                String[][] data;
                if (this.RowsofResult.length == 0) {
                    data = new String[0][0];
                }
                else {
                    data = new String[1][this.RowsofResult.length];
                }
                
                for (int i = 0; i < this.RowsofResult.length; i++) {
                    if (this.RowsofResult[i] == null) {
                        data[0][i] = null;
                    }
                    else {
                        data[0][i] = this.RowsofResult[i].toString();
                    }
                }
                return new COLResultSetMetadata(data, this.Colnames,
                        this.ResultsetType);
            }
        }
        catch (Exception e) {
            throw new BQSQLException(e);
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        this.logger.debug("Function Call getObject Parameter: " + columnIndex);
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        this.ThrowCursorNotValidExeption();
        if (this.RowsofResult == null) {
            throw new BQSQLException("No Valid Rows");
        }
        
        if (columnIndex < 1) {
            throw new BQSQLException("ColumnIndex is not valid");
        }
        else {
            try {
                Object[][] Containter = Object[][].class
                        .cast(this.RowsofResult);
                if (columnIndex > Containter[this.Cursor].length) {
                    throw new BQSQLException("ColumnIndex is not valid");
                }
                else {
                    if (Containter[this.Cursor][columnIndex - 1] == null) {
                        this.wasnull = true;
                    }
                    else {
                        this.wasnull = false;
                    }
                    this.logger.debug("Returning: "
                            + Containter[this.Cursor][columnIndex - 1]
                                    .toString());
                    return Containter[this.Cursor][columnIndex - 1];
                }
            }
            catch (ClassCastException e) {
                if (columnIndex == 1) {
                    if (this.RowsofResult[this.Cursor] == null) {
                        this.wasnull = true;
                    }
                    else {
                        this.wasnull = false;
                    }
                    this.logger.debug("Returning: "
                            + this.RowsofResult[this.Cursor].toString());
                    return this.RowsofResult[this.Cursor];
                }
                else {
                    throw new BQSQLException(e);
                }
            }
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public String getString(int columnIndex) throws SQLException {
        this.logger.debug("Function Call getString Parameter: " + columnIndex);
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        this.ThrowCursorNotValidExeption();
        
        if (this.RowsofResult == null) {
            throw new BQSQLException("No Valid Rows");
        }
        
        if (columnIndex < 1) {
            throw new BQSQLException("ColumnIndex is not valid");
        }
        else {
            try {
                Object[][] Containter = Object[][].class
                        .cast(this.RowsofResult);
                if (columnIndex > Containter[this.Cursor].length) {
                    throw new BQSQLException("ColumnIndex is not valid");
                }
                else
                    if (Containter[this.Cursor][columnIndex - 1] == null) {
                        this.wasnull = true;
                        this.logger.debug("Returning: Null");
                        return null;
                    }
                    else {
                        this.wasnull = false;
                        this.logger.debug("Returning: "
                                + Containter[this.Cursor][columnIndex - 1]
                                        .toString());
                        return Containter[this.Cursor][columnIndex - 1]
                                .toString();
                    }
            }
            catch (ClassCastException e) {
                if (columnIndex == 1) {
                    if (this.RowsofResult[this.Cursor] == null) {
                        this.wasnull = true;
                        this.logger.debug("Returning: Null");
                        return null;
                    }
                    else {
                        this.wasnull = false;
                        this.logger.debug("Returning: "
                                + this.RowsofResult[this.Cursor].toString());
                        return this.RowsofResult[this.Cursor].toString();
                    }
                }
                else {
                    throw new BQSQLException("ColumnIndex is not valid");
                }
            }
        }
    }
}