/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * <p>This class implements the java.sql.ResultSet interface for Object[] and Object[][] types
 * instead of GetQueryResultsResponse.getrows()
 */
package net.starschema.clouddb.jdbc;

import java.sql.SQLException;
import net.starschema.clouddb.jdbc.DMDResultSet.DMDResultSetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

/**
 * this class implements the java.sql.ResultSetMetaData interface for usage in Resultsets which was
 * made from DatabaseMetadata
 *
 * @author Horváth Attila
 */
class COLResultSetMetadata implements java.sql.ResultSetMetaData {
  String[][] data = null;
  String[] labels;

  /** Instance of logger */
  static Logger logger = LoggerFactory.getLogger(COLResultSetMetadata.class);

  DMDResultSetType MetaDataType;

  /**
   * Constructor initializes variables
   *
   * @param data The column informations
   * @param labels The name of columns
   */
  public COLResultSetMetadata(String[][] data, String[] labels, DMDResultSetType type) {
    this.labels = labels;
    this.data = data;
    this.MetaDataType = type;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns this.data[0][0]
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getCatalogName parameter: " + column);
    if (this.MetaDataType == DMDResultSetType.getColumns) {
      return this.data[0][0];
    }
    if (this.MetaDataType == DMDResultSetType.getSchemas) {
      return this.data[column - 1][1];
    } else {
      return "";
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns java.lang.String.class.toString()
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnClassName parameter: " + column);
    return java.lang.String.class.toString();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns this.labels.length
   */
  @Override
  public int getColumnCount() throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnCount");
    return this.labels.length;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns 64*1024
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnDisplaySize");
    return 64 * 1024;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns this.labels[column - 1]
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnLabel parameter: " + column);
    return this.labels[column - 1];
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns this.labels[column - 1]
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnName parameter: " + column);
    return this.labels[column - 1];
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns java.sql.Types.VARCHAR
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnType parameter: " + column);
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns this.data[0][5]
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getColumnTypeName parameter: " + column);
    /*
     * if(this.MetaDataType == DMDResultSetType.getColumns) return
     * this.data[0][5]; else
     */
    return "VARCHAR";
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns 0
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getPrecision parameter: " + column);
    return 0;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns 0
   */
  @Override
  public int getScale(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getScale parameter: " + column);
    return 0;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns this.data[0][1]
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getSchemaName parameter: " + column);
    if (this.MetaDataType == DMDResultSetType.getColumns) {
      return this.data[0][1];
    }
    if (this.MetaDataType == DMDResultSetType.getSchemas) {
      return this.data[column - 1][0];
    } else {
      return "";
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns this.data[0][2]
   */
  @Override
  public String getTableName(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getTableName parameter: " + column);
    if (this.MetaDataType == DMDResultSetType.getColumns) {
      return this.data[0][2];
    } else {
      return "";
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isAutoIncrement parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isCaseSensitive parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isCurrency parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isDefinitelyWritable parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns 1
   */
  @Override
  public int isNullable(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call getTableName parameter: " + column);
    return 1;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns true
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isReadOnly parameter: " + column);
    return true;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isSearchable parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isSigned parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns false
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    COLResultSetMetadata.logger.debug("Function Call isWritable parameter: " + column);
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Always Throws SQLException
   *
   * @throws SQLException always
   */
  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new BQSQLException("not found");
  }
}

/**
 * This class implements the java.sql.ResultSet interface for Object[] and
 *
 * <p>Object[][] types instead of GetQueryResultsResponse.getrows() for giving back Resultset Data
 * from BQDatabaseMetadata
 *
 * @author Horváth Attila
 */
public class DMDResultSet extends ScrollableResultset<Object> implements java.sql.ResultSet {

  /** enum declaration that manages function call identity */
  public static enum DMDResultSetType {
    getAttributes,
    getBestRowIdentifier,
    getCatalogs,
    getClientInfoProperties,
    getColumnPrivileges,
    getColumns,
    getCrossReference,
    getExportedKeys,
    getFunctionColumns,
    getFunctions,
    getImportedKeys,
    getIndexInfo,
    getPrimaryKeys,
    getProcedureColumns,
    getProcedures,
    getSchemas,
    getSuperTables,
    getSuperTypes,
    getTablePrivileges,
    getTables,
    getTableTypes,
    getTypeInfo,
    getUDTs,
    getVersionColumns
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
   * @param objects data to contain
   * @param colnames names of columns
   */
  public DMDResultSet(Object[] objects, String[] colnames, DMDResultSetType type) {
    this.RowsofResult = objects;
    this.Colnames = colnames;
    this.ResultsetType = type;
    this.logger = LoggerFactory.getLogger(DMDResultSet.class);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws SQLException {
    super.close();
    this.Colnames = null;
  }

  /** {@inheritDoc} */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    for (int i = 0; i < this.Colnames.length; i++) {
      if (this.Colnames[i].equals(columnLabel)) {
        return i + 1;
      }
    }
    throw new BQSQLException("No such column");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new BQSQLException("Not implemented.");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new BQSQLException("Not implemented.");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns a COLResultSetMetadata
   */
  @Override
  public java.sql.ResultSetMetaData getMetaData() throws SQLException {
    // logger.debug("Function call getMetaData()");
    try {
      try {
        Object[][] Containter = Object[][].class.cast(this.RowsofResult);

        String[][] data = null;
        if (Containter.length == 0) {
          data = new String[0][0];
        } else {
          data = new String[Containter.length][Containter[0].length];
        }

        for (int i = 0; i < Containter.length; i++) {
          for (int k = 0; k < Containter[i].length; k++) {
            if (Containter[i][k] == null) {
              data[i][k] = null;
            } else {
              data[i][k] = Containter[i][k].toString();
            }
          }
        }

        return new COLResultSetMetadata(data, this.Colnames, this.ResultsetType);

      } catch (ClassCastException e) {

        String[][] data;
        if (this.RowsofResult.length == 0) {
          data = new String[0][0];
        } else {
          data = new String[1][this.RowsofResult.length];
        }

        for (int i = 0; i < this.RowsofResult.length; i++) {
          if (this.RowsofResult[i] == null) {
            data[0][i] = null;
          } else {
            data[0][i] = this.RowsofResult[i].toString();
          }
        }
        return new COLResultSetMetadata(data, this.Colnames, this.ResultsetType);
      }
    } catch (Exception e) {
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
    } else {
      try {
        Object[][] Containter = Object[][].class.cast(this.RowsofResult);
        if (columnIndex > Containter[this.Cursor].length) {
          throw new BQSQLException("ColumnIndex is not valid");
        } else {
          if (Containter[this.Cursor][columnIndex - 1] == null) {
            this.wasnull = true;
            this.logger.debug("Returning: null");
          } else {
            this.wasnull = false;
            this.logger.debug("Returning: " + Containter[this.Cursor][columnIndex - 1].toString());
          }
          return Containter[this.Cursor][columnIndex - 1];
        }
      } catch (ClassCastException e) {
        if (columnIndex == 1) {
          if (this.RowsofResult[this.Cursor] == null) {
            this.wasnull = true;
          } else {
            this.wasnull = false;
          }
          this.logger.debug("Returning: " + this.RowsofResult[this.Cursor].toString());
          return this.RowsofResult[this.Cursor];
        } else {
          throw new BQSQLException(e);
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getString(int columnIndex) throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    this.ThrowCursorNotValidExeption();

    if (this.RowsofResult == null) {
      throw new BQSQLException("No Valid Rows");
    }

    if (columnIndex < 1) {
      throw new BQSQLException("ColumnIndex is not valid");
    } else {
      try {
        Object[][] Containter = Object[][].class.cast(this.RowsofResult);
        if (columnIndex > Containter[this.Cursor].length) {
          throw new BQSQLException("ColumnIndex is not valid");
        } else if (Containter[this.Cursor][columnIndex - 1] == null) {
          this.wasnull = true;
          this.logger.debug("Returning: Null");
          return null;
        } else {
          this.wasnull = false;
          this.logger.debug("Returning: " + Containter[this.Cursor][columnIndex - 1].toString());
          return Containter[this.Cursor][columnIndex - 1].toString();
        }
      } catch (ClassCastException e) {
        if (columnIndex == 1) {
          if (this.RowsofResult[this.Cursor] == null) {
            this.wasnull = true;
            this.logger.debug("Returning: Null");
            return null;
          } else {
            this.wasnull = false;
            this.logger.debug("Returning: " + this.RowsofResult[this.Cursor].toString());
            return this.RowsofResult[this.Cursor].toString();
          }
        } else {
          throw new BQSQLException("ColumnIndex is not valid");
        }
      }
    }
  }
}
