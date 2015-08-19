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
 *
 * This class implements the java.sql.ResultSetMetaData interface
 */

package net.starschema.clouddb.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * This class implements the java.sql.ResultSetMetadata interface
 *
 * @author Horváth Attila
 */
class BQResultsetMetaData implements ResultSetMetaData {

    /** Reference of the bigquery GetQueryResultsResponse object */
    GetQueryResultsResponse result = null;

    /** Reference of the bigquery GetQueryResultsResponse object */
    ResultSet results = null;

    /** Logger instance */
    Logger logger = Logger.getLogger(BQResultsetMetaData.class.getName());

    /**
     * Constructor that initializes variables
     *
     * @param result the bigquery GetQueryResultsResponse object
     */
    public BQResultsetMetaData(GetQueryResultsResponse result) {
        //logger.debug("function call getResultSetMetaData()");
        this.result = result;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the BigQuery Projects ID
     * </p>
     *
     * @return projectID
     */
    @Override
    public String getCatalogName(int column) throws SQLException {
        logger.debug("function call getCatalogName() return is: " +
                this.result.getJobReference().getProjectId());
        return this.result.getJobReference().getProjectId();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        this.logger.debug("Function call getcolumnclassname(" + column + ")");

        String Columntype = null;
        try {
            Columntype = this.result.getSchema().getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException(e);
        } catch (NullPointerException e) {
            throw new BQSQLException(e);
        }

        if (Columntype.equals("FLOAT")) {
            return Float.class.getName();
        } else if (Columntype.equals("BOOLEAN")) {
            return Boolean.class.getName();
        } else if (Columntype.equals("INTEGER")) {
            return Integer.class.getName();
        } else if (Columntype.equals("STRING")) {
            return String.class.getName();
        } else {
            throw new BQSQLException("Unsupported Type");
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnCount() throws SQLException {
        TableSchema schema = this.result.getSchema();
        List<TableFieldSchema> schemafieldlist = null;
        if (schema != null) {
            schemafieldlist = schema.getFields();
        } else {
            return 0;
        }
        if (schemafieldlist != null) {
            return this.result.getSchema().getFields().size();
        } else {
            return 0;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns 64*1024
     * </p>
     */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 1024 * 64;
        // TODO Check the maximum lenght of characters contained in this
        // resulset for each column
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnLabel(int column) throws SQLException {

        if (this.getColumnCount() == 0) {
            throw new IndexOutOfBoundsException();
        }
        try {
            return this.result.getSchema().getFields().get(column - 1)
                    .getName();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnName(int column) throws SQLException {
        if (this.getColumnCount() == 0) {
            throw new BQSQLException("getColumnName(int)",
                    new IndexOutOfBoundsException());
        }
        try {
            return this.result.getSchema().getFields().get(column - 1)
                    .getName();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getColumnName(int)", e);
        }
    }

    /**
     * {@inheritDoc} <br>
     * note: This Can only Return due to bigquery:<br>
     * java.sql.Types.FLOAT<br>
     * java.sql.Types.BOOLEAN<br>
     * java.sql.Types.INTEGER<br>
     * java.sql.Types.VARCHAR
     */
    @Override
    public int getColumnType(int column) throws SQLException {
        if (this.getColumnCount() == 0) {
            return 0;
        }
        String Columntype = "";
        try {
            Columntype = this.result.getSchema().getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getColumnType(int)", e);
        }

        if (Columntype.equals("FLOAT")) {
            return java.sql.Types.FLOAT;
        } else if (Columntype.equals("BOOLEAN")) {
            return java.sql.Types.BOOLEAN;
        } else if (Columntype.equals("INTEGER")) {
            return java.sql.Types.INTEGER;
        } else if (Columntype.equals("STRING")) {
            return java.sql.Types.VARCHAR;
        } else {
            return 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (this.getColumnCount() == 0) {
            throw new BQSQLException("getcolumnTypeName(int)",
                    new IndexOutOfBoundsException());
        }
        String Columntype = "";
        try {
            Columntype = this.result.getSchema().getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getColumnTypeName(int)", e);
        }
        return Columntype;
    }

    /** {@inheritDoc} */
    @Override
    public int getPrecision(int column) throws SQLException {
        if (this.getColumnCount() == 0) {
            return 0;
        }
        String Columntype = "";
        try {
            Columntype = this.result.getSchema().getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getPrecision(int)", e);
        }

        if (Columntype.equals("FLOAT")) {
            return Float.MAX_EXPONENT;
        } else if (Columntype.equals("BOOLEAN")) {
            return 1; // A boolean is 1 bit length, but it asks for byte, so
            // 1
        } else if (Columntype.equals("INTEGER")) {
            return Integer.SIZE;
        } else if (Columntype.equals("STRING")) {
            return 64 * 1024;
        } else {
            return 0;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */
    @Override
    public int getScale(int column) throws SQLException {
        if (this.getColumnType(column) == java.sql.Types.FLOAT) {
            int max = 0;
            for (int i = 0; i < this.result.getRows().size(); i++) {
                String rowdata = (String) this.result.getRows().get(i).getF().get(column - 1).getV();
                if (rowdata.contains(".")) {
                    int pointback = rowdata.length() - rowdata.indexOf(".");
                    if (pointback > max) {
                        pointback = max;
                    }
                }
            }
            return max;
        } else {
            return 0;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * No support in bigquery to get the schema for the column.
     * </p>
     *
     * @return ""
     */
    @Override
    public String getSchemaName(int column) throws SQLException {
        logger.debug("Function call getSchemaName(" + column +
                ") will return empty string ");
        return "";
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * No option in BigQuery api to get the Table name from column.
     * </p>
     *
     * @return ""
     */
    @Override
    public String getTableName(int column) throws SQLException {
        logger.debug("Function call getTableName(" + column +
                ") will return empty string ");
        return "";
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * No ato increment option in bigquery.
     * </p>
     *
     * @return false
     */
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
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
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        //FIXME really?
        // Google bigquery is case insensitive
        return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * We store everything as string.
     * </p>
     *
     * @return false
     */
    @Override
    public boolean isCurrency(int column) throws SQLException {
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
    @Override
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
    @Override
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
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Everything can be in Where.
     * </p>
     *
     * @return true
     */
    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Partly implemented.
     * </p>
     *
     * @return false;
     */
    @Override
    public boolean isSigned(int column) throws SQLException {
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
    @Override
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
    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always throws SQLExceptionL
     * </p>
     *
     * @throws SQLException always
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new BQSQLException("Not found");
    }
}
