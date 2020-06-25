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

import java.sql.*;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * This class implements the java.sql.ResultSetMetadata interface
 *
 * @author Horváth Attila
 */
class BQResultsetMetaData implements ResultSetMetaData {

    TableSchema schema;
    String projectId;

    QueryResponse result = null;

    /** Logger instance */
    Logger logger = Logger.getLogger(BQResultsetMetaData.class.getName());

    /**
     * Constructor that initializes variables
     *
     * @param result the bigquery GetQueryResultsResponse object
     */
    public BQResultsetMetaData(GetQueryResultsResponse result) {
        this(result.getSchema(), result.getJobReference().getProjectId());
    }

    public BQResultsetMetaData(TableSchema schema, String projectId) {
        this.schema = schema;
        this.projectId = projectId;
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
        logger.debug("function call getCatalogName() return is: " + projectId);
        return projectId;
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
            Columntype = schema.getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException(e);
        } catch (NullPointerException e) {
            throw new BQSQLException(e);
        }

        if (Columntype.equals("FLOAT")) {
            return Double.class.getName();
        }
        if (Columntype.equals("BOOLEAN")) {
            return Boolean.class.getName();
        }
        if (Columntype.equals("INTEGER")) {
            return Long.class.getName();
        }
        if (Columntype.equals("STRING")) {
            return String.class.getName();
        }
        if (Columntype.equals("TIMESTAMP")) {
            return Timestamp.class.getName();
        }
        if (Columntype.equals("DATE")) {
            return Date.class.getName();
        }
        if (Columntype.equals("RECORD") || Columntype.equals("STRUCT")) {
            return Struct.class.getName();
        }

        throw new BQSQLException("Unsupported Type: " + Columntype);
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnCount() throws SQLException {
        List<TableFieldSchema> schemafieldlist = null;
        if (schema != null) {
            schemafieldlist = schema.getFields();
        } else {
            return 0;
        }
        if (schemafieldlist != null) {
            return schema.getFields().size();
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
            return schema.getFields().get(column - 1)
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
            return schema.getFields().get(column - 1)
                    .getName();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getColumnName(int)", e);
        }
    }

    /**
     * {@inheritDoc} <br>
     * note: see below for BQ type => Java type enum<br>
     * INTEGER => java.sql.Types.BIGINT<br>
     * NUMERIC => java.sql.Types.NUMERIC<br>
     * FLOAT => java.sql.Types.DOUBLE<br>
     * BOOLEAN => java.sql.Types.BOOLEAN<br>
     * STRING => java.sql.Types.VARCHAR<br>
     * BYTES => java.sql.Types.VARCHAR<br>
     * DATE => java.sql.Types.DATE<br>
     * DATETIME => java.sql.Types.TIMESTAMP<br>
     * TIME => java.sql.Types.TIME<br>
     * TIMESTAMP => java.sql.Types.TIMESTAMP<br>
     * ARRAY => unsupported<br>
     * STRUCT => java.sql.Types.STRUCT<br>
     * GEOGRAPHY => unsupported<br>
     *
     * If making changes to this method, please ensure that these types stay 1:1 with the types listed here:
     *   https://cloud.google.com/bigquery/data-types
     *   https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
     * */
    @Override
    public int getColumnType(int column) throws SQLException {
        if (this.getColumnCount() == 0) {
            return 0;
        }
        String Columntype = "";
        try {
            Columntype = schema.getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getColumnType(int)", e);
        }

        if (Columntype.equals("FLOAT")) {
            return java.sql.Types.DOUBLE; // Float in BigQuery maps to the DOUBLE sql type
        }

        if (Columntype.equals("BOOLEAN")) {
            return java.sql.Types.BOOLEAN;
        }

        if (Columntype.equals("INTEGER")) {
            return java.sql.Types.BIGINT; // INTEGER in BigQuery has 64 bits so it maps BIGINT sql type (Sql INTEGER is "only" 32 bits)
        }

        if (Columntype.equals("STRING")) {
            return java.sql.Types.VARCHAR;
        }

        if (Columntype.equals("TIMESTAMP")) {
            return java.sql.Types.TIMESTAMP;
        }

        if (Columntype.equals("DATE")) {
            return java.sql.Types.DATE;
        }

        if (Columntype.equals("DATETIME")) {
            return java.sql.Types.TIMESTAMP;
        }

        if (Columntype.equals("NUMERIC")) {
            return java.sql.Types.NUMERIC;
        }

        if (Columntype.equals("TIME")) {
            return Types.TIME;
        }

        if (Columntype.equals("BYTES")) {
            return Types.VARCHAR;
        }

        if (Columntype.equals("RECORD") || Columntype.equals("STRUCT")) {
            return java.sql.Types.STRUCT;
        }

        throw new BQSQLException("Unsupported Type: " + Columntype); // May arise if a new data type is added to BigQuery. A new release of the driver would then be needed in order to map it correctly
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
            Columntype = schema.getFields().get(column - 1)
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
            Columntype = schema.getFields().get(column - 1)
                    .getType();
        } catch (IndexOutOfBoundsException e) {
            throw new BQSQLException("getPrecision(int)", e);
        }

        if (Columntype.equals("FLOAT")) {
            return Float.MAX_EXPONENT;
        } 
        if (Columntype.equals("BOOLEAN")) {
            return 1; // A boolean is 1 bit length, but it asks for byte, so
            // 1
        }
        if (Columntype.equals("INTEGER")) {
            return 64; // BigQuery's Integer have 64 bits
        }
        if (Columntype.equals("STRING")) {
            return 64 * 1024;
        }
        if (Columntype.equals("TIMESTAMP")) {
            return 50; // TODO: better computation of the maximum length of a string representation of the date
        }
        if (Columntype.equals("DATE")) {
            return 10;
        }
        if (Columntype.equals("DATETIME")) {
            return 50;
        }
        if (Columntype.equals("RECORD") || Columntype.equals("STRUCT")) {
            return 1024; // TODO: more accurate precision for RECORDs and STRUCTs
        }
        return 0;
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
        // This returns zero always. It would be better for it to throw a NotImplemented exception,
        // but at one point it was written with some code that tried to do something silly with decimals but always
        // ended up returning zero, so for now just go with constant return 0.
        return 0;
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

