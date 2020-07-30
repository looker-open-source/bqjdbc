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
 * This class implements the java.sql.ResultSet interface's Cursor
 */

package net.starschema.clouddb.jdbc;

import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * This class implements the java.sql.ResultSet interface, as a Forward only resultset
 *
 * @author Balazs Gunics
 *
 *
 */
public class BQForwardOnlyResultSet implements java.sql.ResultSet {

    // Logger logger = new Logger(ScrollableResultset.class.getName());
    Logger logger = Logger.getLogger(BQForwardOnlyResultSet.class.getName());
    /** Reference for holding the current InputStream given back by get methods */
    protected InputStream Strm = null;

    /** The boolean that holds if the last get has given back null or not */
    protected boolean wasnull = false;

    /** The Array which get iterated with cursor it's size can be set with FETCH_SIZE*/
    protected List<TableRow> rowsofResult;

    /** True if on init we have been passed all the rows in the result already. */
    private boolean prefetchedAllRows = false;

    /** This holds if the resultset is closed or not */
    protected boolean closed = false;

    /**Paging size, the original result will be paged by FETCH_SIZE rows     */
    private int FETCH_SIZE = 5000;
    /**The Fetched rows count at the original results     */
    protected BigInteger fetchPos = BigInteger.ZERO;
    /** Are we at the first row? */
    protected boolean AT_FIRST = true;
    /** REference for the original statement which created this resultset     */
    private Statement Statementreference;
    /** schema of results */
    private TableSchema schema;
    /** BigQuery Client */
    private Bigquery bigquery;
    /** the ProjectId */
    private String projectId;
    /** Reference for the Job */
    private Job completedJob;
    /** Cursor position which goes from -1 to FETCH_SIZE then 0 to FETCH_SIZE
     * The -1 is needed because of the while(Result.next() == true) { } iterating method*/
    private int Cursor = -1;

    private final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.of("UTC"));

    public BQForwardOnlyResultSet(Bigquery bigquery, String projectId,
                                  Job completedJob, BQStatementRoot bqStatementRoot) throws SQLException {
        this(bigquery, projectId, completedJob, bqStatementRoot, null, false, null);
    }

    /**
     * Constructor for the forward only resultset
     * @param bigquery - the bigquery client to be used to connect
     * @param projectId - the project which contains the Job
     * @param completedJob - the Job ID, which will be used to get the results, can be null if prefetchedAllRows is true
     * @param bqStatementRoot - reference for the Statement which created the result
     * @param prefetchedRows - array of rows already fetched from BigQuery.
     * @param prefetchedAllRows - true if all rows have already been fetched and we should not ask for any more.
     * @throws SQLException - if we fail to get the results
     */
    public BQForwardOnlyResultSet(Bigquery bigquery, String projectId,
                                  Job completedJob, BQStatementRoot bqStatementRoot,
                                  List<TableRow> prefetchedRows, boolean prefetchedAllRows,
                                  TableSchema schema
                                  ) throws SQLException {
        logger.debug("Created forward only resultset TYPE_FORWARD_ONLY");
        this.Statementreference = (Statement) bqStatementRoot;
        this.bigquery = bigquery;
        this.completedJob = completedJob;
        this.projectId = projectId;
        if (prefetchedRows != null || prefetchedAllRows) {
            // prefetchedAllRows can be true with rows null for an empty result set
            this.rowsofResult = prefetchedRows;
            if (prefetchedRows != null)
                fetchPos = fetchPos.add(BigInteger.valueOf(this.rowsofResult.size()));
            this.prefetchedAllRows = prefetchedAllRows;
            this.schema = schema;
        } else {
            // initial load
            GetQueryResultsResponse result;
            try {
                result = BQSupportFuncts.getQueryResultsDivided(bigquery,
                        projectId, completedJob, fetchPos, FETCH_SIZE);
            } catch (IOException e) {
                throw new BQSQLException("Failed to retrieve data", e);
            } //should not happen
            if (result == null) {  //if we don't have results at all
                this.rowsofResult = null;
            } else if (result.getRows() == null) {  //if we got results, but it was empty
                this.rowsofResult = null;
            } else {                        //we got results, it wasn't empty
                this.rowsofResult = result.getRows();
                this.schema = result.getSchema();
                fetchPos = fetchPos.add(BigInteger.valueOf(this.rowsofResult.size()));
            }
        }
    }

    /**
     * Returns the current rows Object at the given index
     * @param columnIndex - the column to be used
     * @return - the stored value parsed to String, Float etc
     * @throws SQLException - if the resultset is closed or the columnIndex is not valid, or the type is unsupported
     */
    public Object getObject(int columnIndex) throws SQLException {

        String result = getString(columnIndex, false);

        if (this.wasNull()) {
            return null;
        }

        String Columntype = this.schema.getFields().get(columnIndex - 1).getType();

        try {
            if (Columntype.equals("STRING")) {
                return result;
            }
            if (Columntype.equals("FLOAT")) {
                return toDouble(result);
            }
            if (Columntype.equals("BOOLEAN")) {
                return toBoolean(result);
            }
            if (Columntype.equals("INTEGER")) {
                return toLong(result);
            }
            if (Columntype.equals("TIMESTAMP")) {
                return toTimestamp(result, null);
            }
            if (Columntype.equals("DATE")) {
                return toDate(result, null);
            }
            if (Columntype.equals("DATETIME")) {
                // Date time represents a "clock face" time and so should NOT be processed into an actual time
                return result;
            }
            if (Columntype.equals("NUMERIC")) {
                return toBigDecimal(result);
            }
            // For an unknown type, return the result as a string, much better than exploding.
            return result;
        }
        catch (NumberFormatException e) {
            throw new BQSQLException(e);
        }
    }

    // toXXX = common "parsing" between Object and primitive types - to prevent discrepancies. The 4 firsts are the core ones (types supported by BigQuery)

    /** Parse boolean types */
    private Boolean toBoolean(String value) throws SQLException {
        return Boolean.valueOf(value);
    }

    /** Parse floating point types */
    private Double toDouble(String value) throws SQLException {
        try {
            return Double.valueOf(new BigDecimal(value).doubleValue());
        }
        catch (NumberFormatException e) {
            throw new BQSQLException(e);
        }
    }


    /** Parse integral types */
    private Long toLong(String value) throws SQLException {
        try {
            return Long.valueOf(new BigDecimal(value).longValue());
        }
        catch (NumberFormatException e) {
            throw new BQSQLException(e);
        }
    }

    /** Parse date/time types */
    private Timestamp toTimestamp(String value, Calendar cal) throws SQLException {
        try {
            long dbValue = new BigDecimal(value).movePointRight(3).longValue(); // movePointRight(3) =  *1000 (done before rounding) - from seconds (BigQuery specifications) to milliseconds (required by java). Precision under millisecond is discarded (BigQuery supports micro-seconds)
            if (cal == null) {
                cal = Calendar.getInstance(TimeZone.getTimeZone("UTC")); // The time zone of the server that host the JVM should NOT impact the results. Use UTC calendar instead (which wont apply any correction, whatever the time zone of the data)

            }

            Calendar dbCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            dbCal.setTimeInMillis(dbValue);
            cal.set(Calendar.YEAR, dbCal.get(Calendar.YEAR));
            cal.set(Calendar.MONTH, dbCal.get(Calendar.MONTH));
            cal.set(Calendar.DAY_OF_MONTH, dbCal.get(Calendar.DAY_OF_MONTH));
            cal.set(Calendar.HOUR_OF_DAY, dbCal.get(Calendar.HOUR_OF_DAY));
            cal.set(Calendar.MINUTE, dbCal.get(Calendar.MINUTE));
            cal.set(Calendar.SECOND, dbCal.get(Calendar.SECOND));
            cal.set(Calendar.MILLISECOND, dbCal.get(Calendar.MILLISECOND));
            return new Timestamp(cal.getTime().getTime());
        }
        catch (NumberFormatException e) {
            throw new BQSQLException(e);
        }
    }

    // Secondary converters

    /** Parse integral or floating types with (virtually) infinite precision */
    private BigDecimal toBigDecimal(String value) {
        return new BigDecimal(value);
    }

    static Date toDate(String value, Calendar cal) throws SQLException {
        // Dates in BigQuery come back in the YYYY-MM-DD format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            java.util.Date date = sdf.parse(value);
            return new java.sql.Date(date.getTime());
        } catch (java.text.ParseException e) {
            throw new BQSQLException(e);
        }
    }

    private Time toTime(String value, Calendar cal) throws SQLException {
        return new java.sql.Time(toTimestamp(value, cal).getTime());
    }

    @Override
    /**
     * Returns the current rows Data at the given index as String
     * @param columnIndex - the column to be used
     * @return - the stored value parsed to String
     * @throws SQLException - if the resultset is closed or the columnIndex is not valid, or the type is unsupported
     */
    public String getString(int columnIndex) throws SQLException {
        return getString(columnIndex, true);
    }

    private String getString(int columnIndex, boolean formatTimestamps) throws SQLException {
        //to make the logfiles smaller!
        //logger.debug("Function call getString columnIndex is: " + String.valueOf(columnIndex));
        this.closestrm();
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        if (this.getMetaData().getColumnCount() < columnIndex
                || columnIndex < 1) {
            throw new BQSQLException("ColumnIndex is not valid");
        }

        if (this.rowsofResult == null) throw new BQSQLException("Invalid position!");
        Object resultObject = this.rowsofResult.get(this.Cursor).getF().get(columnIndex - 1).getV();
        if (Data.isNull(resultObject)) {
            this.wasnull = true;
            return null;
        }
        this.wasnull = false;
        if (formatTimestamps && getMetaData().getColumnTypeName(columnIndex).equals("TIMESTAMP")) {
            Instant instant = Instant.ofEpochMilli((new BigDecimal((String) resultObject).movePointRight(3)).longValue());
            return TIMESTAMP_FORMATTER.format(instant);
        }
        if (resultObject instanceof List || resultObject instanceof Map) {
            Object resultTransformedWithSchema = smartTransformResult(
                    resultObject,
                    schema.getFields().get(columnIndex - 1));
            resultObject = easyToJson(resultTransformedWithSchema);
        }
        return resultObject.toString();
    }

    private Object smartTransformResult(Object resultObject, TableFieldSchema fieldDef) {
        boolean isList = resultObject instanceof List;
        boolean isMap = resultObject instanceof Map;
        if (isMap && ((Map) resultObject).containsKey("v")) {
            // All values in these nested structs from the API are wrapped up as {"v": <the_val>}, so we unwrap here
            return smartTransformResult(((Map) resultObject).get("v"), fieldDef);
        } else if (fieldDef.getMode().equals("REPEATED") && isList) {
            List asList = ((List) resultObject);
            ArrayList<Object> newList = new ArrayList<>();
            for (Object obj : asList) {
                newList.add(smartTransformResult(obj, fieldDef));
            }
            return newList;
        } else if (fieldDef.getType().equals("RECORD") && isMap) {
            Map asMap = ((Map) resultObject);
            Object nested = asMap.get("f");
            List<TableFieldSchema> nestedFields = fieldDef.getFields();
            if (!(nested instanceof List) || nestedFields == null) {
                // The API sent back something we don't understand
                return resultObject;
            }
            List nestedValues = (List) nested;
            if (nestedValues.size() != nestedFields.size()) {
                return resultObject;
            }
            HashMap<String, Object> newMap = new HashMap<>();
            for (int i = 0, n = nestedFields.size(); i < n; i++) {
                TableFieldSchema nestedFieldDef = nestedFields.get(i);
                newMap.put(nestedFieldDef.getName(), smartTransformResult(nestedValues.get(i), nestedFieldDef));
            }
            return newMap;
        } else {
            // This is just a plain value or the API sent back something we don't understand
            return resultObject;
        }
    }


    private String easyToJson(Object resultObject) throws SQLException {
        Writer writer = new StringWriter();
        try {
            JsonGenerator gen = new JacksonFactory().createJsonGenerator(writer);
            gen.serialize(resultObject);
            gen.close();
        } catch (IOException e) {
            // Um, a string writer is not going to throw an IO exception, but fine.
            throw new BQSQLException("Failed to write JSON", e);
        }
        return writer.toString();
    }

    /**
     * Not supported in forward only resultset
     * @param row
     * @return - SQLException
     * @throws SQLException - this isn't a forward only resultset
     */
    @Override
    public boolean absolute(int row) throws SQLException {
        throw new BQSQLException(
                "The Type of the Resultset is TYPE_FORWARD_ONLY, absolute is not supported");
    }

    /**
     * Not supported in forward only resultset
     * @throws SQLException - this isn't a forward only resultset
     */
    @Override
    public void afterLast() throws SQLException {
        if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
            throw new BQSQLException(
                    "The Type of the Resultset is TYPE_FORWARD_ONLY");
        }
    }

    /**
     * Not supported in forward only resultset
     * @throws SQLException - this isn't a forward only resultset
     */
    @Override
    public void beforeFirst() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
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
    public void cancelRowUpdates() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("cancelWorUpdates()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Currently its a noop
     * </p>
     */
    @Override
    public void clearWarnings() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is closed");
            // TODO implement Warnings
        }
    }

    /**
     * Closes the resultset this function never FAILS!
     */
    @Override
    public void close() throws SQLException {
        // TODO free occupied resources
        this.closed = true;
        this.rowsofResult = null;
    }

    /**
     * If the Strm reference is not null it closes the underlying stream, if an
     * error occurs throws SQLException</p>
     *
     * @throws SQLException
     *             if error occurs while trying to close the stream
     */
    protected void closestrm() throws SQLException {
        if (this.Strm != null) {
            try {
                this.Strm.close();
            } catch (IOException e) {
                throw new BQSQLException(e);
            }
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * We support read only functions in the current version.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void deleteRow() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("deleteRow()");
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
    public int findColumn(String columnLabel) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        int columncount = this.getMetaData().getColumnCount();
        for (int i = 1; i <= columncount; i++) {
            if (this.getMetaData().getCatalogName(i).equals(columnLabel)) {
                return i;
            }
        }
        throw new BQSQLException("No Such column labeled: " + columnLabel);
    }

    /**
     * Not supported in forward only resultset
     * @return - SQLException
     * @throws SQLException - this isn't a forward only resultset
     */
    @Override
    public boolean first() throws SQLException {
        throw new BQSQLException(
                "The Type of the Resultset is TYPE_FORWARD_ONLY");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Arrays are not supported in the current version
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getArray(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Arrays are not supported in the current version
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getArray(string)");
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        this.closestrm();
        java.io.InputStream inptstrm;
        String Value = this.getString(columnIndex);
        if (Value == null) {
            this.wasnull = true;
            this.Strm = null;
            return this.Strm;
        }
        this.wasnull = false;
        try {
            inptstrm = new java.io.ByteArrayInputStream(
                    Value.getBytes("US-ASCII"));
        }
        catch (UnsupportedEncodingException e) {
            throw new BQSQLException(e);
        }
        this.Strm = inptstrm;
        return this.Strm;
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return getAsciiStream(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toBigDecimal(value);
    }

    // Implemented Get functions Using Cursor

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        BigDecimal value = this.getBigDecimal(columnIndex);
        if (value == null) {
            return null;
        }
        return value.setScale(scale);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getBigDecimal(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getBigDecimal(columnIndex, scale);
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        this.closestrm();
        java.io.InputStream inptstrm;
        String Value = this.getString(columnIndex);
        if (Value == null) {
            this.wasnull = true;
            this.Strm = null;
            return this.Strm;
        }
        this.wasnull = false;
        inptstrm = new java.io.ByteArrayInputStream(Value.getBytes());
        this.Strm = inptstrm;
        return this.Strm;
    }

    /** {@inheritDoc} */
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return getBinaryStream(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getBlob(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getBlob(string)");
    }

    /** {@inheritDoc} */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return false;
        }
        return toBoolean(value).booleanValue();
    }

    /** {@inheritDoc} */
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getBoolean(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return 0;
        }
        return toLong(Value).byteValue();
    }

    /** {@inheritDoc} */
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getByte(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return Value.getBytes();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getBytes(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        this.closestrm();
        String Value = this.getString(columnIndex);
        if (Value == null) {
            this.wasnull = true;
            return null;
        }
        this.wasnull = false;
        Reader rdr = new StringReader(Value);
        return rdr;
    }

    /** {@inheritDoc} */
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getCharacterStream(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getClob(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getClob(string)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always Returns ResultSet.CONCUR_READ_ONLY
     * </p>
     *
     * @return ResultSet.CONCUR_READ_ONLY
     */
    @Override
    public int getConcurrency() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        return ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public String getCursorName() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getCursorName()");
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toDate(value, null);
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toDate(value, cal);
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getDate(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getDate(columnIndex, cal);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return 0;
        }
        return toDouble(Value).doubleValue();
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getDouble(columnIndex);
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
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
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
    public int getFetchSize() throws SQLException {
        //throw new BQSQLException("Not implemented." + "getfetchSize()");
        return FETCH_SIZE;
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return 0;
        }
        return toDouble(Value).floatValue();
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getFloat(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Read only mode, no commits.
     * </p>
     *
     * @return CLOSE_CURSORS_AT_COMMIT
     */
    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /** {@inheritDoc} */
    @Override
    public int getInt(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return 0;
        }
        return toLong(Value).intValue();
    }

    /** {@inheritDoc} */
    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getInt(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public long getLong(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return 0;
        }
        return toLong(Value).longValue();
    }

    /** {@inheritDoc} */
    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getLong(columnIndex);
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
    public ResultSetMetaData getMetaData() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        return new BQResultsetMetaData(schema, projectId);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value of getCharacterStream(columnIndex)
     * </p>
     *
     * @see #getCharacterStream(int)
     */
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return this.getCharacterStream(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value of getCharacterStream(columnLabel)
     * </p>
     *
     * @see #getCharacterStream(String)
     */
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return this.getCharacterStream(columnLabel);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getNClob(int");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getNClob(string)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value getString(columnIndex)
     * </p>
     *
     * @see #getString(int)
     */
    @Override
    public String getNString(int columnIndex) throws SQLException {
        return this.getString(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value getString(columnLabel)
     * </p>
     *
     * @see #getString(String)
     */
    @Override
    public String getNString(String columnLabel) throws SQLException {
        return this.getString(columnLabel);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getObject(int,Map)");
        // TODO Implement TypeMaps
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getObject(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not s.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getObject(string,Map)");
        // TODO Implement TypeMaps
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getRef(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getref(String)");
    }

    /** {@inheritDoc} */
    @Override
    public int getRow() throws SQLException {
        throw new BQSQLException(
                "The Type of the Resultset is TYPE_FORWARD_ONLY");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getRowId(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getRowId(String)");
    }

    /** {@inheritDoc} */
    @Override
    public short getShort(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return 0;
        }
        return toLong(value).shortValue();
    }

    /** {@inheritDoc} */
    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getShort(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return new net.starschema.clouddb.jdbc.BQSQLXML(
                this.getString(columnIndex));
    }

    /** {@inheritDoc} */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getSQLXML(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always Returns null
     * </p>
     *
     * @return null
     */
    @Override
    public Statement getStatement() throws SQLException {
        return this.Statementreference;
    }

    /** {@inheritDoc} */
    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getString(columnIndex);

    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toTime(value, null);
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toTime(value, cal);
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getTime(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getTime(columnIndex, cal);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toTimestamp(value, null);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        String value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        return toTimestamp(value, cal);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getTimestamp(columnIndex);
    }

    /** {@inheritDoc} */
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return this.getTimestamp(columnIndex, cal);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns ResultSet.TYPE_SCROLL_INSENSITIVE
     * </p>
     *
     * @return ResultSet.TYPE_SCROLL_INSENSITIVE
     */
    @Override
    public int getType() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException(
                "Deprecated. use getCharacterStream in place of getUnicodeStream");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException(
                "Deprecated. use getCharacterStream in place of getUnicodeStream");
    }

    /** {@inheritDoc} */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String Value = this.getString(columnIndex);
        if (this.wasNull()) {
            return null;
        }
        try {
            return new URL(Value);
        }
        catch (MalformedURLException e) {
            throw new BQSQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        int columnIndex = this.findColumn(columnLabel);
        return getURL(columnIndex);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns null
     * </p>
     *
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO implement error handling
        return null;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void insertRow() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("insertRow()");
    }

    /** {@inheritDoc} */
    @Override
    public boolean isAfterLast() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        throw new BQSQLException("Forward_only resultset doesn't support isAfterLast() ");
    }

    /** {@inheritDoc} */
    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        if (Cursor == -1) return true;
        throw new BQSQLException("Forward_only resultset doesn't support isBeforeFirst() ");
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isFirst() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        return AT_FIRST;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLast() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("This Resultset is Closed");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @return false
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean last() throws SQLException {
        throw new BQSQLException(
                "The Type of the Resultset is TYPE_FORWARD_ONLY");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("moveToCurrentRow()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void moveToInsertRow() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("moveToInsertRow()");
    }

    /** {@inheritDoc} */
    @Override
    public boolean next() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Resultset is Closed");
        }
        if (this.rowsofResult == null) {
            return false;
        }
        if (Cursor < rowsofResult.size() - 1) {
            if (Cursor == -1) {
                AT_FIRST = true;
            } else AT_FIRST = false;
           Cursor++;
           return true;
        }

        if (this.prefetchedAllRows) {
            // Nothing more to do, we've scrolled through all the rows we have.
            this.rowsofResult = null; // this is how we remember we are out of rows
            return false;
        }

        GetQueryResultsResponse result;
        try {
             result = BQSupportFuncts.getQueryResultsDivided(bigquery,
                    projectId, completedJob, fetchPos, FETCH_SIZE);
        }
        catch (IOException e) {
            //should not happen ... according to whoever cooked this up back in the day
            throw new BQSQLException("failed to fetch more results", e);
        }
        if (result.getRows() == null) {
            this.rowsofResult = null;
            return false;
        }
        this.rowsofResult = result.getRows();
        fetchPos = fetchPos.add(BigInteger.valueOf((long)this.rowsofResult.size()));
        Cursor = 0;
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean previous() throws SQLException {
        throw new BQSQLException(
                "The Type of the Resultset is TYPE_FORWARD_ONLY");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not supported.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void refreshRow() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("refreshRow()");
    }

    /** {@inheritDoc} */
    @Override
    public boolean relative(int rows) throws SQLException {
        throw new BQSQLException(
                "The Type of the Resultset is TYPE_FORWARD_ONLY");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public boolean rowDeleted() throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public boolean rowInserted() throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public boolean rowUpdated() throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
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
    public void setFetchDirection(int direction) throws SQLException {
        throw new BQSQLException("Not implemented." + "setFetchDirection(int)");
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
    public void setFetchSize(int rows) throws SQLException {
        FETCH_SIZE = rows;
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
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new BQSQLException("Not implemented." + "unwrap(Class<T>)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x,
                                   long length) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBlob(String columnLabel, InputStream inputStream,
                           long length) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader,
                                      int length) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader reader,
                                      long length) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not impel
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader,
                                       long length) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
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
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateRow() throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws BQSQLFeatureNotSupportedException
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean wasNull() throws SQLException {
        return this.wasnull;
    }

}
