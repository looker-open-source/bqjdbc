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
 * <p>This class implements the java.sql.ResultSet interface's Cursor
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.BiEngineReason;
import com.google.api.services.bigquery.model.BiEngineStatistics;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the java.sql.ResultSet interface, as a Forward only resultset
 *
 * @author Balazs Gunics
 */
public class BQForwardOnlyResultSet implements java.sql.ResultSet {

  Logger logger = LoggerFactory.getLogger(BQForwardOnlyResultSet.class);
  /** Reference for holding the current InputStream given back by get methods */
  protected InputStream Strm = null;

  /** The boolean that holds if the last get has given back null or not */
  protected boolean wasnull = false;

  /** The Array which get iterated with cursor it's size can be set with FETCH_SIZE */
  protected List<TableRow> rowsofResult;

  /** True if on init we have been passed all the rows in the result already. */
  private boolean prefetchedAllRows = false;

  /** This holds if the resultset is closed or not */
  protected boolean closed = false;

  /** Paging size, the original result will be paged by FETCH_SIZE rows */
  private int FETCH_SIZE = 5000;
  /** The Fetched rows count at the original results */
  protected BigInteger fetchPos = BigInteger.ZERO;
  /** Are we at the first row? */
  protected boolean AT_FIRST = true;
  /** REference for the original statement which created this resultset */
  private Statement Statementreference;
  /** schema of results */
  private TableSchema schema;
  /** BigQuery Client */
  private Bigquery bigquery;
  /** the ProjectId */
  private String projectId;
  /** Reference for the Job */
  private @Nullable Job completedJob;
  /** The BigQuery query ID; set if the query completed without a Job */
  private final @Nullable String queryId;
  /** The total number of bytes processed while creating this ResultSet */
  private final @Nullable Long totalBytesProcessed;
  /** Whether the ResultSet came from BigQuery's cache */
  private final @Nullable Boolean cacheHit;
  /** Specifies which mode of BI Engine acceleration was performed (if any). */
  private final @Nullable String biEngineMode;
  /**
   * In case of DISABLED or PARTIAL bi_engine_mode, these contain the explanatory reasons as to why
   * BI Engine could not accelerate. In case the full query was accelerated, this field is not
   * populated.
   */
  private final @Nullable List<BiEngineReason> biEngineReasons;
  /**
   * Cursor position which goes from -1 to FETCH_SIZE then 0 to FETCH_SIZE The -1 is needed because
   * of the while(Result.next() == true) { } iterating method
   */
  private int Cursor = -1;

  /**
   * Constructor without query ID for backwards compatibility.
   *
   * @param bigquery Bigquery driver instance for which this is a result
   * @param projectId the project from which these results were queried
   * @param completedJob the query's job, if any
   * @param bqStatementRoot the statement for which this is a result
   * @throws SQLException thrown if the results can't be retrieved
   */
  public BQForwardOnlyResultSet(
      Bigquery bigquery,
      String projectId,
      @Nullable Job completedJob,
      BQStatementRoot bqStatementRoot)
      throws SQLException {
    this(
        bigquery,
        projectId,
        completedJob,
        null,
        bqStatementRoot,
        null,
        false,
        null,
        0L,
        false,
        null,
        null);
  }

  public BQForwardOnlyResultSet(
      Bigquery bigquery,
      String projectId,
      @Nullable Job completedJob,
      @Nullable String queryId,
      BQStatementRoot bqStatementRoot)
      throws SQLException {
    this(
        bigquery,
        projectId,
        completedJob,
        queryId,
        bqStatementRoot,
        null,
        false,
        null,
        0L,
        false,
        null,
        null);
  }

  /**
   * Constructor for the forward only resultset
   *
   * @param bigquery - the bigquery client to be used to connect
   * @param projectId - the project which contains the Job
   * @param completedJob - the Job ID, which will be used to get the results, can be null if
   *     prefetchedAllRows is true
   * @param bqStatementRoot - reference for the Statement which created the result
   * @param prefetchedRows - array of rows already fetched from BigQuery.
   * @param prefetchedAllRows - true if all rows have already been fetched and we should not ask for
   *     any more.
   * @throws SQLException - if we fail to get the results
   */
  public BQForwardOnlyResultSet(
      Bigquery bigquery,
      String projectId,
      @Nullable Job completedJob,
      @Nullable String queryId,
      BQStatementRoot bqStatementRoot,
      List<TableRow> prefetchedRows,
      boolean prefetchedAllRows,
      TableSchema schema,
      @Nullable Long totalBytesProcessed,
      @Nullable Boolean cacheHit,
      @Nullable String biEngineMode,
      @Nullable List<BiEngineReason> biEngineReasons)
      throws SQLException {
    logger.debug("Created forward only resultset TYPE_FORWARD_ONLY");
    this.Statementreference = (Statement) bqStatementRoot;
    this.completedJob = completedJob;
    this.queryId = queryId;
    this.projectId = projectId;
    if (bigquery == null) {
      throw new BQSQLException("Failed to fetch results. Connection is closed.");
    }
    this.bigquery = bigquery;
    Optional<BiEngineStatistics> biEngineStatisticsOptional =
        Optional.ofNullable(completedJob)
            .map(Job::getStatistics)
            .map(JobStatistics::getQuery)
            .map(JobStatistics2::getBiEngineStatistics);
    if (biEngineStatisticsOptional.isPresent()) {
      BiEngineStatistics biEngineStatistics = biEngineStatisticsOptional.get();
      biEngineMode = biEngineStatistics.getBiEngineMode();
      biEngineReasons = biEngineStatistics.getBiEngineReasons();
    }
    this.biEngineMode = biEngineMode;
    this.biEngineReasons = biEngineReasons;

    if (prefetchedRows != null || prefetchedAllRows) {
      // prefetchedAllRows can be true with rows null for an empty result set
      this.rowsofResult = prefetchedRows;
      if (prefetchedRows != null)
        fetchPos = fetchPos.add(BigInteger.valueOf(this.rowsofResult.size()));
      this.prefetchedAllRows = prefetchedAllRows;
      this.schema = schema;
      this.totalBytesProcessed = totalBytesProcessed;
      this.cacheHit = cacheHit;

    } else {
      // initial load
      if (completedJob == null) {
        throw new BQSQLException("Cannot poll results without a job reference");
      }
      GetQueryResultsResponse result;
      try {
        result =
            BQSupportFuncts.getQueryResultsDivided(
                bigquery, projectId, completedJob, fetchPos, FETCH_SIZE);
      } catch (IOException e) {
        throw new BQSQLException("Failed to retrieve data", e);
      } // should not happen
      if (result == null) { // if we don't have results at all
        this.rowsofResult = null;
        this.totalBytesProcessed = totalBytesProcessed;
        this.cacheHit = cacheHit;
      } else {
        if (result.getRows() == null) { // if we got results, but it was empty
          this.rowsofResult = null;
        } else { // we got results, it wasn't empty
          this.rowsofResult = result.getRows();
          this.schema = result.getSchema();
          fetchPos = fetchPos.add(BigInteger.valueOf(this.rowsofResult.size()));
        }
        this.totalBytesProcessed = result.getTotalBytesProcessed();
        this.cacheHit = result.getCacheHit();
      }
    }
  }

  /**
   * Returns the current rows Object at the given index
   *
   * @param columnIndex - the column to be used
   * @return - the stored value parsed to String, Float etc
   * @throws SQLException - if the resultset is closed or the columnIndex is not valid, or the type
   *     is unsupported
   */
  public Object getObject(int columnIndex) throws SQLException {

    String result = rawString(columnIndex);

    if (this.wasNull()) {
      return null;
    }

    // for arrays, just return the string result
    if (this.getBQResultsetMetaData().getColumnMode(columnIndex).equals("REPEATED")) {
      return result;
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
      if (Columntype.equals("DATETIME")) {
        // A BigQuery DATETIME is essentially defined by its string representation;
        // the "clock-calendar parameters" comprising year, month, day, hour, minute, etc.
        // On the other hand, a [java.sql.Timestamp] object is defined as a global instant, similar
        // to BQ TIMESTAMP. It has a [toString] method that interprets that instant in the system
        // default time zone. Thus, in order to produce a [Timestamp] object whose [toString] method
        // has the correct result, we must adjust the value of the instant according to the system
        // default time zone (passing a null Calendar uses the system default).
        return DateTimeUtils.parseDateTime(result, null);
      }
      if (Columntype.equals("TIMESTAMP")) {
        // A BigQuery TIMESTAMP is defined as a global instant in time, so when we create the
        // [java.sql.Timestamp] object to represent it, we must not make any time zone adjustment.
        return DateTimeUtils.parseTimestamp(result);
      }
      if (Columntype.equals("DATE")) {
        return DateTimeUtils.parseDate(result, null);
      }
      if (Columntype.equals("TIME")) {
        return DateTimeUtils.parseTime(result, null);
      }
      if (Columntype.equals("NUMERIC")) {
        return toBigDecimal(result);
      }
      // For an unknown type, return the result as a string, much better than exploding.
      return result;
    } catch (NumberFormatException e) {
      throw new BQSQLException(e);
    }
  }

  // toXXX = common "parsing" between Object and primitive types - to prevent discrepancies. The 4
  // firsts are the core ones (types supported by BigQuery)

  /** Parse boolean types */
  private Boolean toBoolean(String value) throws SQLException {
    return Boolean.valueOf(value);
  }

  /** Parse floating point types */
  private Double toDouble(String value) throws SQLException {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new BQSQLException(e);
    }
  }

  /** Parse integral types */
  private Long toLong(String value) throws SQLException {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new BQSQLException(e);
    }
  }

  // Secondary converters

  /** Parse integral or floating types with (virtually) infinite precision */
  private BigDecimal toBigDecimal(String value) {
    return new BigDecimal(value);
  }

  @Override
  /**
   * Returns the current rows Data at the given index as String
   *
   * @param columnIndex - the column to be used
   * @return - the stored value parsed to String
   * @throws SQLException - if the resultset is closed or the columnIndex is not valid, or the type
   *     is unsupported
   */
  public String getString(int columnIndex) throws SQLException {
    String rawString = rawString(columnIndex);

    BQResultsetMetaData metadata = getBQResultsetMetaData();
    if (metadata.getColumnTypeName(columnIndex).equals("TIMESTAMP")
        && !metadata.getColumnMode(columnIndex).equals("REPEATED")) {
      return DateTimeUtils.formatTimestamp(rawString);
    }

    return rawString;
  }

  private String rawString(int columnIndex) throws SQLException {
    // to make the logfiles smaller!
    // logger.debug("Function call getString columnIndex is: " + String.valueOf(columnIndex));
    this.closestrm();
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    if (this.getMetaData().getColumnCount() < columnIndex || columnIndex < 1) {
      throw new BQSQLException("ColumnIndex is not valid");
    }

    if (this.rowsofResult == null) {
      throw new BQSQLException("Invalid position!");
    }

    // Since the results came from BigQuery's JSON API,
    // the only types we'll ever see for resultObject are JSON-supported types,
    // i.e. strings, numbers, arrays, booleans, or null.
    // Many standard Java types, like timestamps, will be represented as strings.
    Object resultObject = this.rowsofResult.get(this.Cursor).getF().get(columnIndex - 1).getV();

    if (Data.isNull(resultObject)) {
      this.wasnull = true;
      return null;
    }
    this.wasnull = false;
    if (resultObject instanceof List || resultObject instanceof Map) {
      Object resultTransformedWithSchema =
          smartTransformResult(resultObject, schema.getFields().get(columnIndex - 1));
      resultObject = easyToJson(resultTransformedWithSchema);
    }
    return resultObject.toString();
  }

  private Object smartTransformResult(Object resultObject, TableFieldSchema fieldDef) {
    boolean isList = resultObject instanceof List;
    boolean isMap = resultObject instanceof Map;
    if (isMap && ((Map) resultObject).containsKey("v")) {
      // All values in these nested structs from the API are wrapped up as {"v": <the_val>}, so we
      // unwrap here
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
        newMap.put(
            nestedFieldDef.getName(), smartTransformResult(nestedValues.get(i), nestedFieldDef));
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
      JsonGenerator gen = new GsonFactory().createJsonGenerator(writer);
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
   *
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
   *
   * @throws SQLException - this isn't a forward only resultset
   */
  @Override
  public void afterLast() throws SQLException {
    if (this.getType() == ResultSet.TYPE_FORWARD_ONLY) {
      throw new BQSQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
    }
  }

  /**
   * Not supported in forward only resultset
   *
   * @throws SQLException - this isn't a forward only resultset
   */
  @Override
  public void beforeFirst() throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new BQSQLFeatureNotSupportedException("cancelWorUpdates()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Currently its a noop
   */
  @Override
  public void clearWarnings() throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is closed");
      // TODO implement Warnings
    }
  }

  /** Closes the resultset this function never FAILS! */
  @Override
  public void close() throws SQLException {
    // TODO free occupied resources
    this.closed = true;
    this.rowsofResult = null;
  }

  /**
   * If the Strm reference is not null it closes the underlying stream, if an error occurs throws
   * SQLException
   *
   * @throws SQLException if error occurs while trying to close the stream
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * We support read only functions in the current version.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void deleteRow() throws SQLException {
    throw new BQSQLFeatureNotSupportedException("deleteRow()");
  }

  /** {@inheritDoc} */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return CommonResultSet.findColumn(columnLabel, getMetaData());
  }

  /**
   * Not supported in forward only resultset
   *
   * @return - SQLException
   * @throws SQLException - this isn't a forward only resultset
   */
  @Override
  public boolean first() throws SQLException {
    throw new BQSQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Arrays are not supported in the current version
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getArray(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Arrays are not supported in the current version
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
      inptstrm = new java.io.ByteArrayInputStream(Value.getBytes("US-ASCII"));
    } catch (UnsupportedEncodingException e) {
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
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
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
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getBlob(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getClob(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getClob(string)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Always Returns ResultSet.CONCUR_READ_ONLY
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
    return getDate(columnIndex, null);
  }

  /** {@inheritDoc} */
  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    String value = this.getString(columnIndex);
    if (this.wasNull()) {
      return null;
    }
    return DateTimeUtils.parseDate(value, cal);
  }

  /** {@inheritDoc} */
  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(columnLabel, null);
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public int getFetchSize() throws SQLException {
    // throw new BQSQLException("Not implemented." + "getfetchSize()");
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Read only mode, no commits.
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return getBQResultsetMetaData();
  }

  /**
   * Get this ResultSet's BQForwardOnlyResultSetMetadata
   *
   * @return
   * @throws SQLException
   */
  public BQResultsetMetaData getBQResultsetMetaData() throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    return new BQResultsetMetaData(schema, projectId);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns the value of getCharacterStream(columnIndex)
   *
   * @see #getCharacterStream(int)
   */
  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return this.getCharacterStream(columnIndex);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns the value of getCharacterStream(columnLabel)
   *
   * @see #getCharacterStream(String)
   */
  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return this.getCharacterStream(columnLabel);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getNClob(int");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getNClob(string)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns the value getString(columnIndex)
   *
   * @see #getString(int)
   */
  @Override
  public String getNString(int columnIndex) throws SQLException {
    return this.getString(columnIndex);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns the value getString(columnLabel)
   *
   * @see #getString(String)
   */
  @Override
  public String getNString(String columnLabel) throws SQLException {
    return this.getString(columnLabel);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not s.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getObject(string,Map)");
    // TODO Implement TypeMaps
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getRef(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
    throw new BQSQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException("getRowId(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
    return new net.starschema.clouddb.jdbc.BQSQLXML(this.getString(columnIndex));
  }

  /** {@inheritDoc} */
  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return this.getSQLXML(columnIndex);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Always Returns null
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
    return getTime(columnIndex, null);
  }

  /** {@inheritDoc} */
  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    String value = this.getString(columnIndex);
    if (this.wasNull()) {
      return null;
    }
    return DateTimeUtils.parseTime(value, cal);
  }

  /** {@inheritDoc} */
  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(columnLabel, null);
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
    return getTimestamp(columnIndex, null);
  }

  /** {@inheritDoc} */
  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    String value = rawString(columnIndex);
    if (this.wasNull()) {
      return null;
    }
    // Both the TIMESTAMP and DATETIME objects support JDBC's getTimestamp method.
    // DATETIME in BigQuery is analogous to TIMESTAMP in ISO SQL.
    // TIMESTAMP in BigQuery is analogous to TIMESTAMP WITH LOCAL TIME ZONE in ISO SQL.
    String columnType = this.schema.getFields().get(columnIndex - 1).getType();
    return "TIMESTAMP".equals(columnType)
        ? DateTimeUtils.parseTimestamp(value)
        : DateTimeUtils.parseDateTime(value, cal);
  }

  /** {@inheritDoc} */
  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(columnLabel, null);
  }

  /** {@inheritDoc} */
  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return this.getTimestamp(columnIndex, cal);
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Always returns ResultSet.TYPE_SCROLL_INSENSITIVE
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException(
        "Deprecated. use getCharacterStream in place of getUnicodeStream");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
    } catch (MalformedURLException e) {
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Always returns null
   *
   * @return null
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO implement error handling
    return null;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
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
    throw new BQSQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new BQSQLFeatureNotSupportedException("moveToCurrentRow()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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

    if (completedJob == null) {
      throw new BQSQLException("Cannot poll results without a job reference");
    }
    GetQueryResultsResponse result;
    try {
      result =
          BQSupportFuncts.getQueryResultsDivided(
              bigquery, projectId, completedJob, fetchPos, FETCH_SIZE);
    } catch (IOException e) {
      // should not happen ... according to whoever cooked this up back in the day
      throw new BQSQLException("failed to fetch more results", e);
    }
    if (result.getRows() == null) {
      this.rowsofResult = null;
      return false;
    }
    this.rowsofResult = result.getRows();
    fetchPos = fetchPos.add(BigInteger.valueOf((long) this.rowsofResult.size()));
    Cursor = 0;
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean previous() throws SQLException {
    throw new BQSQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not supported.
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
    throw new BQSQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public boolean rowDeleted() throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public boolean rowInserted() throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public boolean rowUpdated() throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new BQSQLException("Not implemented." + "setFetchDirection(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    FETCH_SIZE = rows;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new BQSQLException("Not implemented." + "unwrap(Class<T>)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not impel
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateRow() throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws BQSQLFeatureNotSupportedException
   *
   * @throws BQSQLFeatureNotSupportedException
   */
  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new BQSQLFeatureNotSupportedException();
  }

  /** {@inheritDoc} */
  @Override
  public boolean wasNull() throws SQLException {
    return this.wasnull;
  }

  public @Nullable Long getTotalBytesProcessed() {
    return totalBytesProcessed;
  }

  public @Nullable Boolean getCacheHit() {
    return cacheHit;
  }

  public @Nullable String getBiEngineMode() {
    return biEngineMode;
  }

  public @Nullable List<BiEngineReason> getBiEngineReasons() {
    return biEngineReasons;
  }

  public @Nullable String getJobId() {
    if (this.completedJob != null) {
      return this.completedJob.getId();
    } else {
      return null;
    }
  }

  public @Nullable String getQueryId() {
    return queryId;
  }
}
