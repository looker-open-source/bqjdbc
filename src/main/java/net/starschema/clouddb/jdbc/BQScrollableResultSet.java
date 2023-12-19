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
 * <p>This class implements the java.sql.Resultset interface
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.BiEngineReason;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This class implements the java.sql.ResultSet interface its superclass is ScrollableResultset
 *
 * @author Attila Horváth
 */
public class BQScrollableResultSet extends ScrollableResultset<Object>
    implements java.sql.ResultSet {

  /** to set the maxFieldSize */
  private int maxFieldSize = 0;

  /** This Reference is for storing the Reference for the Statement which created this Resultset */
  private BQStatement Statementreference = null;

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

  private final @Nullable JobReference jobReference;

  /** The BigQuery query ID; set if the query completed without a Job */
  private final @Nullable String queryId;

  private TableSchema schema;

  /**
   * Constructor of BQResultset, that initializes all private variables
   *
   * @param bigQueryGetQueryResultResponse BigQueryGetQueryResultResponse from Bigquery
   * @param bqStatementRoot Reference of the Statement that creates this Resultset
   */
  public BQScrollableResultSet(
      GetQueryResultsResponse bigQueryGetQueryResultResponse, BQStatementRoot bqStatementRoot) {
    this(
        bigQueryGetQueryResultResponse.getRows(),
        bqStatementRoot,
        bigQueryGetQueryResultResponse.getSchema(),
        bigQueryGetQueryResultResponse.getTotalBytesProcessed(),
        bigQueryGetQueryResultResponse.getCacheHit(),
        null,
        null,
        bigQueryGetQueryResultResponse.getJobReference(),
        null);

    BigInteger maxrow;
    try {
      maxrow = BigInteger.valueOf(bqStatementRoot.getMaxRows());
      bigQueryGetQueryResultResponse.setTotalRows(maxrow);
    } catch (SQLException e) {
    } // Should not happen.
  }

  public BQScrollableResultSet(
      List<TableRow> rows,
      BQStatementRoot bqStatementRoot,
      TableSchema schema,
      @Nullable Long totalBytesProcessed,
      @Nullable Boolean cacheHit,
      @Nullable String biEngineMode,
      @Nullable List<BiEngineReason> biEngineReasons,
      @Nullable JobReference jobReference,
      @Nullable String queryId) {
    logger.debug("Created Scrollable resultset TYPE_SCROLL_INSENSITIVE");
    try {
      maxFieldSize = bqStatementRoot.getMaxFieldSize();
    } catch (SQLException e) {
      // Should not happen.
    }

    if (rows != null) {
      this.RowsofResult = rows.toArray();
    }

    if (bqStatementRoot instanceof BQStatement) {
      this.Statementreference = (BQStatement) bqStatementRoot;
    }

    this.schema = schema;
    this.totalBytesProcessed = totalBytesProcessed;
    this.cacheHit = cacheHit;
    this.biEngineMode = biEngineMode;
    this.biEngineReasons = biEngineReasons;
    this.jobReference = jobReference;
    this.queryId = queryId;
  }

  /** {@inheritDoc} */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    final ResultSetMetaData metadata = this.getMetaData();
    int columns = metadata.getColumnCount();
    for (int i = 1; i <= columns; i++) {
      if (metadata.getColumnLabel(i).equals(columnLabel)) {
        return i;
      }
    }
    throw new BQSQLException("No Such column labeled: " + columnLabel);
  }

  /** {@inheritDoc} */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    return new BQResultsetMetaData(this.schema, this.Statementreference.connection.getProjectId());
  }

  /** {@inheritDoc} */
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    // to make the logfiles smaller!
    // logger.debug("Function call getObject columnIndex is: " + String.valueOf(columnIndex));
    this.closestrm();
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    this.ThrowCursorNotValidExeption();
    if (this.RowsofResult == null) {
      throw new BQSQLException("There are no rows in this Resultset");
    }
    if (this.getMetaData().getColumnCount() < columnIndex || columnIndex < 1) {
      throw new BQSQLException("ColumnIndex is not valid");
    }
    String Columntype = schema.getFields().get(columnIndex - 1).getType();

    TableCell field = ((TableRow) this.RowsofResult[this.Cursor]).getF().get(columnIndex - 1);

    if (Data.isNull(field.getV())) {
      this.wasnull = true;
      return null;
    } else {
      String result = field.getV().toString();
      this.wasnull = false;
      try {
        if (Columntype.equals("STRING")) {
          // removing the excess byte by the setmaxFiledSize
          if (maxFieldSize == 0 || maxFieldSize == Integer.MAX_VALUE) {
            return result;
          } else {
            try { // lets try to remove the excess bytes
              return result.substring(0, maxFieldSize);
            } catch (IndexOutOfBoundsException iout) {
              // we don't need to remove any excess byte
              return result;
            }
          }
        }
        if (Columntype.equals("FLOAT")) {
          return Float.parseFloat(result);
        }
        if (Columntype.equals("BOOLEAN")) {
          return Boolean.parseBoolean(result);
        }
        if (Columntype.equals("INTEGER")) {
          return Long.parseLong(result);
        }
        if (Columntype.equals("DATETIME")) {
          // A BigQuery DATETIME is essentially defined by its string representation;
          // the "clock-calendar parameters" comprising year, month, day, hour, minute, etc.
          // On the other hand, a [java.sql.Timestamp] object is defined as a global instant,
          // similar to BQ TIMESTAMP. It has a [toString] method that interprets that instant in the
          // system default time zone. Thus, in order to produce a [Timestamp] object whose
          // [toString] method has the correct result, we must adjust the value of the instant
          // according to the system default time zone (passing a null Calendar uses the system
          // default).
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
          return new BigDecimal(result);
        }
        throw new BQSQLException("Unsupported Type");
      } catch (NumberFormatException e) {
        throw new BQSQLException(e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Statement getStatement() throws SQLException {
    return this.Statementreference;
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new BQSQLException("Not implemented.");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new BQSQLException("Not implemented.");
  }

  /** {@inheritDoc} */
  @Override
  public String getString(int columnIndex) throws SQLException {
    // to make the logfiles smaller!
    // logger.debug("Function call getString columnIndex is: " + String.valueOf(columnIndex));
    this.closestrm();
    this.ThrowCursorNotValidExeption();
    if (this.isClosed()) {
      throw new BQSQLException("This Resultset is Closed");
    }
    String result =
        ((TableRow) this.RowsofResult[this.Cursor]).getF().get(columnIndex - 1).getV().toString();
    if (result == null) {
      this.wasnull = true;
    } else {
      this.wasnull = false;
    }
    // removing the excess byte by the setmaxFiledSize
    if (maxFieldSize == 0 || maxFieldSize == Integer.MAX_VALUE) {
      return result;
    } else {
      try { // lets try to remove the excess bytes
        return result.substring(0, maxFieldSize);
      } catch (IndexOutOfBoundsException iout) {
        // we don't need to remove any excess byte
        return result;
      }
    }
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
    if (this.jobReference != null) {
      return this.jobReference.getJobId();
    } else {
      return null;
    }
  }

  public @Nullable String getQueryId() {
    return queryId;
  }
}
