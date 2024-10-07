package net.starschema.clouddb.jdbc;

import com.google.cloud.bigquery.BigQuerySQLException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** A static utility class of methods common to all {@link java.sql.ResultSet} implementations. */
class CommonResultSet {

  /**
   * A common implementation of {@link java.sql.ResultSet#findColumn(String)}
   *
   * @param label the column-to-find's label
   * @param metadata the metadata from the {@link java.sql.ResultSet}
   * @return the integer index of the labeled column, if it's found
   * @throws SQLException if no column with the given label is found
   */
  static int findColumn(final String label, final ResultSetMetaData metadata) throws SQLException {
    int columnCount = metadata.getColumnCount();
    for (int column = 1; column <= columnCount; column++) {
      if (metadata.getColumnLabel(column).equals(label)) {
        return column;
      }
    }
    throw new BigQuerySQLException("No such Column labeled: " + label);
  }
}
