package net.starschema.clouddb.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * An abstract class that contains tests common to {@link ResultSet} implementations. Its name
 * doesn't end with Test so that JUnit doesn't try to run it.
 *
 * <p>Subclass this in @{link ResultSet} test classes and implement {@link
 * CommonTestsForResultSets#createStatementForCommonTests(Connection)}.
 */
public abstract class CommonTestsForResultSets {

  private Connection connection;

  @Before
  public void connectForCommonTests() throws SQLException, IOException {
    connection =
        ConnectionFromResources.connect("installedaccount1.properties", "&useLegacySql=false");
  }

  @After
  public void closeConnectionForCommonTests() throws SQLException {
    connection.close();
  }

  /**
   * A {@link java.util.function.Consumer} that can throw a {@link SQLException}
   *
   * @param <T> The return value's type
   */
  private interface ConsumerWithSQLException<T> {

    void accept(T arg) throws SQLException;
  }

  /**
   * Create a {@link Statement} that returns the appropriate result set.
   *
   * @param connection the connection from which to create the statement
   * @return a statement used by common tests
   * @throws SQLException if the {@link Connection} raises one
   */
  protected abstract Statement createStatementForCommonTests(final Connection connection)
      throws SQLException;

  /**
   * Assert something about the single row and column returned by a query.
   *
   * @param query must result in a single row and column
   * @param check a Consumer that can throw a {@link SQLException} - put your assertions about the
   *     result set in this
   * @throws SQLException whenever check throws it
   */
  private void assertSingleRowAndColumn(
      final String query, final ConsumerWithSQLException<ResultSet> check) throws SQLException {
    try (Statement stmt = createStatementForCommonTests(connection)) {
      final ResultSet result = stmt.executeQuery(query);
      Assertions.assertThat(result.next()).isTrue();
      check.accept(result);
      Assertions.assertThat(result.next()).isFalse();
    }
  }

  @Test
  public void testFindColumn() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 2 AS two", rs -> Assertions.assertThat(rs.findColumn("two")).isEqualTo(1));
  }

  /**
   * A {@link java.util.function.Function} that can throw a {@link SQLException}
   *
   * @param <T> the argument's type
   * @param <R> the returned value's type
   */
  private interface FunctionWithSQLException<T, R> {
    R apply(T arg) throws SQLException;
  }

  private void assertReaderAsString(
      final String query,
      final FunctionWithSQLException<ResultSet, Reader> getter,
      final String value)
      throws SQLException {
    assertSingleRowAndColumn(
        query,
        rs -> {
          final Reader reader = getter.apply(rs);
          final String contents =
              new BufferedReader(reader).lines().collect(Collectors.joining(""));
          Assertions.assertThat(contents).isEqualTo(value);
        });
  }

  @Test
  public void testGetAsciiStreamByLabel() throws SQLException {
    assertReaderAsString(
        "SELECT 'an ASCII string' AS stream",
        rs -> new InputStreamReader(rs.getAsciiStream("stream"), StandardCharsets.US_ASCII),
        "an ASCII string");
  }

  @Test
  public void testGetBinaryStreamByLabel() throws SQLException {
    assertReaderAsString(
        "SELECT 'a binary string' AS stream",
        rs -> new InputStreamReader(rs.getBinaryStream("stream"), StandardCharsets.US_ASCII),
        "a binary string");
  }

  @Test
  public void testGetBooleanByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT false AS bool", rs -> Assertions.assertThat(rs.getBoolean("bool")).isFalse());
  }

  @Test
  public void testGetByteByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 98 AS byte", rs -> Assertions.assertThat(rs.getByte("byte")).isEqualTo((byte) 98));
  }

  @Test
  public void testGetBytesByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 'abc' AS bytes",
        rs -> Assertions.assertThat(rs.getBytes("bytes")).isEqualTo(new byte[] {'a', 'b', 'c'}));
  }

  @Test
  public void testGetCharacterStreamByLabel() throws SQLException {
    assertReaderAsString(
        "SELECT 'some characters' AS chars",
        rs -> rs.getCharacterStream("chars"),
        "some characters");
  }

  @Test
  public void testGetDateByLabel() throws SQLException {
    final Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(2023, Calendar.DECEMBER, 19);
    final Date expected = calendar.getTime();

    assertSingleRowAndColumn(
        "SELECT '2023-12-19' AS date",
        rs -> {
          final Date value = rs.getDate("date");
          Assertions.assertThat(value).isEqualTo(expected);
        });

    assertSingleRowAndColumn(
        "SELECT '2023-12-19' AS date",
        rs -> {
          final Date value = rs.getDate("date", calendar);
          Assertions.assertThat(value).isEqualTo(expected);
        });
  }

  @Test
  public void testGetDoubleByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 0.1 AS double",
        rs ->
            Assertions.assertThat(rs.getDouble("double"))
                .isEqualTo(0.1, Assertions.withPrecision(1d)));
  }

  @Test
  public void testGetFloatByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 0.1 AS float",
        rs ->
            Assertions.assertThat(rs.getDouble("float"))
                .isEqualTo(0.1, Assertions.withPrecision(1d)));
  }

  @Test
  public void testGetIntByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 6 AS integer", rs -> Assertions.assertThat(rs.getInt("integer")).isEqualTo(6));
  }

  @Test
  public void testGetLongByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 6 AS long", rs -> Assertions.assertThat(rs.getInt("long")).isEqualTo(6L));
  }

  @Test
  public void testGetObjectByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 'an object' AS obj",
        rs -> Assertions.assertThat(rs.getObject("obj")).isEqualTo("an object"));
  }

  @Test
  public void testGetShortByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 32767 AS short",
        rs -> Assertions.assertThat(rs.getShort("short")).isEqualTo((short) 32767));
  }

  @Test
  public void testGetSQLXMLByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT '<a>xml</a>' AS xml",
        rs -> {
          final SQLXML xml = rs.getSQLXML("xml");
          final String actual = xml.getString().trim();
          final String expected = "<a>xml</a>";
          Assertions.assertThat(actual).isEqualTo(expected);
        });
  }

  @Test
  public void testGetStringByLabel() throws SQLException {
    assertSingleRowAndColumn(
        "SELECT 'a string' AS string",
        rs -> Assertions.assertThat(rs.getString("string")).isEqualTo("a string"));
  }

  @Test
  public void testGetTimeByLabel() throws SQLException {
    final Time expected = Time.valueOf("01:02:03");
    assertSingleRowAndColumn(
        "SELECT '01:02:03' AS time",
        rs -> Assertions.assertThat(rs.getTime("time")).isEqualTo(expected));

    final Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(2023, 12, 19);
    assertSingleRowAndColumn(
        "SELECT '01:02:03' AS time",
        rs -> Assertions.assertThat(rs.getTime("time", calendar)).isEqualTo(expected));
  }

  @Test
  public void testGetTimestampByLabel() throws SQLException {
    final Timestamp expected = Timestamp.valueOf("2023-12-19 01:02:03");
    assertSingleRowAndColumn(
        "SELECT '2023-12-19 01:02:03' AS timestamp",
        rs -> Assertions.assertThat(rs.getTimestamp("timestamp")).isEqualTo(expected));

    final Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(2023, 12, 19);
    assertSingleRowAndColumn(
        "SELECT '2023-12-19 01:02:03' AS timestamp",
        rs -> Assertions.assertThat(rs.getTimestamp("timestamp", calendar)).isEqualTo(expected));
  }

  @Test
  public void testGetURLByLabel() throws SQLException, MalformedURLException {
    final URL expected = new URL("https://127.0.0.1/p?q=1");
    assertSingleRowAndColumn(
        "SELECT 'https://127.0.0.1/p?q=1' AS url",
        rs -> Assertions.assertThat(rs.getURL("url")).isEqualTo(expected));
  }
}
