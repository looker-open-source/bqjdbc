package net.starschema.clouddb.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.junit.Test;

/**
 * Checks that string parameters bound to a {@link BQPreparedStatement} stay inside their string
 * literal. These tests only build the SQL text on the client and do not need a BigQuery connection.
 */
public class BQPreparedStatementEscapingTest {

  private static final String TEMPLATE = "SELECT * FROM t WHERE name = ?";
  private static final String PREFIX = "SELECT * FROM t WHERE name = ";

  private static BQPreparedStatement newStatement() {
    return new BQPreparedStatement(TEMPLATE, "test-project", null);
  }

  @Test
  public void quoteStringLiteral_plainValueIsOnlyWrapped() {
    assertThat(BQPreparedStatement.quoteStringLiteral("abc")).isEqualTo("\"abc\"");
    assertThat(BQPreparedStatement.quoteStringLiteral("")).isEqualTo("\"\"");
    assertThat(BQPreparedStatement.quoteStringLiteral("it's ok")).isEqualTo("\"it's ok\"");
  }

  @Test
  public void quoteStringLiteral_escapesQuoteBackslashAndNewlines() {
    assertThat(BQPreparedStatement.quoteStringLiteral("a\"b")).isEqualTo("\"a\\\"b\"");
    assertThat(BQPreparedStatement.quoteStringLiteral("a\\b")).isEqualTo("\"a\\\\b\"");
    assertThat(BQPreparedStatement.quoteStringLiteral("a\nb\rc")).isEqualTo("\"a\\nb\\rc\"");
  }

  @Test
  public void setString_doubleQuoteBreakoutStaysInsideLiteral() throws SQLException {
    BQPreparedStatement stmt = newStatement();
    stmt.setString(1, "\" OR 1=1 UNION ALL SELECT secret FROM other --");

    assertThat(stmt.RunnableStatement)
        .isEqualTo(PREFIX + "\"\\\" OR 1=1 UNION ALL SELECT secret FROM other --\"");
    assertSingleLiteral(stmt.RunnableStatement);
  }

  @Test
  public void setString_backslashBeforeQuoteCannotUnescapeTheQuote() throws SQLException {
    BQPreparedStatement stmt = newStatement();
    // Without escaping the backslash too, \" would become \\" and close the literal.
    stmt.setString(1, "\\\" OR 1=1 --");

    assertThat(stmt.RunnableStatement).isEqualTo(PREFIX + "\"\\\\\\\" OR 1=1 --\"");
    assertSingleLiteral(stmt.RunnableStatement);
  }

  @Test
  public void setString_payloadsStayInsideLiteral() throws SQLException {
    String[] payloads = {
      "\"",
      "\"\"\"",
      "\\",
      "\\\\\"",
      "x\"; DROP TABLE t; --",
      "x\n\" OR 1=1 --",
      "x\r\n\" OR 1=1 --",
      "x' OR '1'='1",
      "x` OR 1=1 --",
    };
    for (String payload : payloads) {
      BQPreparedStatement stmt = newStatement();
      stmt.setString(1, payload);
      assertSingleLiteral(stmt.RunnableStatement);
    }
  }

  @Test
  public void setString_nullValueSetsSqlNull() throws SQLException {
    BQPreparedStatement stmt = newStatement();
    stmt.setString(1, null);
    assertThat(stmt.RunnableStatement).isEqualTo(PREFIX + "NULL");

    stmt = newStatement();
    stmt.setNString(1, null);
    assertThat(stmt.RunnableStatement).isEqualTo(PREFIX + "NULL");
  }

  @Test
  public void otherStringSetters_areEscaped() throws Exception {
    String payload = "x\"; DROP TABLE t; --";
    String expected = PREFIX + "\"x\\\"; DROP TABLE t; --\"";

    BQPreparedStatement stmt = newStatement();
    stmt.setObject(1, payload);
    assertThat(stmt.RunnableStatement).isEqualTo(expected);

    stmt = newStatement();
    stmt.setNString(1, payload);
    assertThat(stmt.RunnableStatement).isEqualTo(expected);

    stmt = newStatement();
    stmt.setCharacterStream(1, new StringReader(payload));
    assertThat(stmt.RunnableStatement).isEqualTo(expected);

    stmt = newStatement();
    stmt.setAsciiStream(1, new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
    assertThat(stmt.RunnableStatement).isEqualTo(expected);
  }

  /**
   * Asserts that everything after the template prefix is exactly one double-quoted literal, i.e.
   * the first unescaped double quote after the opening one is the last character.
   */
  private static void assertSingleLiteral(String sql) {
    assertThat(sql).startsWith(PREFIX + "\"");
    String literal = sql.substring(PREFIX.length());
    int i = 1;
    while (i < literal.length()) {
      char c = literal.charAt(i);
      if (c == '\\') {
        i += 2;
        continue;
      }
      if (c == '"') {
        break;
      }
      assertThat(c).as("raw newline in literal: %s", sql).isNotIn('\n', '\r');
      i++;
    }
    assertThat(i).as("literal closed early in: %s", sql).isEqualTo(literal.length() - 1);
  }
}
