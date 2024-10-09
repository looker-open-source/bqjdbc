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
 */
package net.starschema.clouddb.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeoutTest {

  private java.sql.Connection connection;
  Logger logger = LoggerFactory.getLogger(TimeoutTest.class);

  /**
   * Compares two String[][]
   *
   * @param expected
   * @param reality
   * @return true if they are equal false if not
   */
  private boolean comparer(String[][] expected, String[][] reality) {
    for (int i = 0; i < expected.length; i++) {
      for (int j = 0; j < expected[i].length; j++) {
        if (expected[i][j].toString().equals(reality[i][j]) == false) {
          return false;
        }
      }
    }

    return true;
  }

  @BeforeEach
  public void setConnection() throws SQLException, IOException {
    connection =
        ConnectionFromResources.connect("installedaccount1.properties", "&useLegacySql=true");
  }

  @AfterEach
  public void closeConnection() throws SQLException {
    connection.close();
  }

  @Test
  public void isvalidtest() {
    try {
      assertTrue(connection.isValid(0));
    } catch (SQLException e) {

    }
  }

  @Test
  public void QueryResultTest01() {
    final String sql = "SELECT TOP(word, 10), COUNT(*) FROM publicdata:samples.shakespeare";
    final String description = "The top 10 word from shakespeare #TOP #COUNT";
    String[][] expectation =
        new String[][] {
          {"you", "yet", "would", "world", "without", "with", "will", "why", "whose", "whom"},
          {"42", "42", "42", "42", "42", "42", "42", "42", "42", "42"}
        };

    this.logger.info("Test number: 01");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest02() {
    final String sql =
        "SELECT corpus FROM publicdata:samples.shakespeare GROUP BY corpus ORDER BY corpus LIMIT 5";
    final String description = "The book names of shakespeare #GROUP_BY #ORDER_BY";
    String[][] expectation =
        new String[][] {
          {"1kinghenryiv", "1kinghenryvi", "2kinghenryiv", "2kinghenryvi", "3kinghenryvi"}
        };
    this.logger.info("Test number: 02");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest03() {
    final String sql =
        "SELECT COUNT(DISTINCT web100_log_entry.connection_spec.remote_ip) AS num_clients FROM"
            + " [guid754187384106:m_lab.2010_01] WHERE"
            + " IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) AND"
            + " IS_EXPLICITLY_DEFINED(web100_log_entry.log_time) AND web100_log_entry.log_time >"
            + " 1262304000 AND web100_log_entry.log_time < 1262476800";
    final String description =
        "A sample query from google, but we don't have Access for the query table #ERROR"
            + " #accessDenied #403";

    this.logger.info("Test number: 03");
    this.logger.info("Running query:" + sql);
    this.logger.debug(description);
    try {
      connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.debug("SQLexception" + e.toString());
      // fail("SQLException" + e.toString());
      assertTrue(e.toString().contains("Access Denied: Table guid754187384106:m_lab.2010_01"));
    }
  }

  @Test
  public void QueryResultTest04() {
    final String sql =
        "SELECT corpus FROM publicdata:samples.shakespeare WHERE LOWER(word)=\"lord\" GROUP BY"
            + " corpus ORDER BY corpus DESC LIMIT 5;";
    final String description = "A query which gets 5 of Shakespeare were the word lord is present";
    String[][] expectation =
        new String[][] {
          {"winterstale", "various", "twogentlemenofverona", "twelfthnight", "troilusandcressida"}
        };

    this.logger.info("Test number: 04");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest05() {
    final String sql = "SELECT word FROM publicdata:samples.shakespeare WHERE word=\"huzzah\"";
    final String description =
        "The word \"huzzah\" NOTE: It doesn't appear in any any book, so it returns with a null"
            + " #WHERE";

    this.logger.info("Test number: 05");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
      this.logger.debug("{}", Result.getMetaData().getColumnCount());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);
    try {
      if (Result.getType() != ResultSet.TYPE_FORWARD_ONLY) assertFalse(Result.first());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest06() {
    final String sql =
        "SELECT corpus_date,SUM(word_count) FROM publicdata:samples.shakespeare GROUP BY"
            + " corpus_date ORDER BY corpus_date DESC LIMIT 5;";
    final String description =
        "A query which gets how many words Shapespeare wrote in a year (5 years displayed"
            + " descending)";
    String[][] expectation =
        new String[][] {
          {"1612", "1611", "1610", "1609", "1608"},
          {"26265", "17593", "26181", "57073", "19846"}
        };

    this.logger.info("Test number: 06");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest07() {
    final String sql =
        "SELECT corpus, SUM(word_count) as w_c FROM publicdata:samples.shakespeare GROUP BY corpus"
            + " HAVING w_c > 20000 ORDER BY w_c ASC LIMIT 5;";
    final String description =
        "A query which gets Shakespeare were there are more words then 20000 (only 5 is displayed"
            + " ascending)";
    String[][] expectation =
        new String[][] {
          {"juliuscaesar", "twelfthnight", "titusandronicus", "kingjohn", "tamingoftheshrew"},
          {"21052", "21633", "21911", "21983", "22358"}
        };

    this.logger.info("Test number: 07");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest08() {
    final String sql =
        "SELECT corpus, MAX(word_count) as m, word FROM publicdata:samples.shakespeare GROUP BY"
            + " corpus,word ORDER BY m DESC LIMIT 5;";
    final String description =
        "A query which gets those Shakespeare with the most common word ordered by count descending"
            + " (only 5 is displayed)";
    String[][] expectation =
        new String[][] {
          {"hamlet", "coriolanus", "kinghenryv", "2kinghenryiv", "kingrichardiii"},
          {"995", "942", "937", "894", "848"},
          {"the", "the", "the", "the", "the"}
        };

    this.logger.info("Test number: 08");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  @Test
  public void QueryResultTest09() {
    final String sql =
        "SELECT corpus, corpus_date FROM publicdata:samples.shakespeare GROUP BY corpus,"
            + " corpus_date ORDER BY corpus_date DESC LIMIT 3;";
    final String description = "Shakespeare's 3 latest";
    String[][] expectation =
        new String[][] {
          {"kinghenryviii", "tempest", "winterstale"},
          {"1612", "1611", "1610"}
        };

    this.logger.info("Test number: 09");
    this.logger.info("Running query:" + sql);

    java.sql.ResultSet Result = null;
    try {
      Result = connection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(Result);

    this.logger.debug(description);

    HelperFunctions.printer(expectation);
    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(Result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }
}
