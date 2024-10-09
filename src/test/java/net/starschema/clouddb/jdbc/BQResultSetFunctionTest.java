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
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Junit test tests functions in BQResultset
 *
 * @author Horváth Attila
 * @author Gunics Balazs
 */
public class BQResultSetFunctionTest {

  private java.sql.Connection connection;

  private java.sql.ResultSet result;

  Logger logger = LoggerFactory.getLogger(BQResultSetFunctionTest.class);

  @BeforeEach
  public void setConnection() throws SQLException, IOException {
    connection =
        ConnectionFromResources.connect("installedaccount1.properties", "&useLegacySql=true");
    QueryLoad();
  }

  @AfterEach
  public void closeConnection() throws SQLException {
    connection.close();
  }

  /**
   * Prints a String[][] QueryResult to Log
   *
   * @param input
   */
  private void printer(String[][] input) {
    for (int s = 0; s < input[0].length; s++) {
      String Output = "";
      for (int i = 0; i < input.length; i++) {
        if (i == input.length - 1) {
          Output += input[i][s];
        } else {
          Output += input[i][s] + "\t";
        }
      }
      this.logger.debug(Output);
    }
  }

  public void QueryLoad() {
    final String sql =
        "SELECT TOP(word,10) AS word, COUNT(*) as count FROM publicdata:samples.shakespeare";
    final String description = "The top 10 word from shakespeare #TOP #COUNT";
    String[][] expectation =
        new String[][] {
          {"you", "yet", "would", "world", "without", "with", "will", "why", "whose", "whom"},
          {"42", "42", "42", "42", "42", "42", "42", "42", "42", "42"}
        };

    this.logger.info("Test number: 01");
    this.logger.info("Running query:" + sql);

    try {
      Statement stmt =
          connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(500);
      result = stmt.executeQuery(sql);
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    assertNotNull(result);

    this.logger.debug(description);
    this.printer(expectation);

    try {
      assertTrue(
          this.comparer(expectation, BQSupportMethods.GetQueryResult(result)),
          "Comparing failed in the String[][] array");
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail(e.toString());
    }
  }

  // Comprehensive Tests:

  @Test
  public void ChainedCursorFunctionTest() {
    this.logger.info("ChainedFunctionTest");
    try {
      result.beforeFirst();
      assertTrue(result.next());
      assertEquals("you", result.getString(1));

    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    try {
      assertTrue(result.absolute(10));
      assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertFalse(result.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }

    try {
      assertTrue(result.first());
      assertEquals("you", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertTrue(result.isFirst());
      assertFalse(result.previous());
      result.afterLast();
      assertTrue(result.isAfterLast());
      assertTrue(result.absolute(-1));
      assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertTrue(result.relative(-5));
      assertEquals("without", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertFalse(result.relative(6));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertEquals("without", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
    this.logger.info("chainedfunctiontest end");
  }

  @Test
  public void databaseMetaDataGetTables() {
    // clouddb,ARTICLE_LOOKUP,starschema.net,[Ljava.lang.String;@9e8424
    ResultSet result = null;
    try {
      // Function call getColumns
      // catalog:null,
      // schemaPattern: starschema_net__clouddb,
      // tableNamePattern:OUTLET_LOOKUP, columnNamePattern: null
      // result = con.getMetaData().getTables("OUTLET_LOOKUP", null, "starschema_net__clouddb", null
      // );
      result =
          connection
              .getMetaData()
              .getColumns(null, "starschema_net__clouddb", "OUTLET_LOOKUP", null);
      // Function call getTables(catalog: ARTICLE_COLOR_LOOKUP, schemaPattern: null,
      // tableNamePattern: starschema_net__clouddb, types: TABLE , VIEW , SYSTEM TABLE , SYNONYM ,
      // ALIAS , )
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
    try {
      assertTrue(result.first());
      while (!result.isAfterLast()) {
        String toprint = "";
        toprint += result.getString(1) + " , ";
        toprint += result.getString(2) + " , ";
        toprint += result.getString(3) + " , ";
        toprint += result.getString(4) + " , ";
        toprint += result.getString(5) + " , ";
        toprint += result.getString(6) + " , ";
        toprint += result.getString(7) + " , ";
        toprint += result.getString(8) + " , ";
        toprint += result.getString(9) + " , ";
        toprint += result.getString(10);
        System.err.println(toprint);
        result.next();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

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

  /** For testing isValid() , Close() , isClosed() */
  @Test
  public void isClosedValidtest() {
    try {
      assertEquals(true, connection.isValid(0));
    } catch (SQLException e) {
      fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      assertEquals(true, connection.isValid(10));
    } catch (SQLException e) {
      fail("Got an exception" + e.toString());
      e.printStackTrace();
    }
    try {
      connection.isValid(-10);
    } catch (SQLException e) {
      assertTrue(true);
      // e.printStackTrace();
    }

    try {
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      assertTrue(connection.isClosed());
    } catch (SQLException e1) {
      e1.printStackTrace();
    }

    try {
      connection.isValid(0);
    } catch (SQLException e) {
      assertTrue(true);
      e.printStackTrace();
    }
  }

  @Test
  public void ResultSetMetadata() {
    try {
      this.logger.debug(result.getMetaData().getSchemaName(1));
      this.logger.debug("{}", result.getMetaData().getScale(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
    }
    assertTrue(true);
  }

  @Test
  public void TestResultIndexOutofBound() {
    try {
      this.logger.debug("{}", result.getBoolean(99));
    } catch (SQLException e) {
      assertTrue(true);
      this.logger.error("SQLexception" + e.toString());
    }
  }

  @Test
  public void TestResultSetAbsolute() {
    try {
      assertTrue(result.absolute(1));
      assertEquals("you", result.getString(1));
      assertTrue(result.absolute(2));
      assertEquals("yet", result.getString(1));
      assertTrue(result.absolute(3));
      assertEquals("would", result.getString(1));
      assertTrue(result.absolute(4));
      assertEquals("world", result.getString(1));
      assertTrue(result.absolute(5));
      assertEquals("without", result.getString(1));
      assertTrue(result.absolute(6));
      assertEquals("with", result.getString(1));
      assertTrue(result.absolute(7));
      assertEquals("will", result.getString(1));
      assertTrue(result.absolute(8));
      assertEquals("why", result.getString(1));
      assertTrue(result.absolute(9));
      assertEquals("whose", result.getString(1));
      assertTrue(result.absolute(10));
      assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertFalse(result.absolute(0));
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }

    try {
      assertFalse(result.absolute(11));
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetAfterlast() {
    try {
      result.afterLast();
      assertTrue(result.previous());
      assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      result.afterLast();
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetBeforeFirst() {
    try {
      result.beforeFirst();
      assertTrue(result.next());
      assertEquals("you", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      result.beforeFirst();
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetFirst() {
    try {
      assertTrue(result.first());
      assertTrue(result.isFirst());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetBoolean() {
    try {
      assertTrue(result.absolute(1));
      assertEquals(Boolean.parseBoolean("42"), result.getBoolean(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetFloat() {
    try {
      assertTrue(result.absolute(1));
      assertEquals(new Float(42), result.getFloat(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetInteger() {
    try {
      assertTrue(result.absolute(1));
      assertEquals(42, result.getInt(2));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetRow() {

    try {
      assertTrue(result.absolute(1));
      assertEquals(1, result.getRow());
      assertTrue(result.absolute(10));
      assertEquals(10, result.getRow());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    try {
      result.beforeFirst();
      assertEquals(0, result.getRow());
      result.afterLast();
      assertEquals(0, result.getRow());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetgetString() {
    try {
      assertTrue(result.first());
      assertEquals("you", result.getString(1));
      assertTrue(result.last());
      assertEquals("whom", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetLast() {
    try {
      assertTrue(result.last());
      assertTrue(result.isLast());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
  }

  @Test
  public void TestResultSetNext() {
    try {
      assertTrue(result.first());
      assertTrue(result.next());
      assertEquals("yet", result.getString(1));
      assertTrue(result.next());
      assertEquals("would", result.getString(1));
      assertTrue(result.next());
      assertEquals("world", result.getString(1));
      assertTrue(result.next());
      assertEquals("without", result.getString(1));
      assertTrue(result.next());
      assertEquals("with", result.getString(1));
      assertTrue(result.next());
      assertEquals("will", result.getString(1));
      assertTrue(result.next());
      assertEquals("why", result.getString(1));
      assertTrue(result.next());
      assertEquals("whose", result.getString(1));
      assertTrue(result.next());
      assertEquals("whom", result.getString(1));
      assertFalse(result.next());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }

    try {
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetPrevious() {
    try {
      assertTrue(result.last());
      assertTrue(result.previous());
      assertEquals("whose", result.getString(1));
      assertTrue(result.previous());
      assertEquals("why", result.getString(1));
      assertTrue(result.previous());
      assertEquals("will", result.getString(1));
      assertTrue(result.previous());
      assertEquals("with", result.getString(1));
      assertTrue(result.previous());
      assertEquals("without", result.getString(1));
      assertTrue(result.previous());
      assertEquals("world", result.getString(1));
      assertTrue(result.previous());
      assertEquals("would", result.getString(1));
      assertTrue(result.previous());
      assertEquals("yet", result.getString(1));
      assertTrue(result.previous());
      assertEquals("you", result.getString(1));
      assertFalse(result.previous());
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    try {
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
  }

  @Test
  public void TestResultSetRelative() {
    try {
      assertTrue(result.absolute(1));
      assertEquals("you", result.getString(1));
      assertTrue(result.relative(1));
      assertEquals("yet", result.getString(1));
      assertTrue(result.relative(2));
      assertEquals("world", result.getString(1));
      assertTrue(result.relative(5));
      assertEquals("whose", result.getString(1));
      assertTrue(result.relative(-5));
      assertEquals("world", result.getString(1));
      assertTrue(result.relative(-2));
      assertEquals("yet", result.getString(1));
      assertTrue(result.relative(-1));
      assertEquals("you", result.getString(1));
    } catch (SQLException e) {
      this.logger.error("SQLexception" + e.toString());
      fail("SQLException" + e.toString());
    }
    try {
      assertTrue(result.first());
      assertFalse(result.relative(-1));
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }

    try {
      assertTrue(result.last());
      assertFalse(result.relative(1));
      assertEquals("", result.getString(1));
    } catch (SQLException e) {
      boolean ct = e.toString().contains("Cursor is not in a valid Position");
      if (ct == true) {
        assertTrue(ct);
      } else {
        this.logger.error("SQLexception" + e.toString());
        fail("SQLException" + e.toString());
      }
    }
  }
}
