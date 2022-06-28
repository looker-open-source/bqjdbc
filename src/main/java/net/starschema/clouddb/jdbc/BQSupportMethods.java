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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This Class provides static function to cooperate with the objects of the JDBC driver
 *
 * @author Horváth Attila
 */
public class BQSupportMethods {
  /**
   * This method is to get containing data of a java.sql.ResultSet as a
   * List&lt;List&lt;String&gt;&gt;
   *
   * @param result is a java.sql.ResultSet
   * @return a List {@code<List<String>>} containing data of the ResultSet
   * @throws SQLException if error occurs when obtaining data from the Resultset
   */
  public static String[][] GetQueryResult(java.sql.ResultSet result) throws SQLException {
    int ColumnCount = result.getMetaData().getColumnCount();
    List<List<String>> returned = new ArrayList<List<String>>();
    // TODO finish the converter :D
    for (int i = 0; i < ColumnCount; i++) {
      returned.add(new ArrayList<String>());
    }

    while (result.next() == true) {
      for (int i = 0; i < ColumnCount; i++) {
        returned.get(i).add(result.getString(i + 1));
      }
    }

    String[][] returning = new String[returned.size()][];
    for (int i = 0; i < returned.size(); i++) {
      returning[i] = new String[returned.get(i).size()];
      for (int k = 0; k < returned.get(i).size(); k++) {
        returning[i][k] = returned.get(i).get(k);
      }
    }
    return returning;
  }
}
