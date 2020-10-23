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
 */
package net.starschema.clouddb.jdbc.list;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarLexer;
import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the initializer for the SQL transformer
 * with the {@link #build()} function we're able to build up our
 * tree, which returns with a Node (which is a SelectStatement)
 * from there we're able to reach the transformed query by calling its
 * toPrettyString() function.
 * The Column resolving (because fo the select * ) and all the
 * transformation made On the Fly while parsing out the tree
 *
 * @author Attila Horvath, Balazs Gunics
 *
 */
public class TreeBuilder {

    /** the ANTLR tree made and used by the {@link #build()} function */
    Tree tree;
    /** the sql we want to transform */
    String sqlForParse;
    /** UniquId List made by {@link #makeUniqueId()} */
    List<String> UniqueIds;
    /** callContainer to reduce the Function calls,
     * which function was called once won't be called again*/
    CallContainer callContainer;
    /** the pointer for the UniqueId list */
    int nextIdPosition = -1;
    /** the connection to the JDBC driver */
    Connection connection;
    Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Constructor for the TreeBuilder, after its built, use the
     * {@link #build()} function to start the building of the query
     *
     * @param sqlforparse - the sql to parse
     * @param connection - the JDBC connection
     * @param callContainer - to reduce the Function calls through the JDBC
     */
    public TreeBuilder(String sqlforparse, Connection connection,
                       CallContainer callContainer) {
        this.sqlForParse = sqlforparse;
        this.connection = connection;
        this.callContainer = callContainer;
    }

    /**
     * The starter of everything, first we replace all the ` with "
     * then making an ANTLR tree
     * @return - SelectStatement as Node
     * @throws Exception
     */
    public Node build() throws Exception {
        this.sqlForParse = this.sqlForParse.replace('`', '\"').replace('\r', ' ').replace('\n', ' ');
        this.logger.debug("Building the ANTLR tree");
        CharStream stream = new ANTLRStringStream(this.sqlForParse);
        JdbcGrammarLexer lexer = new JdbcGrammarLexer(stream);
        CommonTokenStream tokenstream = new CommonTokenStream(lexer);
        JdbcGrammarParser parser = new JdbcGrammarParser(tokenstream);
        try {
            this.tree = (Tree) parser.statement().getTree();
        } catch (RecognitionException e) {
            logger.warn("Grammar error: Failed to parse the Query", e);
            throw new Exception("Grammar error: Failed to parse the Query", e);
        }
        this.logger.debug("Creating a new SelectStatement with the builded tree");
        try {
            return new SelectStatement(this.tree, this);
        } catch (TreeParsingException e) {
            logger.warn("Something went wrong with the TreeBuilding", e);
            throw new Exception("Something went wrong with the TreeBuilding", e);
        } catch (ColumnCallException e) {
            logger.warn("Ambiguous ColumnCall found", e);
            throw new Exception("Ambiguous ColumnCall found", e);
        }
    }

    /**
     * Makes a JDBC call to get the possible prefixes of the specified table, if it finds
     * out that this query has already been run, uses the stored results instead
     *
     * @param tableName - the Tables name which prefixes we want to know
     * @return - The prefixes of the Table
     */
    @SuppressWarnings("rawtypes")
    List<String> getPossiblePrefixes(String colName) {
        // we make jdbc calls to get columns
        this.logger.debug("making a jdbc call to get the Prefixes");
        List<String> Columns = new ArrayList<String>();
        try {
            // Try to get result from container first
            this.logger.debug("Try to get result from container first");
            Class[] args = new Class[4];
            args[0] = String.class;
            args[1] = String.class;
            args[2] = String.class;
            args[3] = String.class;
            Method method = null;
            try {
                this.logger.debug("getting the method: getcolumns");
                method = this.connection.getMetaData().getClass()
                        .getMethod("getColumns", args);
            } catch (SecurityException e) {
                // Should not occur
                this.logger.warn("failed to get the method getColumns " + e);
            } catch (NoSuchMethodException e) {
                // Should not occur
                this.logger.warn("failed to get the method getColumns " + e);
            }

            List<Parameter> params = new ArrayList<Parameter>();
            params.add(new Parameter(this.connection.getCatalog()));
            params.add(new Parameter("%"));
            params.add(new Parameter("%"));
            params.add(new Parameter(colName));
            ResultSet res = this.callContainer.getresult(method, params);
            if (res == null) {
                res = this.connection.getMetaData().getColumns(
                        this.connection.getCatalog(), "%", "%", colName);
                this.callContainer.AddCall(res, method, params);
            }
            res.first();
            // Iterating through the results
            while (!res.isAfterLast()) {
                Columns.add(res.getString(2) + "." + res.getString(3));
                logger.debug("found prefix:" + res.getString(2) + "." + res.getString(3));
                res.next();
            }
        } catch (SQLException e) {
            // should not happen
            this.logger.warn("failed to get prefixes for the column: " + colName, e);
        }
        return Columns;
    }

    /**
     * Makes a JDBC call to get the columns of the specified table, and project,
     * and schema if it finds out that this query has already been run,
     * uses the stored results instead
     *
     * @param srcTable - the SourceTable name which columns we want to know<br>
     *              <li>if it's project has been set we'll use it
     *              <li>if it's schema has been set we'll use it
     * @return - The columns of the Table
     */
    @SuppressWarnings("rawtypes")
    List<String> GetColumns(SourceTable srcTable) {
        // we make jdbc calls to get columns
        this.logger.debug("making a jdbc call to get the columns");
        List<String> Columns = new ArrayList<String>();
        try {
            // Try to get result from container first
            this.logger.debug("Try to get result from container first");
            Class[] args = new Class[4];
            args[0] = String.class;
            args[1] = String.class;
            args[2] = String.class;
            args[3] = String.class;
            Method method = null;
            try {
                this.logger.debug("getting the method: MetaData.getColumns");
                method = this.connection.getMetaData().getClass()
                        .getMethod("getColumns", args);
            } catch (SecurityException e) {
                // Should not occur
                this.logger.warn("failed to get the method getColumns " + e);
            } catch (NoSuchMethodException e) {
                // Should not occur
                this.logger.warn("failed to get the method getColumns " + e);
            }
            List<Parameter> params = new ArrayList<Parameter>();
            String projectName = null; // to be the 1st parameter of getColumns
            if (srcTable.project != null) {
                //if there's a projectname stored in the srctable
                projectName = srcTable.getProject().replace(".", "_").replace(":", "__");
            } else {
                //else we use the connections default
                projectName = this.connection.getCatalog();
            }
            String dataset;
            if (srcTable.dataset != null) {
                dataset = srcTable.dataset;
            } else {
                dataset = "%";
            }
            params.add(new Parameter(projectName));
            params.add(new Parameter(dataset));
            params.add(new Parameter(srcTable.getName()));
            params.add(new Parameter("%"));
            ResultSet res = this.callContainer.getresult(method, params);
            if (res == null) {
                res = this.connection.getMetaData().getColumns(
                        projectName, dataset, srcTable.getName(), "%");
                this.callContainer.AddCall(res, method, params);
            }
            res.first();
            // Iterating through the results
            while (!res.isAfterLast()) {
                Columns.add(res.getString(4));
                res.next();
            }
        } catch (SQLException e) {
            // should not happen
            this.logger.warn("failed to get columns for the table: \"" + srcTable.getName() +
                    "\" using * instead of columns", e);
            Columns.add("*");
        }
        return Columns;
    }

    /**
     * Returns an SQL Connection of the JDBC driver,
     * so the JDBC functions are accessable
     *
     * @return - a Connection which can be cast to (BQConnection)
     */
    public Connection getConnection() {
        return this.connection;
    }

    /**
     * Gets a unique id
     *
     * @return - the next UniqueId in the list
     */
    public String getuniqueid() {
        if (this.nextIdPosition < 0) {
            this.makeUniqueId();
        }
        return this.UniqueIds.get(this.nextIdPosition++);
    }

    /**
     * Makes 26 ^ seqWidth UniqueId and stores it in the
     * UniqueIds as UNIQ_ID_ + the generated chars
     *
     * currently seqWidth is 4 so the ID-s start at
     * UNIQ_ID_AAAA and ends at UNIQ_ID_ZZZZ
     *
     * This function runs only one time, to generate all the UNIQ_ID_
     *
     */
    private void makeUniqueId() {
        this.nextIdPosition = 0;
        // This is the configurable param
        int seqWidth = 4;

        Double charSetSize = 26d;

        // The size of the array will be 26 ^ seqWidth. ie: if 2 chars wide, 26
        // * 26. 3 chars, 26 * 26 * 26
        Double total = Math.pow(charSetSize,
                (new Integer(seqWidth)).doubleValue());

        StringBuilder[] sbArr = new StringBuilder[total.intValue()];
        // Initializing the Array
        for (int j = 0; j < total; j++) {
            sbArr[j] = new StringBuilder();
        }

        char ch = 'A';
        // Iterating over the entire length for the 'seqWidth' number of times.
        for (int k = seqWidth; k > 0; k--) {
            // Iterating and adding each char to the entire array.
            for (int l = 1; l <= total; l++) {
                sbArr[l - 1].append(ch);
                if ((l % (Math.pow(charSetSize, k - 1d))) == 0) {
                    ch++;
                    if (ch > 'Z') {
                        ch = 'A';
                    }
                }
            }
        }

        this.UniqueIds = new ArrayList<String>();
        // Use the stringbuilder array, to fill the UniquIds
        for (StringBuilder id : sbArr) {
            this.UniqueIds.add("UNIQ_ID_" + id);
        }
    }
}
