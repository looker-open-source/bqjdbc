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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

import com.google.api.services.bigquery.model.Table;

/**
 * This class extends the basic Node
 * we store the following inside
 * <li> name
 * <li> dataset
 * <li> project
 * <li> alias
 * which made from:
 * "project:dataset.name (AS) alias"
 *
 * @author Balazs Gunics, Attila Horvath
 */
public class SourceTable extends Node {
    /** the Tables name */
    String name = null;
    /** the Tables dataset */
    String dataset = null;
    /** the Tables project stored URLEncoded, because it can contain ":" */
    String project = null;
    /** the Alias for the Table */
    String alias = null;

    TreeBuilder builder;

    private String uniqueId;

    /**
     * Constructor which builds up the SourceTable from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public SourceTable(Tree t, TreeBuilder treeBuilder)
            throws TreeParsingException {
        this.builder = treeBuilder;
        this.uniqueId = this.builder.getuniqueid();
        this.build(t, this.builder);
    }

    /**
     * Builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.SOURCETABLE) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.logger.debug("BUILDING " + this.tokenName);
            this.tokenType = t.getType();
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.NAME:
                        this.name = child.getChild(0).getText();
                        break;
                    case JdbcGrammarParser.DATASETNAME:
                        this.dataset = child.getChild(0).getChild(0).getText();
                        break;
                    case JdbcGrammarParser.PROJECTNAME:
                        this.project = "";
                        for (int k = 0; k < child.getChildCount(); k++) {
                            this.project += child.getChild(k).getChild(0)
                                    .getText();
                        }
                        break;
                    case JdbcGrammarParser.ALIAS:

                        this.alias = child.getChild(0).getText();
                        if (this.alias.equals("\"") || this.alias.equals("\'")) {
                            this.alias = child.getChild(1).getText();
                        }

                        this.logger.debug("SOURCETABLE ALIAS: " + this.alias);
                        break;
                    default:
                        break;
                }
            }
            // if we don't have a dataset, we can't make querys
            if (this.dataset == null) {
                try {
                    // first we get the schemas
                    ResultSet schemas = builder.connection.getMetaData()
                            .getSchemas();
                    schemas.first();
                    while (!schemas.isAfterLast()) {
                        try {
                            // we do a look up in the schemas, for our table
                            List<Table> tables = BQSupportFuncts.getTables(
                                    (BQConnection) builder.connection,
                                    this.builder.connection.getCatalog(),
                                    schemas.getString(1), this.name);
                            if (tables != null) {
                                // it contains, setting it as our dataset
                                this.dataset = schemas.getString(1);
                                return;
                            }
                        } catch (IOException e) {
                            // something went wrong
                        }
                        // it doesn't contains we look for the next schema
                        schemas.next();

                    }
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            logger.debug("Built up the sourcetable: " + this.name +
                    " tables dataset is: " + this.dataset +
                    " project is: " + (project != null ? project : "missing"));
        } else {
            throw new TreeParsingException("This Tree is not a SOURCETABLE");
        }
    }

    /** Getter for the Alias */
    public String getAlias() {
        return this.alias;
    }

    /** Getter for the Dataset */
    public String getDataset() {
        return this.dataset;
    }

    /** Getter for the Name */
    public String getName() {
        return this.name;
    }

    /** Getter for the Project please note: this is URLEncoded,
     * to decode use: <br> URLDecoder.decode(this result, "UTF-8")
     */
    public String getProject() {
        return this.project;
    }

    /** Getter for the Project please note: this is URLEncoded,
     * to decode use: <br> URLDecoder.decode(this result, "UTF-8")
     */
    public String getProjectDecoded() {
        return this.project.replace("__", ":").replace("_", ".");
    }

    /** Getter for the UniqueId */
    public String getUniqueId() {
        return this.uniqueId;
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        String result = "";
        result += this.tab(level);
        if (this.project != null) {
            result += getProjectDecoded() + ":";
        }
        if (this.dataset != null) {
            result += this.dataset + ".";
        }
        result += this.name;
        if (this.alias != null) {
            result += " AS " + this.alias;
        }
        return result;
    }
}
