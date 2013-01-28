/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
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
                        if(this.alias.equals("\"") || this.alias.equals("\'") ){
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
                        }
                        catch (IOException e) {
                            // something went wrong
                        }
                        // it doesn't contains we look for the next schema
                        schemas.next();
                        
                    }
                }
                catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            logger.debug("Built up the sourcetable: " +this.name +
                    " tables dataset is: " + this.dataset +
                    " project is: " + (project != null ? project : "missing"));
        }
        else {
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
        return this.project.replace("__", ":").replace("_", ".") ;
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
