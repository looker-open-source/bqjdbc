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

import java.util.ArrayList;
import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node with the following attributes 
 * <li> SelectStatement - which contains the SubQuery 
 * <li>alias - for the SelectStatement
 * 
 * @author Attila Horvath, Balazs Gunics
 */
public class SubQuery extends Node {
    String alias = null;
    SelectStatement selectStatement = null;
    
    TreeBuilder builder;
    
    /** Is this SubQuery part of a join? */
    private boolean isPartOfJoin=false;
    
    private String uniqueId;
    
    /** Setter for isPartOfJoin, setting it true means that this subquery is part of a join */
    public void setisPartOfJoin()
    {
        isPartOfJoin = true;
    }
    
    /** Getter for isPartOfJoin() */
    public boolean isPartOfJoin() {
        return isPartOfJoin;
    }
    
    /**
     * Constructor for SubQuery, which builds a SubQuery from a selectStatement
     * with the given alias
     * @param alias - the SubQuerys name
     * @param builder - the TreeBuilder for the helper functions
     * @param selectStatement - to be contained in the SubQuery
     */
    public SubQuery(String alias, TreeBuilder builder,
            SelectStatement selectStatement) {
        this.builder = builder;
        this.selectStatement = selectStatement;
        selectStatement.setParent(this);
        this.alias = alias;
        this.tokenType = JdbcGrammarParser.SUBQUERY;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.SUBQUERY];
        this.uniqueId = builder.getuniqueid();
    }
    
    /**
     * Constructor for SubQuery, which builds a SubQuery from a selectStatement
     * with the given alias and with the given uniqueId
     * @param alias - the SubQuerys name
     * @param builder - the TreeBuilder for the helper functions
     * @param selectStatement - to be contained in the SubQuery
     * @param uniqueId - the uniqueId for the subquery
     */
    public SubQuery(String alias, TreeBuilder builder,
            SelectStatement selectStatement, String uniqueId) {
        this.builder = builder;
        this.selectStatement = selectStatement;
        selectStatement.setParent(this);
        this.alias = alias;
        this.tokenType = JdbcGrammarParser.SUBQUERY;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.SUBQUERY];
        this.uniqueId = uniqueId;
    }
    
    /**
     * Constructor for SubQuery, which builds a SubQuery from an ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @throws TreeParsingException - if we fail to parse the ANTLR tree
     */
    public SubQuery(Tree t, TreeBuilder treeBuilder) throws TreeParsingException {
        this.builder = treeBuilder;
        this.uniqueId = this.builder.getuniqueid();
        this.build(t, this.builder);
    }
    
    /**
     * Builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException - if we fail to parse the ANTLR tree
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.SUBQUERY) {
            
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.logger.debug("BUILDING " + this.tokenName);
            this.tokenType = t.getType();
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.SELECTSTATEMENT:
                        try {
                            this.selectStatement = new SelectStatement(child,
                                    builder);
                        }
                        catch (ColumnCallException e) {
                            throw new TreeParsingException(e);
                        }
                        selectStatement.parent = this;
                        break;
                    case JdbcGrammarParser.ALIAS:
                        this.alias = child.getChild(0).getText();
                    default:
                        break;
                }
            }
            if (this.getAlias() == null) {
                // we give alias as unique id
                this.alias = this.uniqueId;
            }
        }
        else {
            throw new TreeParsingException("This Tree is not a SUBQUERY");
        }
    }
    
    /** Getter for the SuqbQuery alias */
    public String getAlias() {
        return this.alias;
    }

    /**
     * Iterates through the Columns and functions of this subquery and
     * puts their Synonyms into a SynonymContainer, then returns these
     * SynonymContainers as a List
     * @return - the Synonyms of the Columns/Functions
     */
    public List<SynonymContainer> getAvailableResources() {
        Expression expression = this.selectStatement.getExpression();
        List<ColumnCall> columns = expression.getColumns();
        List<FunctionCall> functionCalls = expression.getFunctionCalls();
        
        List<SynonymContainer> container = new ArrayList<SynonymContainer>();
        
        if (columns != null) {
            for (ColumnCall columnCall : columns) {
                
                SynonymContainer columnCallSynonymContainer = new SynonymContainer(
                        columnCall);
                
                List<String> synonyms = columnCall.getSynonyms();
                if(synonyms!=null){
                    for (String string : synonyms) {
                        columnCallSynonymContainer.addSynonym(string);
                    }
                }
                container.add(columnCallSynonymContainer);
            }
        }
        if(functionCalls!=null){
            for (FunctionCall functionCall : functionCalls) {
                SynonymContainer columnCallSynonymContainer = new SynonymContainer(
                        functionCall);
                List<String> synonyms = functionCall.getSynonyms();
                if(synonyms!=null){
                    for (String string : synonyms) {
                        columnCallSynonymContainer.addSynonym(string);
                    }
                }
                container.add(columnCallSynonymContainer);
            }
        }
        return container;
    }
    
    /** Getter for the SelectStatement */
    public SelectStatement getSelectStatement() {
        return this.selectStatement;
    }
    
    /** Getter for the SubQuerys uniqueId */
    public String getUniqueId() {
        return this.uniqueId;
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        String result;
        int newlevel = level < 0 ? -1 : level;
        if (this.alias != null) {
            result = this.tab(level - 1) + "("
                    + this.selectStatement.toPrettyString(newlevel) + ") "
                    + this.alias + newline;
        }
        else {
            result = this.tab(level - 1) + "("
                    + this.selectStatement.toPrettyString(newlevel) + ")"
                    + newline;
        }
        return result;
    }
}
