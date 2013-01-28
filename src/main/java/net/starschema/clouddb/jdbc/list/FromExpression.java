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
 * This class extends the basic Node, it contains the FromExpression with its SelectStatement
 * 
 * @author Attila Horvath, Balazs Gunics
 */
public class FromExpression extends Node {
    TreeBuilder builder;
    SelectStatement selectStatement =  null;
    
    /**
     * Constructor to build up a FromExpression from a sourcetable
     * @param sourceTable - to be contained in the fromExpression
     * @param treeBuilder
     * @throws TreeParsingException
     */
    public FromExpression(SourceTable sourceTable, TreeBuilder treeBuilder)
            throws TreeParsingException {
        this.builder = treeBuilder;
        this.tokenType = JdbcGrammarParser.FROMEXPRESSION;
        this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
        this.children.addLast(sourceTable);        
    }
    
    /**
     * Constructor to build up a FromExpression from an ANTLR tree, more info at:
     * {@link #build(Tree, TreeBuilder)}
     * 
     * @param t - the ANTLR tree
     * @param treeBuilder - to reach the helper functions
     * @param selectStatement - which contains the fromExpression
     * @throws Exception
     */
    public FromExpression(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }
    
    /**
     * Constructor to make an empty fromExpression
     * @param treeBuilder - to reach the helper functions
     */
    public FromExpression(TreeBuilder treeBuilder) {
        this.builder = treeBuilder;
    }
    
    /**
     * Constructor to make a FromExpression from a JoinExpression
     * @param builder - to reach the helper functions
     * @param joinExpression - to be contained in the FromExpression
     */
    public FromExpression(TreeBuilder builder, JoinExpression joinExpression) {
        this.builder = builder;
        this.children.addLast(joinExpression);
        this.tokenType = JdbcGrammarParser.FROMEXPRESSION;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.FROMEXPRESSION];
    }
    
    /**
     * The builder to parse out the ANTLR tree, we're making subqueries 
     * from everything, because it's easier to work with only subqueries
     * 
     * <li> Tables, <li> Joins, <li> Join-Joins will be converted into subqueries
     * 
     * @param t - the ANTLR tree
     * @param builder - to reach the helper functions
     * @throws Exception
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.FROMEXPRESSION) {
            this.tokenType = t.getType();
            this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
            this.logger.debug("BUILDING " + this.tokenName + " which has "
                    + t.getChildCount() + " child");
            for (int i = 0; i < t.getChildCount(); i++) {
                JoinExpression joinExpression;
                SubQuery makeSubQueryFromJoinExpression; 
                Tree child = t.getChild(i);
                switch (child.getType()) {                  
                    case JdbcGrammarParser.SOURCETABLE:
                        
                        Resolver resolve = new Resolver(builder);
                        // whats the sourcetable?
                        SourceTable tablenode = new SourceTable(child, builder);
                        String alias = tablenode.getAlias();
                        
                        // getting the columns for it
                        List<ColumnCall> columnlist = resolve
                                .parseSrcTableForJokers(tablenode);
                        // making a new Subquery with those columns +
                        // fromexpression
                        this.logger.debug("got columns");
                        tablenode.alias = null;
                        // making a fromexpression with that sourcetable
                        FromExpression fromexpression = new FromExpression(tablenode, builder);
                        
                        Expression expression = new Expression(columnlist, builder);
                        
                        SelectStatement mySelectStatement = new SelectStatement(expression, fromexpression, builder);
                        expression.setSelectStatement(mySelectStatement);
                        SubQuery mySubQuery = null;
                        if (alias != null) {
                            // putting the selectstatement into a subquery
                            mySubQuery = new SubQuery(alias, builder, mySelectStatement);
                        }
                        else {
                            String newalias = "";
                            if (tablenode.getDataset() != null) {
                                newalias += tablenode.getDataset() + ".";
                            }
                            newalias += tablenode.getName();
                            mySubQuery = new SubQuery(newalias, builder,
                                    mySelectStatement);
                        }
                        
                        // adding the subquery to the fromexpression
                        this.children.addLast(mySubQuery);
                        
                        break;
                    case JdbcGrammarParser.SUBQUERY:
                        this.children.addLast(new SubQuery(child, builder));
                        break;
                    case JdbcGrammarParser.JOINEXPRESSION:
                        // WE make SubQuery from a JOINEXPRESSION
                        try {
                            joinExpression = new JoinExpression(child, builder, this.selectStatement);
                        }
                        catch (ColumnCallException e) {
                            //Parsing failed throwing a ParsingException
                            throw new TreeParsingException(e);
                        }
                        
                        for (int k = 0; k < child.getChildCount(); k++) {
                            Tree schild = child.getChild(k);
                            logger.debug("SEARCHING MULTIJOINEXPRESSION");                            
                            switch (schild.getType()) {
                            case JdbcGrammarParser.MULTIJOINEXPRESSION:
                                System.err.println("BUILDING MULTIJOINEXPRESSION");
                                SubQuery  SubQueryFromJoinExpression = WhereExpressionJoinResolver.mkSubQFromJoinExpr(joinExpression, builder, selectStatement);
                                
                                    try {
                                        joinExpression = new JoinExpression(SubQueryFromJoinExpression, schild, builder, selectStatement);
                                    }
                                    catch (ColumnCallException e) {
                                        //Parsing failed throwing a ParsingException
                                        throw new TreeParsingException(e);
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        
                        
                        makeSubQueryFromJoinExpression = WhereExpressionJoinResolver.mkSubQFromJoinExpr(joinExpression, 
                                builder, this.selectStatement);
                        logger.debug("Printing out SubQuery:\n"+makeSubQueryFromJoinExpression.toPrettyString(1));
                        children.addLast(makeSubQueryFromJoinExpression);
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            throw new TreeParsingException("This Tree is not a FROMEXPRESSION");
        }
    }
    
    /**
     * Returns all SourceTables including SourceTables inside JoinExpressions
     * 
     * @return
     */
    public List<SourceTable> getAllSourceTables() {
        List<SourceTable> sourceTables = this.getSourceTables();
        List<JoinExpression> joinExpressions = this.getJoinExpressions();
        if (joinExpressions != null) {
            for (JoinExpression joinExpression : joinExpressions) {
                Node leftItem = joinExpression.getLeftItem();
                Node rightItem = joinExpression.getRightItem();
                if (leftItem.getTokenType() == JdbcGrammarParser.SOURCETABLE) {
                    if (sourceTables == null) {
                        sourceTables = new ArrayList<SourceTable>();
                    }
                    sourceTables.add((SourceTable) leftItem);
                }
                
                if (rightItem.getTokenType() == JdbcGrammarParser.SOURCETABLE) {
                    if (sourceTables == null) {
                        sourceTables = new ArrayList<SourceTable>();
                    }
                    sourceTables.add((SourceTable) rightItem);
                }
            }
        }
        return sourceTables;
    }
    
    /**
     * Returns all SubQueries including SubQueries inside JoinExpressions
     * 
     * @return
     */
    public List<SubQuery> getAllSubQueries() {
        List<SubQuery> subQueries = this.getSubQueries();
        List<JoinExpression> joinExpressions = this.getJoinExpressions();
        if (joinExpressions != null) {
            for (JoinExpression joinExpression : joinExpressions) {
                Node leftItem = joinExpression.getLeftItem();
                Node rightItem = joinExpression.getRightItem();
                if (leftItem.getTokenType() == JdbcGrammarParser.SUBQUERY) {
                    if (subQueries == null) {
                        subQueries = new ArrayList<SubQuery>();
                    }
                    subQueries.add((SubQuery) leftItem);
                }
                
                if (rightItem.getTokenType() == JdbcGrammarParser.SUBQUERY) {
                    if (subQueries == null) {
                        subQueries = new ArrayList<SubQuery>();
                    }
                    subQueries.add((SubQuery) rightItem);
                }
                
            }
        }
        return subQueries;
    }
    
    /**
     * getter for the Joinexpressions
     */
    public List<JoinExpression> getJoinExpressions() {
        return this.getAllinstancesof(JoinExpression.class,
                JdbcGrammarParser.JOINEXPRESSION);
    }

    /**
     * getter for the sourcetables
     */
    public List<SourceTable> getSourceTables() {
        return this.getAllinstancesof(SourceTable.class,
                JdbcGrammarParser.SOURCETABLE);
    }
    
    /**
     * getter for the SubQueries
     */
    public List<SubQuery> getSubQueries() {
        return this.getAllinstancesof(SubQuery.class,
                JdbcGrammarParser.SUBQUERY);
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        int newlevel = level < 0 ? -1 : level + 1;
        String result = this.tab(level - 1) + " FROM " + newline;
        for (Node node : this.children) {
            result += node.toPrettyString(newlevel) + ",";
        }
        result = result.substring(0, result.length() - 1);
        return result;
    }
}
