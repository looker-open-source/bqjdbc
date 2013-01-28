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

import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node by adding a
 * BooleanExpression to it
 * 
 * @author Balazs Gunics, Attila Horvath
 */
public class WhereExpression extends Node {
    
    Node expression = null;
    
    /** Getter for the Expression of the WhereExpression */
    public Node getExpression() {
        return expression;
    }
    SelectStatement selectStatement;
    TreeBuilder builder;
    
    
    /**
     * Constructor for building WhereExpressions from an ANTLR tree
     * @param t - The ANTLR tree
     * @param treeBuilder - the TreeBuilder to reach the helper functions
     * @param selectStatement - This is where we want to attach the WhereExpression
     * @throws Exception
     */
    public WhereExpression(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }
    
    /**
     * Constructor for building WhereExpressions from BooleanExpression Nodes,
     * When we move the BooleanExpressions from Where to JOIN's ON Clause, 
     * we need to rebuild the whereexpression, from the unused nodes.
     * 
     * @param nodeList - A Node list which contains the BooleanExpressions that needs to be added to the WhereExpression
     * @param treeBuilder - the TreeBuilder to reach the helper functions
     * @param selectStatement - the selectStatement to attach the Where
     * @throws Exception
     */
    public WhereExpression(List<Node> nodeList, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(nodeList);
    }
    
    /**
     * builder to use with the ANTLR tree
     * @param t
     * @param builder
     * @throws Exception
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.WHEREEXPRESSION) {
            this.tokenType = t.getType();
            this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
            this.logger.debug("BUILDING " + this.tokenName);
            
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.DISJUNCTION:
                        this.expression = (new Disjunction(child, builder,
                                this, this.selectStatement));
                        break;
                    case JdbcGrammarParser.CONJUNCTION:
                        logger.debug("BUILDING CONJUNCTION OR DISJUNCTION FROM CONJUNCTION");
                        Node built = Conjunction.buildFromConjunction(child, builder, this, selectStatement);
                        if(built.getTokenType()==JdbcGrammarParser.CONJUNCTION){
                            this.expression = (Conjunction.class.cast(built));
                            logger.debug("CONJUNCTION BUILT AND ADDED TO WHEREEXPRESSION");
                        }
                        else{
                            this.expression = (Disjunction.class.cast(built));
                            logger.debug("DISJUNCTION BUILT AND ADDED TO WHEREEXPRESSION");
                        }   
                        break;
                    case JdbcGrammarParser.NEGATION:
                        this.expression = (new Negation(child, builder, this,
                                this.selectStatement));
                        break;
                    case JdbcGrammarParser.BOOLEANEXPRESSIONITEM:
                        this.expression = (new BooleanExpressionItem(child,
                                builder, this, this.selectStatement));
                        break;
                    default:
                        break;
                }
            }
            
        }
        else {
            throw new TreeParsingException("This Tree is not a WHEREEXPRESSION");
        }
    }
    
    /**
     * builder to use with WhereExpression(List<Node> nodelist, treeBuilder builder)
     * @param booleanExpressionToAdd - A list which contains the expressions
     * @throws Exception
     */
    public void build(List<Node> booleanExpressionToAdd) throws TreeParsingException {            
        this.tokenType = JdbcGrammarParser.WHEREEXPRESSION;
        this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
        this.logger.debug("BUILDING " + this.tokenName + "from booleanExpressions");
            
        if(booleanExpressionToAdd.size() == 1 && 
                booleanExpressionToAdd.get(0).getTokenType() == JdbcGrammarParser.BOOLEANEXPRESSIONITEM){
            this.expression = booleanExpressionToAdd.get(0);
            
            //Resolve new pointed Columns
            WhereExpressionJoinResolver.columnResolver(
                    (BooleanExpressionItem)this.expression, this.selectStatement);
        }
        else {
            this.expression = new Conjunction(booleanExpressionToAdd, selectStatement);
        }
    }
    
    @Override
    public String toPrettyString(int level) {
        System.err.println(this.expression.tokenName);
        return "WHERE " + this.expression.toPrettyString();
    }
}
