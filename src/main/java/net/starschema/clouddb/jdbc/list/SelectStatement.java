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

import java.util.LinkedList;
import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node
 * the childrens will be 
 * <li> EXPRESSION 
 * <li> WHEREEXPRESSION 
 * <li> FROMEXPRESSION
 * <li> ORDERBYEXPRESSION 
 * <li> HAVINGEXPRESSION 
 * <li> LIMITEXPRESSION
 * 
 * @author Balazs Gunics, Attila Horvath
 */
public class SelectStatement extends Node {
    
    TreeBuilder builder;
    Expression expression = null;
    /** the From Expression of this Select */
    FromExpression fromExpression = null;
    /** the Where Expression of this Select */ 
    WhereExpression whereExpression = null;
    /** the Group By Expression of this Select */
    GroupByExpression groupByExpression = null;
    /** the Having Expression of this Select */
    HavingExpression havingExpression = null;
    /** the Order By Expression of this Select */
    OrderbyExpression orderByExpression = null;
    /** the Limit Expression of this Select */
    LimitExpression limitExpression = null;
    /** if this Select Statement is in a SubQuery 
     * we store its parent as a Subquery*/
    SubQuery parent = null;
    
    /** Returns a SubQuery if this selectStatement
     *  is in a SubQuery, or null if it isn't 
     * @return SubQuery or null
     */
    public SubQuery getParent() {
        return parent;
    }
    
    /** Setter for the Parent, if this Select is part
     * of a SubQuery we'll set that as parent 
     * @param parent - the SubQuery containing this Select
     */
    public void setParent(SubQuery parent) {
        this.parent = parent;
    }
    
    /**
     * Creates a SelectStatement with Expression and FromExpression
     * also adds them as childrens towards the expression, and fromExpression
     * @param expression - the Selects expression
     * @param fromExpression - the fromExpression for this select
     * @param treeBuilder - the TreeBuilder for the helper functions
     */
    public SelectStatement(Expression expression,
            FromExpression fromExpression, TreeBuilder treeBuilder) {
        this.builder = treeBuilder;
        this.tokenType = JdbcGrammarParser.SELECTSTATEMENT;
        this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
        this.expression = expression;
        this.fromExpression = fromExpression;
        this.children.addLast(expression);
        this.children.addLast(fromExpression);
    }
    
    /**
     * Constructor for the selectStatement which builds it up from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @throws TreeParsingException - if we fail to parse the tree
     * @throws ColumnCallException - if Ambiguous columns found
     */
    public SelectStatement(Tree t, TreeBuilder treeBuilder) 
            throws TreeParsingException, ColumnCallException {
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }
    
    /**
     * Creates a SelectStatement with Expression and FromExpression
     * to be used with the WhereExpressionResolver
     * @param builder - the TreeBuilder for the helper functions
     * @param expression - the Selects expression
     * @param fromExpression - the fromExpression for this select
     */
    public SelectStatement(TreeBuilder builder, Expression expression,
            FromExpression fromExpression) {

        this.builder = builder;
        this.expression = expression;
        this.fromExpression = fromExpression;
        this.tokenType = JdbcGrammarParser.SELECTSTATEMENT;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.SELECTKEYWORD];
    }
    
    /**
     * the Builder to Parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException - if we fail to Parse out the tree
     * @throws ColumnCallException - if ambiguous columns found
     */
    public void build(Tree t, TreeBuilder builder) 
            throws TreeParsingException, ColumnCallException {
        if (t.getType() == JdbcGrammarParser.SELECTSTATEMENT) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.logger.debug("BUILDING " + this.tokenName + "which has "
                    + t.getChildCount() + " child");
            this.tokenType = t.getType();
            // First we parse out the FROMEXPRESSION
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.FROMEXPRESSION:
                        this.fromExpression = new FromExpression(child,
                                builder, this);
                        this.children.addLast(this.fromExpression);
                        break;
                    default:
                        break;
                }
            }
            
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.WHEREEXPRESSION:
                        this.whereExpression = new WhereExpression(child,
                                builder, this);
                        break;
                } 
            }

            if(this.whereExpression==null && this.fromExpression.children.size()>1) {
                logger.debug("WHEREEXPRESSION WAS NULL");

                    LinkedList<Node> linkedList = this.fromExpression.children;
                    SubQuery left = null;
                    for (Node node : linkedList) {
                        if(left==null) {
                            left = (SubQuery)node;
                        }
                        else {
                            JoinExpression twoSubQIntoJoin = 
                                    WhereExpressionJoinResolver.mkJoinExprFrmTwoSubQ(
                                            left, (SubQuery)node);
                            left = WhereExpressionJoinResolver.mkSubQFromJoinExpr(
                                    twoSubQIntoJoin, builder, this); 
                        }
                    }
                    this.fromExpression.children = new LinkedList<Node>();
                    this.fromExpression.children.addLast(left);
            }
            else if(whereExpression!=null)
            {
                logger.debug(whereExpression.toPrettyString());
                Boolean keepWhere = false;
                switch (this.whereExpression.getExpression().getTokenType()) {
                        case JdbcGrammarParser.CONJUNCTION:
                            SubQuery conjunctionAsJoinExpr = 
                                WhereExpressionJoinResolver.mkJoinExprFrmConjunction(
                                        (Conjunction)whereExpression.getExpression(), this);
                            if(conjunctionAsJoinExpr != null){
                                this.fromExpression.children = new LinkedList<Node>();
                                this.fromExpression.children.add(conjunctionAsJoinExpr);
                            }
                            else {
                                //since we failed to build a Subquery from the conjunction,
                                //we should keep the Where
                                keepWhere = true;
                            }
                        break;
                        case JdbcGrammarParser.BOOLEANEXPRESSIONITEM:
                            SubQuery booleanExprAsJoin = 
                                WhereExpressionJoinResolver.mkJoinExprFrmBooleanExprItem(
                                        (BooleanExpressionItem)whereExpression.getExpression(), this);
                            if(booleanExprAsJoin == null){
                                //since we failed to build a Subquery from the conjunction,
                                //we should keep the Where
                                keepWhere = true;
                            }
                            else{                               
                            this.fromExpression.children = new LinkedList<Node>();
                            this.fromExpression.children.add(booleanExprAsJoin);
                            }
                        break;
                        case JdbcGrammarParser.DISJUNCTION:
                            List<SubQuery> disjunctionAsJoin = 
                                WhereExpressionJoinResolver.mkJoinExprFrmDisjunction(
                                        (Disjunction)whereExpression.getExpression(), this);
                            this.fromExpression.children = new LinkedList<Node>();
                            for (SubQuery subQuery : disjunctionAsJoin) {
                                this.fromExpression.children.add(subQuery);
                            }
                        break;
                    default:
                        break;
                }
                if(!keepWhere) {
                this.whereExpression = null;
                }
            }
            
            // After we take care for EXPRESSION
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.EXPRESSION:
                        logger.debug("BUILDING EXPRESSION");
                        this.expression = new Expression(child, builder,
                                this.fromExpression, this);
                        //this.children.addLast(this.expression);
                        break;
                    default:
                        break;
                }
            }
            
            // After we take care for others
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.GROUPBYEXPRESSION:
                        this.groupByExpression = new GroupByExpression(child,
                                builder, this);
                        this.children.addLast(this.groupByExpression);
                        break;
                    case JdbcGrammarParser.ORDERBYEXPRESSION:
                        this.orderByExpression = new OrderbyExpression(child,
                                builder, this);
                        this.children.addLast(this.orderByExpression);
                        break;
                    case JdbcGrammarParser.HAVINGEXPRESSION:
                        this.havingExpression = new HavingExpression(child,
                                builder, this);
                        this.children.addLast(this.havingExpression);
                        break;
                    case JdbcGrammarParser.LIMITEXPRESSION:
                        this.limitExpression = new LimitExpression(child,
                                builder);
                        this.children.addLast(this.limitExpression);
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            throw new TreeParsingException("This Tree is not a SELECTSTATEMENT");
        }
    }
    
    /** Getter for the TreeBuilder */
    public TreeBuilder getBuilder() {
        return this.builder;
    }
    
    /** Getter for the Expression */
    public Expression getExpression() {
        return this.expression;
    }
    
    /** Getter for the From Expression*/
    public FromExpression getFromExpression() {
        return this.fromExpression;
    }
    
    /** Getter for the Group By Expression*/
    public GroupByExpression getGroupByExpression() {
        return this.groupByExpression;
    }
    
    /** Getter for the Having Expression*/    
    public HavingExpression getHavingExpression() {
        return this.havingExpression;
    }
    
    /** Getter for the Limit Expression*/
    public LimitExpression getLimitExpression() {
        return this.limitExpression;
    }
    
    /** Getter for the Order By Expression*/
    public OrderbyExpression getOrderbyExpression() {
        return this.orderByExpression;
    }
    
    /** Getter for the Where Expression*/
    public WhereExpression getWhereExpression() {
        return this.whereExpression;
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        String result = "SELECT " + newline + this.tab(level)
                + this.expression.toPrettyString(level)
                + this.fromExpression.toPrettyString(level);
        if (this.whereExpression != null) {
            result += newline + this.whereExpression.toPrettyString(level);
        }
        if (this.groupByExpression != null) {
            result += newline + this.groupByExpression.toPrettyString(level);
        }
        if (this.havingExpression != null) {
            result += newline + this.havingExpression.toPrettyString(level);
        }
        if (this.orderByExpression != null) {
            result += newline + this.orderByExpression.toPrettyString(level);
        }
        if (this.limitExpression != null) {
            result += newline + this.limitExpression.toPrettyString(level);
        }
        
        return result;
    }
}
