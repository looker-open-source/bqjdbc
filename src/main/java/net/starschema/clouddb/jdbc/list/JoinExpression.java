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

import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node with the following attributes: <li>LeftItem
 * - Node for the joins left expression <li>RightItem - Node for the joins right
 * expression <li>type - the joins type from JoinType <li>Onclause - the columns
 * to join on
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class JoinExpression extends Node {

    Node leftItem;
    Node rightItem;
    JoinType type = null;
    OnClause onClause = null;
    boolean each = false;
    SelectStatement selectStatement;


    TreeBuilder builder;

    /**
     * Constructor for building JoinExpressions from the ANTLR tree
     * @param t
     * @param treeBuilder
     * @param selectStatement
     * @throws Exception
     */
    public JoinExpression(Tree t, TreeBuilder treeBuilder,
                          SelectStatement selectStatement) throws TreeParsingException, ColumnCallException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.build(t, this.builder);
    }

    /**
     * A Constructor for handling MULTIJOINEXPRESSIONS
     *
     * @param leftItem - the JOIN's left ITEM, which is a subquery
     * @param t - the Tree where we get the join, right, onclause
     * @param treeBuilder
     * @param selectStatement
     * @throws TreeParsingException
     * @throws ColumnCallException
     */
    public JoinExpression(SubQuery leftItem, Tree t, TreeBuilder treeBuilder,
                          SelectStatement selectStatement) throws TreeParsingException, ColumnCallException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.leftItem = leftItem;
        leftItem.setisPartOfJoin();
        this.build(t, this.builder);
    }

    /**
     * A Constructor to build JoinExpressions from two Subqueries
     *
     * @param builder - the TreeBuilder for the helper functions
     * @param leftItem - Subquery for the left side of the join
     * @param rightItem - Subquery for the right side of the join
     * @param onClause - the onClause to link the 2 subqueries
     * @param selectStatement - the selectStatement which contains the join
     */
    public JoinExpression(TreeBuilder builder, Node leftItem, Node rightItem,
                          OnClause onClause, SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
        this.builder = builder;
        this.leftItem = leftItem;
        this.rightItem = rightItem;
        if (leftItem.tokenType == JdbcGrammarParser.SUBQUERY) {
            SubQuery subQuery = SubQuery.class.cast(leftItem);
            subQuery.setisPartOfJoin();
        }
        if (rightItem.tokenType == JdbcGrammarParser.SUBQUERY) {
            SubQuery subQuery = SubQuery.class.cast(rightItem);
            subQuery.setisPartOfJoin();
        }
        this.onClause = onClause;
        this.tokenType = JdbcGrammarParser.JOINEXPRESSION;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.JOINEXPRESSION];
    }


    public void build(Tree t, TreeBuilder builder) throws TreeParsingException, ColumnCallException {
        if (t.getType() == JdbcGrammarParser.JOINEXPRESSION || t.getType() == JdbcGrammarParser.MULTIJOINEXPRESSION) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.LEFTEXPR:
                        switch (child.getChild(0).getType()) {
                            case JdbcGrammarParser.SUBQUERY:
                                SubQuery subQuery = new SubQuery(child.getChild(0),
                                        builder);
                                subQuery.setisPartOfJoin();
                                this.leftItem = subQuery;

                                break;
                            case JdbcGrammarParser.SOURCETABLE:

                                Resolver resolve = new Resolver(builder);
                                // whats the sourcetable?
                                SourceTable tablenode = new SourceTable(child.getChild(0), builder);
                                String alias = tablenode.getAlias();

                                // getting the columns for it
                                List<ColumnCall> columnlist = resolve
                                        .parseSrcTableForJokers(tablenode);
                                // making a new Subquery with those columns +
                                // fromexpression
                                this.logger.debug("got columns");
                                tablenode.alias = null;
                                // making a fromexpression with that sourcetable
                                FromExpression fromexpression = new FromExpression(
                                        tablenode, builder);

                                Expression expression = new Expression(columnlist, builder);

                                SelectStatement mySelectStatement = new SelectStatement(expression,
                                        fromexpression, builder);
                                expression.setSelectStatement(mySelectStatement);
                                SubQuery mySubQuery = null;
                                if (alias != null) {
                                    // putting the selectstatement into a subquery
                                    mySubQuery = new SubQuery(alias, builder,
                                            mySelectStatement);
                                } else {
                                    String newalias = "";
                                    if (tablenode.getDataset() != null) {
                                        newalias += tablenode.getDataset() + ".";
                                    }
                                    newalias += tablenode.getName();
                                    mySubQuery = new SubQuery(newalias, builder,
                                            mySelectStatement);
                                }

                                // adding the subquery to the fromexpression
                                mySubQuery.setisPartOfJoin();
                                this.leftItem = mySubQuery;

                                break;
                            default:
                                break;
                        }
                        break;
                    case JdbcGrammarParser.RIGHTEXPR:
                        switch (child.getChild(0).getType()) {
                            case JdbcGrammarParser.SUBQUERY:
                                SubQuery subQuery = new SubQuery(child.getChild(0),
                                        builder);
                                subQuery.setisPartOfJoin();
                                this.rightItem = subQuery;

                                break;
                            case JdbcGrammarParser.SOURCETABLE:

                                Resolver resolve = new Resolver(builder);
                                // whats the sourcetable?
                                SourceTable tablenode = new SourceTable(child.getChild(0), builder);
                                String alias = tablenode.getAlias();

                                // getting the columns for it
                                List<ColumnCall> columnlist = resolve
                                        .parseSrcTableForJokers(tablenode);
                                // making a new Subquery with those columns +
                                // fromexpression
                                this.logger.debug("got columns");
                                tablenode.alias = null;
                                // making a fromexpression with that sourcetable
                                FromExpression fromexpression = new FromExpression(
                                        tablenode, builder);

                                Expression expression = new Expression(columnlist, builder);

                                SelectStatement mySelectStatement = new SelectStatement(expression,
                                        fromexpression, builder);
                                expression.setSelectStatement(mySelectStatement);
                                SubQuery mySubQuery = null;
                                if (alias != null) {
                                    // putting the selectstatement into a subquery
                                    mySubQuery = new SubQuery(alias, builder,
                                            mySelectStatement);
                                } else {
                                    String newalias = "";
                                    if (tablenode.getDataset() != null) {
                                        newalias += tablenode.getDataset() + ".";
                                    }
                                    newalias += tablenode.getName();
                                    mySubQuery = new SubQuery(newalias, builder,
                                            mySelectStatement);
                                }

                                // adding the subquery to the fromexpression
                                mySubQuery.setisPartOfJoin();
                                this.rightItem = mySubQuery;

                                break;
                            default:
                                break;
                        }
                        break;
                    case JdbcGrammarParser.JOINTYPE:
                        switch (child.getChild(0).getType()) {
                            case JdbcGrammarParser.LEFT_KEYWORD:
                                this.type = JoinType.LEFT;
                                break;
                            case JdbcGrammarParser.RIGHT_KEYWORD:
                                this.type = JoinType.RIGHT;
                                break;
                            case JdbcGrammarParser.FULl_KEYWORD:
                                this.type = JoinType.FULL;
                                break;
                            case JdbcGrammarParser.INNERKEYWORD:
                                this.type = JoinType.INNER;
                                break;
                            default:
                                break;
                        }
                        if (child.getChild(1).getType() == JdbcGrammarParser.EACH) {
                            each = true;
                        }
                        break;
                    default:
                        break;
                }
            }
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.ONCLAUSE:
                        this.onClause = new OnClause(child, builder,
                                this.selectStatement);
                        break;
                    default:
                        break;
                }
            }
            for (Node condition : onClause.children) {

                OnClauseCondition onClauseCondition = OnClauseCondition.class.cast(condition);
                ColumnReference left = onClauseCondition.left;
                ColumnReference right = onClauseCondition.right;

                left.setPointedNode(left.searchPointedNodeInSubQuery((SubQuery) leftItem));
                if (left.getPointedNode() == null) {
                    left.setPointedNode(left.searchPointedNodeInSubQuery((SubQuery) rightItem));
                }
                right.setPointedNode(right.searchPointedNodeInSubQuery((SubQuery) leftItem));
                if (right.getPointedNode() == null) {
                    right.setPointedNode(right.searchPointedNodeInSubQuery((SubQuery) rightItem));
                }
            }
        } else {
            throw new TreeParsingException("This Tree is not an JOINEXPRESSION");
        }
    }

    public Node getLeftItem() {
        return this.leftItem;
    }

    public OnClause getOnclause() {
        return this.onClause;
    }

    public Node getRightItem() {
        return this.rightItem;
    }

    public JoinType getjointype() {
        return this.type;
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        int newlevel = level < 0 ? -1 : level + 1;
        String result = "";
        result += this.leftItem.toPrettyString(newlevel);
        if (this.type != null) {
            result += " " + this.type.toString() + newline + this.tab(newlevel)
                    + " JOIN " + newline;
        } else {
            result += newline + this.tab(level + 1) + (each ? " JOIN EACH " : " JOIN ") + newline;
        }
        result += this.rightItem.toPrettyString(newlevel);
        result += newline + this.onClause.toPrettyString(newlevel);
        return result;
    }
}

enum JoinType {
    LEFT, RIGHT, FULL, INNER
}
