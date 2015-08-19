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

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class BooleanExpressionItem extends Node implements ColumnReferencePlace {

    /** The type of the Expression LIKEEXPRESSION, COMPARISON
     * #BooleanExpressionItemType*/
    BooleanExpressionItemType type = null;
    /** the BooleanExpressions right Node */
    Node right;
    /** the BooleanExpressions left Node */
    Node left;
    /** the operator */
    String comparisonOperator = null;
    Node mainNode;
    SelectStatement selectStatement;

    /**
     * Checks if a BooleanExpressionItem is equivalent to an other
     * @param compareBI
     * @return
     */
    public boolean equals(BooleanExpressionItem compareBI) {
        if (left.tokenType == compareBI.left.tokenType &&
                right.tokenType == compareBI.right.tokenType) {
            boolean leftSideEquals = false;
            boolean rightSideEquals = false;

            if (left.tokenType == JdbcGrammarParser.COLUMN) {
                ColumnReference columnReferenceLeft = ColumnReference.class.cast(left);
                ColumnReference columnReferenceLeftCompare = ColumnReference.class.cast(compareBI.left);

                //Check if pointedNodes are the same and comparisonOperator is the same
                if (columnReferenceLeft.getPointedNode().getUniqueid()
                        .equals(columnReferenceLeftCompare.getPointedNode().getUniqueid())) {
                    leftSideEquals = true;
                }
            }
            if (right.tokenType == JdbcGrammarParser.COLUMN) {
                ColumnReference columnReferenceRight = ColumnReference.class.cast(right);
                ColumnReference columnReferenceRightCompare = ColumnReference.class.cast(compareBI.right);
                if (columnReferenceRight.getPointedNode().getUniqueid()
                        .equals(columnReferenceRightCompare.getPointedNode().getUniqueid())) {
                    rightSideEquals = true;
                }
            }
            if (left.tokenType == JdbcGrammarParser.SUBQUERY) {
                if (left.toPrettyString().equals(compareBI.left.toPrettyString())) {
                    leftSideEquals = true;
                }
            }
            if (right.tokenType == JdbcGrammarParser.SUBQUERY) {
                if (right.toPrettyString().equals(compareBI.right.toPrettyString())) {
                    rightSideEquals = true;
                }
            }
            if (left.tokenType == JdbcGrammarParser.STRINGLIT) {
                if (left.toPrettyString().equals(compareBI.left.toPrettyString())) {
                    leftSideEquals = true;
                }
            }
            if (right.tokenType == JdbcGrammarParser.STRINGLIT) {
                if (right.toPrettyString().equals(compareBI.right.toPrettyString())) {
                    rightSideEquals = true;
                }
            }

            if (leftSideEquals && rightSideEquals && this.comparisonOperator != null &&
                    compareBI.comparisonOperator != null &&
                    this.comparisonOperator.equals(compareBI.comparisonOperator)) {
                return true;
            } else {
                return false;
            }

        } else {
            return false;
        }
    }

    ;

    TreeBuilder builder;

    /**
     * The constructor to build up from an ANTLR tree
     *
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder to reach the helper functions
     * @param mainNode - the Node which contains the expression
     * @param selectStatement - the selectStatement which contains the expression
     * @throws Exception
     */
    public BooleanExpressionItem(Tree t, TreeBuilder treeBuilder,
                                 Node mainNode, SelectStatement selectStatement) throws TreeParsingException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.mainNode = mainNode;
        this.build(t, this.builder);
    }

    /**
     * the builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder to reach the helper functions
     * @throws Exception
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.BOOLEANEXPRESSIONITEM) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.BOOLEANEXPRESSIONITEMRIGHT:
                        switch (child.getChild(0).getType()) {
                            case JdbcGrammarParser.LIKEEXPRESSION:
                                this.type = BooleanExpressionItemType.LIKEEXPRESSION;
                                this.right = new StringLiteral(child
                                        .getChild(0).getChild(0).getText());
                                break;
                            case JdbcGrammarParser.COLUMN:
                                this.right = new ColumnReference(
                                        child.getChild(0), builder,
                                        this.selectStatement, this);
                                break;
                            case JdbcGrammarParser.SUBQUERY:
                                this.right = new SubQuery(child.getChild(0),
                                        builder);
                                break;
                            case JdbcGrammarParser.STRINGLIT:
                                String stringLit = "";
                                Tree grandChild = child.getChild(0);
                                for (int j = 1; j < grandChild.getChildCount() - 1; j++) {
                                    stringLit += grandChild.getChild(j).getText();
                                    stringLit += " ";
                                }
                                this.right = new StringLiteral(stringLit.substring(0, stringLit.length() - 1));

                                break;
                            case JdbcGrammarParser.INTEGERPARAM:
                                this.right = new StringLiteral(child
                                        .getChild(0).getChild(0).getText());
                                break;
                            default:
                                break;
                        }
                        break;
                    case JdbcGrammarParser.BOOLEANEXPRESSIONITEMLEFT:
                        switch (child.getChild(0).getType()) {
                            case JdbcGrammarParser.COLUMN:
                                this.left = new ColumnReference(
                                        child.getChild(0), builder,
                                        this.selectStatement, this);
                                break;
                            case JdbcGrammarParser.SUBQUERY:
                                this.left = new SubQuery(child.getChild(0),
                                        builder);
                                break;
                            case JdbcGrammarParser.STRINGLIT:
                                String stringLit = "";
                                Tree grandChild = child.getChild(0);
                                for (int j = 1; j < grandChild.getChildCount() - 1; j++) {
                                    stringLit += grandChild.getChild(j).getText();
                                    stringLit += " ";
                                }
                                this.left = new StringLiteral(stringLit.substring(0, stringLit.length() - 1));
                                break;
                            default:
                                break;
                        }
                        break;
                    case JdbcGrammarParser.COMPARISONOPERATOR:
                        this.type = BooleanExpressionItemType.COMPARISON;
                        this.comparisonOperator = child.getChild(0).getText();
                        break;
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException(
                    "This Tree is not a BOOLEANEXPRESSIONITEM");
        }
    }

    /**
     * getter for the Left Item
     * @return the left of the Boolean Expression
     */
    public Node getLeft() {
        return this.left;
    }

    /**
     * getter for the mainNode
     */
    public Node getMainNode() {
        return this.mainNode;
    }

    /**
     * getter for the Right Item
     * @return the right of the Boolean Expression
     */
    public Node getRight() {
        return this.right;
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        if (this.type == BooleanExpressionItemType.COMPARISON) {
            System.err.println(left.tokenName + " " + right.tokenName);
            return this.left.toPrettyString() + this.comparisonOperator
                    + this.right.toPrettyString();
        } else {
            return this.left.toPrettyString() + "LIKE"
                    + this.right.toPrettyString();
        }
    }
}

/**
 * An enum for the BooleanExpressions type
 * <li> LIKEEXPRESSION
 * <li> COMPARISON
 */
enum BooleanExpressionItemType {
    LIKEEXPRESSION, COMPARISON
}
