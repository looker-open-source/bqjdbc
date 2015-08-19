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
 * This class extends the basic Node with the following attributes <li>left -
 * Column <li>right - Column <li>operator - String
 *
 * @author Balazs Gunics, Attila Horvath
 */
public class OnClauseCondition extends Node implements ColumnReferencePlace {
    /** The Reference to the Left Column */
    public ColumnReference left;
    /** The Reference to the Right Column */
    public ColumnReference right;
    /** Opearter between the two columns */
    public String operator;

    SelectStatement selectStatement;
    TreeBuilder builder;

    /**
     * Constructor to build OnClauseConditions from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - for the helper functions
     * @param selectStatement - which contains the Join which contains the OnClause
     * @throws TreeParsingException
     */
    public OnClauseCondition(Tree t, TreeBuilder treeBuilder,
                             SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }

    /**
     * Constructor to build up an OnClause with the two references to the Columns
     * and the operator between them
     * @param builder - treeBuilder for the helper functions
     * @param operator - ie: =, etc
     * @param left - Column 1
     * @param right - Column 2
     */
    public OnClauseCondition(TreeBuilder builder, String operator,
                             ColumnReference left, ColumnReference right) {
        this.builder = builder;
        this.operator = operator;
        this.left = left;
        this.right = right;
        this.tokenType = JdbcGrammarParser.CONDITION;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.CONDITION];
    }

    /**
     * Builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {

        if (t.getType() == JdbcGrammarParser.CONDITION) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.CONDITIONLEFT:
                        this.left = new ColumnReference(child.getChild(0),
                                builder, this.selectStatement);
                        break;
                    case JdbcGrammarParser.CONDITIONRIGHT:
                        this.right = new ColumnReference(child.getChild(0),
                                builder, this.selectStatement);
                        break;
                    case JdbcGrammarParser.COMPARISONOPERATOR:
                        this.operator = child.getChild(0).getText();
                        break;
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException("This Tree is not a CONDITION");
        }
    }

    /** Getter for the left Column of this OnClause */
    public ColumnReference getLeft() {
        return this.left;
    }

    @Override
    public Node getMainNode() {
        // TODO Auto-generated method stub
        return this;
    }

    /** Getter for the right Column of this OnClause */
    public ColumnReference getRight() {
        return this.right;
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        String result = this.left.toPrettyString() + this.operator
                + this.right.toPrettyString();
        return result;
    }
}
