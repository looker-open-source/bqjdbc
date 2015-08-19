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
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node with an expression to store
 * BooleanExpression
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class GroupByExpression extends Node implements ColumnReferencePlace {

    TreeBuilder builder;
    SelectStatement selectStatement;

    /**
     * Constructor for the GroupByExpression which builds up from a Group By
     * from an ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param selectStatement - the selectstatement which contains the group by
     * @throws TreeParsingException
     */
    public GroupByExpression(Tree t, TreeBuilder treeBuilder,
                             SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }

    /**
     * The function which parse out the Group by Expression from the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {

        if (t.getType() == JdbcGrammarParser.GROUPBYEXPRESSION) {
            this.tokenType = t.getType();
            this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.COLUMN:
                        this.children.addLast(new ColumnReference(child,
                                builder, selectStatement, this));
                        break;
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException(
                    "This Tree is not a HAVINGEXPRESSION");
        }
    }

    /** Returns all of the columns contained by the Group By */
    public List<ColumnReference> getColumns() {
        this.logger.debug("CALLED GETCOLUMNS");
        return this.getAllinstancesof(ColumnReference.class,
                JdbcGrammarParser.COLUMN);
    }

    /** Returns the current Node */
    @Override
    public Node getMainNode() {
        return this;
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        String forreturn = "GROUP BY ";
        for (Node column : this.children) {
            forreturn += newline + this.tab(level + 1)
                    + column.toPrettyString(level) + ",";
        }
        return forreturn.substring(0, forreturn.length() - 1);
    }
}
