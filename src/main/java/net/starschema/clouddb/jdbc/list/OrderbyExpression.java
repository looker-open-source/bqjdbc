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
 * This class extends the basic Node for the
 * Order By handling
 *
 * @author Balazs Gunics, Attila Horvath
 *
 */
public class OrderbyExpression extends Node {
    TreeBuilder builder;
    SelectStatement selectStatement;

    /**
     * Constructor to build OrderBy from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param selectStatement - which contains the OrderBy
     * @throws TreeParsingException
     */
    public OrderbyExpression(Tree t, TreeBuilder treeBuilder,
                             SelectStatement selectStatement) throws TreeParsingException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.build(t);
    }

    /**
     * Builder to parse the ANTLR tree
     * @param t - the ANTLR tree
     * @throws TreeParsingException
     */
    public void build(Tree t) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.ORDERBYEXPRESSION) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.logger.debug("BUILDING " + this.tokenName);
            this.tokenType = t.getType();
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.ORDERBYCLAUSE:
                        this.children.addLast(new OrderbyClause(child, builder,
                                this.selectStatement));
                        break;
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException(
                    "This Tree is not an ORDERBYEXPRESSION");
        }
    }

    /**
     * Getter for Order By Clauses
     * @return - all the OrderByClauses in a list
     */
    public List<OrderbyClause> getOrderbyClauses() {
        return this.getAllinstancesof(OrderbyClause.class,
                JdbcGrammarParser.ORDERBYCLAUSE);
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        String result = "ORDER BY " + newline;
        for (Node node : this.children) {
            result += node.toPrettyString() + ", ";
        }
        result = result.substring(0, result.length() - 2);
        return result;
    }
}
