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
 * with the following attributes:
 * <li> order -> we order by DESC or ASC
 * <li> orderByColumn -> the columns to order by
 *
 * @author Balazs Gunics, Attila Horvath
 */
public class OrderbyClause extends Node implements ColumnReferencePlace {

    OrderbyOrder order = null;
    SelectStatement selectStatement;
    ColumnReference orderByColumn = null;

    TreeBuilder builder;

    /**
     * Constructor to build Order By Clauses from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param selectStatement - which contains this Order By
     * @throws TreeParsingException
     */
    public OrderbyClause(Tree t, TreeBuilder treeBuilder,
                         SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }

    /**
     * Builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.ORDERBYCLAUSE) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.COLUMN:
                        this.orderByColumn = new ColumnReference(child,
                                builder, this.selectStatement, this);
                        break;
                    case JdbcGrammarParser.ORDERBYORDER:
                        if (child.getChildCount() == 0) { //missing order
                            this.order = OrderbyOrder.ASC;
                        } else {
                            switch (child.getChild(0).getType()) {
                                case JdbcGrammarParser.DESC:
                                    this.order = OrderbyOrder.DESC;
                                    break;
                                case JdbcGrammarParser.ASC:
                                    this.order = OrderbyOrder.ASC;
                                    break;
                                default:
                                    break;
                            }
                            break;
                        }
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException("This Tree is not an ORDERBYCLAUSE");
        }
    }

    /** Return this (unused) */
    @Override
    public Node getMainNode() {
        return this;
    }

    /** Getter for the Order By Column */
    public ColumnReference getOrderByColumn() {
        return this.orderByColumn;
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {

        String result = this.orderByColumn.toPrettyString(level);
        if (this.order != null && this.order == OrderbyOrder.ASC) {
            result += " " + "ASC";
        } else if (this.order != null && this.order == OrderbyOrder.DESC) {
            result += " " + "DESC";
        }
        return result;
    }
}

/**
 * <li> DESC <li> ASC
 *
 */
enum OrderbyOrder {
    DESC, ASC
}
