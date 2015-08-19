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
 * This class extends the basic Node with an expression to store
 * BooleanExpression
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class HavingExpression extends Node {

    Node expression = null;
    TreeBuilder builder;
    SelectStatement selectStatement;

    /**
     * Constructor for Having Expression which builds it up from the ANTLR tree
     *
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param selectStatement - the statement which contains the Having Expression
     * @throws TreeParsingException
     */
    public HavingExpression(Tree t, TreeBuilder treeBuilder,
                            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }

    /**
     * Builder for handling the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {

        if (t.getType() == JdbcGrammarParser.HAVINGEXPRESSION) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.DISJUNCTION:
                        this.expression = (new Disjunction(child, builder,
                                this, this.selectStatement));
                        break;
                    case JdbcGrammarParser.CONJUNCTION:
                        Node built = Conjunction.buildFromConjunction(child, builder, this, selectStatement);
                        if (built.getTokenType() == JdbcGrammarParser.CONJUNCTION) {
                            this.expression = (Conjunction.class.cast(built));
                        } else {
                            this.expression = (Disjunction.class.cast(built));
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
        } else {
            throw new TreeParsingException("This Tree is not a HAVINGEXPRESSION");
        }
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        return "HAVING " + this.expression.toPrettyString();
    }
}
