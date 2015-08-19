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
 * Class to store the Where Expressions negation
 *
 * @author Attila Horvath
 */
public class Negation extends Node {

    public Negation(Tree t, TreeBuilder builder, Node mainnode,
                    SelectStatement selectstatement) throws TreeParsingException {
        this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
        this.tokenType = t.getType();
        this.logger.debug("BUILDING " + this.tokenName);
        for (int i = 0; i < t.getChildCount(); i++) {
            Tree child = t.getChild(i);
            switch (child.getType()) {
                case JdbcGrammarParser.DISJUNCTION:
                    this.children.addLast(new Disjunction(child, builder,
                            mainnode, selectstatement));
                    break;
                case JdbcGrammarParser.CONJUNCTION:
                    Node built = Conjunction.buildFromConjunction(child,
                            builder, mainnode, selectstatement);
                    if (built.getTokenType() == JdbcGrammarParser.CONJUNCTION) {
                        this.children.addLast(Conjunction.class.cast(built));
                    } else {
                        this.children.addLast(Disjunction.class.cast(built));
                    }
                    break;
                case JdbcGrammarParser.NEGATION:
                    this.children.addLast(new Negation(child, builder, mainnode,
                            selectstatement));
                    break;
                case JdbcGrammarParser.BOOLEANEXPRESSIONITEM:
                    this.children.addLast(new BooleanExpressionItem(child,
                            builder, mainnode, selectstatement));
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public String toPrettyString() {
        String result = "NOT (";
        for (Node item : this.children) {
            result += item.toPrettyString();
        }
        return result + ")";
    }
}
