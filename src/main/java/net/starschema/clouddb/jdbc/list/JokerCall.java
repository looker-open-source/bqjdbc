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
 * A class to store the JokerCalls "*"
 * it returns a * when the toPrettyString Called
 *
 * @author Balazs Gunics, Attila Horvath
 *
 */
public class JokerCall extends Node {
    TreeBuilder builder;
    FromExpression fromExpression;

    /**
     * Makes a JokerCall from an ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - for the helper functions (unused currently)
     * @param fromExpression - the FromExpression containing this JokerCall
     * @throws TreeParsingException
     */
    public JokerCall(Tree t, TreeBuilder treeBuilder,
                     FromExpression fromExpression) throws TreeParsingException {
        this.builder = treeBuilder;
        this.fromExpression = fromExpression;
        this.build(t);
    }

    /**
     * Builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @throws TreeParsingException
     */
    public void build(Tree t) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.JOKERCALL) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
        } else {
            throw new TreeParsingException(
                    "This Tree is not a JOKERCALL");
        }
    }

    @Override
    public String toPrettyString() {
        return "*";
    }
}
