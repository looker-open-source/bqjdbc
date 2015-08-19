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

import java.util.ArrayList;
import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;
import org.antlr.runtime.tree.Tree;

/**
 * MultiCall to be used with the MultiCallResolver
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class MultiCall extends Node {

    /** we store the Scopes divided by "." */
    List<String> scopes = null;
    TreeBuilder builder;
    FromExpression fromExpression;

    /**
     * Constructor to build up MultiCalls from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param fromExpression - which contains the MultiCall
     * @throws TreeParsingException
     */
    public MultiCall(Tree t, TreeBuilder treeBuilder,
                     FromExpression fromExpression) throws TreeParsingException {
        this.builder = treeBuilder;
        this.fromExpression = fromExpression;
        this.build(t, this.builder);
    }

    /**
     * Builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder -  - the TreeBuilder for the helper functions (unused)
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.MULTIPLECALL) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.SCOPE:
                        if (this.scopes == null) {
                            this.scopes = new ArrayList<String>();
                        }
                        this.scopes.add(child.getChild(0).getText());
                        break;
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException("This Tree is not a MULTIPLECALL");
        }
    }

    /** Getter for the {@link #scopes} */
    public List<String> getScopes() {
        return this.scopes;
    }

    @Override
    public String toPrettyString() {
        String result = "";
        for (String scope : this.scopes) {
            result += scope + ".";
        }
        result += "*";
        return result;
    }

}
