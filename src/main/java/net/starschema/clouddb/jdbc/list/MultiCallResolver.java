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

import org.apache.log4j.Logger;

/**
 * Resolves the first SubQuery since its already in a join or union,
 * and check for columns which can be matched with the scope
 *
 * @author Attila Horvath
 */
public class MultiCallResolver extends Resolver {

    FromExpression fromExpression;
    MultiCall multiCall;
    Expression parent;
    Logger logger = Logger.getLogger(this.getClass());

    /**
     * Constructor for MultiCall resolver
     * @param call - the MultiCall which contains the Scopes
     * @param fromExpression - where we get the columns
     * @param builder - the TreeBuilder for the helper functions
     */
    public MultiCallResolver(MultiCall call, FromExpression fromExpression,
                             TreeBuilder builder) {
        super(builder);
        this.fromExpression = fromExpression;
        this.multiCall = call;
    }

    /**
     * Resolves the first SubQuery since its already in a join or union,
     * and check for columns which can be matched with the scope
     */
    public List<ColumnCall> getSubstitutesforJokerCall() {
        List<ColumnCall> returnList = new ArrayList<ColumnCall>();
        List<ColumnCall> parseSubQueryForJokerCalls =
                parseSubQForJokers((SubQuery) this.fromExpression.children.get(0));
        for (ColumnCall columnCall : parseSubQueryForJokerCalls) {
            Node pointedNode = columnCall.getPointedNode();
            List<String> synonyms = null;
            if (pointedNode.getTokenType() == JdbcGrammarParser.COLUMN) {
                synonyms = ColumnCall.class.cast(pointedNode).getSynonyms();
            } else if (pointedNode.getTokenType() == JdbcGrammarParser.FUNCTIONCALL) {
                synonyms = FunctionCall.class.cast(pointedNode).getSynonyms();
            }

            List<String> scopesTwo = this.multiCall.getScopes();
            String scope = "";
            for (String scopeString : scopesTwo) {
                scope += scopeString + ".";
            }

            boolean found = false;
            for (String synonymString : synonyms) {
                if (synonymString.startsWith(scope) &&
                        !synonymString.substring(scope.length()).contains(".")) {
                    found = true;
                }
            }
            if (found) {
                returnList.add(columnCall);
            }
        }
        return returnList;
    }
}
