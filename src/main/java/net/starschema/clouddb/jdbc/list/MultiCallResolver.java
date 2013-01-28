/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
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
                parseSubQForJokers((SubQuery)this.fromExpression.children.get(0));
        for (ColumnCall columnCall : parseSubQueryForJokerCalls) {
            Node pointedNode = columnCall.getPointedNode();
            List<String> synonyms = null;
            if(pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                synonyms =ColumnCall.class.cast(pointedNode).getSynonyms();
            }
            else if(pointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                synonyms = FunctionCall.class.cast(pointedNode).getSynonyms();
            }

            List<String> scopesTwo = this.multiCall.getScopes();
            String scope = "";
            for (String scopeString : scopesTwo) {
                scope+=scopeString+".";
            }
            
            boolean found = false;
            for (String synonymString : synonyms) {
                if(synonymString.startsWith(scope) && 
                        !synonymString.substring(scope.length()).contains(".")) {
                    found = true;
                }
            }
            if(found) {
                returnList.add(columnCall);
            }
        }
        return returnList;
    }
}
