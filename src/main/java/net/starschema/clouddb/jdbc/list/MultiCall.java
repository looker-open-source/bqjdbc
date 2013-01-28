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
        }
        else {
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
