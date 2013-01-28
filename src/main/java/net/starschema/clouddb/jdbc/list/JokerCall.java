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
        }
        else {
            throw new TreeParsingException(
                    "This Tree is not a JOKERCALL");
        }
    }
    
    @Override
    public String toPrettyString() {
        return "*";
    }
}
