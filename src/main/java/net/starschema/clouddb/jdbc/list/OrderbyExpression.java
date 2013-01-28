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
        }
        else {
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
