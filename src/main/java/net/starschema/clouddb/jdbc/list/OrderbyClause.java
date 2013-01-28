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
                                builder, this.selectStatement,this);
                        break;
                    case JdbcGrammarParser.ORDERBYORDER:
                        if(child.getChildCount() == 0) { //missing order
                            this.order = OrderbyOrder.ASC;
                        }
                        else {
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
        }
        else {
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
        }
        else
            if (this.order != null && this.order == OrderbyOrder.DESC) {
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
