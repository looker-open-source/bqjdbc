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
 * This class extends the basic Node with an expression to store
 * BooleanExpression
 * 
 * @author Attila Horvath, Balazs Gunics
 */
public class GroupByExpression extends Node implements ColumnReferencePlace {
    
    TreeBuilder builder;
    SelectStatement selectStatement;
    
    /**
     * Constructor for the GroupByExpression which builds up from a Group By
     * from an ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param selectStatement - the selectstatement which contains the group by
     * @throws TreeParsingException
     */
    public GroupByExpression(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }
    
    /**
     * The function which parse out the Group by Expression from the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        
        if (t.getType() == JdbcGrammarParser.GROUPBYEXPRESSION) {
            this.tokenType = t.getType();
            this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.COLUMN:
                        this.children.addLast(new ColumnReference(child,
                                builder, selectStatement,this));
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            throw new TreeParsingException(
                    "This Tree is not a HAVINGEXPRESSION");
        }
    }
    
    /** Returns all of the columns contained by the Group By */
    public List<ColumnReference> getColumns() {
        this.logger.debug("CALLED GETCOLUMNS");
        return this.getAllinstancesof(ColumnReference.class,
                JdbcGrammarParser.COLUMN);
    }
    
    /** Returns the current Node */
    @Override
    public Node getMainNode() {
        return this;
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        String forreturn = "GROUP BY ";
        for (Node column : this.children) {
            forreturn += newline + this.tab(level + 1)
                    + column.toPrettyString(level) + ",";
        }
        return forreturn.substring(0, forreturn.length() - 1);
    }
}
