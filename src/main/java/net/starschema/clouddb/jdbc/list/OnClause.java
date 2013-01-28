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
 * <li> AND <li> OR
 */
enum LogicalOperator {
    AND, OR
}

/**
 * This class extends the basic Node with
 * operators which is a List containing LogicalOperator types
 * 
 * @author Balazs Gunics, Attila Horvath
 */
public class OnClause extends Node {
    
    List<LogicalOperator> operators;
    SelectStatement selectStatement;
    
    TreeBuilder builder;
    
    /**
     * Constructor for the Onclause which builds it up from the ANTLR tree
     * @param t - the ANTLR tree
     * @param treeBuilder - for the helper functions
     * @param selectStatement - which contains this OnClause
     * @throws TreeParsingException
     */
    public OnClause(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.build(t, this.builder);
    }
    
    /**
     * Constructor for the OnClause which builds up from OnClauseConditions, and
     * LogicalOperators
     * @param builder - the TreeBuilder for helper functions
     * @param conditions - the OnClause conditions
     * @param operators - the operators for the OnClause
     */
    public OnClause(TreeBuilder builder, List<OnClauseCondition> conditions,
            List<LogicalOperator> operators) {
        this.operators = operators;
        this.builder = builder;
        for (OnClauseCondition onclauseCondition : conditions) {
            this.children.addLast(onclauseCondition);
        }
        this.tokenType = JdbcGrammarParser.ONCLAUSE;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.ONCLAUSE];
    }
    
    /**
     * Builder to Parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.ONCLAUSE) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.CONDITION:
                        this.children.addLast(new OnClauseCondition(child,
                                builder, this.selectStatement));
                        break;
                    case JdbcGrammarParser.LOGICALOPERATOR:
                        if (this.operators == null) {
                            this.operators = new ArrayList<LogicalOperator>();
                        }
                        switch (child.getChild(0).getType()) {
                            case JdbcGrammarParser.AND:
                                this.operators.add(LogicalOperator.AND);
                                break;
                            case JdbcGrammarParser.OR:
                                this.operators.add(LogicalOperator.OR);
                                break;
                            
                            default:
                                break;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            throw new TreeParsingException("This Tree is not an ONCLAUSE");
        }
    }
    
    /** Getter for all of the OnClauseConditions */
    public List<OnClauseCondition> getOnclauseConditions() {
        this.logger.debug("CALLED GETCOLUMNS");
        return this.getAllinstancesof(OnClauseCondition.class,
                JdbcGrammarParser.CONDITION);
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        String result = this.tab(level) + "ON ";
        int i = 0;
        for (Node node : this.children) {
            if (this.operators != null && this.operators.size() > i) {
                result += "(" + node.toPrettyString() + ")"
                        + this.operators.get(i);
            }
            else {
                result += "(" + node.toPrettyString() + ")";
            }
            i++;
        }
        return result + newline;
    }
}
