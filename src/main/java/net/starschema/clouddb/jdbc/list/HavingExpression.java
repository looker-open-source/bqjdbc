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
 * This class extends the basic Node with an expression to store
 * BooleanExpression
 * 
 * @author Attila Horvath, Balazs Gunics
 */
public class HavingExpression extends Node {
    
    Node expression = null;
    TreeBuilder builder;
    SelectStatement selectStatement;
    
    /**
     * Constructor for Having Expression which builds it up from the ANTLR tree
     * 
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder for the helper functions
     * @param selectStatement - the statement which contains the Having Expression
     * @throws TreeParsingException
     */
    public HavingExpression(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }
    
    /**
     * Builder for handling the ANTLR tree
     * @param t - the ANTLR tree 
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        
        if (t.getType() == JdbcGrammarParser.HAVINGEXPRESSION) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.DISJUNCTION:
                        this.expression = (new Disjunction(child, builder,
                                this, this.selectStatement));
                        break;
                    case JdbcGrammarParser.CONJUNCTION:
                        Node built = Conjunction.buildFromConjunction(child, builder, this, selectStatement);
                        if(built.getTokenType()==JdbcGrammarParser.CONJUNCTION) {
                            this.expression = (Conjunction.class.cast(built));
                        }
                        else {
                            this.expression = (Disjunction.class.cast(built));
                        }   
                        break;
                    case JdbcGrammarParser.NEGATION:
                        this.expression = (new Negation(child, builder, this,
                                this.selectStatement));
                        break;
                    case JdbcGrammarParser.BOOLEANEXPRESSIONITEM:
                        this.expression = (new BooleanExpressionItem(child,
                                builder, this, this.selectStatement));
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            throw new TreeParsingException("This Tree is not a HAVINGEXPRESSION");
        }
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        return "HAVING " + this.expression.toPrettyString();
    }
}
