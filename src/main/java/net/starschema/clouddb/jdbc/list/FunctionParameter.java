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
 * This class extends the basic Node with a refobject to store the
 * FunctionParameters <li>column <li>integer <li>string
 * 
 * @author Attila Horvath, Balazs Gunics
 */
public class FunctionParameter extends Node implements ColumnReferencePlace {
    
    Node refObject;
    SelectStatement selectStatement;
    
    TreeBuilder builder;
    
    /**
     * Constructor for the FunctionParameter which builds up the parameter
     * from the ANTLR tree
     * 
     * @param t - the ANTLR tree 
     * @param treeBuilder - TreeBuilder for the helper functions
     * @param selectStatement - the selectStatement which contains the Function
     * @throws TreeParsingException
     */
    public FunctionParameter(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.build(t, this.builder);
    }
    
    /**
     * Builder to parse out the ANTLR tree
     * 
     * @param t - the ANTLR tree
     * @param builder - TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.FUNCTIONPARAMETERS) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.INTEGERPARAM:
                        this.refObject = new StringLiteral(child.getChild(0)
                                .getText());
                        break;
                    case JdbcGrammarParser.STRINGLIT:
                        this.refObject = new StringLiteral(child.getChild(0)
                                .getText());
                        break;
                    case JdbcGrammarParser.COLUMN:
                        this.refObject = new ColumnReference(child, builder,
                                this.selectStatement,this);
                        break;
                    case JdbcGrammarParser.JOKER:
                        this.refObject = new StringLiteral("*");
                        break;
                    default:
                        break;
                }
            }
        }
        else {
            throw new TreeParsingException(
                    "This Tree is not a FUNCTIONPARAMETER");
        }
    }
    
    @Override
    public Node getMainNode() {
        return this;
    }

    /**
     * Getter for the function parameter which can be:
     * <li> INTEGERPARAM
     * <li> STRINGLIT
     * <li> COLUMN 
     * <li> JOKER
     * 
     * @return - A Node which type describe the parameters type
     */
    public Node getRefobject() {
        return this.refObject;
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        
        return this.refObject.toPrettyString(level);
    }
}
