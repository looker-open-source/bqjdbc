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
 * A class that stores a Function of the Query
 * 
 * @author Attila Horvath, Balazs Gunics
 */
public class FunctionCall extends Node implements UniQueIdContainer{
    
    String name = null;
    String alias = null;
    String uniqueId = null;
    Expression parentNode = null;
    
    SelectStatement selectStatement;
    TreeBuilder builder;
    FromExpression fromExpression;
    
    /** Getter for parentNode */
    public Expression getParentNode() {
        return parentNode;
    }
    /** Setter for parentNode */
    public void setParentNode(Expression parentNode) {
        this.parentNode = parentNode;
    }
    
    /** returns the synonyms a.k.a Alias of this function
     * if there's a parent with an alias we also return
     * the alias with its parents alias
     * 
     * @return A Stringlist that contains the aliases, or null
     */
    public List<String> getSynonyms()
    {
        List<String> returnList = new ArrayList<String>();
        SubQuery parent = this.parentNode.selectStatement.parent;

        if(this.getAlias()!=null){
            returnList.add(this.getAlias());
            if(parent!=null && parent.getAlias()!=null){
                returnList.add(parent.getAlias()+"."+this.getAlias());
            }
            return returnList;
        }
        else
        {
            returnList.add(this.uniqueId);
            returnList.add(parent.getAlias()+"."+this.uniqueId);
            return returnList;
        }
    }

    /**
     * Constructor for Functioncall which builds it from the ANTLR tree,
     * with it's selectStatement
     * 
     * @param t - the ANTLR tree
     * @param treeBuilder - TreeBuilder for the helper functions
     * @param selectStatement - the SelectStatement which contains the FunctionCall
     * @throws TreeParsingException
     */
    public FunctionCall(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement)
            throws TreeParsingException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.fromExpression = selectStatement.getFromExpression();
        this.uniqueId = this.builder.getuniqueid();
        
        this.build(t, this.builder);
    }
    
    /**
     * The builder to parse out the ANTLR tree
     * 
     * @param t - the ANTLR tree
     * @param builder - TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.FUNCTIONCALL) {
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.FUNCTIONPARAMETERS:
                        this.children.addLast(new FunctionParameter(child,
                                builder, this.selectStatement));
                        break;
                    case JdbcGrammarParser.ALIAS:
                        this.alias = child.getChild(0).getText();
                        break;
                    case JdbcGrammarParser.NAME:
                        this.name = child.getChild(0).getText();
                        break;
                    default:
                        break;
                }
            }
            if(this.getName().toLowerCase().equals("COUNT".toLowerCase())) {
                if(children.size()==1) {
                    for (Node Child : this.children) {
                        FunctionParameter functionParameter = FunctionParameter.class.cast(Child);
                        if(functionParameter.getRefobject().tokenType == JdbcGrammarParser.STRINGLIT){
                            StringLiteral stringLiteral = StringLiteral.class.cast(functionParameter.getRefobject());
                            if(stringLiteral.data.equals("*")) {
                                FromExpression fromExpression2 = this.selectStatement.getFromExpression();
                                List<SubQuery> subQueries = fromExpression2.getSubQueries();
                                if(subQueries!=null) {
                                    SubQuery subQuery = subQueries.get(0);
                                    Expression expression = subQuery.getSelectStatement().getExpression();
                                    List<ColumnCall> columns = expression.getColumns();
                                    if(columns!=null) {
                                        for (ColumnCall columnCall : columns) {
                                            columnCall.addNodePointingToThis(this);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (this.getAlias() == null) {
                // we give alias as unique id
                this.alias = this.uniqueId;
            }
        }
        else {
            throw new TreeParsingException("This Tree is not a FUNCTIONCALL");
        }
    }
    
    /** Returns the Functions alias */
    public String getAlias() {
        return this.alias;
    }
    
    /**
     * Returns the functions parameter(s)
     * @return - A List that contains the FunctionParameters
     */
    public List<FunctionParameter> getFunctionParameters() {
        return this.getAllinstancesof(FunctionParameter.class,
                JdbcGrammarParser.FUNCTIONPARAMETERS);
    }
    
    /** Getter for the Function name */
    public String getName() {
        return this.name;
    }
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        String result = this.name + "(";
        for (Node node : this.children) {
            result += node.toPrettyString() + ",";
        }
        result = result.substring(0, result.length() - 1);
        result += ")";
        return result+" AS "+this.uniqueId;
    }
    
    /** Getter for the UniqueID */
    @Override
    public String getUniqueid() {
       return uniqueId;
    }
}
