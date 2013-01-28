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

import net.starschema.clouddb.jdbc.JdbcGrammarLexer;
import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * <p>
 * A Column Node which will store the Columns with their aliases and prefixes
 * <p>
 * 
 * @author Balazs Gunics, Horvath Attila
 * 
 */
public class ColumnReference extends Node implements UniQueIdContainer{
    
    String name = null;
    String[] prefixes = null; // a.k.a scopes
    String[] aliases = null;
    String uniqueId = null;
    private UniQueIdContainer pointedNode;
    private List<UniQueIdContainer> extraPointedNodes = null;
    SelectStatement selectStatement;
    
    TreeBuilder builder;
    
    /**
     * This sets pointed nodes for the sql cleaner when disjunctive where expression join resolving is made
     * @param uniQueIdContainer
     */
    public void addExtraPointedNode(UniQueIdContainer uniQueIdContainer)
    {
        if(this.extraPointedNodes==null)
        {
            this.extraPointedNodes = new ArrayList<UniQueIdContainer>();
        }
        
        extraPointedNodes.add(uniQueIdContainer);
        
        if(Node.class.cast(uniQueIdContainer).tokenType==JdbcGrammarParser.COLUMN){
            ColumnCall columnCall = ColumnCall.class.cast(uniQueIdContainer);
            columnCall.addNodePointingToThis(this);
        }
    }
    
    /**
     * A setter to set the Column which the reference points to
     * @param pointedNode - The Node we want to point to
     */
    public void setPointedNode(UniQueIdContainer pointedNode) {
        if(this.pointedNode!=null) {
            if(this.pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                ColumnCall columnCall = ColumnCall.class.cast(this.pointedNode);
                columnCall.removeNodePointingToThis(this);
            }
        }
        this.pointedNode = pointedNode;
        if(this.pointedNode!=null) {
            if(this.pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                ColumnCall columnCall = ColumnCall.class.cast(this.pointedNode);
                columnCall.addNodePointingToThis(this);
            }
        }
    }
    
    /**
     * A constructor to build the ColumnReference
     * @param prefixes - the Column's prefixes
     * @param builder - the TreeBuilder to reach the helper functions
     * @param name - the Column's name
     */
    public ColumnReference(String[] prefixes, TreeBuilder builder, String name) {
        this.name = name;
        this.prefixes = prefixes;
        this.builder = builder;
        this.uniqueId = builder.getuniqueid();
        this.tokenType = JdbcGrammarParser.COLUMN;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.COLUMN];
        
    }
    /**
     * A constructor to build the ColumnReference
     * @param prefixes - the Column's prefixes
     * @param builder - the TreeBuilder to reach the helper functions
     * @param name - the Column's name
     * @param pointedNode - the Node which the Reference points to
     */    
    public ColumnReference(String[] prefixes, TreeBuilder builder, String name,
            UniQueIdContainer pointedNode) {
        this.name = name;
        this.prefixes = prefixes;
        this.builder = builder;
        setPointedNode(pointedNode);
        this.uniqueId = builder.getuniqueid();
        this.tokenType = JdbcGrammarParser.COLUMN;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.COLUMN];
        
    }
    
    /**
     * A constructor to build ColumnReference from the ANTLR tree
     * 
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder to reach the helper functions
     * @param selectStatement - the SelectStatement which contains the Columns
     * @throws TreeParsingException
     */
    public ColumnReference(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement) throws TreeParsingException {
        this.selectStatement = selectStatement;
        this.builder = treeBuilder;
        this.uniqueId = this.builder.getuniqueid();
        this.build(t, this.builder);
    }
    
    /**
     * A constructor to build ColumnReference from the ANTLR tree
     * 
     * @param t - the ANTLR tree
     * @param treeBuilder - the TreeBuilder to reach the helper functions
     * @param selectStatement - 
     * @param node - 
     * @throws TreeParsingException
     */
    public ColumnReference(Tree t, TreeBuilder treeBuilder,
            SelectStatement selectStatement, ColumnReferencePlace node)
            throws TreeParsingException {
        this.selectStatement = selectStatement;
        
        ArrayList<String> scopeList = new ArrayList<String>();
        
        for (int i = 0; i < t.getChildCount(); i++) {
            Tree child = t.getChild(i);
            switch (child.getType()) {
                // setting the columns name
                case JdbcGrammarParser.NAME:
                    this.name = child.getChild(0).getText();
                    break;
                // getting the dataset, projectname etc
                case JdbcGrammarParser.SCOPE:
                    scopeList.add(child.getChild(0).getText());
                    break;
                default:
                    break;
            }
        }
        this.prefixes = scopeList.toArray(new String[scopeList.size()]);
  
        switch (node.getMainNode().getTokenType()) {
            case JdbcGrammarParser.WHEREEXPRESSION:                  
                try { 
                    List<UniQueIdContainer> pointedNodeInFromExpression = this
                            .searchPointedNodeInFromExpression(selectStatement
                                    .getFromExpression());
                    setPointedNode(pointedNodeInFromExpression.get(0));
                    for (UniQueIdContainer uniQueIdContainer : pointedNodeInFromExpression) {
                        this.addExtraPointedNode(uniQueIdContainer);
                    }
                }
                catch (Exception e) {
                    logger.warn("Failed to parse out the WhereExpression", e);
                }
                break;
            case JdbcGrammarParser.HAVINGEXPRESSION:
                try {
                    UniQueIdContainer pointedNodeInFromExpression = this
                            .searchPointedNodeInExpression(selectStatement
                                    .getExpression());
                    setPointedNode((UniQueIdContainer)pointedNodeInFromExpression);
                }
                catch (Exception e) {
                    logger.warn("Failed to parse out HavingExpression", e);
                }
                break;
            case JdbcGrammarParser.FUNCTIONPARAMETERS:
                try {
                    
                    List<UniQueIdContainer> pointedNodeInFromExpression = this
                            .searchPointedNodeInFromExpression(selectStatement
                                    .getFromExpression());
                    setPointedNode(pointedNodeInFromExpression.get(0));
                    for (UniQueIdContainer uniQueIdContainer : pointedNodeInFromExpression) {
                        this.addExtraPointedNode(uniQueIdContainer);
                    }
                }
                catch (Exception e) {
                    logger.warn("Failed to parse out FunctionParameters", e);
                }
                break;
            case JdbcGrammarParser.CONDITION: // ONCLAUSE CONDITION
                try {
                    List<UniQueIdContainer> pointedNodeInFromExpression = this
                            .searchPointedNodeInFromExpression(selectStatement
                                    .getFromExpression());
                    setPointedNode(pointedNodeInFromExpression.get(0));
                    for (UniQueIdContainer uniQueIdContainer : pointedNodeInFromExpression) {
                        this.addExtraPointedNode(uniQueIdContainer);
                    }
                }
                catch (Exception e) {
                    logger.warn("Failed to parse out Onclause", e);
                }
                break;
            case JdbcGrammarParser.ORDERBYCLAUSE:
                try {
                    UniQueIdContainer searchPointedNodeInExpression = this.searchPointedNodeInExpression(this.selectStatement.getExpression());
                    this.setPointedNode(searchPointedNodeInExpression);
                }
                catch (Exception e) {
                    logger.warn("Failed to parse out Order by Clause", e);
                }
                break;
            
            case JdbcGrammarParser.GROUPBYEXPRESSION:
                try {
                    UniQueIdContainer searchPointedNodeInExpression = this.searchPointedNodeInExpression(this.selectStatement.getExpression());
                    this.setPointedNode(searchPointedNodeInExpression);
                }
                catch (Exception e) {
                    logger.warn("Failed to parse out Group By Expression", e);
                }
                break;
            default:
                break;
        }
        
        this.builder = treeBuilder;
        this.uniqueId = this.builder.getuniqueid();
        this.build(t, this.builder);
    }
    
    /**
     * Adds a String to the end of the Prefixes
     * @param prefix - the prefix to add to the prefixlist
     */
    public void addPrefixToEnd(String prefix) {
        if (this.prefixes == null) {
            this.prefixes = new String[] { prefix };
        }
        else {
            String[] temp = this.prefixes;
            this.prefixes = new String[this.prefixes.length + 1];
            int i = 0;
            for (String string : temp) {
                this.prefixes[i] = string;
                i++;
            }
            this.prefixes[this.prefixes.length - 1] = prefix;
        }
    }
    
    /**
     * Adds a String to the front of the Prefixes
     * @param prefix - the prefix to add to the prefixlist
     */
    public void addPrefixtoFront(String prefix) {
        if (this.prefixes == null) {
            this.prefixes = new String[] { prefix };
        }
        else {
            String[] temp = this.prefixes;
            this.prefixes = new String[this.prefixes.length + 1];
            this.prefixes[0] = prefix;
            int i = 1;
            for (String string : temp) {
                this.prefixes[i] = string;
                i++;
            }
        }
    }
    
    /**
     * The builder to parse out the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException
     */
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarLexer.COLUMN) {            
            // setting the columns type
            this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
            this.tokenType = t.getType();
            this.logger.debug("BUILDING " + this.tokenName);
            
            ArrayList<String> scopeList = new ArrayList<String>();
            ArrayList<String> aliasList = new ArrayList<String>();
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                // setting the columns name
                    case JdbcGrammarParser.NAME:
                        this.name = child.getChild(0).getText();
                        break;
                    // getting the dataset, projectname etc
                    case JdbcGrammarParser.SCOPE:
                        scopeList.add(child.getChild(0).getText());
                        break;
                    // getting the alias
                    case JdbcGrammarParser.ALIAS:
                        aliasList.add(child.getChild(0).getText());
                        break;
                    default:
                        break;
                }
            }
            if (aliasList.size() != 0) {
                this.aliases = aliasList.toArray(new String[aliasList.size()]);
            }
            if (scopeList.size() != 0) {
                this.prefixes = scopeList.toArray(new String[scopeList.size()]);
            }
        }
        else {
            throw new TreeParsingException(JdbcGrammarLexer.COLUMN,t.getType());
        }
    }
    
    /**
     * Getter for alias
     * 
     * @return null if we don't have an alias, else the aliases divided by "_"
     */
    public String getAlias() {
        String forreturn = "";
        if (this.aliases == null) {
            return null;
        }
        for (String iter : this.aliases) {
            forreturn += iter + "_";
        }
        return forreturn.substring(0, forreturn.length() - 1);
    }
    
    /**
     * Getter for the Aliases
     * 
     * @return the String[] which contains the aliases
     */
    public String[] getAliases() {
        return this.aliases;
    }
    
    /**
     * Getter for the Columns name
     * 
     * @return String
     */
    public String getName() {
        return this.name;
    }
    
    /**
     * Getter for the pointed node
     * 
     * @return Node
     */
    public UniQueIdContainer getPointedNode() {
        return this.pointedNode;
    }
    
    /**
     * Returns the prefixes
     * Same as getScopes()
     * 
     * @return String[] which contains the prefixes
     */
    public String[] getPrefixes() {
        return this.prefixes;
    }

    /**
     * Returns the prefixes
     * Same as getPrefixes()
     * 
     * @return String[] which contains the prefixes
     */
    public String[] getScopes() {
        return this.prefixes;
    }
    
    /**
     * returns the Columns whole name
     * 
     * @return String prefixes + name divided by "."
     */
    public String getwholename() {
        String name = "";
        if (this.getScopes() != null) {
            for (String scope : this.getScopes()) {
                name += scope + ".";
            }
        }
        name += this.getName();
        return name;
    }
    
    /**
     * Returns the Node in the FromExpression
     * 
     * @param fromExpression
     * @return the pointed Node / null
     * @throws ColumnCallException - when Ambiguous column found
     */
    private List<UniQueIdContainer> searchPointedNodeInFromExpression(FromExpression fromExpression)
            throws ColumnCallException {
        logger.debug("IN SEARCH");
        List<SubQuery> subQueries = fromExpression.getSubQueries();
        if (subQueries != null) {
            // We only search in the first, since in the fromexpression we have
            // now unions or a single Subquery
            List<UniQueIdContainer> validPointedNodes = new ArrayList<UniQueIdContainer>();
            for (SubQuery subQuery : subQueries) {
                List<SynonymContainer> availableResources = subQuery
                        .getAvailableResources();
                for (SynonymContainer synonymContainer : availableResources) {
                    List<String> synonyms = synonymContainer.getSynonyms();
                    
                    for (String string : synonyms) {
                        if (string != null && string.equals(this.getwholename())) {
                            this.logger.debug("ADDING NODE");
                            validPointedNodes.add(synonymContainer.getPointedResource());
                        }
                    }
                }
            }
            if (validPointedNodes.size() > 1) {
                boolean sameUniqueId = true;
                String uniqueId = validPointedNodes.get(0).getUniqueid();
                for (UniQueIdContainer uniQueIdContainer : validPointedNodes) {
                    if(!uniQueIdContainer.getUniqueid().equals(uniqueId))
                    {
                        sameUniqueId = false;
                    }
                }
                if(!sameUniqueId)
                {
                    throw new ColumnCallException("AMBIGOUS COLUMNCALL: " + this.getName());
                }
                else
                {
                    return validPointedNodes;
                }
            }
            else {
                if (validPointedNodes.size() == 1) {
                    return validPointedNodes;
                }
                else {
                    return null;
                }
            }
        }
        return null;
    }
    
    /**
     * searches the pointed Node in the expression
     * 
     * @param expression - to search the Nodes in
     * @return - the ColumnReferences pointed Node
     * @throws ColumnCallException - if Ambiguous call found
     */
    private UniQueIdContainer searchPointedNodeInExpression(Expression expression)
            throws ColumnCallException {
        System.err.println("SEARCHING POINTED NODE IN EXPRESSION");
        List<ColumnCall> columns = expression.getColumns();
        List<FunctionCall> functionCalls = expression.getFunctionCalls();
        List<Node> possibleNodes = new ArrayList<Node>();
        if (columns != null) {
            for (ColumnCall columnCall : columns) {
                List<String> synonyms = columnCall.getSynonyms();
                for (String string : synonyms) {
                    if(string.equals(this.getwholename())){
                        possibleNodes.add(columnCall);
                    }
                }
            }
        }
        if (functionCalls != null) {
            for (FunctionCall functionCall : functionCalls) {
                if (functionCall.getAlias() != null) {
                    if (functionCall.getAlias().equals(this.getwholename())) {
                        possibleNodes.add(functionCall);
                    }
                }
            }
        }
        if (possibleNodes.size() > 1) {
            throw new ColumnCallException("AMBIGOUS COLUMNCALL: " + this.getName());
        }
        else {
            if (possibleNodes.size() == 1) {
                return (UniQueIdContainer)possibleNodes.get(0);
            }
            else {
                return null;
            }
        }
    }
    
    /**
     * searches the pointed Node in the SubQuery
     * 
     * @param subQuery - to search the Nodes in
     * @return - the ColumnReferences pointed Node
     * @throws ColumnCallException - if Ambiguous call found
     */
   public UniQueIdContainer searchPointedNodeInSubQuery(SubQuery subQuery) throws ColumnCallException {
       List<UniQueIdContainer> validPointedNodes = new ArrayList<UniQueIdContainer>();                
       List<SynonymContainer> availableResources = subQuery.getAvailableResources();
                
       for (SynonymContainer synonymContainer : availableResources) {
           List<String> synonyms = synonymContainer.getSynonyms();
           for (String string : synonyms) {
               String wholeName = this.getwholename();
               if (string != null && string.equals(wholeName)) {
                   this.logger.debug("ADDING NODE");
                   validPointedNodes.add(synonymContainer.getPointedResource());
                   this.logger.debug("FOUND");
               }
           }
       }            
       if (validPointedNodes.size() > 1) {
           throw new ColumnCallException("AMBIGOUS COLUMNCALL: " + this.getName());
       }
       else {
           if (validPointedNodes.size() == 1) {
               return validPointedNodes.get(0);
           }
           else {
               return null;
           }
       }
    }
    
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {

        if (this.getPointedNode() != null) {
            if (this.getPointedNode().getTokenType() == JdbcGrammarParser.COLUMN) {
                String retString = "";
                if(ColumnCall.class.cast(this.getPointedNode()).parentNode!=null &&
                        ColumnCall.class.cast(this.getPointedNode()).parentNode.selectStatement!=null &&
                        ColumnCall.class.cast(this.getPointedNode()).parentNode.selectStatement.parent!=null && 
                        ColumnCall.class.cast(this.getPointedNode()).parentNode.selectStatement.parent.isPartOfJoin() &&
                        ColumnCall.class.cast(this.getPointedNode()).parentNode.selectStatement.parent.alias!=null) {
                    retString = ColumnCall.class.cast(this.getPointedNode()).parentNode.selectStatement.parent.alias+".";
                }
                return retString+ColumnCall.class.cast(this.getPointedNode()).uniqueId;
            }
            else {
                String retString = "";
                FunctionCall cast = FunctionCall.class.cast(this.getPointedNode());
                if(
                        cast.parentNode!=null &&
                        cast.parentNode.selectStatement!=null &&
                        cast.parentNode.selectStatement.parent!=null && 
                        cast.parentNode.selectStatement.parent.isPartOfJoin() &&
                        cast.parentNode.selectStatement.parent.alias!=null) {
                    retString = cast.parentNode.selectStatement.parent.alias+".";
                }
                return retString+cast.uniqueId;
            }
        }
        else {
            return this.getName();
        }
    }


    @Override
    public String getUniqueid() {
        return uniqueId;
    }
}
