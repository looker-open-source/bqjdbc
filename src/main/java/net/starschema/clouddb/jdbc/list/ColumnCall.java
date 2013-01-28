package net.starschema.clouddb.jdbc.list;

import java.util.ArrayList;
import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarLexer;
import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * 
 * 
 * @author Attila Horvath, Balazs Gunics
 *
 */
public class ColumnCall extends Node implements UniQueIdContainer {
    private String name = null;
    private String[] prefixes = null; // a.k.a scopes
    private List<String> aliases = null;
    String uniqueId = null;
    private UniQueIdContainer pointedNode = null;
    private List<UniQueIdContainer> extraPointedNodes = null;
    private boolean equivalentCol = false;
    Expression parentNode = null;
    private List<UniQueIdContainer> nodesPointingtoThis = null;
    
    public List<UniQueIdContainer> getNodesPointingtoThis() {
        return nodesPointingtoThis;
    }
    
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
    
    TreeBuilder builder;    
    FromExpression fromExpression = null;
    
    public void setPointedNode(UniQueIdContainer pointedNode) {
        if(this.pointedNode!=null) {
            if(this.pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                ColumnCall columnCall = ColumnCall.class.cast(this.pointedNode);
                columnCall.removeNodePointingToThis(this);
            }
        }
        this.pointedNode = pointedNode;
        if(this.pointedNode!=null)
        {
            if(this.pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                ColumnCall columnCall = ColumnCall.class.cast(this.pointedNode);
                columnCall.addNodePointingToThis(this);
            }
        }
    }
    
    public boolean isPointedTo() {
        if(nodesPointingtoThis==null || nodesPointingtoThis.size()==0) {
            return false;
        }
        else {
            return true;
        }
    }
    
    public void addNodePointingToThis(UniQueIdContainer node) {
        if(node!=null) {
            boolean found = false;
            for (UniQueIdContainer uniQueIdContainer : nodesPointingtoThis) {
                if(node.getUniqueid().equals(uniQueIdContainer.getUniqueid())) {
                    found = true;
                    break;
                }
            }
            if(!found) {
                nodesPointingtoThis.add(node);
            }
        }
            
    }
    
    public void removeNodePointingToThis(UniQueIdContainer node) {
        if(node!=null) {
            boolean found = false;
            int i = 0;
            int location = 0;
            for (UniQueIdContainer uniQueIdContainer : nodesPointingtoThis) {
                if(node.getUniqueid().equals(uniQueIdContainer.getUniqueid())) {
                    found = true;
                    location = i;
                    break;
                }
                i++;
            }
            if(found) {
                nodesPointingtoThis.remove(location);
            }
        }
    }
    
    /** Getter for the Columns parent Node */
    public Expression getParentNode() {
        return parentNode;
    }
    
    /** Setter for the Columns parent Node */    
    public void setParentNode(Expression parentNode) {
        this.parentNode = parentNode;
    }
    
    /** Getter for the equivalentCol */
    public boolean getEquivalentCol() {
        return equivalentCol;
    }
    
    /** Getter for the equivalentCol */
    public void setEquivalentCol(boolean equivalentCol) {
        this.equivalentCol = equivalentCol;
    }
    
    /** returns the synonyms a.k.a Alias of this Column
     * if there's a parent with an alias we also return
     * the alias with its parents alias
     * 
     * @return A Stringlist that contains the aliases, or null
     */
    public List<String> getSynonyms() {
        
        List<String> returnList = new ArrayList<String>();
        if (this.getAlias() != null) {
            SubQuery parent = this.parentNode.selectStatement.parent;            
            if (parent != null) {
                if (parent.getAlias() != null) {
                    returnList.add(parent.getAlias() + "." + this.getAlias());
                    returnList.add(this.getAlias());
                }
                else {
                    returnList.add(this.getAlias());
                }
            }
            else {
                returnList.add(this.getAlias());
            }
        }
        else {
            if (pointedNode != null) {
                if (pointedNode.getTokenType() == JdbcGrammarParser.COLUMN) {
                    SubQuery parent = this.parentNode.selectStatement.parent;
                    ColumnCall columnCall = ColumnCall.class.cast(pointedNode);
                    List<String> synonyms = columnCall.getSynonyms();
                    
                    if (synonyms != null) {
                        for (String string : synonyms) {                            
                            if (parent != null && parent.getAlias() != null) {
                                returnList.add(parent.getAlias() + "." + string);
                                returnList.add(string);
                            }
                            else {
                                returnList.add(string);
                            }
                        }
                    }
                    
                }
                else
                    if (pointedNode.getTokenType() == JdbcGrammarParser.FUNCTIONCALL) {
                        FunctionCall functionCall = FunctionCall.class
                                .cast(pointedNode);
                        
                        SubQuery parent = this.parentNode.selectStatement.parent;
                        
                        if (this.getAlias() != null) {
                            if (parent != null) {
                                if (parent.getAlias() != null) {
                                    returnList.add(parent.getAlias() + "."
                                            + this.getAlias());
                                    returnList.add(this.getAlias());
                                }
                            }
                            else {
                                returnList.add(this.getAlias());
                            }
                        }
                        else {
                            List<String> synonyms = functionCall.getSynonyms();
                            
                            if (synonyms != null) {
                                for (String string : synonyms) {
                                    
                                    if (parent != null) {
                                        if (parent.getAlias() != null) {
                                            returnList.add(parent.getAlias()
                                                    + "." + string);
                                            returnList.add(string);
                                        }
                                        else {
                                            returnList.add(string);
                                        }
                                    }
                                    else {
                                        returnList.add(string);
                                    }
                                }
                            }
                        }
                    }
            }
            else {
                returnList.add(getWholeName());
                SubQuery parent = this.parentNode.selectStatement.parent;
                if (parent != null) {
                    if (parent.getAlias() != null) {
                        //FIXME schema.TABLE helyett kellene egy schema.TABLE valamint egy TABLE :/                        
                        returnList.add(parent.getAlias() + "." + getWholeName());
                    }
                }
            }
        }
        
        if (returnList.size() != 0) {
            return returnList;
        }
        else {
            return null;
        }
    }
    

    
    /**
     * Makes a new ColumnCall with prefixes, and aliases
     * 
     * @param prefixes - the prefixes for the column
     * @param aliases - the aliases for the column
     * @param builder - the Treebuilder, to generate uniqueids
     * @param name - the columns name
     */
    public ColumnCall(String[] prefixes, List<String> aliases,
            TreeBuilder builder, String name, Expression expression) {
        nodesPointingtoThis = new ArrayList<UniQueIdContainer>();
        this.name = name;
        this.parentNode = expression;
        this.prefixes = prefixes;
        this.aliases = aliases;
        this.builder = builder;
        this.uniqueId = builder.getuniqueid();
        this.tokenType = JdbcGrammarParser.COLUMN;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.COLUMN];
    }
    
    /**
     * Makes a new ColumnCall with prefixes, and aliases
     * 
     * @param prefixes - the prefixes for the column
     * @param aliases - the aliases for the column
     * @param builder - the Treebuilder, to generate uniqueids
     * @param name - the columns name
     */
    public ColumnCall(String[] prefixes, List<String> aliases,
            TreeBuilder builder, String name, UniQueIdContainer pointedNode,
            Expression parent) {
        this.parentNode = parent;
        if (pointedNode.getTokenType() == JdbcGrammarParser.COLUMN) {
            try {
                ColumnCall columnCall = ColumnCall.class.cast(pointedNode);
                if (columnCall.getEquivalentCol()) {
                    equivalentCol = true;
                }                
            }
            catch (ClassCastException e) {
                //TODO Is this normal that we don't care?
            }
        }
        this.name = name;
        this.prefixes = prefixes;
        this.aliases = aliases;
        this.builder = builder;
        this.uniqueId = builder.getuniqueid();
        this.tokenType = JdbcGrammarParser.COLUMN;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.COLUMN];
        nodesPointingtoThis = new ArrayList<UniQueIdContainer>();
        setPointedNode(pointedNode);
    }
    
    /**
     * Makes the columns of a fromExpression
     * 
     * @param t - the ANTLR tree
     * @param treeBuilder - the builder, to make the uniqueIDs
     * @param fromExpression - FromExpression which contains the ColumnCall
     * @throws TreeParsingException - if we can't parse out the ANTLR tree
     */
    public ColumnCall(Tree t, TreeBuilder treeBuilder,
            FromExpression fromExpression, Expression parent) throws TreeParsingException {
        nodesPointingtoThis = new ArrayList<UniQueIdContainer>();
        this.parentNode = parent;
        this.builder = treeBuilder;
        this.uniqueId = this.builder.getuniqueid();
        this.fromExpression = fromExpression;
        this.build(t, this.builder);
    }
    
    /**
     * Adds a String to the end of the prefixlist
     * @param prefix - the String to add
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
     * Adds a String to the front of the prefixlist
     * @param prefix - the String to add
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
     * Parse out the ANTLR tree
     * 
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @throws TreeParsingException 
     */
    @SuppressWarnings("serial")
    public void build(Tree t, TreeBuilder builder) throws TreeParsingException {
        if (t.getType() == JdbcGrammarLexer.COLUMN) {
            nodesPointingtoThis = new ArrayList<UniQueIdContainer>();
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
                this.aliases = aliasList;
            }
            if (scopeList.size() != 0) {
                this.prefixes = scopeList.toArray(new String[scopeList.size()]);
            }            
            if (this.aliases == null) {
                String alias = "";
                if (this.prefixes != null) {
                    for (String string : scopeList) {
                        alias += string + ".";
                    }
                    alias += this.name;
                    final String alias2 = alias;
                    this.aliases = new ArrayList<String>() {
                        {
                            this.add(alias2);
                        }
                    };
                }
            }
            logger.debug("CALLING SEARCH");
            try {
                List<UniQueIdContainer> pointedNodesInFromExpression = 
                        this.searchPointedNodeInFromExpression(fromExpression);
                
                if(pointedNodesInFromExpression == null){ // we didn't find a fitting node in the fromexpression? oO
                    //lets get some prefixes and try with that!                    
                    List<String> possiblePrefixes = builder.getPossiblePrefixes(name);
                    for(int i=0; i<possiblePrefixes.size() 
                            && pointedNodesInFromExpression == null; i++ ) {                        
                        if(possiblePrefixes.get(i) != null){
                            logger.debug("Possible prefix for the column: " + name + "is:" + possiblePrefixes.get(i).toString());
                            String prefixString = possiblePrefixes.get(i);
                            this.prefixes = new String[1];
                            this.prefixes[0] = prefixString;
                            pointedNodesInFromExpression = 
                                    this.searchPointedNodeInFromExpression(fromExpression);
                        }
                        else {
                            logger.debug("Possible prefix for the column: " + name + "is NULL"); 
                        }
                    }
                }
                
                setPointedNode(pointedNodesInFromExpression.get(0));
                for (UniQueIdContainer uniQueIdContainer : pointedNodesInFromExpression) {
                    this.addExtraPointedNode(uniQueIdContainer);
                }
            }
            catch (ColumnCallException e) {
                throw new TreeParsingException(e);
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
        String forReturn = "";
        if (this.aliases == null) {
            return null;
        }
        else {
            for (String iter : this.aliases) {
                forReturn += iter + "_";
            }
            return forReturn.substring(0, forReturn.length() - 1);
        }
    }
    
    /**
     * Getter for the aliases, <br>another way to get the Aliases is:
     * {@link #getAlias()}
     * @return List containing all of the aliases
     */
    public List<String> getAliases() {
        return this.aliases;
    }
    
    /** Getter for the Columns name */
    public String getName() {
        return this.name;
    }
    
    /** Returns the pointed Node  */
    public Node getPointedNode() {
        return (Node) this.pointedNode;
    }
    
    /** Getter for the Scopes/Prefixes of the Column */
    public String[] getScopes() {
        return this.prefixes;
    }
    
    /** Returns the Columns name with its scopes/prefixes divided by "." */
    public String getWholeName() {
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
     * Removes a prefix/scope from the prefixlist if
     * the prefixlist not empty and the prefix exists in it
     * @param prefix - the String to be removed
     */
    public void removePrefix(String prefix) {
        if (this.prefixes == null) {
            return;
        }
        boolean removed = false;
        int i = 0;
        while (!removed && i < this.prefixes.length) {
            if (this.prefixes[i].equals(prefix)) {
                // match
                this.prefixes[i] = null;
                removed = true;
            }
            i++;
        }
        if (removed) {
            String[] newPrefixes = new String[this.prefixes.length - 1];
            i = 0;
            for (String string : this.prefixes) {
                if (string != null) {
                    newPrefixes[i] = string;
                    i++;
                }
            }
            this.prefixes = newPrefixes;
            this.logger.debug("Removed prefix " + prefix);
        }
        // no match
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
                        if (string != null && string.equals(this.getWholeName())) {
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
    
    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    public String toPrettyString(int level) {
        
        if (this.getPointedNode() != null) {
            if (this.getPointedNode().tokenType == JdbcGrammarParser.COLUMN) {
                return ColumnCall.class.cast(this.getPointedNode()).uniqueId
                        + " AS " + this.uniqueId;
                
            }
            else {
                return FunctionCall.class.cast(this.getPointedNode()).uniqueId
                        + " AS " + this.uniqueId;
            }
        }
        else {
            return this.getName() + " AS " + this.uniqueId;
        }
    }

    /** Getter for the Unique Id    */
    @Override
    public String getUniqueid() {
        return uniqueId;
    }
}
