package net.starschema.clouddb.jdbc.list;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;
import org.antlr.runtime.tree.Tree;

/**
 * Disjunction to be used within the Where Clause
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class Disjunction extends Node {
    /**
     * Constructor for the Disjunction which builds up a disjunction
     * from the ANTLR tree
     * @param t - the ANTLR tree
     * @param builder - the TreeBuilder for the helper functions
     * @param mainnode - for linking the Conjunctions, Disjunctions
     * @param selectstatement - which contains the Disjunction
     * @throws TreeParsingException
     */
    public Disjunction(Tree t, TreeBuilder builder, Node mainnode,
            SelectStatement selectstatement) throws TreeParsingException {
        //TODO When two Conjunctions are placed in a disjunction the one which is less restrictive shall be the only one left. 
        this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
        this.tokenType = t.getType();
        this.logger.debug("BUILDING " + this.tokenName);
        
        //We make a list to unite disjunctions into 1 disjunction
        List<Disjunction> disjunctionContainer = new ArrayList<Disjunction>();
        for (int i = 0; i < t.getChildCount(); i++) {
            Tree child = t.getChild(i);       
            switch (child.getType()) {
                case JdbcGrammarParser.DISJUNCTION:
                    disjunctionContainer.add(new Disjunction(child, builder, mainnode, selectstatement));
                    this.logger.debug("APPENDING DISJUNCTION TO DISJUNCTION");
                    break;
                case JdbcGrammarParser.CONJUNCTION:
                    Node built = Conjunction.buildFromConjunction(child, builder, mainnode, selectstatement);
                    if(built.getTokenType()==JdbcGrammarParser.CONJUNCTION) {
                        this.children.addLast(Conjunction.class.cast(built));
                        this.logger.debug("APPENDING CONJUNCTION AS PART OF DISJUNCTION");
                    }
                    else {
                        disjunctionContainer.add(Disjunction.class.cast(built));
                        this.logger.debug("APPENDING DISJUNCTION AS PART OF DISJUNCTION");
                    }   
                    break;
                case JdbcGrammarParser.NEGATION:
                    this.children.addLast(new Negation(child, builder, mainnode, selectstatement));
                    break;
                case JdbcGrammarParser.BOOLEANEXPRESSIONITEM:
                    BooleanExpressionItem item = new BooleanExpressionItem(child,
                            builder, mainnode, selectstatement);
                    this.children.addLast(item);
                    this.logger.debug("APPENDING BOOLEANEXPRESSIONITEM TO DISJUNCTION: "+item.toPrettyString());
                    break;
                default:
                    break;
            }
        }
        if(disjunctionContainer.size()!=0) {
            for (Disjunction disjunction : disjunctionContainer) {
                for (Node item : disjunction.children) {
                    this.children.addLast(item);
                }
            }
        }
        System.err.println("PRINTING RESULT");
        for ( Node node : this.children) {
            System.err.println(node.toPrettyString());
        }
        
        
        
        //We need to delete Conjunctions with more restrictions compared to the other Conjunctions and BooleanExpressionItems since this is a Disjunction
        List<Node> selectedChildren = new ArrayList<Node>();
        
        for (int i = 0; i< this.children.size();i++) {
            Node mainNode = this.children.get(i);
            
            if(mainNode.tokenType==JdbcGrammarParser.CONJUNCTION)
            { 
                Conjunction conjunctionMain = Conjunction.class.cast(mainNode);
                boolean foundLessRestrictive = false;
                for (int k = 0; k< this.children.size();k++) {
                    Node compareNode = this.children.get(k);
                    
                    
                    if(compareNode.tokenType==JdbcGrammarParser.CONJUNCTION)
                    {
                        Conjunction conjunctionCompare = Conjunction.class.cast(compareNode);
                        if(conjunctionCompare.children.size()<conjunctionMain.children.size())
                        {
                            boolean allfound = true;
                            for (Node BexprItem : conjunctionCompare.children) {
                                BooleanExpressionItem booleanExpressionItemC = BooleanExpressionItem.class.cast(BexprItem);
                                
                                boolean found = false;
                                for (Node BexprItem2 : conjunctionMain.children) {
                                    BooleanExpressionItem booleanExpressionItemM = BooleanExpressionItem.class.cast(BexprItem2);
                                    if(booleanExpressionItemC.equals(booleanExpressionItemM))
                                    {
                                        found = true;
                                    }
                                }
                                if(!found)
                                {
                                    allfound = false;
                                }
                            }
                            if(allfound)
                            {
                                foundLessRestrictive = true;
                            }
                        }
                    }
                    else if(compareNode.tokenType==JdbcGrammarParser.BOOLEANEXPRESSIONITEM)
                    {
                        boolean found = false;
                        for (Node BexprItem : conjunctionMain.children) {
                            BooleanExpressionItem booleanExpressionItemM = BooleanExpressionItem.class.cast(BexprItem);
                            if(BooleanExpressionItem.class.cast(compareNode).equals(booleanExpressionItemM))
                            {
                                found = true;
                            }
                        }
                        if(found)
                        {
                            foundLessRestrictive=true;
                        }
                    }
                }
                if(!foundLessRestrictive)
                {
                    selectedChildren.add(conjunctionMain);
                }
            }
            else if(mainNode.tokenType==JdbcGrammarParser.BOOLEANEXPRESSIONITEM)
            {
                boolean found = false;
                for (Node selectedNode : selectedChildren) {
                    if(selectedNode.tokenType==JdbcGrammarParser.BOOLEANEXPRESSIONITEM)
                    {
                        if(BooleanExpressionItem.class.cast(selectedNode).equals(BooleanExpressionItem.class.cast(mainNode)))
                        {
                            found = true;
                        }
                    }
                }
                if(!found)
                {
                    selectedChildren.add(this.children.get(i));
                }
            }
        }
        this.children = new LinkedList<Node>();
        //add the new list
        for (Node node : selectedChildren) {
            this.children.addLast(node);
        }
        
    }

    /** Getter for the children */
    public LinkedList<Node> getChildren() {
        return this.children;
    }
    
    private List<List<Node>> getallalternatives(List<Disjunction> disjunctionList)
    {
        logger.debug("DISTJUNCTION LIST SIZE:"+disjunctionList.size());
        
        List<List<Node>> alternativesReturn = new ArrayList<List<Node>>();
        if(disjunctionList.size()!=1) {
            List<List<Node>> alternativesnext = 
                    getallalternatives(disjunctionList.subList(1, disjunctionList.size()));
            for (Node node : disjunctionList.get(0).children) {
                
                for (List<Node> list : alternativesnext) {
                    List<Node> listTwo = new ArrayList<Node>();
                    listTwo.add(node);
                    listTwo.addAll(list);
                    alternativesReturn.add(listTwo);
                }
            }
        }
        else {
            for (Node node : disjunctionList.get(0).children) {
                List<Node> list2 = new ArrayList<Node>();
                list2.add(node);
                logger.debug("ADDING NODE TO DISTLIST:"+ node.toPrettyString());
                alternativesReturn.add(list2);
            }
        }
        return alternativesReturn; 
    }
    
    /**
     * Constructor to build up a Disjunction from Disjunctions, Conjunctions, 
     * and negations or booleanExpressions
     * @param disjunctionContainer - Disjunctions to be contained
     * @param conjunctionContainer - Conjunctions to be contained
     * @param negationContainer - Negations to be contained
     * @param booleanExpressionItemContainer - Boolean Expressions to be contained
     */
    public Disjunction(List<Disjunction> disjunctionContainer,
            List<Conjunction> conjunctionContainer,
            List<Negation> negationContainer,
            List<BooleanExpressionItem> booleanExpressionItemContainer) {
        
        logger.debug("BUILDING DISTRIBUTIVE DISJUNCTION");
        Node mainConjunction = null;
        if(conjunctionContainer.size()==0 && negationContainer.size()==0 
                && booleanExpressionItemContainer.size()==1) {
            mainConjunction = booleanExpressionItemContainer.get(0);
        }
        else if(conjunctionContainer.size()==1 && negationContainer.size()==0 
                && booleanExpressionItemContainer.size()==0) {
            mainConjunction = conjunctionContainer.get(0);
        }
        else if(conjunctionContainer.size()==0 && negationContainer.size()==1 
                && booleanExpressionItemContainer.size()==0) {
            mainConjunction = negationContainer.get(0);
        }
        else {
            mainConjunction = new Conjunction(conjunctionContainer, 
                    negationContainer, booleanExpressionItemContainer);
        }
         
        logger.debug("MAINJUNCTION BUILT");
        Disjunction mainDisjunction = disjunctionContainer.get(0);
        if(disjunctionContainer.size()!=1) {
            logger.debug("MORE ALTERNATIVES");
            //we must make one disjunction from it containing all possibilities
            List<Conjunction> disjunctionConjunctions = new ArrayList<Conjunction>();
            List<List<Node>> getAllAlternatives = getallalternatives(disjunctionContainer);
            for (List<Node> list : getAllAlternatives) {
                Conjunction conj = new Conjunction(list);
                logger.debug("ADDING ALTERNATIVE: "+conj.toPrettyString());
                disjunctionConjunctions.add(conj);
            }
            mainDisjunction = new Disjunction(disjunctionConjunctions);
        }
        
        List<Conjunction> conjunctions = new ArrayList<Conjunction>();
        for (Node node : mainDisjunction.children) {
            List<Node> nodelist = new ArrayList<Node>();
            nodelist.add(mainConjunction);
            nodelist.add(node);
            conjunctions.add(new Conjunction(nodelist));
        }
        for (Conjunction conjunction : conjunctions) {
            logger.debug("APPENDING CONJUNCTION TO DISJUNCTION");
            this.children.addLast(conjunction);
        }
        this.tokenType = JdbcGrammarParser.DISJUNCTION;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.DISJUNCTION];
    }
    
    /**
     * Constructor to build up Disjunction from conjunctions
     * @param disjunctionConjunctions
     */
    public Disjunction(List<Conjunction> disjunctionConjunctions)
    {
        this.tokenType = JdbcGrammarParser.DISJUNCTION;
        this.tokenName = JdbcGrammarParser.tokenNames[JdbcGrammarParser.DISJUNCTION];
        for (Node node : disjunctionConjunctions) {
            this.children.addLast(node);
        }
    }
    
    @Override
    public String toPrettyString(int level) {
        System.err.println("PRINTING DISJUNCTION");
        String result = "(";
        
        for (Node item : this.children) {
            result+=item.toPrettyString() +" OR ";
        }
        result=result.substring(0,result.length()-4)+")";
        return result;
    }
}
