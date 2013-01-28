package net.starschema.clouddb.jdbc.list;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * Class to store the Where Expressions negation
 *
 * @author Attila Horvath
 */
public class Negation extends Node {
    
    public Negation(Tree t, TreeBuilder builder, Node mainnode,
            SelectStatement selectstatement) throws TreeParsingException {
        this.tokenName = JdbcGrammarParser.tokenNames[t.getType()];
        this.tokenType = t.getType();
        this.logger.debug("BUILDING " + this.tokenName);
        for (int i = 0; i < t.getChildCount(); i++) {
            Tree child = t.getChild(i);
            switch (child.getType()) {
                case JdbcGrammarParser.DISJUNCTION:
                    this.children.addLast(new Disjunction(child, builder,
                            mainnode, selectstatement));
                    break;
                case JdbcGrammarParser.CONJUNCTION:
                    Node built = Conjunction.buildFromConjunction(child, 
                            builder, mainnode, selectstatement);
                    if(built.getTokenType()==JdbcGrammarParser.CONJUNCTION) {
                        this.children.addLast(Conjunction.class.cast(built));
                    }
                    else {
                        this.children.addLast(Disjunction.class.cast(built));
                    }   
                    break;
                case JdbcGrammarParser.NEGATION:
                    this.children.addLast(new Negation(child, builder, mainnode, 
                            selectstatement));
                    break;
                case JdbcGrammarParser.BOOLEANEXPRESSIONITEM:
                    this.children.addLast(new BooleanExpressionItem(child,
                            builder, mainnode, selectstatement));
                    break;
                default:
                    break;
            }
        }
    }
    
    @Override
    public String toPrettyString() {
        String result= "NOT (";
        for (Node item : this.children) {
            result+=item.toPrettyString();
        }
        return result+")";
    }
}
