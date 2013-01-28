package net.starschema.clouddb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;
import net.starschema.clouddb.jdbc.list.CallContainer;
import net.starschema.clouddb.jdbc.list.ColumnCall;
import net.starschema.clouddb.jdbc.list.Expression;
import net.starschema.clouddb.jdbc.list.FunctionCall;
import net.starschema.clouddb.jdbc.list.Node;
import net.starschema.clouddb.jdbc.list.SQLCleaner;
import net.starschema.clouddb.jdbc.list.SelectStatement;
import net.starschema.clouddb.jdbc.list.TreeBuilder;

import org.antlr.runtime.RecognitionException;
import org.apache.log4j.Logger;

/**
 * The initializer for the Query Transformer
 * It's possible to turn on or off the querytransformation
 * by setting the TransformQuery at the BQConnection class
 *
 * @author Balazs Gunics
 */
public class BQQueryParser {
    private String queryToParse = null;
    private BQConnection connection = null;
    Logger logger = Logger.getLogger(this.getClass());
    /** Was the parsing Successfull? */
    public boolean successFullParsing = false;
    private boolean formatted = true;
    public Node myNode = null;
    
    /**
     * Constructor for the Query Parser
     * @param queryToParse - the query we want to transform
     * @param connection - the BQConnection Connection
     */
    public BQQueryParser(String queryToParse, Connection connection) {
        this.setQueryToParse(queryToParse);
        this.connection = (BQConnection)connection;
    }
    
    /**
     * A setter to activate, deactivate the formatted output
     * 
     * @param formatted
     */
    public void asFormatted(boolean formatted) {
        this.formatted = formatted;
    }
    
    /** Getter for the Connection which can be cast to BQConnection */
    public Connection getConnection() {
        return this.connection;
    }
    
    /** Getter for the inputted
    public String getQueryToParse() {
        return this.queryToParse;
    }
    
    /**
     * 
     * @return true - if the query will be formatted by tabs
     */
    public boolean isFormatted() {
        return this.formatted;
    }
    
    /**
     * 
     * @return a parsed SQL, which is runnable by bigquery
     */
    public String parse() {
        if(connection.getTransformQuery() == false){
            //we don't need Parsing
            return this.queryToParse;
        }
        this.successFullParsing = true;
        // ANTLR Parsing
        String catalog = "";
        try {
            catalog = this.connection.getCatalog();
            String catalogDecoded = catalog.replace("__", ":").replace("_", ".");
            //this will replace the catalog
            queryToParse = queryToParse.replace(catalog, catalogDecoded);
        }
        catch (SQLException e2) {
            logger.warn("failed to replace the catalog in the query, catalog for the connection is: " + catalog);
        }
        this.logger.debug("The Query before parsing: " + this.queryToParse);
        try {
            TreeBuilder builder = new TreeBuilder(this.queryToParse,
                    this.connection, new CallContainer());
            SelectStatement seleNode = null;
            try {
                seleNode = (SelectStatement)builder.build();
                SQLCleaner.Clean(seleNode);
            }
            catch (TreeParsingException e1) {
                logger.debug("Parsing failed", e1);
            }
            this.myNode = seleNode;

            String MainExpression = "SELECT " + Node.newline;
            
            Expression expression = ((SelectStatement) seleNode).getExpression();

            LinkedList<Node> children = expression.getChildren();
            
            //Now we search for the right names
            int i =0;
            for (Node node : children) {
                if(node.getTokenType()==JdbcGrammarParser.COLUMN) {
                    ColumnCall columnCall = ColumnCall.class.cast(node);
                    if(columnCall.getAlias()==null) {
                        //we search for the shortest not ambigous scope
                        List<String> synonyms = columnCall.getSynonyms();
                        List<String> notAmbigous = new ArrayList<String>();
                        for (String string : synonyms) {
                            boolean found = false;
                            int k = 0;
                            for (Node node2 : children) {
                                if(k!=i) {
                                    if(node2.getTokenType()==JdbcGrammarParser.COLUMN) {
                                        ColumnCall columnCall2 = ColumnCall.class.cast(node2);
                                        List<String> synonyms2 = columnCall2.getSynonyms();
                                        for (String string2 : synonyms2) {
                                            if(string2.equals(string)) {
                                                System.err.println(string2+"  "+string);
                                                found = true;
                                            }
                                        }
                                    }
                                    else if(node2.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                                        FunctionCall functionCall2 = FunctionCall.class.cast(node2);
                                        List<String> synonyms2 = functionCall2.getSynonyms();
                                        for (String string2 : synonyms2) {
                                            if(string2.equals(string)) {
                                                //System.err.println(string2+"  "+string);
                                                found = true;
                                            }
                                        }
                                    }
                                }
                                k++;
                            }
                            if(!found) {
                                notAmbigous.add(string);
                            }
                            
                        }
                        //System.err.println(notAmbigous.size());
                        
                        List<String> selected = new ArrayList<String>();
                        //we only select those that contain the name of the columncall
                        for (String string : notAmbigous) {
                            //logger.debug("EXAMINATING "+ string+" with "+columnCall.getName());
                            if(string.contains(columnCall.getName())) {
                                selected.add(string);
                            }
                        }
                        //System.err.println(selected.size());
                        //we select the shortest (with least .-s)
                        String shortest = null;
                        int minOccurences = Integer.MAX_VALUE;
                        for (String string : selected) {
                            //System.err.println("EXAMINATING lvl2: "+ string);
                            int occurrences = 0;
                            int index = 0;
                            while (index < string.length() && (index = string.indexOf(".", index)) >= 0) {
                                occurrences++;
                                index += 1;
                            }
                            //System.err.println(occurrences);
                            if(occurrences<minOccurences) {
                                minOccurences = occurrences;
                                //System.err.println("CHANGING");
                                shortest = string;
                            }
                        }
                        MainExpression+=columnCall.getUniqueid()+" AS "+shortest;
                    }
                    else {
                        MainExpression+=columnCall.getUniqueid()+" AS "+columnCall.getAlias();
                    }
                }
                else if(node.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                    FunctionCall functionCall = FunctionCall.class.cast(node);
                    if(functionCall.getAlias()==null) {
                      //we search for the shortest not ambigous scope
                        List<String> synonyms = functionCall.getSynonyms();
                        List<String> notAmbigous = new ArrayList<String>();
                        for (String string : synonyms) {
                            boolean found = false;
                            int k = 0;
                            for (Node node2 : children) {
                                if(k!=i) {
                                    if(node2.getTokenType()==JdbcGrammarParser.COLUMN) {
                                        ColumnCall columnCall2 = ColumnCall.class.cast(node2);
                                        List<String> synonyms2 = columnCall2.getSynonyms();
                                        for (String string2 : synonyms2) {
                                            if(string2.equals(string)) {
                                                System.err.println(string2+"  "+string);
                                                found = true;
                                            }
                                        }
                                    }
                                    else if(node2.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                                        FunctionCall functionCall2 = FunctionCall.class.cast(node2);
                                        List<String> synonyms2 = functionCall2.getSynonyms();
                                        for (String string2 : synonyms2) {
                                            if(string2.equals(string))
                                            {
                                                System.err.println(string2+"  "+string);
                                                found = true;
                                            }
                                        }
                                    }
                                }
                                k++;
                            }
                            if(!found) {
                                notAmbigous.add(string);
                            }
                        }

                        //we select the shortest (with least .-s)
                        String shortest = null;
                        int minOccurences = Integer.MAX_VALUE;
                        for (String string : notAmbigous) {
                            //System.err.println("EXAMINATING: "+string);
                            int occurrences = 0;
                            int index = 0;
                            while (index < string.length() && (index = string.indexOf(".", index)) >= 0) {
                                occurrences++;
                                index += 1;
                            }
                            if(occurrences<minOccurences) {
                                minOccurences = occurrences;
                                shortest = string;
                            }
                        }
                        MainExpression+=functionCall.getUniqueid()+" AS "+shortest;
                    }
                    else {
                        MainExpression+=functionCall.getUniqueid()+" AS "+functionCall.getAlias();
                    }
                }
                i++;
                if(i<children.size()) {
                    MainExpression+=", " + Node.newline;
                }
            }
            MainExpression+=Node.newline + " FROM (";
            this.queryToParse = this.formatted ? 
                    ((SelectStatement) seleNode).toPrettyString(1) 
                    : 
                    ((SelectStatement) seleNode).toPrettyString();
                    
            this.queryToParse = MainExpression+this.queryToParse+")";
        }
        catch (RecognitionException e1) {
            this.logger.info("Parsing failed", e1);
            this.successFullParsing = false;
        }
        catch (Exception e) {
            this.logger.info("Parsing failed", e);
            this.successFullParsing = false;
        }
        return this.queryToParse;
    }
    
    /** Setter for the Connection, it must be a BQConnection */
    public void setConnection(Connection connection) {
        this.connection = (BQConnection)connection;
    }
    
    /** Setter for the Query to Parse */
    public void setQueryToParse(String queryToParse) {
        this.queryToParse = queryToParse;
    }
}
