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
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * The basic Node for the List
 * it contains the parent, and multiple sublists in children <li>tokenname - the
 * tokens name from the antlr grammar <li>data - the tokens string the SQL-s
 * text
 * 
 * @author Attila Horvath
 * @author Balazs Gunics
 * 
 */
public class Node implements NodeBuilder {
    
    public static String newline = System.getProperty("line.separator");
    protected Logger logger = Logger.getLogger(this.getClass().toString());
    protected Node prev = null;
    protected Node next = null;
    protected LinkedList<Node> children = null;
    protected String tokenName = null;
    protected String data = null;
    protected int tokenType = 0;
    
    /**
     * Makes a new Node
     * parent, tokenName, data will be set to null
     * 
     */
    public Node() {
        this.children = new LinkedList<Node>();
    }
    
    public Node(boolean endnode) {
        
    }
    
    /**
     * Makes a new Node
     * tokenname will be set to null
     * 
     * @param parent
     *            - the Nodes parent
     * @param data
     *            - the data to store
     */
    public Node(String data, Node parent) {
        this.data = data;
        this.prev = parent;
        this.children = new LinkedList<Node>();
    }
    
    /**
     * The constructor for the Node
     * 
     * @param tokenname
     *            - the tokens name
     * @param data
     *            - the String to store
     * @param parent
     *            - the Nodes parent
     */
    public Node(String tokenname, String data, Node parent) {
        this.tokenName = tokenname;
        this.data = data;
        this.prev = parent;
        this.children = new LinkedList<Node>();
    }
    
    @SuppressWarnings("unchecked")
    protected <T> List<T> getAllinstancesof(Class<T> myclass, int type) {
        List<T> nodeList = null;
        for (Node node : this.children) {
            if (node.tokenType == type) {
                if (nodeList == null) {
                    nodeList = new ArrayList<T>();
                }
                nodeList.add((T) node);
            }
        }
        return nodeList;
    }
    
    /** Getter for the TokenName */
    public String getTokenName() {
        return this.tokenName;
    }
    
    /** Getter for the TokenType */
    public int getTokenType() {
        return this.tokenType;
    }
    
    /** Returns with a String that contains N tabulators
     *
     * @param howmany - how many tabs do we want
     * @return - "" or the given number of tabs (\t)
     */
    public String tab(int howmany) {
        String forReturn = "";
        if (howmany <= 0) {
            return forReturn;
        }
        else {
            for (int i = 0; i < howmany; i++) {
                forReturn += "\t";
            }
        }
        return forReturn;
    }
    
    @Override
    /**
     * Returns the stored information in way we can use them to build our queries
     * Calling this will result in an untabbed result, for a human-readable result use
     * toPrettyString()
     */
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }
    
    @Override
    /**
     * Returns the stored information in way we can use them to build our queries
     * @param level - how many tabulator should we use? -1 means no formatting
     */
    public String toPrettyString(int level) {
        return this.data;
    }
    
}
