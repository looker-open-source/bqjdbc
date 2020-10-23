/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc.list;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
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
        } else {
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
