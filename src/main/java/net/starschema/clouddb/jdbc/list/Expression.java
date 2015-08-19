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

import java.util.LinkedList;
import java.util.List;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

import org.antlr.runtime.tree.Tree;

/**
 * This class extends the basic Node
 *
 * @author Attila Horvath, Balazs Gunics
 */
public class Expression extends Node {
    TreeBuilder builder;
    SelectStatement selectStatement;

    public LinkedList<Node> getChildren() {
        return this.children;
    }


    /**
     * getter for the selectStatement
     */
    public SelectStatement getSelectStatement() {
        return selectStatement;
    }

    /** pointer to the Fromexpression that this expression queries from */
    FromExpression fromExpression;

    /**
     * Constructor to make an expression from the ColumnCalls
     * @param columns - to contain in the expression
     * @param treeBuilder - The TreeBuilder for the helper functions
     */
    public Expression(List<ColumnCall> columns, TreeBuilder treeBuilder) {
        this.builder = treeBuilder;
        this.build(columns);
    }

    /**
     * Setter for the selectStatement
     * the fromExpression will be overrided too
     * @param selectStatement - the new selectStatement
     */
    public void setSelectStatement(SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
        this.fromExpression = selectStatement.getFromExpression();
    }

    /**
     * Constructor for builduing expressions from the ANTLR tree
     *
     * @param t - the ANTLR tree
     * @param treeBuilder - The TreeBuilder for the helper functions
     * @param fromExpression - which contains the Expression
     * @param selectStatement - which contains the Expression
     * @throws Exception
     */
    public Expression(Tree t, TreeBuilder treeBuilder,
                      FromExpression fromExpression, SelectStatement selectStatement)
            throws TreeParsingException {
        this.builder = treeBuilder;
        this.selectStatement = selectStatement;
        this.fromExpression = fromExpression;
        this.build(t);
    }

    /**
     * Adds a columnCall to the Expression
     *
     * @param columnCall
     */
    public void addColumnCall(ColumnCall columnCall) {
        columnCall.setParentNode(this);
        this.children.addLast(columnCall);
    }

    /**
     * Builder for the Expression which builds up an expression from ColumnCalls
     *
     * @param columns - to be contained in the Expression
     * @throws TreeParsingException
     */
    public void build(List<ColumnCall> columns) {
        this.tokenType = JdbcGrammarParser.EXPRESSION;
        this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
        this.logger.debug("Adding columns " + this.tokenName);

        for (int i = 0; i < columns.size(); i++) {
            ColumnCall columnCall = columns.get(i);
            columnCall.setParentNode(this);
            this.children.addLast(columnCall);
        }
    }

    /**
     * Builder for the Expression which builds up an expression from the ANTLR tree
     *
     * @param t - the ANTLR tree
     * @throws TreeParsingException
     */
    public void build(Tree t) throws TreeParsingException {
        if (t.getType() == JdbcGrammarParser.EXPRESSION) {
            this.tokenType = t.getType();
            this.tokenName = JdbcGrammarParser.tokenNames[this.tokenType];
            this.logger.debug("BUILDING " + this.tokenName);
            for (int i = 0; i < t.getChildCount(); i++) {
                Tree child = t.getChild(i);
                switch (child.getType()) {
                    case JdbcGrammarParser.COLUMN:
                        System.err.println("BUILDING COLUMN");
                        ColumnCall columnCall2 = new ColumnCall(child,
                                this.builder, this.fromExpression, this);
                        this.children.addLast(columnCall2);
                        break;
                    case JdbcGrammarParser.FUNCTIONCALL:
                        FunctionCall functionCall = new FunctionCall(child,
                                this.builder,
                                this.selectStatement);
                        functionCall.setParentNode(this);
                        this.children.addLast(functionCall);
                        break;
                    case JdbcGrammarParser.JOKERCALL:
                        // We don't add jokercalls, we substitute them!!!!!!!
                        Resolver resolver = new Resolver(builder);
                        List<ColumnCall> parseSubQueryForJokerCalls = resolver.parseSubQForJokers((SubQuery) selectStatement.getFromExpression().children.get(0));
                        for (int j = 0; j < parseSubQueryForJokerCalls.size(); j++) {

                            ColumnCall columnCall = parseSubQueryForJokerCalls.get(j);
                            columnCall.setParentNode(this);
                            this.children.addLast(columnCall);
                        }
                        break;
                    case JdbcGrammarParser.MULTIPLECALL:
                        // We don't add MultiCalls, we substitute them!!!!!!!
                        // so we don't even need MultiCall class any more ^^
                        MultiCallResolver multiCallResolver = new MultiCallResolver(
                                new MultiCall(child, this.builder,
                                        this.fromExpression),
                                this.fromExpression, this.builder);
                        List<ColumnCall> substitutesforJokerCalls1 = multiCallResolver
                                .getSubstitutesforJokerCall();
                        for (ColumnCall columnCall : substitutesforJokerCalls1) {
                            columnCall.setParentNode(this);
                            this.children.addLast(columnCall);
                        }
                        break;
                    default:
                        break;
                }
            }
        } else {
            throw new TreeParsingException("This Tree is not an EXPRESSION");
        }
    }

    /**
     * removes all children from the expression
     */
    public void deleteAllChildren() {
        this.children = new LinkedList<Node>();
    }

    /**
     * Getter for the Columns
     * @return - the Columns of this expression
     */
    public List<ColumnCall> getColumns() {
        this.logger.debug("CALLED GETCOLUMNS");
        return this.getAllinstancesof(ColumnCall.class, JdbcGrammarParser.COLUMN);
    }

    /**
     * Getter for the functioncalls
     * @return - the Functions of this expression
     */
    public List<FunctionCall> getFunctionCalls() {
        this.logger.debug("CALLED GETFUNCTIONCALLS");
        return this.getAllinstancesof(FunctionCall.class, JdbcGrammarParser.FUNCTIONCALL);
    }

    /**
     * Getter for the JokerCall
     * @return - the JokerCall of this expression
     */
    public JokerCall getJokerCall() {
        for (Node node : this.children) {
            if (node.tokenType == JdbcGrammarParser.JOKERCALL) {
                return (JokerCall) node;
            }
        }
        return null;
    }

    /**
     * Getter for the MultiCalls
     * @return
     */
    public List<MultiCall> getMultiCalls() {
        return this.getAllinstancesof(MultiCall.class, JdbcGrammarParser.MULTIPLECALL);
    }

    @Override
    public String toPrettyString() {
        return this.toPrettyString(-1);
    }

    @Override
    public String toPrettyString(int level) {
        String result = "";
        for (Node node : this.children) {

            result += node.toPrettyString() + ",";
        }
        String newLine = System.getProperty("line.separator");
        if (result.length() == 0) {
            return "";
        }
        return result.substring(0, result.length() - 1).replace(",",
                "," + newLine + this.tab(level))
                + newLine;
    }
}
