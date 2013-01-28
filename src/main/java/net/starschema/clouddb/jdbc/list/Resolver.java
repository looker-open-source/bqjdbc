package net.starschema.clouddb.jdbc.list;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class Resolver {
    Connection connection;
    TreeBuilder builder;
    CallContainer container;
    protected Logger logger = Logger.getLogger(this.getClass());
    
    public Resolver(TreeBuilder builder) {
        
        this.connection = builder.getConnection();
        this.builder = builder;
        this.container = builder.callContainer;
    }
    
    /**
     * Gets a sourceTable's all possible columns
     * 
     * @param sourceTable
     * @return a List with the columns
     */
    protected List<ColumnCall> parseSrcTableForJokers(
            SourceTable sourceTable) {
        List<ColumnCall> returnlist = new ArrayList<ColumnCall>();
        List<String> columnstrings = this.builder.GetColumns(sourceTable);
        for (String string : columnstrings) {
            ColumnCall columnforreturn = new ColumnCall(null, null,
                    this.builder, string,null);
            returnlist.add(columnforreturn);
        }
        return returnlist;
    }
    

    protected List<ColumnCall> parseSubQForJokers(SubQuery subQuery) {
        List<ColumnCall> returnlist = new ArrayList<ColumnCall>();
        this.logger.debug("GETTING EXPRESSION");
        Expression expression = subQuery.getSelectStatement().getExpression();
        if (expression != null) {
            this.logger.debug("EXPRESSION is NOT NULL");
            this.logger.debug(expression.toPrettyString());
        }
        List<ColumnCall> columns = expression.getColumns();
        List<FunctionCall> functionCalls = expression.getFunctionCalls();
        
        if (functionCalls != null) {
            this.logger.debug("HAS FUNCTIONCALLS");
            for (FunctionCall functionCall : functionCalls) {
                returnlist.add(new ColumnCall(new String[] { subQuery.getUniqueId() },
                        null, this.builder, functionCall.getAlias(), functionCall,null));
            }
        }
        
        if (columns != null) {
            this.logger.debug("HAS COLUMNS");
            for (ColumnCall column : columns) {
                if (column.getAlias() != null) {
                    ColumnCall returncolumn = new ColumnCall(null, null,
                            this.builder, column.getAlias(), column,null);
                    returncolumn.addPrefixtoFront(subQuery.getAlias());
                    
                    returnlist.add(returncolumn);
                }
                else {
                    ColumnCall returncolumn = new ColumnCall(null, null,
                            this.builder, column.getName(), column,null);
                   
                    returncolumn.addPrefixtoFront(subQuery.getAlias());
                    returnlist.add(returncolumn);
                }
            }
        }
        return returnlist;
    }
    
    
}
