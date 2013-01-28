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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import net.starschema.clouddb.jdbc.antlr.sqlparse.ColumnCallException;
import net.starschema.clouddb.jdbc.antlr.sqlparse.TreeParsingException;

/**
 * The class where we store the resolvers, we need 
 * these in order to build our tree on the fly
 * 
 * @author Attila Horvath, Balazs Gunics
 *
 */
public class WhereExpressionJoinResolver {
    static Logger logger = Logger.getLogger("net.starschema.clouddb.jdjbc.list.WhereExpressionJoinResolver");
    /**
     * Creates a SubQuery from a JoinExpression
     * @param joinExpression - the join expression to be contained in the subquery
     * @param builder - the TreeBuilder for the helper functions
     * @param selectStatement - which contains the subquery
     * @return
     */
    public static SubQuery mkSubQFromJoinExpr(
            JoinExpression joinExpression, TreeBuilder builder,
            SelectStatement selectStatement) {

        String id = builder.getuniqueid();        // generate a uniqueid
        Collection<ColumnCall> callsRight;
        Collection<ColumnCall> callsLeft;
        
        Resolver resolver = new Resolver(builder);
        if (joinExpression.leftItem.tokenType == JdbcGrammarParser.SUBQUERY) {
            callsLeft = resolver.parseSubQForJokers((SubQuery) joinExpression.leftItem);
        }
        else {
            callsLeft = resolver.parseSrcTableForJokers((SourceTable) joinExpression.leftItem);
        }
        if (joinExpression.rightItem.tokenType == JdbcGrammarParser.SUBQUERY) {
            callsRight = resolver.parseSubQForJokers((SubQuery) joinExpression.rightItem);
        }
        else {
            callsRight = resolver.parseSrcTableForJokers((SourceTable) joinExpression.rightItem);
        } 
        
        List<ColumnCall> allColumns = new ArrayList<ColumnCall>();
        allColumns.addAll(callsLeft);
        allColumns.addAll(callsRight);
        FromExpression fromExpressionForBuild = new FromExpression(builder, joinExpression);
        
        Expression expressionForBuild = null;
        expressionForBuild = new Expression(allColumns, builder);
        
        for (ColumnCall columnCall : allColumns) {
            columnCall.setParentNode(expressionForBuild);
        }
        //TODO we might use the other Constructor:
        //public SelectStatement(Expression expression, FromExpression fromExpression, 
        //TreeBuilder treeBuilder) 
        SelectStatement selectStatementForBuild = new SelectStatement(builder,
                expressionForBuild, fromExpressionForBuild);
        expressionForBuild.setSelectStatement(selectStatementForBuild);
        
        return new SubQuery(id, builder, selectStatementForBuild, id);
    }
    
    /**
     * Makes a JoinExpression from Two SubQuery as a Quartezian join
     * for this we're adding 1-1 column "true AS EQUIVALENT_COLUMN"
     * which can be put in the OnClause
     * 
     * @param left - subquery 1
     * @param right - subquery 2
     * @return a JoinExpression left JOIN right 
     * ON left.EQUIVALENT_COLUMN = right.EQUIVALENT_COLUMN
     */
    public static JoinExpression mkJoinExprFrmTwoSubQ(SubQuery left,
            SubQuery right) {
        // Add True as EQUIVALENT COLUMN to subqueries
        
        ColumnCall leftAddition = new ColumnCall(null,
                (new ArrayList<String>() {
                    private static final long serialVersionUID = 1L;
                    {
                        this.add("EQUIVALENT_COLUMN");
                    }
                }), left.builder, "true",left.getSelectStatement().getExpression());
        
        ColumnCall rightAddition = new ColumnCall(null,
                (new ArrayList<String>() {
                    private static final long serialVersionUID = 1L;
                    {
                        this.add("EQUIVALENT_COLUMN");
                    }
                }), left.builder, "true",right.getSelectStatement().getExpression());
        
        leftAddition.setEquivalentCol(true);
        rightAddition.setEquivalentCol(true);
        
        left.getSelectStatement().getExpression().children.addLast(leftAddition);
        right.getSelectStatement().getExpression().children.addLast(rightAddition);
        
        String leftAlias = left.getAlias();
        String rightAlias = right.getAlias();
        
        List<OnClauseCondition> conditions = new ArrayList<OnClauseCondition>();
        OnClauseCondition condition = new OnClauseCondition(
                left.builder, "=", new ColumnReference(
                        new String[] { leftAlias }, left.builder,
                        "EQUIVALENT_COLUMN", leftAddition),
                new ColumnReference(new String[] { rightAlias }, left.builder,
                        "EQUIVALENT_COLUMN", rightAddition));
        conditions.add(condition);
        
        OnClause onClause = new OnClause(left.builder,conditions, null);
        
        JoinExpression joinExpression = new JoinExpression(
                left.builder, left, right, onClause,
                left.selectStatement);
        return joinExpression;
    }
    
    /**
     * Gives back a list of SubQueries that has the same Selected Columns, so can be used in a union
     * @param disjunction
     * @param selectStatement
     * @return
     */
    public static List<SubQuery> mkJoinExprFrmDisjunction(Disjunction disjunction, SelectStatement selectStatement)
    {
        LinkedList<Node> childList = disjunction.getChildren();
        
        List<SubQuery> subqueries = new ArrayList<SubQuery>();
        
        for (Node node : childList) {
            if(node.getTokenType()==JdbcGrammarParser.CONJUNCTION) {
                subqueries.add(mkJoinExprFrmConjunction((Conjunction)node, selectStatement));
            }
            else if(node.getTokenType()==JdbcGrammarParser.BOOLEANEXPRESSIONITEM) {
                logger.debug(("THE BOOLEANEXPRESSIONITEM: "+(BooleanExpressionItem.class.cast(node).toPrettyString())));
                subqueries.add(mkJoinExprFrmBooleanExprItem((BooleanExpressionItem)node, selectStatement));
            }
        }
        
        SubQuery mainSubQuery = subqueries.get(0);
        
        Expression mainExpression = mainSubQuery.getSelectStatement().getExpression();
        List<ColumnCall> columns2 = mainExpression.getColumns();
        
        List<ColumnCall> columns = new ArrayList<ColumnCall>();
        for (ColumnCall columnCall : columns2) {
            if(!columnCall.getEquivalentCol()) {
                columns.add(columnCall);
            }
            else {
            mainExpression.children.remove(columnCall);
            }
        }
        List<Node> pointedNodeList = new ArrayList<Node>();
        
        //Get All the pointed Nodes for the outer columns
        for (ColumnCall columnCall : columns) {
            
            Node pointedNode = columnCall.getPointedNode();
            Node nextPointedNode = null;
            
            if(pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                nextPointedNode = ((ColumnCall)pointedNode).getPointedNode();
            }
            else if(pointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                nextPointedNode = null;
            }
            
            while(nextPointedNode!=null) {
                logger.debug("Getting last pointed node:");
                if(nextPointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                    nextPointedNode = ((ColumnCall)nextPointedNode).getPointedNode();
                }
                else if(nextPointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                    nextPointedNode = null;
                }
            }
            pointedNodeList.add(pointedNode);
        }
        //We change all the subqueries main columns to match for the union
        for(int i=1;i<subqueries.size();i++) {
            List<ColumnCall> subcolumns = new ArrayList<ColumnCall>();
            List<ColumnCall> subcolumns2 = subqueries.get(i).getSelectStatement().getExpression().getColumns();
            for (ColumnCall columnCall : subcolumns2) {
                if(!columnCall.getEquivalentCol()) {
                    subcolumns.add(columnCall);
                }
                else {
                    subqueries.get(i).getSelectStatement().getExpression().children.remove(columnCall);
                }
            }
            
            List<Node> subPointedNodeList = new ArrayList<Node>();
            
            //Get All the pointed Nodes for the inner columns
            for (ColumnCall columnCall : subcolumns) {
                
                Node pointedNode = columnCall.getPointedNode();
                Node nextPointedNode = null;
                
                if(pointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                    nextPointedNode = ((ColumnCall)pointedNode).getPointedNode();
                }
                else if(pointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                    nextPointedNode = null;
                }
                
                while(nextPointedNode!=null) {
                    if(nextPointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                        nextPointedNode = ((ColumnCall)nextPointedNode).getPointedNode();
                    }
                    else if(nextPointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                        nextPointedNode = null;
                    }
                }
                subPointedNodeList.add(pointedNode);
            
            //now we make a compare
            int main = 0;
            for (Node mainPointedNode : pointedNodeList) {                
                int sub = 0;
                for (Node subPointedNode : subPointedNodeList) {
                    if(subPointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                        ColumnCall subpointedColumnCall = ColumnCall.class.cast(subPointedNode);
                        if(mainPointedNode.getTokenType()==JdbcGrammarParser.COLUMN) {
                            ColumnCall mainpointedColumnCall = ColumnCall.class.cast(mainPointedNode);
                            if(mainpointedColumnCall.uniqueId.equals(subpointedColumnCall.uniqueId)) {           
                                subcolumns.get(sub).uniqueId = columns.get(main).uniqueId;
                                break;
                            }
                        }
                        else if(subPointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                            FunctionCall subpointedFunctionCall = FunctionCall.class.cast(subPointedNode);
                            if(mainPointedNode.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                                FunctionCall mainpointedFunctionCall = FunctionCall.class.cast(mainPointedNode);
                                if(mainpointedFunctionCall.uniqueId.equals(subpointedFunctionCall.uniqueId)) {
                                    subcolumns.get(sub).uniqueId = columns.get(main).uniqueId;
                                    break;
                                }
                            }
                        }
                        sub++;
                    }
                }
                main++;
            } // end of compare
            } // end of "for (columnCall : subcolumns)"       
        }
        return subqueries;
    }
    
    /**
     * Makes a Subquery with a JoinExpression inside from a
     * SubQuery and a BooleanExpression
     * 
     * @param item - the BooleanExpression to be the Onclause of the Join
     * @param selectStatement - which contains the Tables/subquerys for the join
     * @return - a SubQuery
     */
   public static SubQuery mkJoinExprFrmBooleanExprItem(BooleanExpressionItem item, 
           SelectStatement selectStatement) {           
       if(isValidJoinTerm(item)) {
           List<Node> referencedNodes = new ArrayList<Node>();
           
           for (Node node : selectStatement.getFromExpression().children) {
               logger.debug("FromExpressions Children " + node.toPrettyString().replace("\n", "").replace("\r", ""));
             switch (node.getTokenType()) {
               case JdbcGrammarParser.SUBQUERY:
                   for (Node call : SubQuery.class.cast(node).getSelectStatement().getExpression().children) {
                       
                       UniQueIdContainer callCast = UniQueIdContainer.class.cast(call);
                       
                       UniQueIdContainer pointedNodeLeft = ColumnReference.class.cast(item.left).getPointedNode();
                       UniQueIdContainer pointedNodeRight = ColumnReference.class.cast(item.right).getPointedNode();
                       String uniqueIdLeft = pointedNodeLeft.getUniqueid();
                       String uniqueIdRight = pointedNodeRight.getUniqueid();
                       
                       if(callCast.getUniqueid().equals(uniqueIdLeft) || callCast.getUniqueid().equals(uniqueIdRight))
                       {
                           referencedNodes.add(node);
                       }
                    }
               break;
             default:
               break;
             }   
           }
           logger.debug("Referenced Nodes size is: " + referencedNodes.size());
           JoinElement element = new JoinElement(item, referencedNodes);
           
           JoinChain chain = new JoinChain();
           chain.addElement(element);
           
           SubQuery right = chain.getJoinExpression();
           
           //Add all notincluded nodes as a Quartezien join
           LinkedList<Node> children = selectStatement.getFromExpression().children;
           
           for (Node node : children) {
               boolean found = false;
               for (Node node2 : chain.getIncludednodes()) {
                   if(node.equals(node2)) {
                       found = true;
                   }
               }
               if(!found) {
                   JoinExpression expr = WhereExpressionJoinResolver.
                           mkJoinExprFrmTwoSubQ(right, (SubQuery)node);
                   right = WhereExpressionJoinResolver.
                           mkSubQFromJoinExpr(expr, expr.builder, expr.selectStatement);
               }
           }
           return right;
       }
       else {
           FromExpression fromExpression = selectStatement.getFromExpression();
           List<SubQuery> subQueries = fromExpression.getSubQueries();
           
           //we only got 1 or less subQuery, we can't make join from these
           if(subQueries==null || subQueries.size()==1){
               return null;
           }           
           SubQuery left = null;
           for (SubQuery subQuery : subQueries) {
               if(left == null) {
                   left = subQuery;
               }
               else {
                   JoinExpression createJoinExpressionFromTwoSubQueries 
                       = mkJoinExprFrmTwoSubQ(left, subQuery);                                                        
                   left = mkSubQFromJoinExpr(createJoinExpressionFromTwoSubQueries, 
                           selectStatement.builder, selectStatement);

                   List<Node> nodes = new ArrayList<Node>();
                   nodes.add(item);
                   try {
                    left.selectStatement.whereExpression = 
                            new WhereExpression(nodes, left.selectStatement.builder, 
                                    left.selectStatement);
                   }
                   catch (TreeParsingException e) {
                       // TODO Auto-generated catch block
                       e.printStackTrace();
                   }
               }
           }
           return left;
       }
   }
   
    /**
     * Makes a Subquery with a JoinExpression inside from a
     * Conjunction and a SelectStatement from the valid Conjunction items
     * we don't use the invalid ones
     * 
     * @param conjunction - to be the Onclause
     * @param selectStatement - which contains the Join elements
     * @return - A Subquery
     */
    public static SubQuery mkJoinExprFrmConjunction(Conjunction conjunction, 
            SelectStatement selectStatement) {
        List<Node> validNodes = new ArrayList<Node>();
        List<Node> notValidNodes = new ArrayList<Node>();
        
        for (Node item : conjunction.children) {
            if(isValidJoinTerm((BooleanExpressionItem)item)) {
                validNodes.add(item);
            }
            else {
                notValidNodes.add(item);
            }
        }
        
        if(validNodes.isEmpty()){
            return null;
        }
        List<BooleanExpressionItem> keys = new ArrayList<BooleanExpressionItem>();
        List<List<Node>> referencedNodes = new ArrayList<List<Node>>();
        
        //Now we sort out those which add a different Source To the Join, They can only refer to 2
        FromExpression fromExpression = selectStatement.getFromExpression();
        
        for (Node validnode : validNodes) {
            
          keys.add((BooleanExpressionItem)validnode);
          List<Node> nodelist = new ArrayList<Node>();
            
          for (Node node : fromExpression.children) {
           switch (node.getTokenType()) {
             case JdbcGrammarParser.SUBQUERY:
               for (Node call : SubQuery.class.cast(node).getSelectStatement().
                       getExpression().children) {
                 String uniqueIdLeft = null;
                 String uniqueIdRight = null;

                 //getting the pointed Node of the Boolean Expression
                 UniQueIdContainer pointedNodeLeft = 
                         ((ColumnReference)( ((BooleanExpressionItem) validnode).left)).getPointedNode();
                 //TODO maybe we should remove these if-else to a single:
                 //uniqueIdLeft = pointedNodeLeft.uniqueId;
                 if(pointedNodeLeft.getTokenType()==JdbcGrammarParser.COLUMN) {
                    uniqueIdLeft = ((ColumnCall)pointedNodeLeft).uniqueId;
                 }
                 else if(pointedNodeLeft.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                    uniqueIdLeft = ((FunctionCall)pointedNodeLeft).uniqueId;
                 }
                 //getting the pointed Node of the Boolean Expression   
                 UniQueIdContainer pointedNodeRight =
                         ((ColumnReference)(((BooleanExpressionItem) validnode).right)).getPointedNode();
                 //TODO maybe we should remove these if-else to a single:
                 //uniqueIdRight = pointedNodeRight.uniqueId;
                 if(pointedNodeRight.getTokenType()==JdbcGrammarParser.COLUMN) {
                    uniqueIdRight = ((ColumnCall)pointedNodeRight).uniqueId;
                 }
                 else if(pointedNodeRight.getTokenType()==JdbcGrammarParser.FUNCTIONCALL) {
                    uniqueIdRight = ((FunctionCall)pointedNodeRight).uniqueId;
                 }
           
                 if(call.tokenType==JdbcGrammarParser.COLUMN) {
                     ColumnCall cast = ColumnCall.class.cast(call);
                     if(cast.uniqueId.equals(uniqueIdLeft)||(cast.uniqueId.equals(uniqueIdRight))) {
                         nodelist.add(node);
                     }
                 }
                 if(call.tokenType==JdbcGrammarParser.FUNCTIONCALL) {
                     FunctionCall cast = FunctionCall.class.cast(call);
                     if(cast.uniqueId.equals(uniqueIdLeft)||(cast.uniqueId.equals(uniqueIdRight))) {
                         nodelist.add(node);
                     }
                 }
                 //end of foreach on selects children
               }
               break;
             default:
               break;
               //end of switch
            }
           //end of foreach on fromExpression.children
          }            
            referencedNodes.add(nodelist);
        }
        
        List<JoinElement> joinElements = new ArrayList<JoinElement>();
        int l =0;
        for (Node node : validNodes) {
            
            List<Node> nodeList = referencedNodes.get(l);
            JoinElement element = new JoinElement(
                    BooleanExpressionItem.class.cast(node),nodeList);
            joinElements.add(element);
            l++;
        }
        
        List<JoinChain> joinChains = new ArrayList<JoinChain>();
        
        JoinChain chain = new JoinChain();
        for (JoinElement joinElement : joinElements) {
            chain.addElement(joinElement);
        }
        joinChains.add(chain);
        
        //If there were notconnected nodes left
       List<JoinElement> joinElementsLeft =  chain.getNotconnected();
       while(joinElementsLeft.size()!=0) {
           JoinChain chaintemp = new JoinChain();
           for (JoinElement element : joinElementsLeft) {
               chaintemp.addElement(element);
           }
           joinElementsLeft = chaintemp.getNotconnected();
           joinChains.add(chaintemp);
       }

       List<Node> includedNodes = new ArrayList<Node>();
       List<SubQuery> subQueries = new ArrayList<SubQuery>();
       for (JoinChain joinChain : joinChains) {
           subQueries.add(joinChain.getJoinExpression());
           includedNodes.addAll(joinChain.getIncludednodes());
       }
       
       //Make Quartezien joins from all disjunctive conditions 
       //like aa.column1=bb.column1 and cc.column1=dd.column1 
       //since they dont have to do nothing with each other
       SubQuery right = null;
       for(int i=subQueries.size()-1;i>-1;i--)
       {
           if(right == null) {
               right =subQueries.get(i);
           }
           else {
               JoinExpression expr = WhereExpressionJoinResolver
                       .mkJoinExprFrmTwoSubQ(right, subQueries.get(i));
               right = WhereExpressionJoinResolver.mkSubQFromJoinExpr(
                       expr, expr.builder, expr.selectStatement);
           }
       }
       
       //Add all notincluded nodes as a Quartezien join
       LinkedList<Node> children = fromExpression.children;
       
       for (Node node : children) {
           boolean found = false;
           for (Node node2 : includedNodes) {
               if(node.equals(node2)) {
                   found = true;
               }
           }
           if(!found) {
               JoinExpression expr = WhereExpressionJoinResolver
                       .mkJoinExprFrmTwoSubQ(right, (SubQuery)node);
               right = WhereExpressionJoinResolver
                       .mkSubQFromJoinExpr(expr, expr.builder, expr.selectStatement);
           }
       }
         
       logger.debug("ADDING NOT VALID NODES AT THE END");
       if(notValidNodes.size()!=0){
           try {
               
               right.selectStatement.whereExpression = 
                       new WhereExpression(notValidNodes,
                               right.selectStatement.builder,right.selectStatement);
               logger.debug("ADDING NOT VALID NODES AT THE END PHASE 2");
               //this should do
           }
           catch (TreeParsingException e) {
               logger.debug("Failed to make whereExpression",e);
           }
       }
       return right;
    }
    
    /**
     * Makes a WhereExpression
     * 
     * Makes a Subquery with a JoinExpression inside from a
     * Conjunction and a SelectStatement from the valid Conjunction items
     * we don't use the invalid ones
     * 
     * @param conjunction - to be the Onclause
     * @param selectStatement - which contains the Join elements
     * @return - A Subquery
     */
    public static WhereExpression mkWhereExprFrmConjunction(Conjunction conjunction, SelectStatement selectStatement)
    {
        List<Node> validNodes = new ArrayList<Node>();
        List<Node> notValidNodes = new ArrayList<Node>();
        
        for (Node item : conjunction.children) {
            if(isValidJoinTerm((BooleanExpressionItem)item)) {
                validNodes.add(item);
            }
            else {
                notValidNodes.add(item);
            }
        }
        
       logger.debug("ADDING NOT VALID NODES AT THE END");
       if(notValidNodes.size()!=0){
           try {
               
               return new WhereExpression(notValidNodes,
                               selectStatement.builder,selectStatement);
               //this should do
           }
           catch (TreeParsingException e) {
               logger.debug("Failed to make whereExpression",e);
           }
       }
       return null;
    }
    
    /**
     * Creates a Join from left and right item with the OnClauseConditionlist
     * @param conditions - to be the Onclause between left and right
     * @param left - Node 1
     * @param right - Node 2
     * @param selectstatement
     * @return
     */
    public static JoinExpression makeknownJoin(List<BooleanExpressionItem> conditions, 
            Node left, Node right, SelectStatement selectstatement) {
        List<OnClauseCondition> onclConditions = new ArrayList<OnClauseCondition>();
        List<LogicalOperator> logicalOperators = new ArrayList<LogicalOperator>();
        int i=0;
        for (BooleanExpressionItem booleanExpressionItem : conditions) {
            //We must reresolve the columnsreferences pointednodes, since they have changed.
            ColumnReference columnReferenceLeft = ColumnReference.class
                    .cast(booleanExpressionItem.getLeft());
            ColumnReference columnReferenceRight = ColumnReference.class
                    .cast(booleanExpressionItem.getRight());
            try {
                columnReferenceLeft.setPointedNode(columnReferenceLeft
                        .searchPointedNodeInSubQuery((SubQuery)left));
                if(columnReferenceLeft.getPointedNode() == null) {
                    columnReferenceLeft.setPointedNode(columnReferenceLeft
                            .searchPointedNodeInSubQuery((SubQuery)right));
                }
                columnReferenceRight.setPointedNode(columnReferenceRight
                        .searchPointedNodeInSubQuery((SubQuery)left));
                if(columnReferenceRight.getPointedNode()==null) {
                    columnReferenceRight.setPointedNode(columnReferenceRight
                            .searchPointedNodeInSubQuery((SubQuery)right));
                }
            }
            catch (Exception e) {
                logger.debug("Failed to make Join from the BooleanExpressionItem" , e);
            }
            onclConditions.add(new OnClauseCondition(selectstatement.builder, "=",
                    columnReferenceLeft, columnReferenceRight));
            i++;
            if(i%2==0) {
                logicalOperators.add(LogicalOperator.AND);
            }
        }
        OnClause onClause = new OnClause(selectstatement.builder,onclConditions, logicalOperators);
        return new JoinExpression(selectstatement.builder, left, right, onClause, selectstatement);
    }
    
    /**
     * Checks if the BooleanExpressionItem can be used as a JoinTerm
     * @param booleanExpressionItem
     * @param expression
     * @return
     */
    public static boolean isValidJoinTerm(BooleanExpressionItem booleanExpressionItem)
    {       
        if (booleanExpressionItem.left.tokenType==JdbcGrammarParser.COLUMN &&
                booleanExpressionItem.right.tokenType==JdbcGrammarParser.COLUMN &&
                booleanExpressionItem.comparisonOperator.equals("=") ) {
            
            //They cannot point to the same node
            ColumnReference leftcolref = ColumnReference.class.cast(booleanExpressionItem.left);
            ColumnReference rightcolref = ColumnReference.class.cast(booleanExpressionItem.right);
            
            if(leftcolref.getPointedNode().getTokenType()==JdbcGrammarParser.FUNCTIONCALL 
            && rightcolref.getPointedNode().getTokenType() == JdbcGrammarParser.FUNCTIONCALL){

                FunctionCall pfunctionCallLeft = FunctionCall.class.cast(leftcolref.getPointedNode());
                FunctionCall pfunctionCallRight = FunctionCall.class.cast(rightcolref.getPointedNode());
                if(pfunctionCallLeft.parentNode.equals(pfunctionCallRight.parentNode)) {
                    return false;
                }
            }

            if(leftcolref.getPointedNode().getTokenType()==JdbcGrammarParser.COLUMN 
            && rightcolref.getPointedNode().getTokenType() == JdbcGrammarParser.COLUMN){
                ColumnCall pColumnCallLeft = ColumnCall.class.cast(leftcolref.getPointedNode());
                ColumnCall pColumnCallRight = ColumnCall.class.cast(rightcolref.getPointedNode());
                
                if(pColumnCallLeft.parentNode.equals(pColumnCallRight.parentNode)) {
                    return false;
                }
            }
            return true;
        }
        else {
            return false;
        }
    }
    
    /**
     * ColumnResolver for the BooleanExpressionItem, this function resolves 
     * the pointed columns of the BooleanExpressionItem by looking them in the
     * selectStatements subqueries and/or joins
     * @param exprToResolve - to be resolved
     * @param caller - for the sources to be checked
     */
    public static void columnResolver(BooleanExpressionItem exprToResolve, SelectStatement caller){
      //Resolve new pointed Columns
        
        List<SubQuery> subQueries = caller.getFromExpression().getSubQueries();
        List<JoinExpression> joinExpressions = caller.getFromExpression().getJoinExpressions();
        if(joinExpressions!=null) {
            for (JoinExpression joinExpression : joinExpressions) {
                if(subQueries==null) {
                    subQueries= new ArrayList<SubQuery>();
                }
                subQueries.add(SubQuery.class.cast(joinExpression.getLeftItem()));
                subQueries.add(SubQuery.class.cast(joinExpression.getRightItem()));
            }
        }
        
        if(exprToResolve.left.tokenType==JdbcGrammarParser.COLUMN) {
            ColumnReference leftColRef = ColumnReference.class.cast(exprToResolve.left);
            if(subQueries!=null) {
                for (SubQuery subQuery : subQueries) {
                    try {
                        UniQueIdContainer PointedNodeInSubQuery = leftColRef.searchPointedNodeInSubQuery(subQuery);
                        if(PointedNodeInSubQuery!=null) {
                            leftColRef.setPointedNode(PointedNodeInSubQuery);
                        }
                    }
                    catch (ColumnCallException e) {
                        logger.debug(e);
                    }
                }
            }
        }
        
        if(exprToResolve.right.tokenType==JdbcGrammarParser.COLUMN) {
            ColumnReference rightColRef = ColumnReference.class.cast(exprToResolve.right);
            if(subQueries!=null) {
                for (SubQuery subQuery : subQueries) {
                    try {
                        UniQueIdContainer PointedNodeInSubQuery = rightColRef.searchPointedNodeInSubQuery(subQuery);
                        if(PointedNodeInSubQuery!=null){
                            rightColRef.setPointedNode(PointedNodeInSubQuery);
                        }
                    }
                    catch (ColumnCallException e) {
                        logger.debug(e);
                    }
                }
            }
        }
    } //end of function
    
}//end of class

/**
 * 
 * This Class implements a chain of JoinElements with the possibility to add 
 * new JoinElement to it, and it nest it properly inside the chain
 * 
 * @author Horváth Attila
 */
class JoinChain
{
    List<JoinElement> notConnected = new ArrayList<JoinElement>();
    
    List<Node> includedNodes = new ArrayList<Node>();
    
    /** Getter for the included nodes */
    public List<Node> getIncludednodes() {
        return includedNodes;
    }
    
    LinkedList<List<JoinElement>> connected = new LinkedList<List<JoinElement>>();
    
    /**getter for the NotConnected Elements */
     public List<JoinElement> getNotconnected() {
        return notConnected;
    }

     /**
      * Resolves the Joins in the Joinchains and passes back a SubQuery
      * @return - SubQuery containing joins
      */
    public SubQuery getJoinExpression() {
        SubQuery left = (SubQuery)includedNodes.get(0);
        SubQuery right = (SubQuery)includedNodes.get(1);
        int number = 0;

        for (List<JoinElement> joinElementList : this.connected) {
            List<BooleanExpressionItem> conditions = new ArrayList<BooleanExpressionItem>();
            for (JoinElement joinElement : joinElementList) {
                conditions.add(joinElement.item);
            }
            JoinExpression joinExpressionForBuild = WhereExpressionJoinResolver
                    .makeknownJoin(conditions, left, right, left.selectStatement);
            left = WhereExpressionJoinResolver
                    .mkSubQFromJoinExpr(joinExpressionForBuild,
                            joinExpressionForBuild.builder,
                            joinExpressionForBuild.selectStatement);
            number++;
            if(number+1<includedNodes.size()) {
                right = (SubQuery)includedNodes.get(number+1);
            }
        }
        return left;
    }
     
    /** An empty constructor that does absolutely nothing */
    public JoinChain() {
    }
    
    /**
     * Adds a JoinElement to the JoinChain, if all the referenced nodes are in,
     * then finds where to put it, if only 1 is found, then puts it at the end, 
     * if none found, then puts it to wait
     * 
     * @param element
     * @return - true if the add was successfull, false otherwise
     */
    public boolean addElement(JoinElement element) {
        if(includedNodes.size()==0) {
            for (Node node : element.getReferencedNodes()) {
                includedNodes.add(node);
            }
            List<JoinElement> elements = new ArrayList<JoinElement>();
            elements.add(element);
            connected.addLast(elements);
            return true;
        }
        else {
            boolean allFound = true;
            Node oneFound = null;
            for (Node referencedNode : element.getReferencedNodes()) {
                boolean found = false;
                for (Node node : includedNodes) {
                    if(node.equals(referencedNode)) {
                        found = true;
                        oneFound = node;
                    }
                }
                if(!found) {
                    allFound=false;
                }
            }
            //If(includednodes have all ponintednodes)
            if(allFound) {
                //We search for the last occurence of the referencednodes
                int lastoccurence = 0;
                int i=0;
                for (List<JoinElement> list : this.connected) {
                    boolean found = false;
                    for (JoinElement joinElement : list) {
                        for (Node refnode : joinElement.referencedNodes) {
                            for (Node refnode2 : element.referencedNodes) {
                                if(refnode.equals(refnode2)) {
                                    found = true;
                                }
                            }
                        }
                    }
                    if(found) {
                        lastoccurence=i;
                    }
                    i++;
                }
                //we add it there
                this.connected.get(lastoccurence).add(element);
                return true;
            }
            //If(includednodes have 1 pointednode)
            else if(oneFound!=null) {
                //we add it to the end of the line
                ArrayList<JoinElement> newelementlist = new ArrayList<JoinElement>();
                newelementlist.add(element);
                this.connected.addLast(newelementlist);
                for (Node refnode : element.referencedNodes) {
                    boolean found = false;
                    for (Node includednode : includedNodes) {
                        if(includednode.equals(refnode)) {
                            found = true;
                        }
                    }
                    if(!found) {
                        this.includedNodes.add(refnode);
                    }
                }
                //we recursively call the function since the includednodes changed so we might connect notconnected nodes
                
                boolean managed = true;
                while(managed!=false) {
                    if(notConnected.size()==0) {
                        managed = false;
                    }
                    else {
                        for (int i=0; i<notConnected.size();i++) {
                            
                            JoinElement elementTemp = notConnected.get(i);
                            notConnected.remove(i);   
                            managed = this.addElement(elementTemp);
                            if(!managed) {
                                notConnected.add(i, elementTemp);
                            }
                            else {
                                break;
                            }
                        }
                    }
                }
                return true;
            }
            //If(includednodes have not got any pointednode
            else {
                boolean found = false;
                for (JoinElement elementnotc : notConnected) {
                    if(elementnotc.equals(element)) {
                        found = true;
                    }
                }
                if(!found) {
                    this.notConnected.add(element);
                }
                return false;
            }   
        }
    }
    
}

/**
 * Class to store the Joins
 *
 */
class JoinElement
{
    BooleanExpressionItem item;
    List<Node> referencedNodes;
    
    /**
     * Constructor for the JoinElement
     * @param item - the OnClause of the Join
     * @param nodes - the 2 side of the Join
     */
    public JoinElement(BooleanExpressionItem item,List<Node> nodes) {
        this.item = item;
        this.referencedNodes = nodes;
    }
    
    /** returns the onClauses BolleanExpression Item */
    public BooleanExpressionItem getItem() {
        return item;
    }
    
    /** Getter for the referenced Nodes which we references to */
    public List<Node> getReferencedNodes() {
        return referencedNodes;
    }
}