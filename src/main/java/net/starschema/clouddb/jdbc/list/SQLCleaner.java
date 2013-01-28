package net.starschema.clouddb.jdbc.list;

import java.util.ArrayList;
import java.util.List;


public class SQLCleaner {
    public static void Clean(SelectStatement mainSelectStatement)
    {
        FromExpression fromExpression = mainSelectStatement.getFromExpression();
        List<SubQuery> subQueries = fromExpression.getSubQueries();
        if(subQueries!=null)
        {  
            for (SubQuery subQuery : subQueries) {
                Clean(subQuery);
            }
        }
    }
    
    public static void Clean(SubQuery subQuery)
    {
        SelectStatement selectstatement = subQuery.getSelectStatement();
        Expression expression = selectstatement.getExpression();
        List<ColumnCall> columns = expression.getColumns();
        if(columns!=null)
        {
            for (ColumnCall columnCall : columns) {
                if(!columnCall.isPointedTo())
                {                   
                    //CHECK if its not an end column
                    if(!(columnCall.getPointedNode()==null)){
                        expression.children.remove(columnCall);
                    }
                }
            }
        }
        
        FromExpression fromExpression = selectstatement.getFromExpression();
        
        List<SubQuery> subQueries = fromExpression.getSubQueries();
        
        List<JoinExpression> joinExpressions = fromExpression.getJoinExpressions();
        if(joinExpressions!=null)
        {
            for (JoinExpression joinExpression : joinExpressions) {
                if(subQueries==null)
                {
                    subQueries = new ArrayList<SubQuery>();
                }
                
                Node leftItem = joinExpression.getLeftItem();
                Node rightItem = joinExpression.getRightItem();
                
                subQueries.add((SubQuery)leftItem);
                subQueries.add((SubQuery)rightItem);
            }
        }
        
        if(subQueries!=null)
        {  
            for (SubQuery subQuery1 : subQueries) {
                Clean(subQuery1);
            }
        }
    }
    
}
