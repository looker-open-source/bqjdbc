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

import java.util.ArrayList;
import java.util.List;


public class SQLCleaner {
    public static void Clean(SelectStatement mainSelectStatement) {
        FromExpression fromExpression = mainSelectStatement.getFromExpression();
        List<SubQuery> subQueries = fromExpression.getSubQueries();
        if (subQueries != null) {
            for (SubQuery subQuery : subQueries) {
                Clean(subQuery);
            }
        }
    }

    public static void Clean(SubQuery subQuery) {
        SelectStatement selectstatement = subQuery.getSelectStatement();
        Expression expression = selectstatement.getExpression();
        List<ColumnCall> columns = expression.getColumns();
        if (columns != null) {
            for (ColumnCall columnCall : columns) {
                if (!columnCall.isPointedTo()) {
                    //CHECK if its not an end column
                    if (!(columnCall.getPointedNode() == null)) {
                        expression.children.remove(columnCall);
                    }
                }
            }
        }

        FromExpression fromExpression = selectstatement.getFromExpression();

        List<SubQuery> subQueries = fromExpression.getSubQueries();

        List<JoinExpression> joinExpressions = fromExpression.getJoinExpressions();
        if (joinExpressions != null) {
            for (JoinExpression joinExpression : joinExpressions) {
                if (subQueries == null) {
                    subQueries = new ArrayList<SubQuery>();
                }

                Node leftItem = joinExpression.getLeftItem();
                Node rightItem = joinExpression.getRightItem();

                subQueries.add((SubQuery) leftItem);
                subQueries.add((SubQuery) rightItem);
            }
        }

        if (subQueries != null) {
            for (SubQuery subQuery1 : subQueries) {
                Clean(subQuery1);
            }
        }
    }

}
