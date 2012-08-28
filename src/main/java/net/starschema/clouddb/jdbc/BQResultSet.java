/**
 *  Starschema Big Query JDBC Driver
 *  Copyright (C) 2012, Starschema Ltd.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 * This class implements the java.sql.Resultset interface
 */
package net.starschema.clouddb.jdbc;
 
//import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.TableRow;
 
public class BQResultSet extends ScrollableResultset<Object> implements java.sql.ResultSet
{

	private GetQueryResultsResponse Result = null;
    private BQStatement Statementreference = null;
     
    public BQResultSet (GetQueryResultsResponse BigQueryGetQueryResultResponse, BQStatement bqStatement){
 
        this.Result = BigQueryGetQueryResultResponse;
        if(Result.getRows()==null)
            RowsofResult=null;
        else
            RowsofResult = Result.getRows().toArray();
        this.Statementreference = bqStatement;
    }
 
    @Override
    public String getString(int columnIndex) throws SQLException {
    		closestrm();    
    		ThrowCursorNotValidExeption();
            if(this.isClosed())throw new SQLException("This Resultset is Closed");
            String result = ((TableRow) RowsofResult[Cursor]).getF().get(columnIndex-1).getV();
            if(result == null)
            	this.wasnull = true;
            else
            	this.wasnull = false;
            return result;
    }

    @Override
	public Object getObject(int columnIndex) throws SQLException {
    	 closestrm(); 
    	 if(this.isClosed())throw new SQLException("This Resultset is Closed");
		 ThrowCursorNotValidExeption();
         if(this.RowsofResult == null)throw new SQLException("There are no rows in this Resultset");
         if(this.getMetaData().getColumnCount()< columnIndex || columnIndex < 1)throw new SQLException("ColumnIndex is not valid");
         String Columntype = Result.getSchema().getFields().get(columnIndex-1).getType();
         String result = ((TableRow) RowsofResult[Cursor]).getF().get(columnIndex-1).getV();
         if(result == null)
         {
         	this.wasnull = true;
         	return null;
         }
         else
         {
        	 this.wasnull = false;
        	 try
        	 {
		         if(Columntype.equals("FLOAT"))
		         {
		        	 return Float.parseFloat(result);
		         }
		         else if(Columntype.equals("BOOLEAN"))
		         {
		        	 return Boolean.parseBoolean(result);
		         }
		         else if(Columntype.equals("INTEGER"))
		         {
		        	 return Integer.parseInt(result);
		         }
		         else if(Columntype.equals("STRING"))
		         {
		        	 return (result);
		         }
		         else
		         {
		        	 throw new SQLException("Unsupported Type");
		         }
        	 }
        	 catch (NumberFormatException e)
        	 {
        		 throw new SQLException(e);
        	 }
         }
	}
	
    //Helper Functions:
     
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if(this.isClosed())throw new SQLException("This Resultset is Closed");
        int columncount = this.getMetaData().getColumnCount();
        for(int i=1; i<= columncount;i++)
        {
            if(this.getMetaData().getCatalogName(i).equals(columnLabel)){
                return i;
            }
        }
        SQLException e = new SQLException("No Such column labeled: "+columnLabel);
        throw e;
    }
     
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if(this.isClosed())throw new SQLException("This Resultset is Closed");
        return new BQResultsetMetaData(Result);
    }
    
    @Override
	public Statement getStatement() throws SQLException {
		return this.Statementreference;
	}
    
    //BQ get Functions
    /*
    public Array BQgetArray(int columnIndex) throws SQLException {
    	
    	if(this.isClosed())throw new SQLException("This Resultset is Closed");
            if(this.RowsofResult == null)throw new SQLException("There are no rows in this Resultset");
            if(this.getMetaData().getColumnCount()< columnIndex || columnIndex < 1)throw new SQLException("ColumnIndex is not valid");
            Object objects = null;
            String Columntype = Result.getSchema().getFields().get(columnIndex-1).getType();
            if(Columntype.equals("FLOAT"))
            {
                objects = new Float[RowsofResult.length];
            }
            else if(Columntype.equals("BOOLEAN"))
            {
                objects = new Boolean[RowsofResult.length];
            }
            else if(Columntype.equals("INTEGER"))
            {
                objects = new Integer[RowsofResult.length];
            }
            else if(Columntype.equals("STRING"))
            {
                objects = new String[RowsofResult.length];
            }
             
            int i = 0;
            for(Object row:RowsofResult)
            {
                if(Columntype.equals("FLOAT"))
                {
                    ((Float[])objects)[i] = Float.parseFloat(((TableRow) row).getF().get(columnIndex-1).getV());
                }
                else if(Columntype.equals("BOOLEAN"))
                {
                    ((Boolean[])objects)[i] = Boolean.parseBoolean(((TableRow) row).getF().get(columnIndex-1).getV());
                }
                else if(Columntype.equals("INTEGER"))
                {
                    ((Integer[])objects)[i] = Integer.parseInt((((TableRow) row).getF().get(columnIndex-1).getV()));
                }
                else if(Columntype.equals("STRING"))
                {
                    ((String[])objects)[i] = ((TableRow) row).getF().get(columnIndex-1).getV();
                }
                i++;
            }
            BQArray myarray = new BQArray(objects);
             
            return myarray;
          
    }
    
    public Array BQgetArray(String columnLabel) throws SQLException {
    	
    	if(this.isClosed())throw new SQLException("This Resultset is Closed");
        try {
            int colplace = this.findColumn(columnLabel);
            return this.getArray(colplace);                     
        } catch (SQLException e) {
            throw e;
        }
    }
    */
    

}

