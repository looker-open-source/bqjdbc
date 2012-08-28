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
 * This class implements the java.sql.ResultSet interface's Cursor
 * 
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * This class implements the java.sql.ResultSet interface's Cursor and is a Superclass for BQResultset and DMDResultSet 
 * @author Horváth Attila
 *
 * @param <T> Type of Array that cursor operates with
 */
abstract class ScrollableResultset<T> implements java.sql.ResultSet
{
	protected InputStream Strm = null;
    protected boolean wasnull = false;
	protected T[] RowsofResult;
	protected int Cursor = -1;
	protected Boolean Closed = false;
	
	public void ThrowCursorNotValidExeption() throws SQLException
	{
		if(this.RowsofResult==null || RowsofResult.length==0)
		{
			throw new SQLException("There are no rows in this Resultset");
		}
		else
		{
			if(Cursor == RowsofResult.length || Cursor == -1)
			{
				throw new SQLException("Cursor is not in a valid Position");
			}
		}
	}
	@Override
	public boolean absolute(int row) throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult==null)return false;
		if(row > 0)
		{
			if(row <= RowsofResult.length)
			{
				this.Cursor = row-1;
				return true;
			}
			else
			{
				//An attempt to position the cursor beyond the first/last row in the result set leaves the cursor before the first row or after the last row. 
				this.Cursor = RowsofResult.length;
				//false if the cursor is before the first row or after the last row 
				return false;
			}
		}
		//If the given row number is negative, the cursor moves to an absolute row position with respect to the end of the result set.
		else if(row < 0)
		{
			if(Math.abs(row) <= RowsofResult.length)
			{
				this.Cursor = RowsofResult.length + row;
				return true;
			}
			else
			{
				//An attempt to position the cursor beyond the first/last row in the result set leaves the cursor before the first row or after the last row. 
				this.Cursor = -1;
				//false if the cursor is before the first row or after the last row 
				return false;
			}
		}
		//if 0
		else 
		{
			/*
			//Check if cursor is before first of after last row
			if (this.Cursor == RowsofResult.size() || this.Cursor == -1)
				return false;
			else
				return true;
			*/
			if (this.Cursor == RowsofResult.length || this.Cursor == -1)
				return false;
			else
				this.Cursor = -1;
				return false;
		}
	}

	@Override
	public void afterLast() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(RowsofResult==null) return;
		if (RowsofResult.length >0)
			this.Cursor = RowsofResult.length;
	}

	@Override
	public void beforeFirst() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(RowsofResult==null) return;
		if (RowsofResult.length>0)
			this.Cursor = -1;
	}

	@Override
	public boolean first() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult==null || RowsofResult.length==0)return false;
		else 
		{
			Cursor = 0;
			return true;
		}
	}
	
	@Override
	public boolean last() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult==null || RowsofResult.length==0)return false;
		else 
		{
			Cursor = RowsofResult.length-1;
			return true;
		}
	}
	
	@Override
	public boolean isFirst() throws SQLException {
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(Cursor == 0 && this.RowsofResult!=null && RowsofResult.length!=0)
			return true;
		else
			return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult!=null && Cursor == RowsofResult.length-1  && RowsofResult.length-1>=0)
			return true;
		else
			return false;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult!=null && Cursor == RowsofResult.length && RowsofResult.length!=0)
			return true;
		else
			return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult!=null && Cursor == -1 && RowsofResult.length!=0)
			return true;
		else
			return false;
	}

	@Override
	public boolean next() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult==null) return false;
		if(RowsofResult.length > this.Cursor+1)
		{
			this.Cursor++;
			return true;
		}
		else {
			this.Cursor = RowsofResult.length;
			return false;
		}
	}

	@Override
	public boolean previous() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(RowsofResult==null) return false;
		if(Cursor > 0)
		{
			this.Cursor--;
			return true;
		}
		else {
			this.Cursor = -1;
			return false;
		}
	}
	
	@Override
	public boolean relative(int rows) throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(RowsofResult==null) return false;
		if(rows==0)
		{
			if(RowsofResult.length!=0 && this.Cursor < RowsofResult.length && this.Cursor > -1)
				return true;
			else
				return false;
		}
		else 
		{
			if(rows < 0)
			{
				if(this.Cursor + rows < 0)
				{
					this.Cursor = -1;
					return false;
				}
				else
				{
					this.Cursor = Cursor + rows;
					return true;
				}
			}
			else 
			{
				if(rows + this.Cursor > (RowsofResult.length-1))
				{
					this.Cursor = RowsofResult.length;
					return false;
				}
				else
				{
					this.Cursor += rows;
					return true;
				}
			}	
		}
	}
	
	@Override
	public int getRow() throws SQLException {
		if (this.getType() == TYPE_FORWARD_ONLY)
			throw new SQLException("The Type of the Resultset is TYPE_FORWARD_ONLY");
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		if(this.RowsofResult==null || RowsofResult.length==0 || this.Cursor == -1 || this.Cursor > RowsofResult.length-1)
			return 0;
		else
			return Cursor+1;
	}
	
	@Override
	public int getType() throws SQLException {
		if(this.isClosed())throw new SQLException("This Resultset is Closed");
		// TODO IMPLEMENT TYPE CHECK OF STATEMENT!!!!!!!
		return TYPE_SCROLL_INSENSITIVE;
	}
	
//Implemented Get functions Using Cursor
    
    @Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return new net.starschema.clouddb.jdbc.BQSQLXML(this.getString(columnIndex));
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
        return this.getSQLXML(columnIndex); 
	}
    
	@Override
	public boolean wasNull() throws SQLException {
		return wasnull;
	}
	

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
        return this.getObject(columnIndex);
	}
	
    @Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		
		String coltype = this.getMetaData().getColumnTypeName(columnIndex);
		if(coltype.equals("STRING"))
		{
			String Value = this.getString(columnIndex);
			if(wasNull())
				return null;
			else {
				try {
					return new java.math.BigDecimal(Value);
				}
				catch(NumberFormatException e) {
					throw new SQLException(e);
				}
			}
		}
		else if(coltype.equals("INTEGER"))
		{
			int Value = this.getInt(columnIndex);
			if(wasNull())
				return null;
			else
				return new java.math.BigDecimal(Value);
			
		}
		else if(coltype.equals("FLOAT"))
		{
			Float Value = this.getFloat(columnIndex);
			if(wasNull())
				return null;
			else
				return new java.math.BigDecimal(Value);
		}
		else if(coltype.equals("BOOLEAN"))
		{
			throw new NumberFormatException("Cannot format Boolean to BigDecimal");
		}
		else
		{
			throw new NumberFormatException("Undefined format");
		}
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
        return this.getBigDecimal(columnIndex);   
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		return this.getBigDecimal(columnIndex).setScale(scale);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		int columnIndex = this.findColumn(columnLabel);
        return this.getBigDecimal(columnIndex, scale);
	}
    
    //Time:
    
    @Override
	public Time getTime(int columnIndex) throws SQLException {
		Long value = this.getLong(columnIndex);
		if(wasNull())
			return null;
		else
		{
			return new java.sql.Time(value);
		}
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		 int columnIndex = this.findColumn(columnLabel);
         return this.getTime(columnIndex);   
   }

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		/*
		Select STRFTIME_UTC_USEC(NOW(),'%x-%X%Z') AS One, FORMAT_UTC_USEC(NOW()) as Two";
		Result:
		One						Two
		08/21/12-15:40:45GMT	2012-08-21 15:40:45.703908
		*/
		Long value = this.getLong(columnIndex);
		if(wasNull())
			return null;
		else
		{
			return new java.sql.Time(cal.getTimeZone().getRawOffset()+value);
		}
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		 int columnIndex = this.findColumn(columnLabel);
         return this.getTime(columnIndex, cal);   
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Long value = this.getLong(columnIndex);
		if(wasNull())
			return null;
		else
		{
			return new java.sql.Timestamp(value);
		}
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		 int columnIndex = this.findColumn(columnLabel);
         return this.getTimestamp(columnIndex);  
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		Long value = this.getLong(columnIndex);
		if(wasNull())
			return null;
		else
		{
			return new java.sql.Timestamp(cal.getTimeZone().getRawOffset()+value);
		}
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		 int columnIndex = this.findColumn(columnLabel);
         return this.getTimestamp(columnIndex, cal);  
	}
	//Date:
	
	@Override
	public Date getDate(int columnIndex) throws SQLException {
		Long value = this.getLong(columnIndex);
		if(wasNull())
			return null;
		else
		{
			return new java.sql.Date(value);
		}
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		 int columnIndex = this.findColumn(columnLabel);
         return this.getDate(columnIndex);  
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		Long value = this.getLong(columnIndex);
		if(wasNull())
			return null;
		else
		{
			return new java.sql.Date(value+cal.getTimeZone().getRawOffset());
		}
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		 int columnIndex = this.findColumn(columnLabel);
         return this.getDate(columnIndex,cal);  
	}

	//other:
    
    @Override
	public URL getURL(int columnIndex) throws SQLException {
		String Value = this.getString(columnIndex);
		if(this.wasNull())
		{
			return null;
		}
		else
		{
			try {
				return new URL(Value);
			} catch (MalformedURLException e) {
				throw new SQLException(e);
			}
		}
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		String Value = this.getString(columnLabel);
		if(this.wasNull())
		{
			return null;
		}
		else
		{
			try {
				return new URL(Value);
			} catch (MalformedURLException e) {
				throw new SQLException(e);
			}
		}
	}
	
	 @Override
		public short getShort(int columnIndex) throws SQLException {
	    	String Value = this.getString(columnIndex);
			if(wasNull())
			{
				return 0;
			}
			else
			{
				try
				{
					return Short.parseShort(Value);
				}
				catch (NumberFormatException e)
				{
					throw new SQLException(e);
				}
			}
		}

		@Override
		public short getShort(String columnLabel) throws SQLException {
			int columnIndex = this.findColumn(columnLabel);
	        return this.getShort(columnIndex);
		}
	    
	    @Override
		public long getLong(int columnIndex) throws SQLException {
	    	String Value = this.getString(columnIndex);
			if(wasNull())
			{
				return 0;
			}
			else
			{
				try
				{
					return Long.parseLong(Value);
				}
				catch (NumberFormatException e)
				{
					throw new SQLException(e);
				}
			}
		}

		@Override
		public long getLong(String columnLabel) throws SQLException {
			int columnIndex = this.findColumn(columnLabel);
	        return this.getLong(columnIndex);
		}
	    
	    @Override
		public byte getByte(int columnIndex) throws SQLException {
			String Value = this.getString(columnIndex);
			if(wasNull())
			{
				return 0;
			}
			else
			{
				try
				{
				return Byte.parseByte(Value);
				}
				catch (NumberFormatException e)
				{
					throw new SQLException(e);
				}
			}
		}

		@Override
		public byte getByte(String columnLabel) throws SQLException {
			int columnIndex = this.findColumn(columnLabel);
	        return this.getByte(columnIndex);
		}

		@Override
		public byte[] getBytes(int columnIndex) throws SQLException {
			String Value = this.getString(columnIndex);
			if(wasNull())
			{
				return null;
			}
			else
			{
				return Value.getBytes();
			}
		}

		@Override
		public byte[] getBytes(String columnLabel) throws SQLException {
			int columnIndex = this.findColumn(columnLabel);
	        return this.getBytes(columnIndex);
		}
		
		@Override
		public double getDouble(int columnIndex) throws SQLException {
			String Value = this.getString(columnIndex);
			if(wasNull())
			{
				return 0;
			}
			else
			{
				try
				{
				return Double.parseDouble(Value);
				}
				catch (NumberFormatException e)
				{
					throw new SQLException(e);
				}
			}
		}

		@Override
		public double getDouble(String columnLabel) throws SQLException {
			 int columnIndex = this.findColumn(columnLabel);
	         return this.getDouble(columnIndex);
		}
		
		//Using ColumnLabel
		
		  @Override
		    public boolean getBoolean(String columnLabel) throws SQLException {
		            int columnIndex = this.findColumn(columnLabel);
		            return this.getBoolean(columnIndex);        
		    }
		    
		    @Override
		    public float getFloat(String columnLabel) throws SQLException {
		                int columnIndex = this.findColumn(columnLabel);
		                return this.getFloat(columnIndex);
		    }
		    
		    @Override
		    public int getInt(String columnLabel) throws SQLException {
		        int columnIndex = this.findColumn(columnLabel);
		        return this.getInt(columnIndex);
		    }
		    
		    @Override
		    public String getString(String columnLabel) throws SQLException {
		        int columnIndex = this.findColumn(columnLabel);
		        return this.getString(columnIndex);
		        
		    }
		    
		    @Override
			public InputStream getAsciiStream(int columnIndex) throws SQLException {
				closestrm();
				java.io.InputStream inptstrm;
				String Value = this.getString(columnIndex);
				if(Value == null)
				{
					this.wasnull = true;
					this.Strm = null;
					return Strm;
				}
				else
				{
					this.wasnull = false;
					try {
						inptstrm = new java.io.ByteArrayInputStream(Value.getBytes("US-ASCII"));
					} catch (UnsupportedEncodingException e) {
						throw new SQLException(e);
					}
					this.Strm = inptstrm;
					return Strm;
				}
			}

			@Override
			public InputStream getAsciiStream(String columnLabel) throws SQLException {
				closestrm();
				java.io.InputStream inptstrm;
				String Value = this.getString(columnLabel);
				if(Value == null)
				{
					this.wasnull = true;
					this.Strm = null;
					return Strm;
				}
				else
				{
					this.wasnull = false;
					try {
						inptstrm = new java.io.ByteArrayInputStream(Value.getBytes("US-ASCII"));
					} catch (UnsupportedEncodingException e) {
						throw new SQLException(e);
					}
					this.Strm = inptstrm;
					return Strm;
				}
			}
			
			@Override
			public InputStream getBinaryStream(int columnIndex) throws SQLException {
				closestrm();
				java.io.InputStream inptstrm;
				String Value = this.getString(columnIndex);
				if(Value == null)
				{
					this.wasnull = true;
					this.Strm = null;
					return Strm;
				}
				else
				{
					this.wasnull = false;
					inptstrm = new java.io.ByteArrayInputStream(Value.getBytes());
					this.Strm = inptstrm;
					return Strm;
				}
			}

			@Override
			public InputStream getBinaryStream(String columnLabel) throws SQLException {
				closestrm();
				java.io.InputStream inptstrm;
				String Value = this.getString(columnLabel);
				if(Value == null)
				{
					this.wasnull = true;
					this.Strm = null;
					return Strm;
				}
				else
				{
					this.wasnull = false;
					inptstrm = new java.io.ByteArrayInputStream(Value.getBytes());
					this.Strm = inptstrm;
					return Strm;
				}
			}
			
			@Override
			public Reader getCharacterStream(int columnIndex) throws SQLException {
				closestrm();
				String Value = this.getString(columnIndex);
				if(Value == null)
				{
					this.wasnull = true;
					return null;
				}
				else
				{
					this.wasnull = false;
				Reader rdr = new StringReader(Value);
				return rdr;
				}
			}

			@Override
			public Reader getCharacterStream(String columnLabel) throws SQLException {
				closestrm();
				String Value = this.getString(columnLabel);
				if(Value == null)
				{
					this.wasnull = true;
					return null;
				}
				else
				{
					this.wasnull = false;
				Reader rdr = new StringReader(Value);
				return rdr;
				}
			}
			
			 protected void closestrm() throws SQLException{
			    	if(Strm!=null){
			    		try {
							this.Strm.close();
						} catch (IOException e) {
							throw new SQLException(e);
						}
			    	}
			    }
			 
			   @Override
			    public boolean getBoolean(int columnIndex) throws SQLException {
			    	String Value = this.getString(columnIndex);
					if(wasNull())
					{
						return false;
					}
					else
					{
						return Boolean.parseBoolean(Value);
					}
			    }
			 	
			    @Override
			    public float getFloat(int columnIndex) throws SQLException {
			    	String Value = this.getString(columnIndex);
					if(wasNull())
					{
						return 0;
					}
					else
					{
						try
						{
						return Float.parseFloat(Value);
						}
						catch (NumberFormatException e)
						{
							throw new SQLException(e);
						}
					}
			    }
			 
			    @Override
			    public int getInt(int columnIndex) throws SQLException {
			    	String Value = this.getString(columnIndex);
					if(wasNull())
					{
						return 0;
					}
					else
					{
						try
						{
						return Integer.parseInt(Value);
						}
						catch (NumberFormatException e)
						{
							throw new SQLException(e);
						}
					}
			    }
			    
			    @Override
			    public boolean isClosed() throws SQLException {
			        return Closed;
			    }
			    

				@Override
				public void close() throws SQLException {
					//TODO free occupied resources
					this.Closed = true;
				}
				
				//Not Implemented Functions:
			    
			    @Override
			    public Array getArray(int columnIndex) throws SQLException {
			    	throw new SQLFeatureNotSupportedException();
			    }
			 
			    @Override
			    public Array getArray(String columnLabel) throws SQLException {
			    	throw new SQLFeatureNotSupportedException();
			    }
			    
				@Override
				public boolean isWrapperFor(Class<?> iface) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@SuppressWarnings("hiding")
				@Override
				public <T> T unwrap(Class<T> iface) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void cancelRowUpdates() throws SQLException {
					throw new SQLFeatureNotSupportedException();
					
				}

				@Override
				public void clearWarnings() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void deleteRow() throws SQLException {
					throw new SQLFeatureNotSupportedException();
					
				}

				@Override
				public Blob getBlob(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
					
				}

				@Override
				public Blob getBlob(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public Clob getClob(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public Clob getClob(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public int getConcurrency() throws SQLException {
					return ResultSet.CONCUR_READ_ONLY;
				}

				@Override
				public String getCursorName() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public int getFetchDirection() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public int getFetchSize() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}
				
				@Override
				public int getHoldability() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public Reader getNCharacterStream(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public Reader getNCharacterStream(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public NClob getNClob(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public NClob getNClob(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public String getNString(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public String getNString(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				

				@Override
				public Object getObject(int columnIndex, Map<String, Class<?>> map)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
					// TODO Implement TypeMaps
				}

				@Override
				public Object getObject(String columnLabel, Map<String, Class<?>> map)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
					// TODO Implement TypeMaps
				}

				@Override
				public Ref getRef(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public Ref getRef(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public RowId getRowId(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public RowId getRowId(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public InputStream getUnicodeStream(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException("Deprecated. use getCharacterStream in place of getUnicodeStream");
				}

				@Override
				public InputStream getUnicodeStream(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException("Deprecated. use getCharacterStream in place of getUnicodeStream");
				}

				@Override
				public SQLWarning getWarnings() throws SQLException {
					//TODO implement error handling
					
					return null;
				}

				@Override
				public void insertRow() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void moveToCurrentRow() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void moveToInsertRow() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void refreshRow() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public boolean rowDeleted() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public boolean rowInserted() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public boolean rowUpdated() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void setFetchDirection(int direction) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void setFetchSize(int rows) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateArray(int columnIndex, Array x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateArray(String columnLabel, Array x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateAsciiStream(int columnIndex, InputStream x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateAsciiStream(String columnLabel, InputStream x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateAsciiStream(int columnIndex, InputStream x, int length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateAsciiStream(String columnLabel, InputStream x, int length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateAsciiStream(int columnIndex, InputStream x, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateAsciiStream(String columnLabel, InputStream x, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBigDecimal(int columnIndex, BigDecimal x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBigDecimal(String columnLabel, BigDecimal x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBinaryStream(int columnIndex, InputStream x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBinaryStream(String columnLabel, InputStream x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBinaryStream(int columnIndex, InputStream x, int length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBinaryStream(String columnLabel, InputStream x, int length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBinaryStream(int columnIndex, InputStream x, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBinaryStream(String columnLabel, InputStream x,
						long length) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBlob(int columnIndex, Blob x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBlob(String columnLabel, Blob x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBlob(int columnIndex, InputStream inputStream)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBlob(String columnLabel, InputStream inputStream)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBlob(int columnIndex, InputStream inputStream, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBlob(String columnLabel, InputStream inputStream,
						long length) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBoolean(int columnIndex, boolean x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBoolean(String columnLabel, boolean x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateByte(int columnIndex, byte x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateByte(String columnLabel, byte x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBytes(int columnIndex, byte[] x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateBytes(String columnLabel, byte[] x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateCharacterStream(int columnIndex, Reader x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateCharacterStream(String columnLabel, Reader reader)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateCharacterStream(int columnIndex, Reader x, int length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateCharacterStream(String columnLabel, Reader reader,
						int length) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateCharacterStream(int columnIndex, Reader x, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateCharacterStream(String columnLabel, Reader reader,
						long length) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateClob(int columnIndex, Clob x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateClob(String columnLabel, Clob x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateClob(int columnIndex, Reader reader) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateClob(String columnLabel, Reader reader)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateClob(int columnIndex, Reader reader, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateClob(String columnLabel, Reader reader, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateDate(int columnIndex, Date x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateDate(String columnLabel, Date x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateDouble(int columnIndex, double x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateDouble(String columnLabel, double x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateFloat(int columnIndex, float x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateFloat(String columnLabel, float x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateInt(int columnIndex, int x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateInt(String columnLabel, int x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateLong(int columnIndex, long x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateLong(String columnLabel, long x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNCharacterStream(int columnIndex, Reader x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNCharacterStream(String columnLabel, Reader reader)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNCharacterStream(int columnIndex, Reader x, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNCharacterStream(String columnLabel, Reader reader,
						long length) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNClob(String columnLabel, NClob nClob)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNClob(int columnIndex, Reader reader) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNClob(String columnLabel, Reader reader)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNClob(int columnIndex, Reader reader, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();	
				}

				@Override
				public void updateNClob(String columnLabel, Reader reader, long length)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNString(int columnIndex, String nString)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNString(String columnLabel, String nString)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNull(int columnIndex) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateNull(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateObject(int columnIndex, Object x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateObject(String columnLabel, Object x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateObject(int columnIndex, Object x, int scaleOrLength)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateObject(String columnLabel, Object x, int scaleOrLength)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateRef(int columnIndex, Ref x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateRef(String columnLabel, Ref x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateRow() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateRowId(int columnIndex, RowId x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateRowId(String columnLabel, RowId x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateSQLXML(int columnIndex, SQLXML xmlObject)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateSQLXML(String columnLabel, SQLXML xmlObject)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateShort(int columnIndex, short x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateShort(String columnLabel, short x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateString(int columnIndex, String x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateString(String columnLabel, String x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateTime(int columnIndex, Time x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateTime(String columnLabel, Time x) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateTimestamp(int columnIndex, Timestamp x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}

				@Override
				public void updateTimestamp(String columnLabel, Timestamp x)
						throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}
				
				@Override
				public Statement getStatement() throws SQLException {
					return null;
				}
				
				@Override
				public int findColumn(String columnLabel) throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}				
				
				@Override
				public ResultSetMetaData getMetaData() throws SQLException {
					throw new SQLFeatureNotSupportedException();
				}
}

