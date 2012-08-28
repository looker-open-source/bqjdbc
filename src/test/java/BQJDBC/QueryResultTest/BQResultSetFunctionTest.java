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
*/
package BQJDBC.QueryResultTest;

import static org.junit.Assert.fail;

import java.net.URLEncoder;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQJDBCAPI;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class BQResultSetFunctionTest {

private static java.sql.Connection con=null;
private static java.sql.ResultSet Result =null;

Logger logger = Logger.getLogger(BQResultSetFunctionTest.class);
	
	/**
	 * Makes a new Bigquery Connection to Hardcoded URL and gives back the Connection to static con member.
	 */
	@Before
	public void NewConnection(){

		try {
			if(con==null || !con.isValid(0)){
				BasicConfigurator.configure();		
			 	logger.info("Testing the JDBC driver");
				try {
					Class.forName("net.starschema.clouddb.jdbc.BQDriver");
					con = DriverManager.getConnection("jdbc:BQDriver:"+
							URLEncoder.encode("serviceacc230262422504-8hee1pe5017iq1i48jehgnj3vj517694@developer.gserviceaccount.com","UTF-8")+":"+
							URLEncoder.encode("C:\\key.p12","UTF-8")+":"+
							URLEncoder.encode("starschema.net:clouddb","UTF-8"));
					
	/*				System.out.println("jdbc:BQDriver:"+
	  						URLEncoder.encode("serviceacc230262422504-8hee1pe5017iq1i48jehgnj3vj517694@developer.gserviceaccount.com","UTF-8")+":"+
							URLEncoder.encode("C:\\key.p12","UTF-8")+":"+
							URLEncoder.encode("starschema.net:clouddb","UTF-8"));*/
				} 
				catch (Exception e){
					logger.fatal("Error in connection" + e.toString());
					fail("General Exception:" + e.toString());
				}
				logger.info(((BQConnection) con).getURLPART());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void QueryLoadTest() {
		final String sql = "SELECT TOP(word, 10), COUNT(*) FROM publicdata:samples.shakespeare";
		final String description =  "The top 10 word from shakespeare #TOP #COUNT";
		String[][] expectation = new String[][]{
		{"you","yet","would","world","without","with","will","why","whose","whom"},
		{"42","42","42","42","42","42","42","42","42","42"}};
		
		logger.info("Test number: 01");
		logger.info("Running query:" + sql);
		
		try {
			Statement stmt = con.createStatement();
			stmt.setQueryTimeout(500);
			Result = stmt.executeQuery(sql);
		} 
		catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		Assert.assertNotNull(Result);

		logger.debug(description);
		if(logger.getLevel() == org.apache.log4j.Level.DEBUG)
		printer(expectation);
		
		try {
			Assert.assertTrue("Comparing failed in the String[][] array", 
					comparer(expectation, BQJDBCAPI.GetQueryResult(Result)));
		} 
		catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail(e.toString());
		}
	}
	
	//Comprehensive Tests:
	
	@Test
	public void TestResultIndexOutofBound() 
	{
		try {
			logger.debug(Result.getBoolean(99));
		} catch (SQLException e) {
			Assert.assertTrue(true);
			logger.fatal("SQLexception" + e.toString());
		}
	}
	
	@Test
	public void TestResultSetFirst() 
	{
		try {
			Assert.assertTrue(Result.first());
			Assert.assertTrue(Result.isFirst());
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}
	
	@Test
	public void TestResultSetLast() 
	{
		try {
			Assert.assertTrue(Result.last());
			Assert.assertTrue(Result.isLast());
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}
	@Test
	public void TestResultSetgetString() 
	{
		try {
			Assert.assertTrue(Result.first());
			Assert.assertEquals("you", Result.getString(1));
			Assert.assertTrue(Result.last());
			Assert.assertEquals("whom", Result.getString(1));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}		
	@Test
	public void TestResultSetNext() 
	{
		try {
			Assert.assertTrue(Result.first());
			Assert.assertTrue(Result.next());
			Assert.assertEquals("yet", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("would", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("world", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("without", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("with", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("will", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("why", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("whose", Result.getString(1));
			Assert.assertTrue(Result.next());
			Assert.assertEquals("whom", Result.getString(1));
			Assert.assertFalse(Result.next());
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
	}
	
	@Test
	public void TestResultSetPrevious() 
	{
		try {
			Assert.assertTrue(Result.last());
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("whose", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("why", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("will", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("with", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("without", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("world", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("would", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("yet", Result.getString(1));
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("you", Result.getString(1));
			Assert.assertFalse(Result.previous());
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		try {
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
	}
	
	@Test
	public void TestResultSetBeforeFirst() 
	{
		try {
			Result.beforeFirst();
			Assert.assertTrue(Result.next());
			Assert.assertEquals("you", Result.getString(1));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Result.beforeFirst();
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
	}
	
	@Test
	public void TestResultSetAfterlast() 
	{
		try {
			Result.afterLast();
			Assert.assertTrue(Result.previous());
			Assert.assertEquals("whom", Result.getString(1));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Result.afterLast();
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
	}
	
	
	@Test
	public void TestResultSetAbsolute() 
	{
		try {
			Assert.assertTrue(Result.absolute(1));
			Assert.assertEquals("you", Result.getString(1));
			Assert.assertTrue(Result.absolute(2));
			Assert.assertEquals("yet", Result.getString(1));
			Assert.assertTrue(Result.absolute(3));
			Assert.assertEquals("would", Result.getString(1));
			Assert.assertTrue(Result.absolute(4));
			Assert.assertEquals("world", Result.getString(1));
			Assert.assertTrue(Result.absolute(5));
			Assert.assertEquals("without", Result.getString(1));
			Assert.assertTrue(Result.absolute(6));
			Assert.assertEquals("with", Result.getString(1));
			Assert.assertTrue(Result.absolute(7));
			Assert.assertEquals("will", Result.getString(1));
			Assert.assertTrue(Result.absolute(8));
			Assert.assertEquals("why", Result.getString(1));
			Assert.assertTrue(Result.absolute(9));
			Assert.assertEquals("whose", Result.getString(1));
			Assert.assertTrue(Result.absolute(10));
			Assert.assertEquals("whom", Result.getString(1));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertFalse(Result.absolute(0));
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
		
		try {
			Assert.assertFalse(Result.absolute(11));
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
	}
	
	@Test
	public void TestResultSetRelative() 
	{
		try {
			Assert.assertTrue(Result.absolute(1));
			Assert.assertEquals("you", Result.getString(1));
			Assert.assertTrue(Result.relative(1));
			Assert.assertEquals("yet", Result.getString(1));
			Assert.assertTrue(Result.relative(2));
			Assert.assertEquals("world", Result.getString(1));
			Assert.assertTrue(Result.relative(5));
			Assert.assertEquals("whose", Result.getString(1));
			Assert.assertTrue(Result.relative(-5));
			Assert.assertEquals("world", Result.getString(1));
			Assert.assertTrue(Result.relative(-2));
			Assert.assertEquals("yet", Result.getString(1));
			Assert.assertTrue(Result.relative(-1));
			Assert.assertEquals("you", Result.getString(1));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		try {
			Assert.assertTrue(Result.first());
			Assert.assertFalse(Result.relative(-1));
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
		
		try {
			Assert.assertTrue(Result.last());
			Assert.assertFalse(Result.relative(1));
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
	}
	
	@Test
	public void TestResultSetgetRow() 
	{
		
		try {
			Assert.assertTrue(Result.absolute(1));
			Assert.assertEquals(1, Result.getRow());
			Assert.assertTrue(Result.absolute(10));
			Assert.assertEquals(10, Result.getRow());
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		try {
			Result.beforeFirst();
			Assert.assertEquals(0, Result.getRow());
			Result.afterLast();
			Assert.assertEquals(0, Result.getRow());
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}
	
	@Test
	public void TestResultSetgetBoolean() 
	{
		try {
			Assert.assertTrue(Result.absolute(1));
			Assert.assertEquals(Boolean.parseBoolean("42"),Result.getBoolean(2));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}
	@Test
	public void TestResultSetgetFloat() 
	{
		try {
			Assert.assertTrue(Result.absolute(1));
			Assert.assertEquals(new Float(42),Result.getFloat(2));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}
	@Test
	public void TestResultSetgetInteger() 
	{
		try {
			Assert.assertTrue(Result.absolute(1));
			Assert.assertEquals(42,Result.getInt(2));
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
	}
	
	
	@Test
	public void ChainedCursorFunctionTest() {
		
		try {
			Result.beforeFirst();
			Assert.assertTrue(Result.next());
			Assert.assertEquals("you", Result.getString(1));
		
		} catch (SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertTrue(Result.absolute(10));
			Assert.assertEquals("whom",Result.getString(1));
		} catch (SQLException e) {	
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertFalse(Result.next());
		} catch (SQLException e) {	
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertEquals("",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}		
			
		}
		
		try {
			Assert.assertTrue(Result.first());
			Assert.assertEquals("you", Result.getString(1));
		} catch (SQLException e) {	
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertTrue(Result.isFirst());
			Assert.assertFalse(Result.previous());
			Result.afterLast();
			Assert.assertTrue(Result.isAfterLast());
			Assert.assertTrue(Result.absolute(-1));
			Assert.assertEquals("whom",Result.getString(1));
		} catch (SQLException e) {	
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try 
		{
			Assert.assertTrue(Result.relative(-5));
			Assert.assertEquals("without",Result.getString(1));
		}
		catch(SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try 
		{
			Assert.assertFalse(Result.relative(6));	
		}
		catch(SQLException e) {
			logger.fatal("SQLexception" + e.toString());
			fail("SQLException" + e.toString());
		}
		
		try {
			Assert.assertEquals("without",Result.getString(1));
		} catch (SQLException e) {
			boolean ct = e.toString().contains("Cursor is not in a valid Position");
			if(ct == true)Assert.assertTrue(ct);
			else {
				logger.fatal("SQLexception" + e.toString());
				fail("SQLException" + e.toString());
			}				
		}
		
	}
	/**
	 * For testing isValid() , Close() , isClosed()
	 */
	@Test
	public void isClosedValidtest() {
		try {
			Assert.assertEquals(true, con.isValid(0));
		} catch (SQLException e) {
			fail("Got an exception" + e.toString());
			e.printStackTrace();
		}
		try {
			Assert.assertEquals(true, con.isValid(10));
		} catch (SQLException e) {
			fail("Got an exception" + e.toString());
			e.printStackTrace();
		}
		try {
			con.isValid(-10);
		} catch (SQLException e) {
			Assert.assertTrue(true);
			//e.printStackTrace();
		}
		
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			Assert.assertTrue(con.isClosed());
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		try {
			con.isValid(0);
		} catch (SQLException e) {
			Assert.assertTrue(true);
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Prints a String[][] QueryResult to Log
	 * @param input
	 */
	private void printer(String[][] input){
		for(int s=0;s<input[0].length;s++)
		{
			String Output = "";
			for(int i=0;i<input.length;i++)
			{
				if(i==input.length-1)Output += input[i][s];
				else
				Output += input[i][s]+"\t";
			}
			logger.debug(Output);
		}
	}
	
	
	/**
	 * Compares two String[][]
	 * @param expected
	 * @param reality
	 * @return true if they are equal false if not
	 */
	private boolean comparer(String[][] expected, String[][] reality){
		for (int i = 0; i < expected.length; i++) {
			for (int j = 0; j < expected[i].length; j++) {
				if(expected[i][j].toString().equals(reality[i][j])== false) return false;	
			}
		}
		
		return true;
	}

}
