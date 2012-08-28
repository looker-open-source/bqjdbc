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
 *  <p>
* BQDriver - This class implements the java.sql.Driver interface
* 
* The driver URL is jdbc:BQDriver:<Service account id>:<Path to private key>:<ProjectID>.
* 
* Any Java program can use this driver for JDBC purpose by specifying 
* this URL format.
* </p>
*/

package net.starschema.clouddb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class BQDriver implements java.sql.Driver
{
	
	//Driver URL prefix.
	private static final String URL_PREFIX = "jdbc:BQDriver:";
	
	private static final int MAJOR_VERSION = 1;
	private static final int MINOR_VERSION = 0;
	
	static
	{
		try
		{
			//Register the JWDriver with DriverManager
			BQDriver driverInst = new BQDriver();
			DriverManager.registerDriver(driverInst);
			
			//System.setSecurityManager(new RMISecurityManager());
		}
		catch(Exception e)
		{}
	}
	
	/**
	 * It returns the URL prefix for using JWDriver
	 */
	public static String getURLPrefix()
	{
		return URL_PREFIX;
	}
	
	/**
	 * This method create a remote connection and then
	 * returns the JWConnection object holding a Remote Connection
	 */
	public Connection connect(String url,Properties loginProp)
			throws SQLException
	{	
		BQConnection localConInstance = null;
		
		if(acceptsURL(url))
		{			
				//extract the remote server location coming with URL
				String serverdata = url.substring(URL_PREFIX.length(),url.length());
				
				//Create the local JWConnection holding remote Connection

				localConInstance = new BQConnection(serverdata);

		}
					
		return (Connection)localConInstance;
	}	
	
	/**
	 * This method returns true if the given URL starts with the JWDriver url.
	 * It is called by DriverManager.
	 */
	public boolean acceptsURL(String url)
		   throws SQLException
	{						
		return url.startsWith(URL_PREFIX);
	}	

	public int getMajorVersion()
	{
		return MAJOR_VERSION;
	}
	
	public static int getMajorVersionAsStatic()
	{
		return MAJOR_VERSION;
	}
		
	public int getMinorVersion()
	{
		return MINOR_VERSION;
	}

	public static int getMinorVersionAsStatic()
	{
		return MINOR_VERSION;
	}
	public java.sql.DriverPropertyInfo[] getPropertyInfo(String url,Properties loginProps)
		throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	public boolean jdbcCompliant()
	{
		return false;
	}
}
