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
 * The driver URL is:
 * 
 * If Service account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=true
 * Properties File:
 * username: account email (NOT URLENCODED)
 * password: path to key file (NOT URLENCODED)
 * 
 * If Installed account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=false
 * or
 * jdbc:BQDriver:projectid(urlencoded)
 * Properties File:
 * username: accountid (NOT URLENCODED)
 * password: clientsecret (NOT URLENCODED)
 * 
 * You can also specify the login username and password in the url directly:
 * 
 * If Service account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=true&user=accountemail(urlencoded)&password=keypath(urlencoded)
 * 
 * If Installed account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=false&user=accountid(urlencoded)&password=clientsecret(urlencoded)
 * or
 * jdbc:BQDriver:projectid(urlencoded)&user=accountid(urlencoded)&password=clientsecret(urlencoded)
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

import org.apache.log4j.Logger;

/**
 * This Class implements the java.sql.Driver interface
 * 
 * @author Horváth Attila
 */
public class BQDriver implements java.sql.Driver {

    /** Instance log4j.Logger */
    static Logger logger;
    /** Url_Prefix for using this driver */
    private static final String URL_PREFIX = "jdbc:BQDriver:";
    /** MAJOR Version of the driver */
    private static final int MAJOR_VERSION = 1;
    /** Minor Version of the driver */
    private static final int MINOR_VERSION = 0;

    /** Registers the driver with the drivermanager */
    static {
        BQDriver.logger = Logger.getLogger("BQDriver");
        try {
            BQDriver.logger.debug("Registering the driver");
            BQDriver driverInst = new BQDriver();
            DriverManager.registerDriver(driverInst);
            BQDriver.logger.info("Registered the driver");
        } catch (Exception e) {
        }
    }

    /**
     * Gets Major Version of the Driver as static
     * 
     * @return Major Version of the Driver as static
     */
    public static int getMajorVersionAsStatic() {
        return BQDriver.MAJOR_VERSION;
    }

    /**
     * Gets Minor Version of the Driver as static
     * 
     * @return Minor Version of the Driver as static
     */
    public static int getMinorVersionAsStatic() {
        return BQDriver.MINOR_VERSION;
    }

    /** It returns the URL prefix for using BQDriver */
    public static String getURLPrefix() {
        return BQDriver.URL_PREFIX;
    }

    /** {@inheritDoc} */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(BQDriver.URL_PREFIX);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * This method create a new BQconnection and then returns it
     * </p>
     */
    @Override
    public Connection connect(String url, Properties loginProp)
            throws SQLException {
        BQConnection localConInstance = null;

        if (this.acceptsURL(url))
            localConInstance = new BQConnection(url, loginProp);

        return localConInstance;
    }

    /** {@inheritDoc} */
    @Override
    public int getMajorVersion() {
        return BQDriver.MAJOR_VERSION;
    }

    /** {@inheritDoc} */
    @Override
    public int getMinorVersion() {
        return BQDriver.MINOR_VERSION;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Gets information about the possible properties for this driver.
     * </p>
     * 
     * @return a default DriverPropertyInfo
     */
    @Override
    public java.sql.DriverPropertyInfo[] getPropertyInfo(String url,
            Properties loginProps) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always returns false, since the driver is not jdbcCompliant
     * </p>
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }
}
