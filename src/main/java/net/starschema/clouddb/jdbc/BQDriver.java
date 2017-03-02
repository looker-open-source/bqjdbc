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
 *
 * BQDriver - This class implements the java.sql.Driver interface
 *
 * The driver URL is:
 *
 * If Service account: jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=true
 * Properties File: username: account email (NOT URLENCODED) password: path to
 * key file (NOT URLENCODED)
 *
 * If Installed account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=false or
 * jdbc:BQDriver:projectid(urlencoded) Properties File: username: accountid (NOT
 * URLENCODED) password: clientsecret (NOT URLENCODED)
 *
 * You can also specify the login username and password in the url directly:
 *
 * If Service account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=true&user
 * =accountemail(urlencoded)&password=keypath(urlencoded)
 *
 * If Installed account:
 * jdbc:BQDriver:projectid(urlencoded)?ServiceAccount=false
 * &user=accountid(urlencoded)&password=clientsecret(urlencoded) or
 * jdbc:BQDriver
 * :projectid(urlencoded)&user=accountid(urlencoded)&password=clientsecret
 * (urlencoded)
 *
 * Any Java program can use this driver for JDBC purpose by specifying this URL
 * format.
 * </p>
 */

package net.starschema.clouddb.jdbc;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

/**
 * This Class implements the java.sql.Driver interface
 *
 * @author Horváth Attila
 */
public class BQDriver implements java.sql.Driver {

    /** Instance log4j.Logger */
    // static Logger logg = new Logger(BQDriver.class.getName());
    static Logger logg = Logger.getLogger(BQDriver.class.getName());
    /** Url_Prefix for using this driver */
    private static final String URL_PREFIX = "jdbc:BQDriver:";
    /** MAJOR Version of the driver */
    private static final int MAJOR_VERSION = 1;
    /** Minor Version of the driver */
    private static final int MINOR_VERSION = 3;

    /** Registers the driver with the drivermanager */
    static {
        try {

            BQDriver driverInst = new BQDriver();
            DriverManager.registerDriver(driverInst);

            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(System
                        .getProperty("user.home")
                        + File.separator
                        + ".bqjdbc"
                        + File.separator + "log4j.properties"));
                PropertyConfigurator.configure(properties);
            } catch (IOException e) {
                BasicConfigurator.configure();
            }

            logg = Logger.getLogger(driverInst.getClass());
            logg.debug("Registered the driver");

        } catch (Exception e) {
            e.printStackTrace();
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

        logg.debug("Creating Connection With url: " + url);

        if (this.acceptsURL(url)) {
            localConInstance = new BQConnection(url, loginProp);
        }

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

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Not implemented.");
    }
}
