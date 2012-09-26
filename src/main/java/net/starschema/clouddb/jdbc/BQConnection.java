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
 * This class implements the java.sql.Connection interface
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.starschema.clouddb.cmdlineverification.Oauth2Bigquery;

import org.apache.log4j.Logger;

import com.google.api.services.bigquery.Bigquery;

/**
 * The connection class which builds the connection between BigQuery and the
 * Driver
 * 
 * @author Gunics Balázs, Horváth Attila
 * 
 */
public class BQConnection implements Connection {
    /** Variable to store auto commit mode */
    private boolean autoCommitEnabled = false;
    
    /** Instance log4j.Logger */
    Logger logger;
    /**
     * The bigquery client to access the service.
     */
    private Bigquery bigquery = null;
    
    /**
     * The projectid which needed for the queries.
     */
    private String projectid = null;
    /** Boolean to determine if the Connection is closed */
    private boolean isclosed = false;
    
    /** List to contain sql warnings in */
    private List<SQLWarning> SQLWarningList = new ArrayList<SQLWarning>();
    
    /** String to contain the url except the url prefix */
    private String URLPART = null;
    
    /**
     * Extracts the JDBC URL then makes a connection to the Bigquery.
     * 
     * @param serverdata
     *            the JDBC connection URL
     * @param loginProp
     * 
     * @throws GeneralSecurityException
     * @throws IOException
     * @throws SQLException
     */
    public BQConnection(String url, Properties loginProp) throws SQLException {
        
        this.logger = Logger.getLogger(BQConnection.class);
        this.URLPART = url;
        this.isclosed = false;
        
        if (url.contains("&user=") && url.contains("&password=")) {
            this.logger.debug("url contains &user and &password");
            int passwordindex = url.indexOf("&password=");
            int userindex = url.indexOf("&user=");
            
            String id;
            String key;
            try {
                id = URLDecoder.decode(url.substring(
                        userindex + "&user=".length(), passwordindex), "UTF-8");
                key = URLDecoder.decode(
                        url.substring(passwordindex + "&password=".length()),
                        "UTF-8");
            }
            catch (UnsupportedEncodingException e2) {
                throw new BQSQLException(e2);
            }
            
            String projectid;
            try {
                projectid = URLDecoder.decode(
                        url.substring(url.lastIndexOf(":") + 1), "UTF-8");
                this.logger.debug("projectid + end of url: " + projectid);
            }
            catch (UnsupportedEncodingException e1) {
                throw new BQSQLException(e1);
            }
            if (url.contains("?withServiceAccount=true")) {
                this.logger.debug("url contains ?withServiceAccount=true");
                this.projectid = projectid.substring(0, projectid.indexOf("?"));
                this.logger.debug("Project id is: " + this.projectid);
                try {
                    this.bigquery = Oauth2Bigquery.authorizeviaservice(id, key);
                    this.logger.info("Authorized with service account");
                }
                catch (GeneralSecurityException e) {
                    throw new BQSQLException(e);
                }
                catch (IOException e) {
                    throw new BQSQLException(e);
                }
            }
            else
                if (url.contains("?withServiceAccount=false")) {
                    this.logger.debug("url contains ?withServiceAccount=false");
                    this.projectid = projectid.substring(0,
                            projectid.indexOf("?"));
                    this.logger.debug("Project id is: " + this.projectid);
                    this.bigquery = Oauth2Bigquery.authorizeviainstalled(id,
                            key);
                    this.logger.info("Authorized with Oauth");
                }
                else {
                    this.logger
                            .debug("url doesn't contains ?withServiceAccount");
                    this.projectid = projectid.substring(0,
                            projectid.indexOf("&user"));
                    this.logger.debug("Project id is: " + this.projectid);
                    this.bigquery = Oauth2Bigquery.authorizeviainstalled(id,
                            key);
                    this.logger.info("Authorized with Oauth");
                }
        }
        else {
            this.logger.debug("url doesn't contains &user and &password");
            String id = loginProp.getProperty("user");
            String key = loginProp.getProperty("password");
            String projectid;
            try {
                projectid = URLDecoder.decode(
                        url.substring(url.lastIndexOf(":") + 1), "UTF-8");
                this.logger.debug("Project id with end of url: " + projectid);
            }
            catch (UnsupportedEncodingException e1) {
                throw new BQSQLException(e1);
            }
            
            if (url.contains("?withServiceAccount=true")) {
                this.logger.debug("url contains ?withServiceAccount=true");
                this.projectid = projectid.substring(0, projectid.indexOf("?"));
                this.logger.debug("Project id is: " + this.projectid);
                try {
                    this.bigquery = Oauth2Bigquery.authorizeviaservice(id, key);
                    this.logger.info("Authorized with service account");
                }
                catch (GeneralSecurityException e) {
                    throw new BQSQLException(e);
                }
                catch (IOException e) {
                    throw new BQSQLException(e);
                }
            }
            else {
                if (url.contains("?withServiceAccount=false")) {
                    this.logger.debug("url contains ?withServiceAccount=false");
                    this.projectid = projectid.substring(0,
                            projectid.indexOf("?"));
                    this.logger.debug("Project id is: " + this.projectid);
                }
                else {
                    this.logger
                            .debug("url doesn't contains ?withServiceAccount");
                    this.projectid = projectid;
                    this.logger.debug("Project id is: " + this.projectid);
                }
                this.logger.debug("Authorizing with Oauth as installed");
                this.bigquery = Oauth2Bigquery.authorizeviainstalled(id, key);
                this.logger.info("Authorized with oauth");
            }
        }
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Uses SQLWarningList.clear() to clear all warnings
     * </p>
     */
    @Override
    public void clearWarnings() throws SQLException {
        if (this.isclosed) {
            throw new BQSQLException("Connection is closed.");
        }
        this.SQLWarningList.clear();
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Sets bigquery to null and isclosed to true if the connection is not
     * already closed else no operation is performed
     * </p>
     */
    @Override
    public void close() throws SQLException {
        if (!this.isclosed) {
            this.bigquery = null;
            this.isclosed = true;
        }
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Throws Exception
     * </p>
     * 
     * @throws SQLException
     *             <p>
     *             There is no Commit in Google BigQuery + Connection Status
     *             </p>
     */
    @Override
    public void commit() throws SQLException {
        if (this.isclosed) {
            throw new BQSQLException(
                    "There's no commit in Google BigQuery.\nConnection Status: Closed.");
        }
        else {
            throw new BQSQLException(
                    "There's no commit in Google BigQuery.\nConnection Status: Open.");
        }
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "createArrayOf(String typeName, Object[] elements)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Blob createBlob() throws SQLException {
        throw new BQSQLException("Not implemented." + "createBlob()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Clob createClob() throws SQLException {
        throw new BQSQLException("Not implemented." + "createClob()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public NClob createNClob() throws SQLException {
        throw new BQSQLException("Not implemented." + "createNClob()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new BQSQLException("Not implemented." + "createSQLXML()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Creates a new BQStatement object with the projectid in this Connection
     * </p>
     * 
     * @return a new BQStatement object with the projectid in this Connection
     * @throws SQLException
     *             if the Connection is closed
     */
    @Override
    public Statement createStatement() throws SQLException {
        if (this.isclosed) {
            throw new BQSQLException("Connection is closed.");
        }
        return new BQStatement(this.projectid, this);
    }
    
    /** {@inheritDoc} */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Connection is Closed");
        }
        return new BQStatement(this.projectid, this, resultSetType,
                resultSetConcurrency);
    }
    
    /** {@inheritDoc} */
    @Override
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "createStaement(int,int,int)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "createStruct(string,object[])");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Getter for autoCommitEnabled
     * </p>
     * 
     * @return auto commit state;
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommitEnabled;
    }
    
    /**
     * Getter method for the authorized bigquery client
     */
    public Bigquery getBigquery() {
        return this.bigquery;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Return projectid
     * </p>
     * 
     * @return projectid Contained in this Connection instance
     */
    @Override
    public String getCatalog() throws SQLException {
        return this.projectid;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        throw new BQSQLException("Not implemented." + "getClientInfo()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new BQSQLException("Not implemented." + "");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * There's no commit.
     * </p>
     * 
     * @return CLOSE_CURSORS_AT_COMMIT
     */
    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Return a new BQDatabaseMetadata object constructed from this Connection
     * instance
     * </p>
     * 
     * @return a new BQDatabaseMetadata object constructed from this Connection
     *         instance
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        BQDatabaseMetadata metadata = new BQDatabaseMetadata(this);
        return metadata;
    }
    
    /**
     * Getter method for projectid
     */
    public String getprojectid() {
        return this.projectid;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Transactions are not supported.
     * </p>
     * 
     * @return TRANSACTION_NONE
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        return java.sql.Connection.TRANSACTION_NONE;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new BQSQLException("Not implemented." + "getTypeMap()");
    }
    
    /**
     * 
     * @return The URL which is in the JDBC drivers connection URL
     */
    public String getURLPART() {
        return this.URLPART;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * If SQLWarningList is empty returns null else it returns the first item
     * Contained inside <br>
     * Subsequent warnings will be chained to this SQLWarning.
     * </p>
     * 
     * @return SQLWarning (The First item Contained in SQLWarningList) + all
     *         others chained to it
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (this.isclosed) {
            throw new BQSQLException("Connection is closed.");
        }
        if (this.SQLWarningList.isEmpty()) {
            return null;
        }
        
        SQLWarning forreturn = this.SQLWarningList.get(0);
        this.SQLWarningList.remove(0);
        if (!this.SQLWarningList.isEmpty()) {
            for (SQLWarning warning : this.SQLWarningList) {
                forreturn.setNextWarning(warning);
            }
        }
        return forreturn;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns the status of isclosed boolean
     * </p>
     */
    @Override
    public boolean isClosed() throws SQLException {
        return this.isclosed;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * The driver is read only at this stage.
     * </p>
     * 
     * @return true
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Sends a query to BigQuery to get all the datasets contained in the
     * project accociated with this Connection object and checks if it's
     * succeeded
     * </p>
     * 
     * @throws SQLException
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (this.isclosed) {
            return false;
        }
        if (timeout < 0) {
            throw new BQSQLException(
                    "Timeout value can't be negative. ie. it must be 0 or above; timeout value is: "
                            + String.valueOf(timeout));
        }
        try {
            this.bigquery.datasets().list(this.projectid).execute();
        }
        catch (IOException e) {
            return false;
        }
        return true;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns false to everything
     * </p>
     * 
     * @return false
     */
    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Implement
        return false;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * We returns the original sql statement
     * </p>
     * 
     * @return sql - the original statement
     */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
        // TODO
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new BQSQLException("Not implemented." + "prepareCall(string)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new BQSQLException("Not implemented."
                + "prepareCall(String,int,int)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "prepareCall(string,int,int,int)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Creates and returns a PreparedStatement object
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        this.logger.debug("Creating Prepared Statement with parameters:");
        this.logger.debug(sql);
        this.logger.debug(this.projectid);
        PreparedStatement stm = new BQPreparedStatement(sql, this.projectid,
                this);
        return stm;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "prepareStatement(string,int)");
    }
    
    /** {@inheritDoc} */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        PreparedStatement stm = new BQPreparedStatement(sql, this.projectid,
                this, resultSetType, resultSetConcurrency);
        return stm;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "prepareStatement(String,int,int,int)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "prepareStatement(String,int[])");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        throw new BQSQLException("Not implemented."
                + "prepareStatement(String,String[])");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new BQSQLException("Not implemented."
                + "releaseSavepoint(Savepoint)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void rollback() throws SQLException {
        throw new BQSQLException("Not implemented." + "rollback()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new BQSQLException("Not implemented." + "rollback(savepoint)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Setter for autoCommitEnabled
     * </p>
     * 
     * 
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommitEnabled = autoCommit;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new BQSQLException("Not implemented." + "setCatalog(catalog)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        SQLClientInfoException e = new SQLClientInfoException();
        e.setNextException(new BQSQLException(
                "Not implemented. setClientInfo(properties)"));
        throw e;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        SQLClientInfoException e = new SQLClientInfoException();
        e.setNextException(new BQSQLException(
                "Not implemented. setClientInfo(properties)"));
        throw e;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (this.isclosed) {
            throw new BQSQLException("Connection is closed.");
        }
        throw new BQSQLException("Not implemented." + "setHoldability(int)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new BQSQLException("Not implemented." + "setReadOnly(bool)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new BQSQLException("Not implemented." + "setSavepoint()");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new BQSQLException("Not implemented." + "setSavepoint(String)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new BQSQLException("Not implemented."
                + "setTransactionIsolation(int)");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new BQSQLException("Not implemented."
                + "setTypeMap(Map<String, Class<?>>");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always throws SQLException
     * </p>
     * 
     * @return nothing
     * @throws SQLException
     *             Always
     */
    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new BQSQLException("Not found");
    }
    
}
