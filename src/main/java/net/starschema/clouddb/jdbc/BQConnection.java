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

package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.Bigquery;
import net.starschema.clouddb.cmdlineverification.Oauth2Bigquery;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

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
    private String dataset = null;

    /**
     * The projectid which needed for the queries.
     */
    private String projectId = null;
    /** Boolean to determine if the Connection is closed */
    private boolean isclosed = false;

    private Long maxBillingBytes;

    private boolean useQueryApi = false;

    private final Set<BQStatementRoot> runningStatements = Collections.synchronizedSet(new HashSet<BQStatementRoot>());

    /** Boolean to determine, to use or doesn't use the ANTLR parser */
    private boolean transformQuery = false;

    /** getter for transformQuery */
    public boolean getTransformQuery() {
        return transformQuery;
    }

    /** Boolean to determine whether or not to use legacy sql (default: true) **/
    private boolean useLegacySql = true;

    /** getter for useLegacySql */
    public boolean getUseLegacySql() {
        return useLegacySql;
    }

    /** List to contain sql warnings in */
    private List<SQLWarning> SQLWarningList = new ArrayList<SQLWarning>();

    /** String to contain the url except the url prefix */
    private String URLPART = null;

    /**
     * Extracts the JDBC URL then makes a connection to the Bigquery.
     *
     * @param url
     *            the JDBC connection URL
     * @param loginProp
     *
     * @throws GeneralSecurityException
     * @throws IOException
     * @throws SQLException
     */
    public BQConnection(String url, Properties loginProp) throws SQLException {
        this.logger = Logger.getLogger(this.getClass());
        this.URLPART = url;
        this.isclosed = false;

        try {
            Pattern pathParamsMatcher = Pattern.compile("^jdbc:BQDriver::?([^?]*)", Pattern.CASE_INSENSITIVE);
            Matcher pathParamsMatchData = pathParamsMatcher.matcher(URLDecoder.decode(url, "UTF-8"));
            String pathParams;
            if (pathParamsMatchData.find()){
                pathParams = pathParamsMatchData.group(1);
            } else {
                pathParams = URLDecoder.decode(url.substring(url.lastIndexOf(":") + 1, url.indexOf('?')), "UTF-8");
            }

            Pattern projectAndDatasetMatcher = Pattern.compile("^([^/$]+)(?:/([^$]*))?$");

            Matcher matchData = projectAndDatasetMatcher.matcher(pathParams);

            if (matchData.find()) {
                this.projectId = matchData.group(1);
                this.dataset = matchData.group(2);
            } else {
                this.projectId = pathParams;
            }
        } catch (UnsupportedEncodingException e1) {
            throw new BQSQLException(e1);
        }
        // lets replace the : with __ and . with _
        this.projectId = this.projectId.replace(":", "__").replace(".", "_");

        Properties caseInsensitiveLoginProps = new Properties();

        if (loginProp != null) {
            Iterator props = loginProp.keySet().iterator();
            while (props.hasNext()) {
                String prop = (String) props.next();
                caseInsensitiveLoginProps.setProperty(prop.toLowerCase(), loginProp.getProperty(prop));
            }
        }

        Properties caseInsensitiveProps;

        try {
            // parse the connection string and override anything passed via loginProps.
            caseInsensitiveProps = BQSupportFuncts.getUrlQueryComponents(url, caseInsensitiveLoginProps);
        } catch (UnsupportedEncodingException e2) {
            throw new BQSQLException(e2);
        }

        String userId = caseInsensitiveProps.getProperty("user");
        String userKey = caseInsensitiveProps.getProperty("password");
        String userPath = caseInsensitiveProps.getProperty("path");

        // extract withServiceAccount property
        String withServiceAccountParam = caseInsensitiveProps.getProperty("withserviceaccount");
        Boolean serviceAccount = (withServiceAccountParam != null) && Boolean.parseBoolean(withServiceAccountParam);

        String useQueryApiParam = caseInsensitiveProps.getProperty("useQueryApi");
        useQueryApi = (useQueryApiParam != null) && Boolean.parseBoolean(useQueryApiParam);

        // extract transformQuery property
        String transformQueryParam = caseInsensitiveProps.getProperty("transformquery");
        this.transformQuery = (transformQueryParam != null) && Boolean.parseBoolean(transformQueryParam);

        // extract useLegacySql property
        String legacySqlParam = caseInsensitiveProps.getProperty("uselegacysql");
        this.useLegacySql = (legacySqlParam == null) || Boolean.parseBoolean(legacySqlParam);

        String readTimeoutString = caseInsensitiveProps.getProperty("readtimeout");
        Integer readTimeout = null;
        if (readTimeoutString != null) {
            try {
                readTimeout = Integer.parseInt(readTimeoutString);
                if (readTimeout < 0) {
                    throw new BQSQLException("readTimeout must be positive.");
                }
            } catch (NumberFormatException e) {
                throw new BQSQLException("could not parse readTimeout parameter.", e);
            }
        }

        String connectTimeoutString = caseInsensitiveProps.getProperty("connecttimeout");
        Integer connectTimeout = null;
        if (connectTimeoutString != null) {
            try {
                connectTimeout = Integer.parseInt(connectTimeoutString);
                if (connectTimeout < 0) {
                    throw new BQSQLException("connectTimeout must be positive.");
                }
            } catch (NumberFormatException e) {
                throw new BQSQLException("could not parse connectTimeout parameter.", e);
            }
        }

        String maxBillingBytesParam = caseInsensitiveProps.getProperty("maxbillingbytes");
        if (maxBillingBytesParam != null) {
            try {
                this.maxBillingBytes = Long.parseLong(maxBillingBytesParam);
            } catch (NumberFormatException e) {
                throw new BQSQLException("Bad number for maxBillingBytes", e);
            }
        }

        // extract UA String
        String userAgent = caseInsensitiveProps.getProperty("useragent");

        // Create Connection to BigQuery
        if (serviceAccount) {
            try {
                // Support for old behavior, passing no actual password, but passing the path as 'password'
                if (userPath == null) {
                    userPath = userKey;
                    userKey = null;
                }
                this.bigquery = Oauth2Bigquery.authorizeviaservice(userId, userPath, userKey, userAgent, connectTimeout, readTimeout);
                this.logger.info("Authorized with service account");
            } catch (GeneralSecurityException e) {
                throw new BQSQLException(e);
            } catch (IOException e) {
                throw new BQSQLException(e);
            }
        }
        //let use Oauth
        else {
            this.bigquery = Oauth2Bigquery.authorizeviainstalled(userId, userKey, userAgent);
            this.logger.info("Authorized with Oauth");
        }
        logger.debug("The project id for this connections is: " + this.projectId);
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
            this.cancelRunningQueries();
            this.bigquery = null;
            this.isclosed = true;
        }
    }

    public String getDataSet() {
        return this.dataset;
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
        }/*
        else {
            throw new BQSQLException(
                    "There's no commit in Google BigQuery.\nConnection Status: Open.");
        }*/
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
        logger.debug("Creating statement with resultsettype: forward only," +
                " concurrency: read only");
        return new BQStatement(this.projectId, this);
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Connection is Closed");
        }
        logger.debug("Creating statement with resultsettype: " + resultSetType
                + " concurrency: " + resultSetConcurrency);
        return new BQStatement(this.projectId, this, resultSetType,
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

    public void setSchema(String schema) throws SQLException {
        throw new BQSQLException("Not implemented.");
    }

    public String getSchema() throws SQLException {
        throw new BQSQLException("Not implemented.");
    }

    public void abort(Executor executor) throws SQLException {
        throw new BQSQLException("Not implemented.");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new BQSQLException("Not implemented.");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new BQSQLException("Not implemented.");
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
        logger.debug("function call getCatalog returning projectId: " + projectId);
        return this.projectId;
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
    public String getProjectId() {
        return this.projectId;
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
            this.bigquery.datasets().list(this.projectId.replace("__", ":").replace("_", ".")).execute();
        } catch (IOException e) {
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
        logger.debug("Function called nativeSQL() " + sql);
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
        this.logger.debug("Creating Prepared Statement project id is: "
                + this.projectId + " with parameters:");
        this.logger.debug(sql);
        PreparedStatement stm = new BQPreparedStatement(sql, this.projectId,
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
        this.logger.debug("Creating Prepared Statement" +
                " project id is: " + this.projectId +
                ", resultSetType (int) is: " + String.valueOf(resultSetType) +
                ", resultSetConcurrency (int) is: " + String.valueOf(resultSetConcurrency)
                + " with parameters:");
        this.logger.debug(sql);
        PreparedStatement stm = new BQPreparedStatement(sql, this.projectId,
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
        logger.debug("function call: rollback() not implemented ");
        //throw new BQSQLException("Not implemented." + "rollback()");
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
     * BigQuery is ReadOnly always so this is a noop
     * </p>
     *
     * @throws BQSQLException
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Connection is Closed");
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

    public void addRunningStatement(BQStatementRoot stmt) {
        this.runningStatements.add(stmt);
    }

    public void removeRunningStatement(BQStatementRoot stmt) {
        this.runningStatements.remove(stmt);
    }

    public int getNumberRunningQueries() {
        return this.runningStatements.size();
    }

    public int cancelRunningQueries() {
        int numFailed = 0;
        synchronized (this.runningStatements) {
            for(BQStatementRoot stmt : this.runningStatements) {
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    numFailed++;
                }
            }
        }
        return numFailed;
    }

    public Long getMaxBillingBytes() {
        return maxBillingBytes;
    }

    /**
     * Returns true if queries on this connection should use the synchronous jobs.query api to run queries.
     *
     * This API is faster by avoiding the async overhead of job polling, but does not currently support maxBillingGBs.
     * */
    boolean getUseQueryApi() {
        return useQueryApi;
    }
}
