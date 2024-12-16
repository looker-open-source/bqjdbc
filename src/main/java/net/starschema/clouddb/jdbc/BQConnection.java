/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.http.HttpTransport;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The connection class which builds the connection between BigQuery and the Driver
 *
 * @author Gunics Balázs, Horváth Attila
 */
public class BQConnection implements Connection {

  // We permit using either a "." or ":" as the delimiter between the dataset and project ids.
  private static final String PROJECT_DELIMITERS = ":.";
  // The following regex uses a lookahead to match the last occurrence of a project delimiter.
  private static final String LAST_PROJECT_DELIMITER_REGEX =
      "[" + PROJECT_DELIMITERS + "](?=[^" + PROJECT_DELIMITERS + "]*$)";

  /** Variable to store auto commit mode */
  private boolean autoCommitEnabled = false;

  /** Instance log4j.Logger */
  Logger logger;
  /** The bigquery client to access the service. */
  private Bigquery bigquery = null;

  /** The default dataset id to configure on queries processed by this connection. */
  private String dataset = null;

  /**
   * The default dataset project id to configure on queries processed by this connection.
   *
   * <p>We follow the same naming convention as the corresponding system variable <a
   * href="https://cloud.google.com/bigquery/docs/reference/system-variables">@@dataset_project_id</a>.
   */
  private String datasetProjectId = null;

  /** The ProjectId to use for billing on queries processed by this connection. */
  private final String projectId;

  /** Boolean to determine if the Connection is closed */
  private boolean isclosed = false;

  private Long maxBillingBytes;

  private final Integer timeoutMs;

  private final Map<String, String> labels;

  private final boolean useQueryCache;

  private final Set<BQStatementRoot> runningStatements =
      Collections.synchronizedSet(new HashSet<BQStatementRoot>());

  /** Boolean to determine whether or not to use legacy sql (default: false) * */
  private final boolean useLegacySql;

  /**
   * Enum that describes whether to create a job in projects that support stateless queries. Copied
   * from <a
   * href="https://github.com/googleapis/java-bigquery/blob/v2.34.0/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/QueryJobConfiguration.java#L98-L111">google-cloud-bigquery
   * 2.34.0</a>
   */
  public enum JobCreationMode {
    /** If unspecified JOB_CREATION_REQUIRED is the default. */
    JOB_CREATION_MODE_UNSPECIFIED,
    /** Default. Job creation is always required. */
    JOB_CREATION_REQUIRED,

    /**
     * Job creation is optional. Returning immediate results is prioritized. BigQuery will
     * automatically determine if a Job needs to be created. The conditions under which BigQuery can
     * decide to not create a Job are subject to change. If Job creation is required,
     * JOB_CREATION_REQUIRED mode should be used, which is the default.
     *
     * <p>Note that no job ID will be created if the results were returned immediately.
     */
    JOB_CREATION_OPTIONAL;

    JobCreationMode() {}
  }

  /** The job creation mode - */
  private final JobCreationMode jobCreationMode;

  /** getter for useLegacySql */
  public boolean getUseLegacySql() {
    return useLegacySql;
  }

  /** List to contain sql warnings in */
  private final List<SQLWarning> SQLWarningList = new ArrayList<>();

  /** String to contain the url except the url prefix */
  private String URLPART = null;

  /**
   * Extracts the JDBC URL then makes a connection to the Bigquery.
   *
   * @param url the JDBC connection URL
   * @param loginProp
   * @throws GeneralSecurityException
   * @throws IOException
   * @throws SQLException
   */
  public BQConnection(String url, Properties loginProp) throws SQLException {
    this(url, loginProp, Oauth2Bigquery.HTTP_TRANSPORT);
  }

  static Properties toCaseInsensitive(Properties props) {
    final Properties ret = new Properties();
    if (props != null) {
      for (Object o : props.keySet()) {
        String prop = (String) o;
        ret.setProperty(prop.toLowerCase(), props.getProperty(prop));
      }
    }
    return ret;
  }

  /**
   * Like {@link BQConnection(String,Properties)} but allows setting the {@link HttpTransport} for
   * testing.
   */
  public BQConnection(String url, Properties loginProp, HttpTransport httpTransport)
      throws SQLException {
    this.logger = LoggerFactory.getLogger(this.getClass());
    this.URLPART = url;
    this.isclosed = false;

    try {
      Pattern pathParamsMatcher =
          Pattern.compile("^jdbc:BQDriver::?([^?]*)", Pattern.CASE_INSENSITIVE);
      Matcher pathParamsMatchData = pathParamsMatcher.matcher(URLDecoder.decode(url, "UTF-8"));
      String pathParams;
      if (pathParamsMatchData.find()) {
        pathParams = pathParamsMatchData.group(1);
      } else {
        pathParams =
            URLDecoder.decode(url.substring(url.lastIndexOf(":") + 1, url.indexOf('?')), "UTF-8");
      }

      Pattern projectAndDatasetMatcher = Pattern.compile("^([^/$]+)(?:/([^$]*))?$");

      Matcher matchData = projectAndDatasetMatcher.matcher(pathParams);

      if (matchData.find()) {
        this.projectId = CatalogName.toProjectId(matchData.group(1));
        configureDataSet(matchData.group(2), this.projectId);
      } else {
        this.projectId = CatalogName.toProjectId(pathParams);
      }
    } catch (UnsupportedEncodingException e1) {
      throw new BQSQLException(e1);
    }

    final Properties caseInsensitiveProps;
    try {
      // parse the connection string and override anything passed via loginProps.
      caseInsensitiveProps = BQSupportFuncts.getUrlQueryComponents(url, toCaseInsensitive(loginProp));
    } catch (UnsupportedEncodingException e2) {
      throw new BQSQLException(e2);
    }

    String userId = caseInsensitiveProps.getProperty("user");
    String userKey = caseInsensitiveProps.getProperty("password");
    String userPath = caseInsensitiveProps.getProperty("path");

    // extract a list of "delegate" service accounts leading to a "target" service account to use
    // for impersonation. if only a single account is provided, then it will be used as the "target"
    List<String> targetServiceAccounts =
        parseArrayQueryParam(caseInsensitiveProps.getProperty("targetserviceaccount"), ',');

    // extract OAuth access token
    String oAuthAccessToken = caseInsensitiveProps.getProperty("oauthaccesstoken");

    // extract withServiceAccount property
    boolean serviceAccount =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("withserviceaccount"), false);

    // extract withApplicationDefaultCredentials
    boolean applicationDefaultCredentials =
        parseBooleanQueryParam(
            caseInsensitiveProps.getProperty("withapplicationdefaultcredentials"), false);

    // extract useLegacySql property
    this.useLegacySql =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("uselegacysql"), false);

    String jsonAuthContents = caseInsensitiveProps.getProperty("jsonauthcontents");

    // extract timeoutMs property
    this.timeoutMs = parseIntQueryParam("timeoutMs", caseInsensitiveProps.getProperty("timeoutms"));

    // extract readTimeout property
    Integer readTimeout =
        parseIntQueryParam("readTimeout", caseInsensitiveProps.getProperty("readtimeout"));

    // extract connectTimeout property
    Integer connectTimeout =
        parseIntQueryParam("connectTimeout", caseInsensitiveProps.getProperty("connecttimeout"));

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

    // extract any labels
    this.labels = tryParseLabels(caseInsensitiveProps.getProperty("labels"));
    // extract custom endpoint for connections through restricted VPC
    String rootUrl = caseInsensitiveProps.getProperty("rooturl");
    // see if the user wants to use or bypass BigQuery's cache
    // by default, the cache is enabled
    this.useQueryCache =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("querycache"), true);

    this.jobCreationMode = determineJobCreationMode(caseInsensitiveProps);

    // Create Connection to BigQuery
    if (serviceAccount) {
      try {
        // Support for old behavior, passing no actual password, but passing the path as 'password'
        if (userPath == null) {
          userPath = userKey;
          userKey = null;
        }
        this.bigquery =
            Oauth2Bigquery.authorizeViaService(
                userId,
                userPath,
                userKey,
                userAgent,
                jsonAuthContents,
                readTimeout,
                connectTimeout,
                rootUrl,
                httpTransport,
                targetServiceAccounts,
                this.getProjectId());
        this.logger.info("Authorized with service account");
      } catch (GeneralSecurityException | IOException e) {
        throw new BQSQLException(e);
      }
    } else if (oAuthAccessToken != null) {
      try {
        this.bigquery =
            Oauth2Bigquery.authorizeViaToken(
                oAuthAccessToken,
                userAgent,
                connectTimeout,
                readTimeout,
                rootUrl,
                httpTransport,
                targetServiceAccounts,
                this.getProjectId());
        this.logger.info("Authorized with OAuth access token");
      } catch (SQLException e) {
        throw new BQSQLException(e);
      }
    } else if (applicationDefaultCredentials) {
      try {
        this.bigquery =
            Oauth2Bigquery.authorizeViaApplicationDefault(
                userAgent,
                connectTimeout,
                readTimeout,
                rootUrl,
                httpTransport,
                targetServiceAccounts,
                this.getProjectId());
      } catch (IOException e) {
        throw new BQSQLException(e);
      }
    } else {
      throw new IllegalArgumentException("Must provide a valid mechanism to authenticate.");
    }
    logger.debug("The project id for this connections is: " + projectId);
  }

  public BQConnection(Bigquery bigquery, Properties properties) throws SQLException {
    final Properties caseInsensitiveProps = toCaseInsensitive(properties);

    // required final fields
    this.timeoutMs = parseIntQueryParam("timeoutMs", caseInsensitiveProps.getProperty("timeoutms"));
    this.labels = tryParseLabels(caseInsensitiveProps.getProperty("labels"));
    this.useQueryCache =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("querycache"), true);
    this.useLegacySql =
        parseBooleanQueryParam(caseInsensitiveProps.getProperty("uselegacysql"), false);
    this.jobCreationMode = determineJobCreationMode(caseInsensitiveProps);
  }

  static JobCreationMode determineJobCreationMode(Properties caseInsensitiveProps)
      throws SQLException {
    final String jobCreationModeString = caseInsensitiveProps.getProperty("jobcreationmode");
    if (jobCreationModeString == null) {
      return JobCreationMode.JOB_CREATION_MODE_UNSPECIFIED;
    }
    try {
      return JobCreationMode.valueOf(jobCreationModeString);
    } catch (IllegalArgumentException e) {
      throw new BQSQLException(
          "could not parse " + jobCreationModeString + " as job creation mode", e);
    }
  }

  private static Map<String, String> tryParseLabels(@Nullable String labels) {
    if (labels == null) {
      return Collections.emptyMap();
    }
    try {
      return Splitter.on(",").withKeyValueSeparator("=").split(labels);
    } catch (IllegalArgumentException ex) {
      return Collections.emptyMap();
    }
  }

  /**
   * Return {@code defaultValue} if {@code paramValue} is null. Otherwise, return true iff {@code
   * paramValue} is "true" (case-insensitive).
   */
  private static boolean parseBooleanQueryParam(@Nullable String paramValue, boolean defaultValue) {
    return paramValue == null ? defaultValue : Boolean.parseBoolean(paramValue);
  }

  /**
   * Return null if {@code paramValue} is null. Otherwise, return an Integer iff {@code paramValue}
   * can be parsed as a positive int.
   */
  private static Integer parseIntQueryParam(String param, @Nullable String paramValue)
      throws BQSQLException {
    Integer val = null;
    if (paramValue != null) {
      try {
        val = Integer.parseInt(paramValue);
        if (val < 0) {
          throw new BQSQLException(param + " must be positive.");
        }
      } catch (NumberFormatException e) {
        throw new BQSQLException("could not parse " + param + " parameter.", e);
      }
    }
    return val;
  }

  /**
   * Return an empty list if {@code string} is null. Otherwise, return an array of strings iff
   * {@code string} can be parsed as an array when split by {@code delimiter}.
   */
  private static List<String> parseArrayQueryParam(@Nullable String string, Character delimiter) {
    return string == null
        ? Collections.emptyList()
        : Arrays.asList(string.split(delimiter + "\\s*"));
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Uses SQLWarningList.clear() to clear all warnings
   */
  @Override
  public void clearWarnings() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("Connection is closed.");
    }
    this.SQLWarningList.clear();
  }

  /**
   * Returns a series of labels to add to every query.
   * https://cloud.google.com/bigquery/docs/adding-labels#job-label
   *
   * <p>A label that has a key with an empty value is used as a tag.
   * https://cloud.google.com/bigquery/docs/adding-labels#adding_a_tag
   */
  public Map<String, String> getLabels() {
    return this.labels;
  }

  /**
   * Return whether or not to use BigQuery's cache, as determined by the {@code cache} JDBC
   * parameter. Note that use of the cache is enabled by default.
   */
  public boolean getUseQueryCache() {
    return this.useQueryCache;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Sets bigquery to null and isclosed to true if the connection is not already closed else no
   * operation is performed
   */
  @Override
  public void close() throws SQLException {
    if (!this.isclosed) {
      this.cancelRunningQueries();
      this.bigquery = null;
      this.isclosed = true;
    }
  }

  /**
   * Parses the input dataset expression and sets the values for the dataset and datasetProjectId
   * instance variables.
   *
   * @param datasetExpr Dataset expression, generally taken from the BQJDBC connection string. Can
   *     be null.
   * @param defaultProjectId Project id to set as the value for datasetProjectId if no project id is
   *     found in {@code datasetExpr}.
   */
  private void configureDataSet(String datasetExpr, String defaultProjectId) {
    DatasetReference datasetRef = parseDatasetRef(datasetExpr);
    this.dataset = datasetRef.getDatasetId();
    this.datasetProjectId =
        datasetRef.getProjectId() == null ? defaultProjectId : datasetRef.getProjectId();
  }

  /**
   * Returns the default dataset that should be configured on queries processed by this connection.
   */
  public String getDataSet() {
    return this.dataset;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Throws Exception
   *
   * @throws SQLException
   *     <p>There is no Commit in Google BigQuery + Connection Status
   */
  @Override
  public void commit() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("There's no commit in Google BigQuery.\nConnection Status: Closed.");
    } /*
      else {
          throw new BQSQLException(
                  "There's no commit in Google BigQuery.\nConnection Status: Open.");
      }*/
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new BQSQLException(
        "Not implemented." + "createArrayOf(String typeName, Object[] elements)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Blob createBlob() throws SQLException {
    throw new BQSQLException("Not implemented." + "createBlob()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Clob createClob() throws SQLException {
    throw new BQSQLException("Not implemented." + "createClob()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public NClob createNClob() throws SQLException {
    throw new BQSQLException("Not implemented." + "createNClob()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new BQSQLException("Not implemented." + "createSQLXML()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Creates a new BQStatement object with the projectid in this Connection
   *
   * @return a new BQStatement object with the projectid in this Connection
   * @throws SQLException if the Connection is closed
   */
  @Override
  public Statement createStatement() throws SQLException {
    if (this.isclosed) {
      throw new BQSQLException("Connection is closed.");
    }
    logger.debug(
        "Creating statement with resultsettype: forward only," + " concurrency: read only");
    return new BQStatement(projectId, this);
  }

  /** {@inheritDoc} */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (this.isClosed()) {
      throw new BQSQLException("The Connection is Closed");
    }
    logger.debug(
        "Creating statement with resultsettype: "
            + resultSetType
            + " concurrency: "
            + resultSetConcurrency);
    return new BQStatement(projectId, this, resultSetType, resultSetConcurrency);
  }

  /** {@inheritDoc} */
  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new BQSQLException("Not implemented." + "createStaement(int,int,int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new BQSQLException("Not implemented." + "createStruct(string,object[])");
  }

  @Override
  public void setSchema(String schema) {
    configureDataSet(schema, this.projectId);
  }

  @Override
  public String getSchema() throws SQLException {
    return getDataSet();
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Getter for autoCommitEnabled
   *
   * @return auto commit state;
   */
  @Override
  public boolean getAutoCommit() throws SQLException {
    return this.autoCommitEnabled;
  }

  /** Getter method for the authorized bigquery client */
  public Bigquery getBigquery() {
    return this.bigquery;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Return projectid
   *
   * @return projectid Contained in this Connection instance
   */
  @Override
  public String getCatalog() throws SQLException {
    logger.debug("function call getCatalog returning projectId: " + projectId);
    return projectId;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Properties getClientInfo() throws SQLException {
    throw new BQSQLException("Not implemented." + "getClientInfo()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new BQSQLException("Not implemented." + "");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * There's no commit.
   *
   * @return CLOSE_CURSORS_AT_COMMIT
   */
  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Return a new BQDatabaseMetadata object constructed from this Connection instance
   *
   * @return a new BQDatabaseMetadata object constructed from this Connection instance
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    BQDatabaseMetadata metadata = new BQDatabaseMetadata(this);
    return metadata;
  }

  /** Getter method for the projectId to use for billing. */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Returns the default dataset project id that should be configured on queries processed by this
   * connection.
   */
  public String getDataSetProjectId() {
    return this.datasetProjectId;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Transactions are not supported.
   *
   * @return TRANSACTION_NONE
   */
  @Override
  public int getTransactionIsolation() throws SQLException {
    return java.sql.Connection.TRANSACTION_NONE;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new BQSQLException("Not implemented." + "getTypeMap()");
  }

  /** @return The URL which is in the JDBC drivers connection URL */
  public String getURLPART() {
    return this.URLPART;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * If SQLWarningList is empty returns null else it returns the first item Contained inside <br>
   * Subsequent warnings will be chained to this SQLWarning.
   *
   * @return SQLWarning (The First item Contained in SQLWarningList) + all others chained to it
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * returns the status of isclosed boolean
   */
  @Override
  public boolean isClosed() throws SQLException {
    return this.isclosed;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * The driver is read only at this stage.
   *
   * @return true
   */
  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Sends a query to BigQuery to get all the datasets contained in the project accociated with this
   * Connection object and checks if it's succeeded
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
      this.bigquery.datasets().list(projectId).execute();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Returns false to everything
   *
   * @return false
   */
  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    // TODO Implement
    return false;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * We returns the original sql statement
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareCall(string)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareCall(String,int,int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareCall(string,int,int,int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Creates and returns a PreparedStatement object
   *
   * @throws BQSQLException
   */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    this.logger.debug(
        "Creating Prepared Statement project id is: " + projectId + " with parameters:");
    this.logger.debug(sql);
    PreparedStatement stm = new BQPreparedStatement(sql, projectId, this);
    return stm;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareStatement(string,int)");
  }

  /** {@inheritDoc} */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    this.logger.debug(
        "Creating Prepared Statement"
            + " project id is: "
            + projectId
            + ", resultSetType (int) is: "
            + String.valueOf(resultSetType)
            + ", resultSetConcurrency (int) is: "
            + String.valueOf(resultSetConcurrency)
            + " with parameters:");
    this.logger.debug(sql);
    PreparedStatement stm =
        new BQPreparedStatement(sql, projectId, this, resultSetType, resultSetConcurrency);
    return stm;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareStatement(String,int,int,int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareStatement(String,int[])");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new BQSQLException("Not implemented." + "prepareStatement(String,String[])");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new BQSQLException("Not implemented." + "releaseSavepoint(Savepoint)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void rollback() throws SQLException {
    logger.debug("function call: rollback() not implemented ");
    // throw new BQSQLException("Not implemented." + "rollback()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new BQSQLException("Not implemented." + "rollback(savepoint)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Setter for autoCommitEnabled
   */
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    this.autoCommitEnabled = autoCommit;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new BQSQLException("Not implemented." + "setCatalog(catalog)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    SQLClientInfoException e = new SQLClientInfoException();
    e.setNextException(new BQSQLException("Not implemented. setClientInfo(properties)"));
    throw e;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    SQLClientInfoException e = new SQLClientInfoException();
    e.setNextException(new BQSQLException("Not implemented. setClientInfo(properties)"));
    throw e;
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * BigQuery is ReadOnly always so this is a noop
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
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new BQSQLException("Not implemented." + "setSavepoint()");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new BQSQLException("Not implemented." + "setSavepoint(String)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new BQSQLException("Not implemented." + "setTransactionIsolation(int)");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Not implemented yet.
   *
   * @throws BQSQLException
   */
  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new BQSQLException("Not implemented." + "setTypeMap(Map<String, Class<?>>");
  }

  /**
   *
   *
   * <h1>Implementation Details:</h1>
   *
   * <br>
   * Always throws SQLException
   *
   * @return nothing
   * @throws SQLException Always
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
      for (BQStatementRoot stmt : this.runningStatements) {
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

  public Integer getTimeoutMs() {
    return timeoutMs;
  }

  public JobCreationMode getJobCreationMode() {
    return jobCreationMode;
  }

  /**
   * Returns a DatasetReference extracted from the input dataset expression, which may optionally
   * include a project id reference.
   *
   * <p>This method parses the dataset expression into discrete components, using either a dot ('.')
   * or colon (':') as the delimiter between the dataset and project identifiers. We split the
   * string on the last occurrence of either delimiter, and we use the length of the split array to
   * determine whether the input contains a project id reference. If no project id is found, we set
   * the project id field to null in the returned DatasetReference. If the {@code datasetExpr} is
   * null, we return a DatasetReference with both its project id and dataset id set to null.
   *
   * <p>We don't perform any validation on the result; while there are well-defined rules regarding
   * <a href="https://cloud.google.com/resource-manager/docs/creating-managing-projects">project
   * ids</a> and <a href="https://cloud.google.com/bigquery/docs/datasets#dataset-naming">dataset
   * names</a>, we must handle references to both that don't adhere to the documented requirements
   * (such as having a domain name with a colon as part of the project id). Rather than deal with
   * the various corner cases for each type of identifier, we defer to the BigQuery API to validate
   * the default dataset configured on queries.
   *
   * <p>Visible for testing.
   *
   * @param datasetExpr Dataset expression, generally taken from the BQJDBC connection string. Can
   *     be null.
   * @return DatasetReference
   */
  static DatasetReference parseDatasetRef(String datasetExpr) {
    if (datasetExpr == null) {
      return new DatasetReference().setDatasetId(null).setProjectId(null);
    }
    // We split datasetExpr on the last occurrence of a project delimiter. To account for each
    // delimiter appearance in the expression, we pass -1 as the limit value to disable discarding
    // trailing empty strings.
    String[] datasetComponents = datasetExpr.split(LAST_PROJECT_DELIMITER_REGEX, -1);
    boolean isDatasetIdOnly = datasetComponents.length == 1;
    String datasetId = isDatasetIdOnly ? datasetComponents[0] : datasetComponents[1];
    String datasetProjectId =
        isDatasetIdOnly ? null : CatalogName.toProjectId(datasetComponents[0]);
    return new DatasetReference().setDatasetId(datasetId).setProjectId(datasetProjectId);
  }
}
