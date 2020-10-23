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
 * <p>
 * This class implements functions such as connecting to Bigquery, Checking out
 * the results and displaying them on console
 * </p>
 */

package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.*;
import com.google.api.services.bigquery.model.ProjectList.Projects;
import com.google.api.services.bigquery.model.TableList.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

/**
 * This class contains static methods for interacting with BigQuery
 *
 * @author Gunics Balázs, Horváth Attila
 */
public class BQSupportFuncts {
    /** log4j.Logger instance */
    static Logger logger = LoggerFactory.getLogger(BQSupportFuncts.class);

    /**
     * Constructs a valid BigQuery JDBC driver URL from the specified properties
     * File
     *
     * @param properties Properties File most commonly read by
     *                   ReadUrlFromPropFile(String)
     * @return a valid BigQuery JDBC driver URL or null if it fails to load
     * @throws UnsupportedEncodingException
     */
    public static String constructUrlFromPropertiesFile(Properties properties, boolean full, String dataset)
            throws UnsupportedEncodingException {
        String ProjectId = properties.getProperty("projectid");
        logger.debug("projectId is: " + ProjectId);
        String User = properties.getProperty("user");
        String Password = properties.getProperty("password");
        String path = properties.getProperty("path");
        dataset = dataset == null ? properties.getProperty("dataset") : dataset;

        String forreturn = "";

        if (properties.getProperty("type").equals("installed")) {
            if (User != null && Password != null && ProjectId != null) {
                forreturn = BQDriver.getURLPrefix()
                        + URLEncoder.encode(ProjectId, "UTF-8");
            } else {
                return null;
            }
        } else if (properties.getProperty("type").equals("service")) {
            if (User != null && Password != null && ProjectId != null) {
                forreturn = BQDriver.getURLPrefix()
                        + URLEncoder.encode(ProjectId, "UTF-8")
                        + (dataset != null && full ? "/" + URLEncoder.encode(dataset, "UTF-8") : "")
                        + "?withServiceAccount=true";
                if (full) {
                    forreturn += "&user=" + URLEncoder.encode(User, "UTF-8") + "&password=" + URLEncoder.encode(Password, "UTF-8");
                    if (path != null) {
                        forreturn += "&path=" + URLEncoder.encode(path, "UTF-8");
                    }
                }
            } else {
                return null;
            }
        } else if (properties.getProperty("type").equals("oauth")) {
            String accessToken = properties.getProperty("oauthaccesstoken");
            if (accessToken != null && ProjectId != null) {
                forreturn = BQDriver.getURLPrefix()
                    + URLEncoder.encode(ProjectId, "UTF-8")
                    + (dataset != null && full ? "/" + URLEncoder.encode(dataset, "UTF-8") : "");
                if (full) {
                    forreturn += "?oAuthAccessToken=" + URLEncoder.encode(accessToken, "UTF-8");
                }
            } else {
                return null;
            }
        } else {
            return null;
        }

        String useLegacySql = properties.getProperty("useLegacySql");
        if (useLegacySql != null) {
            if (properties.getProperty("type").equals("service")) {
                forreturn += "&useLegacySql=" + useLegacySql;
            }
            else {
                forreturn += "?useLegacySql=" + useLegacySql;
            }
        }

        return forreturn;
    }

    public static String constructUrlFromPropertiesFile(Properties properties) throws UnsupportedEncodingException {
        return constructUrlFromPropertiesFile(properties, false, null);
    }

    public static Properties getUrlQueryComponents(String url, Properties defaults) throws UnsupportedEncodingException {
        String[] splitAtQP = url.split("\\?");
        Properties components = (Properties) defaults.clone();

        if (splitAtQP.length == 1) {
            return components;
        }

        String queryString = splitAtQP[1];

        String[] querySubComponents = queryString.split("&");
        for (String subComponent : querySubComponents) {
            Matcher m = Pattern.compile("(.*)=(.*)").matcher(subComponent);
            if (m.find()) {
                components.setProperty(m.group(1).toLowerCase(), URLDecoder.decode(m.group(2), "UTF-8"));
            }
        }

        return components;
    }

    /**
     * Displays results of a Query on the console
     *
     * @param bigquery     A valid authorized Bigquery instance
     * @param projectId    The exact Id of the Project associated with the completed
     *                     BigQuery job
     * @param completedJob The Job reference of the completed job
     * @throws IOException <p>
     *                     If the request fails to get the QueryResult of the specified
     *                     job in the given ProjectId
     *                     </p>
     */
    public static void displayQueryResults(Bigquery bigquery, String projectId,
                                           Job completedJob) throws IOException {
        projectId = projectId.replace("__", ":").replace("_", ".");
        JobReference completedJobReference = completedJob.getJobReference();
        GetQueryResultsResponse queryResult = bigquery
                .jobs()
                .getQueryResults(projectId, completedJobReference.getJobId())
                .setLocation(completedJobReference.getLocation())
                .execute();
        List<TableRow> rows = queryResult.getRows();
        System.out.print("\nQuery Results:\n------------\n");
        for (TableRow row : rows) {
            for (TableCell field : row.getF()) {
                System.out.printf("%-20s", field.getV());
            }
            System.out.println();
        }
    }

    /**
     * Return a list of Projects which contains the String catalogname
     *
     * @param catalogName The String which the id of the result Projects must contain
     * @param Connection  A valid BQConnection instance
     * @return a list of Projects which contains the String catalogname
     * @throws IOException <p>
     *                     if the initialization of requesting the list of all Projects
     *                     (to be sorted from) fails
     *                     </p>
     */
    private static List<Projects> getCatalogs(String catalogName,
                                              BQConnection Connection) throws IOException {
        logger.debug("Function call getCatalogs catalogName: " +
                (catalogName != null ? catalogName : "null"));
        List<Projects> projects = Connection.getBigquery().projects().list()
                .execute().getProjects();

        //since we'll use . -> _ : -> __ conversion, it's easier to
        // replace all the . : before we do any compare
        if (projects != null && projects.size() != 0) {             //we got projects!
            for (Projects projects2 : projects) {
                projects2.setId(projects2.getId().replace(".", "_").replace(":", "__"));
                //updating the reference too
                ProjectReference projRef = projects2.getProjectReference();
                projRef.setProjectId(projRef.getProjectId().replace(".", "_").replace(":", "__"));
                projects2.setProjectReference(projRef);
            }
            if (catalogName != null) {
                List<Projects> ProjectsSearch = new ArrayList<Projects>();
                for (Projects project : projects) {
                    if (project.getId().contains(catalogName)) {
                        ProjectsSearch.add(project);
                    }
                }
                if (ProjectsSearch.size() == 0) {
                    return null;
                } else {
                    return ProjectsSearch;
                }
            } else {
                return projects;
            }
        } else {
            return null;
        }
    }

    /**
     * Parses a (instance of dataset).getId() and gives back the id only for the
     * dataset
     *
     * @param getId (instance of dataset).getId()
     * @return the id only for the dataset
     */
    public static String getDatasetIdFromDatasetDotGetId(String getId) {
        /*logger.debug("Function call getDatasetIdFromDatasetDotGetId" + getId +
                "return is: " + getId.substring(getId.lastIndexOf(":") + 1));*/
        return getId.substring(getId.lastIndexOf(":") + 1);
    }

    /**
     * Parses a (instance of table).getid() and gives back the id only for the
     * dataset
     *
     * @param getId (instance of table).getid()
     * @return the id only for the dataset
     */
    public static String getDatasetIdFromTableDotGetId(String getId) {
        /*logger.debug("Function call getDatasetIdFromTableDotGetId" + getId +
                "return is: " + getId.substring(getId.lastIndexOf(":") + 1, getId.lastIndexOf(".")));*/
        return getId.substring(getId.lastIndexOf(":") + 1, getId.lastIndexOf("."));
    }

    /**
     * Returns a list of Datasets, which are associated with the Project which's
     * id is exactly ProjectId, and their name matches datasetnamepattern
     *
     * @param datasetname The String the dataset's id must contain
     * @param projectId   The Id of the Project the dataset is preferably contained in
     * @param connection  A valid BQConnection Instance
     * @return a list of Datasets, which are associated with the Project which's
     * id is exactly ProjectId, and their name contains datasetname
     * @throws IOException <p>
     *                     if the request to get Projects that match the given ProjectId
     *                     fails
     *                     </p>
     */
    private static List<Datasets> getDatasets(String datasetname,
                                              String projectId, BQConnection connection) throws IOException {
        logger.debug("function call getDatasets, " +
                "datasetName: " + (datasetname != null ? datasetname : "null") +
                ", projectId: " + (projectId != null ? projectId : "null"));
        projectId = projectId.replace("__", ":").replace("_", ".");
        List<Datasets> datasetcontainer = connection.getBigquery().datasets()
                .list(projectId).execute().getDatasets();

        if (datasetcontainer != null && datasetcontainer.size() != 0) {
            if (datasetname != null) {
                List<Datasets> datasetsSearch = new ArrayList<Datasets>();
                for (Datasets in : datasetcontainer) {
                    if (matchPattern(getDatasetIdFromDatasetDotGetId(in.getId()),
                            datasetname)) {
                        datasetsSearch.add(in);
                    }
                }
                if (datasetsSearch.size() == 0) {
                    return null;
                } else {
                    return datasetsSearch;
                }
            } else {
                return datasetcontainer;
            }
        } else {
            return null;
        }
    }

    /**
     * Parses a (instance of dataset)/(instance of table).getid() and gives back
     * the id only for the Project
     *
     * @param getId (instance of dataset)/(instance of table).getid()
     * @returnthe the id only for the Project
     */
    public static String getProjectIdFromAnyGetId(String getId) {
        int pos = getId.indexOf(":");        // The first appearance of ":"
        if (getId.indexOf(":", pos + 1) != -1) {
            // If there's a second ":" we'll use it
            // (there must be a second ":" !!)
            pos = getId.indexOf(":", pos + 1);
        }
        String ret = getId.substring(0, pos); // Cutting out the project id
        return ret;
    }

    /**
     * Returns the result of a completed query
     *
     * @param bigquery     Instance of authorized Bigquery client
     * @param projectId    The id of the Project the completed job was run in
     * @param completedJob The Job instance of the completed job
     * @return the result of a completed query specified by projectId and
     * completedJob
     * @throws IOException <p>
     *                     if the request to get QueryResults specified by the given
     *                     ProjectId and Job id fails
     *                     </p>
     */
    public static GetQueryResultsResponse getQueryResults(Bigquery bigquery,
                                                          String projectId, Job completedJob) throws IOException {
        JobReference completedJobReference = completedJob.getJobReference();
        GetQueryResultsResponse queryResult = bigquery.jobs()
                .getQueryResults(projectId, completedJobReference.getJobId())
                .setLocation(completedJobReference.getLocation()).execute();
        long totalRows = queryResult.getTotalRows().longValue();
        if (totalRows == 0) {
            return queryResult;
        }
        while (totalRows > (long) queryResult.getRows().size()) {
            queryResult.getRows().addAll(
                    bigquery.jobs()
                            .getQueryResults(projectId, completedJobReference.getJobId())
                            .setLocation(completedJobReference.getLocation())
                            .setStartIndex(BigInteger.valueOf((long) queryResult.getRows().size()))
                            .execute()
                            .getRows());
        }
        return queryResult;
    }

    /**
     * Returns the result of a completed query
     *
     * @param bigquery     Instance of authorized Bigquery client
     * @param projectId    The id of the Project the completed job was run in
     * @param completedJob The Job instance of the completed job
     * @return the result of a completed query specified by projectId and
     * completedJob
     * @throws IOException <p>
     *                     if the request to get QueryResults specified by the given
     *                     ProjectId and Job id fails
     *                     </p>
     */
    public static GetQueryResultsResponse getQueryResultsDivided(Bigquery bigquery,
                                                                 String projectId, Job completedJob, BigInteger startAtRow, int fetchCount) throws IOException {
        GetQueryResultsResponse queryResult;
        JobReference completedJobReference = completedJob.getJobReference();
        queryResult = bigquery.jobs()
                .getQueryResults(projectId, completedJobReference.getJobId())
                .setLocation(completedJobReference.getLocation())
                .setStartIndex(startAtRow)
                .setMaxResults((long) fetchCount).execute();
        return queryResult;
    }

    /**
     * Returns the status of a job
     *
     * @param myjob     Instance of Job
     * @param bigquery  Instance of authorized Bigquery client
     * @param projectId The id of the Project the job is contained in
     * @return the status of the job
     * @throws IOException <p>
     *                     if the request to get the job specified by myjob and
     *                     projectId fails
     *                     </p>
     */
    public static String getQueryState(Job myjob, Bigquery bigquery,
                                       String projectId) throws IOException {
        projectId = projectId.replace("__", ":").replace("_", ".");
        JobReference myjobReference = myjob.getJobReference();
        Job pollJob = bigquery.jobs()
                .get(projectId, myjobReference.getJobId())
                .setLocation(myjobReference.getLocation())
                .execute();
        BQSupportFuncts.logger.info("Job status: "
                + pollJob.getStatus().getState()
                + " ; "
                + pollJob.getJobReference().getJobId()
                + " ; "
                + (System.currentTimeMillis() - pollJob.getStatistics()
                .getStartTime()));
        return pollJob.getStatus().getState();
    }

    /**
     * Cancels a job. Uses the fact that it returns a JobCancelResponse to help enforce actually calling .execute().
     *
     * @param jobReference  Instance of JobReference to cancel
     * @param bigquery  Instance of authorized Bigquery client
     * @param projectId The id of the Project the job is contained in
     */
    public static JobCancelResponse cancelQuery(JobReference jobReference, Bigquery bigquery, String projectId) throws IOException {
        return bigquery.jobs().cancel(projectId, jobReference.getJobId()).setLocation(jobReference.getLocation()).execute();
    }

    /**
     * Parses a (instance of table).getid() and gives back the id only for the
     * table
     *
     * @param getId (instance of table).getid()
     * @return the id only for the table
     */
    public static String getTableIdFromTableDotGetId(String getId) {
        return getId.substring(getId.lastIndexOf(".") + 1);
    }

    /**
     * Returns a list of Tables which's id matches TablenamePattern and are
     * exactly in the given Project and Dataset
     *
     * @param tableNamePattern  String that the tableid must contain
     * @param projectId  The exact Id of the Project that the tables must be in
     * @param datasetId  The exact Id of the Dataset that the tables must be in
     * @param connection Instance of a valid BQConnection
     * @return a list of Tables which's id contains Tablename and are exactly in
     * the given Project and Dataset
     * @throws IOException <p>
     *                     if the request to get all tables (to sort from) specified by
     *                     ProjectId, DatasetId fails
     *                     </p>
     */
    public static List<Tables> getTables(String tableNamePattern,
                                         String projectId, String datasetId, BQConnection connection)
            throws IOException {
        logger.debug("Function call getTables : " +
                "tableNamePattern: " + (tableNamePattern != null ? tableNamePattern : "null") +
                ", projectId: " + (projectId != null ? projectId : "null") +
                ", datasetID:" + (datasetId != null ? datasetId : "null") +
                "connection");
        projectId = projectId.replace("__", ":").replace("_", ".");
        Bigquery.Tables.List listCall = connection.getBigquery().tables()
                .list(projectId, datasetId).setMaxResults(10000000L);  // Really big number that we'll never hit
        List<Tables> tables = listCall.execute().getTables();
        if (tables != null && tables.size() != 0) {
            if (tableNamePattern != null) {
                List<Tables> tablesSearch = new ArrayList<Tables>();
                for (Tables in : tables) {
                    if (matchPattern(getTableIdFromTableDotGetId(in.getId()),
                            tableNamePattern)) {
                        tablesSearch.add(in);
                    }
                }
                if (tablesSearch.size() == 0) {
                    logger.debug("returning null");
                    return null;
                } else {
                    return tablesSearch;
                }
            } else {
                return tables;
            }
        } else {
            logger.debug("returning null");
            return null;
        }
    }

    /**
     * Gets Tables information from specific projects matching catalog,
     * tablenamepattern and datasetidpatterns
     *
     * @param connection Valid instance of BQConnection
     * @return List of Table
     * @throws IOException <p>
     *                     if the initialization of requesting the list of all Projects
     *                     (to be sorted from) fails<br>
     *                     if the request to get Projects that match the given ProjectId
     *                     fails<br>
     *                     if the request to get all tables (to sort from) specified by
     *                     ProjectId, DatasetId fails<br>
     *                     if the request to get table information based on ProjectId
     *                     DatasetId TableId fails
     *                     <p>
     */
    public static List<Table> getTables(BQConnection connection,
                                        String catalog, String schema, String tablename) throws IOException {
        List<Table> RET = new ArrayList<Table>();
        logger.debug("Function call getTables : " +
                "catalog: " + (catalog != null ? catalog : "null") +
                ", schema: " + (schema != null ? schema : "null") +
                ", tablename:" + (tablename != null ? tablename : "null") +
                "connection");
        //getting the projects for this connection
        List<Projects> Projects = BQSupportFuncts.getCatalogs(catalog,
                connection);

        if (Projects != null && Projects.size() != 0) {
            for (Projects proj : Projects) {
                List<Datasets> datasetlist = null;
                datasetlist = BQSupportFuncts.getDatasets(schema, proj.getId(),
                        connection);
                if (datasetlist != null && datasetlist.size() != 0) {
                    for (Datasets dataset : datasetlist) {
                        List<Tables> tables = null;

                        tables = BQSupportFuncts.getTables(tablename, proj
                                        .getId(),
                                BQSupportFuncts
                                        .getDatasetIdFromDatasetDotGetId(dataset
                                                .getId()), connection);

                        if (tables != null && tables.size() != 0) {
                            for (Tables table : tables) {
                                //TODO replace projID __ -> : _ -> .
                                String datasetString = BQSupportFuncts
                                        .getDatasetIdFromDatasetDotGetId(dataset.getId());
                                String tableString = BQSupportFuncts
                                        .getTableIdFromTableDotGetId(table.getId());
                                logger.debug("Calling connection.getBigquery().tables() " +
                                        "dataset is: " + datasetString +
                                        ", table is: " + tableString +
                                        ", project is: " + proj.getId().replace("__", ":").replace("_", "."));
                                Table tbl = connection.getBigquery().tables()
                                        .get(proj.getId().replace("__", ":").replace("_", "."),
                                                datasetString,
                                                tableString)
                                        .execute();
                                if (tbl != null) {
                                    RET.add(tbl);
                                }
                            }
                        }
                    }
                }
            }
            if (RET.size() == 0) {
                return null;
            } else {
                return RET;
            }
        } else {
            return null;
        }
    }

    /**
     * <br>
     * Some DatabaseMetaData methods take arguments that are String patterns.
     * These arguments all have names such as fooPattern. Within a pattern
     * String, "%" means match any substring of 0 or more characters, and "_"
     * means match any one character. Only metadata entries matching the search
     * pattern are returned. If a search pattern argument is set to null, that
     * argument's criterion will be dropped from the search.
     *
     * Checks if the input String matches the pattern string which may contain
     * %, which means it can be any character
     *
     * @param input
     * @param pattern
     * @return true if matches, false if not
     */
    public static boolean matchPattern(String input, String pattern) {
        if (pattern == null) {
            return true;
        }

        boolean regexexpression = false;
        String regexexpressionstring = null;

        if (pattern.contains("%")) {
            regexexpression = true;

            if (regexexpressionstring == null) {
                regexexpressionstring = pattern.replace("%", ".*");
            }
        }
        if (pattern.contains("_")) {
            regexexpression = true;

            if (regexexpressionstring == null) {
                regexexpressionstring = pattern.replace("_", ".");
            } else {
                regexexpressionstring = regexexpressionstring.replace("_", ".");
            }
        }
        if (regexexpression) {
            return input.matches(regexexpressionstring);
        } else {
            //return input.contains(pattern);
            return input.equals(pattern);
        }
    }

    /**
     * Convert Bigquery field type to java.sql.Types
     *
     * @param columntype String of the Column type
     * @return java.sql.Types value of the given Columtype
     */
    public static int parseToSqlFieldType(String columntype) {
        if (columntype.equals("FLOAT")) {
            return java.sql.Types.FLOAT;
        } else if (columntype.equals("BOOLEAN")) {
            return java.sql.Types.BOOLEAN;
        } else if (columntype.equals("INTEGER")) {
            return java.sql.Types.INTEGER;
        } else if (columntype.equals("STRING")) {
            return java.sql.Types.VARCHAR;
        } else if (columntype.equals("BIGINT")) {
            return java.sql.Types.BIGINT;
        } else {
            return 0;
        }
    }

    /**
     * Reads Properties File from location
     *
     * @param filePath Location of the Properties File
     * @return Properties The Properties object made from the Properties File
     * @throws IOException if the load from file fails
     */
    public static Properties readFromPropFile(String filePath)
            throws IOException {
        // Read properties file.
        Properties properties = new Properties();
        properties.load(new FileInputStream(filePath));

        return properties;
    }

    /**
     * Run a query using the synchronous jobs.query() BigQuery endpoint.
     *
     * @param bigquery The BigQuery API wrapper
     * @param projectId
     * @param querySql The SQL to execute
     * @param dataSet default dataset, can be null
     * @param useLegacySql
     * @param maxBillingBytes Maximum bytes that the API will allow to bill
     * @param queryTimeoutMs The timeout at which point the API will return with an incomplete result
     *                         NOTE: this does _not_ mean the query fails, just we have to get the results async
     * @param maxResults The maximum number of rows to return with the synchronous response
     *                     Can be null for no max, but the API always has a 10MB limit
     *                     If more results exist, we need to fetch them in subsequent API requests.
     *
     * @return A [QueryResponse] with the results of the query, may be incomplete, may not have all rows.
     *
     * @throws IOException
     */
    static QueryResponse runSyncQuery(Bigquery bigquery, String projectId,
                                              String querySql, String dataSet, Boolean useLegacySql,
                                              Long maxBillingBytes, Long queryTimeoutMs, Long maxResults
    ) throws IOException {
        projectId = projectId.replace("__", ":").replace("_", ".");

        QueryRequest qr = new QueryRequest()
                .setTimeoutMs(queryTimeoutMs)
                .setQuery(querySql)
                .setUseLegacySql(useLegacySql)
                .setMaximumBytesBilled(maxBillingBytes);
        if (dataSet != null) {
            qr.setDefaultDataset(new DatasetReference().setDatasetId(dataSet).setProjectId(projectId));
        }
        if (maxResults != null) {
            qr.setMaxResults(maxResults);
        }

        return bigquery.jobs().query(querySql, qr)
                .setProjectId(projectId)
                .execute();
    }


    /**
     * Starts a new query in async mode.
     *
     * @param bigquery  The bigquery instance, which is authorized
     * @param projectId The project's ID
     * @param querySql  The sql query which we want to run
     * @return A JobReference which we'll use to poll the bigquery, for its
     * state, then for its mined data.
     * @throws IOException <p>
     *                     if the request for initializing or executing job fails
     *                     </p>
     */
    public static Job startQuery(Bigquery bigquery, String projectId,
                                 String querySql, String dataSet, Boolean useLegacySql,
                                 Long maxBillingBytes) throws IOException {
        projectId = projectId.replace("__", ":").replace("_", ".");
        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        queryConfig.setUseLegacySql(useLegacySql);
        queryConfig.setMaximumBytesBilled(maxBillingBytes);
        config.setQuery(queryConfig);
        String jobId = UUID.randomUUID().toString().replace("-", "");
        JobReference jobReference = new JobReference().setProjectId(projectId).setJobId(jobId);
        job.setJobReference(jobReference);

        if (dataSet != null)
            queryConfig.setDefaultDataset(new DatasetReference().setDatasetId(dataSet).setProjectId(projectId));

        job.setConfiguration(config);
        queryConfig.setQuery(querySql);

        Insert insert = bigquery.jobs().insert(querySql, job);
        insert.setProjectId(projectId);
        BQSupportFuncts.logger.info("Inserting Query Job (" + jobId + "): " + querySql.replace("\t", "").replace("\n", " ").replace("\r", ""));
        return insert.execute();
    }

}
