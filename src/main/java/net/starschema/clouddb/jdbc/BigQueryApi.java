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
 *  This class implements functions such as connecting to Bigquery, Checking out the results and displaying them on console
 *  </p>
 */

package net.starschema.clouddb.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.ProjectList.Projects;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableRow;

/**
 * This class contains static methods for interacting with BigQuery
 * 
 * @author Gunics Balázs, Horváth Attila
 * 
 */
public class BigQueryApi {
    /** log4j.Logger instance */
    static Logger logger = Logger.getLogger(BigQueryApi.class);

    /**
     * Displays results of a Query on the console
     * 
     * @param bigquery
     *            A valid authorized Bigquery instance
     * @param projectId
     *            The exact Id of the Project associated with the completed
     *            BigQuery job
     * @param completedJob
     *            The Job reference of the completed job
     * @throws IOException
     *             <p>
     *             If the request fails to get the QueryResult of the specified
     *             job in the given ProjectId
     *             </p>
     */
    public static void displayQueryResults(Bigquery bigquery, String projectId,
	    Job completedJob) throws IOException {
	GetQueryResultsResponse queryResult = bigquery
		.jobs()
		.getQueryResults(projectId,
			completedJob.getJobReference().getJobId()).execute();
	List<TableRow> rows = queryResult.getRows();
	System.out.print("\nQuery Results:\n------------\n");
	for (TableRow row : rows) {
	    for (TableRow.F field : row.getF())
		System.out.printf("%-20s", field.getV());
	    System.out.println();
	}
    }

    /**
     * Return a list of Projects which contains the String catalogname
     * 
     * @param catalogname
     *            The String which the id of the result Projects must contain
     * @param Connection
     *            A valid BQConnection instance
     * @return a list of Projects which contains the String catalogname
     * @throws IOException
     *             <p>
     *             if the initialization of requesting the list of all Projects
     *             (to be sorted from) fails
     *             </p>
     */
    private static List<Projects> getcatalogs(String catalogname,
	    BQConnection Connection) throws IOException {
	List<Projects> Projects = Connection.getBigquery().projects().list()
		.execute().getProjects();

	if (Projects != null && Projects.size() != 0) {
	    if (catalogname != null) {
		List<Projects> ProjectsSearch = new ArrayList<Projects>();
		for (Projects in : Projects)
		    if (in.getId().contains(catalogname))
			ProjectsSearch.add(in);
		if (ProjectsSearch.size() == 0)
		    return null;
		else
		    return ProjectsSearch;
	    } else
		return Projects;
	} else
	    return null;
    }

    /**
     * Parses a (instance of dataset).getId() and gives back the id only for the
     * dataset
     * 
     * @param getid
     *            (instance of dataset).getId()
     * @return the id only for the dataset
     */
    public static String getdatasetidfrom_datasetgetid(String getid) {
	String ret = getid.substring(getid.lastIndexOf(":") + 1);
	BigQueryApi.logger.debug(ret);
	System.out.println(ret);
	return ret;
    }

    /**
     * Parses a (instance of table).getid() and gives back the id only for the
     * dataset
     * 
     * @param getid
     *            (instance of table).getid()
     * @return the id only for the dataset
     */
    public static String getdatasetidfrom_tablegetid(String getid) {
	String ret = getid.substring(getid.lastIndexOf(":") + 1,
		getid.lastIndexOf("."));
	BigQueryApi.logger.debug(ret);
	System.out.println(ret);
	return ret;
    }

    /**
     * Returns a list of Datasets, which are associated with the Project which's
     * id is exactly ProjectId, and their name contains datasetname
     * 
     * @param datasetname
     *            The String the dataset's id must contain
     * @param ProjectId
     *            The Id of the Project the dataset is preferably contained in
     * @param Connection
     *            A valid BQConnection Instance
     * @return a list of Datasets, which are associated with the Project which's
     *         id is exactly ProjectId, and their name contains datasetname
     * @throws IOException
     *             <p>
     *             if the request to get Projects that match the given ProjectId
     *             fails
     *             </p>
     */
    private static List<Datasets> getdatasets(String datasetname,
	    String ProjectId, BQConnection Connection) throws IOException {
	List<Datasets> datasetcontainer = Connection.getBigquery().datasets()
		.list(ProjectId).execute().getDatasets();

	if (datasetcontainer != null && datasetcontainer.size() != 0) {
	    if (datasetname != null) {
		List<Datasets> datasetsSearch = new ArrayList<Datasets>();
		for (Datasets in : datasetcontainer)
		    if (in.getId().contains(datasetname))
			datasetsSearch.add(in);
		if (datasetsSearch.size() == 0)
		    return null;
		else
		    return datasetsSearch;
	    } else
		return datasetcontainer;
	} else
	    return null;
    }

    /**
     * Parses a (instance of dataset)/(instance of table).getid() and gives back
     * the id only for the Project
     * 
     * @param getid
     *            (instance of dataset)/(instance of table).getid()
     * @returnthe the id only for the Project
     */
    public static String getprojectidfrom_anygetid(String getid) {
	String ret = getid.substring(0, getid.indexOf(":"));
	BigQueryApi.logger.debug(ret);
	System.out.println(ret);
	return ret;
    }

    /**
     * Returns the result of a completed query
     * 
     * @param bigquery
     *            Instance of authorized Bigquery client
     * @param projectId
     *            The id of the Project the completed job was run in
     * @param completedJob
     *            The Job instance of the completed job
     * @return the result of a completed query specified by projectId and
     *         completedJob
     * @throws IOException
     *             <p>
     *             if the request to get QueryResults specified by the given
     *             ProjectId and Job id fails
     *             </p>
     */
    public static GetQueryResultsResponse GetQueryResults(Bigquery bigquery,
	    String projectId, Job completedJob) throws IOException {
	GetQueryResultsResponse queryResult = bigquery
		.jobs()
		.getQueryResults(projectId,
			completedJob.getJobReference().getJobId()).execute();
	return queryResult;
    }

    /**
     * Returns the status of a job
     * 
     * @param myjob
     *            Instance of Job
     * @param bigquery
     *            Instance of authorized Bigquery client
     * @param projectId
     *            The id of the Project the job is contained in
     * @return the status of the job
     * @throws IOException
     *             <p>
     *             if the request to get the job specified by myjob and
     *             projectId fails
     *             </p>
     */
    public static String getQueryState(Job myjob, Bigquery bigquery,
	    String projectId) throws IOException {
	Job pollJob = bigquery.jobs()
		.get(projectId, myjob.getJobReference().getJobId()).execute();
	BigQueryApi.logger.info("Job status: "
		+ pollJob.getStatus().getState()
		+ " ; "
		+ pollJob.getJobReference().getJobId()
		+ " ; "
		+ (System.currentTimeMillis() - pollJob.getStatistics()
			.getStartTime()));
	return pollJob.getStatus().getState();
    }

    /**
     * Parses a (instance of table).getid() and gives back the id only for the
     * table
     * 
     * @param getid
     *            (instance of table).getid()
     * @return the id only for the table
     */
    public static String gettableidfrom_tablegetid(String getid) {
	String ret = getid.substring(getid.lastIndexOf(".") + 1);
	BigQueryApi.logger.debug(ret);
	System.out.println(ret);
	return ret;
    }

    /**
     * Returns a list of Tables which's id contains Tablename and are exactly in
     * the given Project and Dataset
     * 
     * @param Tablename
     *            String that the tableid must contain
     * @param ProjectId
     *            The exact Id of the Project that the tables must be in
     * @param DatasetId
     *            The exact Id of the Dataset that the tables must be in
     * @param Connection
     *            Instance of a valid BQConnection
     * @return a list of Tables which's id contains Tablename and are exactly in
     *         the given Project and Dataset
     * @throws IOException
     *             <p>
     *             if the request to get all tables (to sort from) specified by
     *             ProjectId, DatasetId fails
     *             </p>
     */
    public static List<Tables> gettables(String Tablename, String ProjectId,
	    String DatasetId, BQConnection Connection) throws IOException {
	List<Tables> tables = Connection.getBigquery().tables()
		.list(ProjectId, DatasetId).execute().getTables();
	if (tables != null && tables.size() != 0) {
	    if (Tablename != null) {
		List<Tables> tablesSearch = new ArrayList<Tables>();
		for (Tables in : tables)
		    if (in.getId().contains(Tablename))
			tablesSearch.add(in);
		if (tablesSearch.size() == 0)
		    return null;
		else
		    return tablesSearch;
	    } else
		return tables;
	} else
	    return null;
    }

    /**
     * Gets Tables information from specific projects matching catalog, specific
     * tablenames and dataset ids
     * 
     * @param Connection
     *            Valid instance of BQConnection
     * @return List of Table
     * @throws IOException
     *             <p>
     *             if the initialization of requesting the list of all Projects
     *             (to be sorted from) fails<br>
     *             if the request to get Projects that match the given ProjectId
     *             fails<br>
     *             if the request to get all tables (to sort from) specified by
     *             ProjectId, DatasetId fails<br>
     *             if the request to get table information based on ProjectId
     *             DatasetId TableId fails
     *             <p>
     */
    public static List<Table> GetTables(BQConnection Connection,
	    String catalog, String schema, String tablename) throws IOException {
	List<Table> RET = new ArrayList<Table>();

	List<Projects> Projects = BigQueryApi.getcatalogs(catalog, Connection);

	if (Projects != null && Projects.size() != 0) {
	    for (Projects proj : Projects) {
		List<Datasets> datasetlist = null;
		datasetlist = BigQueryApi.getdatasets(schema, proj.getId(),
			Connection);
		if (datasetlist != null && datasetlist.size() != 0)
		    for (Datasets dataset : datasetlist) {
			List<Tables> tables = null;

			tables = BigQueryApi.gettables(tablename, proj.getId(),
				BigQueryApi
					.getdatasetidfrom_datasetgetid(dataset
						.getId()), Connection);

			if (tables != null && tables.size() != 0)
			    for (Tables table : tables) {
				Table tbl = Connection
					.getBigquery()
					.tables()
					.get(proj.getId(),
						BigQueryApi
							.getdatasetidfrom_datasetgetid(dataset
								.getId()),
						BigQueryApi
							.gettableidfrom_tablegetid(table
								.getId()))
					.execute();
				if (tbl != null)
				    RET.add(tbl);
			    }
		    }
	    }
	    if (RET.size() == 0)
		return null;
	    else
		return RET;
	} else
	    return null;
    }

    /**
     * Reads Properties File from location
     * 
     * @param FilePath
     *            Location of the Properties File
     * @return Properties The Properties object made from the Properties File
     * @throws IOException
     *             if the load from file fails
     */
    public static Properties ReadFromPropFile(String FilePath)
	    throws IOException {

	// Read properties file.
	Properties properties = new Properties();
	properties.load(new FileInputStream(FilePath));

	return properties;
    }

    /**
     * Constructs a valid BigQuery JDBC driver URL from the specified properties
     * File
     * 
     * @param properties
     *            Properties File most commonly read by
     *            ReadUrlFromPropFile(String)
     * @see #ReadUrlFromPropFile(String)
     * @return a valid BigQuery JDBC driver URL or null if it fails to load
     * @throws UnsupportedEncodingException
     */
    public static String ConstructUrlFromPropertiesFile(Properties properties)
	    throws UnsupportedEncodingException {
	String ProjectId = properties.getProperty("projectid");
	String User = properties.getProperty("user");
	String Password = properties.getProperty("password");

	if (properties.getProperty("type").equals("installed")) {
	    if (User != null && Password != null && ProjectId != null) {
		return BQDriver.getURLPrefix()
			+ URLEncoder.encode(ProjectId, "UTF-8");
	    } else
		return null;
	} else if (properties.getProperty("type").equals("service")) {
	    System.out.println(properties.getProperty("type"));
	    if (User != null && Password != null && ProjectId != null) {
		return BQDriver.getURLPrefix()
			+ URLEncoder.encode(ProjectId, "UTF-8")
			+ "?ServiceAccount=true";
	    } else
		return null;
	} else
	    return null;
    }

    /**
     * Convert Bigquery field type to java.sql.Types
     * 
     * @param Columntype
     *            String of the Column type
     * @return java.sql.Types value of the given Columtype
     */
    public static int ParseToSqlFieldType(String Columntype) {
	if (Columntype.equals("FLOAT"))
	    return java.sql.Types.FLOAT;
	else if (Columntype.equals("BOOLEAN"))
	    return java.sql.Types.BOOLEAN;
	else if (Columntype.equals("INTEGER"))
	    return java.sql.Types.INTEGER;
	else if (Columntype.equals("STRING"))
	    return java.sql.Types.VARCHAR;
	else
	    return 0;
    }

    /**
     * Starts a new query in async mode.
     * 
     * @param bigquery
     *            The bigquery instance, which is authorized
     * @param projectId
     *            The project's ID
     * @param querySql
     *            The sql query which we want to run
     * @return A JobReference which we'll use to poll the bigquery, for its
     *         state, then for its mined data.
     * @throws IOException
     *             <p>
     *             if the request for initializing or executing job fails
     *             </p>
     */
    public static Job startQuery(Bigquery bigquery, String projectId,
	    String querySql) throws IOException {
	BigQueryApi.logger.info("Inserting Query Job: " + querySql);

	Job job = new Job();
	JobConfiguration config = new JobConfiguration();
	JobConfigurationQuery queryConfig = new JobConfigurationQuery();
	config.setQuery(queryConfig);

	job.setConfiguration(config);
	queryConfig.setQuery(querySql);

	Insert insert = bigquery.jobs().insert(querySql, job);
	insert.setProjectId(projectId);
	Job myjob = insert.execute();

	return myjob;
    }
}
