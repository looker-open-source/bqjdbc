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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.ProjectList.Projects;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableRow;

public class BigQueryApi 
{
	static Logger logger = Logger.getLogger(BigQueryApi.class);

	//we'll use this to get the version of bigquery
	private static String ServicePath = null;
	public static String getServicePath() {
		return ServicePath;
	}
	
	/**
	 * Displays result of a Query on the console
	 * @param bigquery
	 * @param projectId
	 * @param completedJob
	 * @throws IOException
	 */
	public static void displayQueryResults(Bigquery bigquery,
            String projectId, Job completedJob) throws IOException {
			GetQueryResultsResponse queryResult = bigquery.jobs()
			.getQueryResults(
			projectId, completedJob
			.getJobReference()
			.getJobId()
			).execute();
			List<TableRow> rows = queryResult.getRows();
			System.out.print("\nQuery Results:\n------------\n");
			for (TableRow row : rows) {
			for (TableRow.F field : row.getF()) {
			System.out.printf("%-20s", field.getV());
			}
			System.out.println();
			}
	}
	/**
	 * Returns the result of a completed query
	 * @param bigquery
	 * @param projectId
	 * @param completedJob
	 * @return 
	 * @throws IOException
	 */
	public static GetQueryResultsResponse GetQueryResults(Bigquery bigquery,
            String projectId, Job completedJob) throws IOException {
			GetQueryResultsResponse queryResult = bigquery.jobs()
			.getQueryResults(
			projectId, completedJob
			.getJobReference()
			.getJobId()
			).execute();
			return queryResult;			
	}
	
	/**
	 * Starts a new query in async mode.
	 * @param bigquery The bigquery instance, which is authorized
	 * @param projectId The project's ID
	 * @param querySql The sql query which we want to run
	 * @return A JobReference  which we'll use to poll the bigquery, for its state, then for its mined data.
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static Job startQuery(Bigquery bigquery, String projectId,
            String querySql) throws IOException, InterruptedException 
    {
		logger.info("Inserting Query Job: " + querySql);
		
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);
		
		job.setConfiguration(config);
		queryConfig.setQuery(querySql);
		
		
		Insert insert = bigquery.jobs().insert(querySql, job);
		insert.setProjectId(projectId);
		Job myjob = insert.execute();
		
		//long startTime = System.currentTimeMillis();
	    //long elapsedTime;
	    return myjob;
	}
	
	public static String getQueryState(Job myjob,Bigquery bigquery, String projectId) throws IOException{
		Job pollJob = bigquery.jobs().get(projectId,  myjob.getJobReference().getJobId()).execute();
	    logger.info("Job status: " +pollJob.getStatus().getState() + " ; " + pollJob.getJobReference().getJobId() 
	    			+ " ; " + (System.currentTimeMillis()-pollJob.getStatistics().getStartTime()) );
	    return pollJob.getStatus().getState();
	}
	
	/**
	 * Convert Bigquery field type to java.sql.Types
	 * @param Columntype
	 * @return
	 */
	public static int ParseToSqlFieldType(String Columntype)
	{
		if(Columntype.equals("FLOAT"))
		{
			return java.sql.Types.FLOAT;
		}
		else if(Columntype.equals("BOOLEAN"))
		{
			return java.sql.Types.BOOLEAN;
		}
		else if(Columntype.equals("INTEGER"))
		{
			return java.sql.Types.INTEGER;
		}
		else if(Columntype.equals("STRING"))
		{
			return java.sql.Types.VARCHAR;
		}
		else {
			return 0;
		}
	}
	
	
	
	public static String getdatasetidfrom_tablegetid(String getid)
	{
		String ret = getid.substring(getid.lastIndexOf(":")+1,getid.lastIndexOf("."));
		logger.debug(ret);
		System.out.println(ret);
		return ret;
	}

	
	public static String gettableidfrom_tablegetid(String getid)
	{
		String ret = getid.substring(getid.lastIndexOf(".")+1);
		logger.debug(ret);
		System.out.println(ret);
		return ret;
	}
	
	public static String getdatasetidfrom_datasetgetid(String getid)
	{
		String ret = getid.substring(getid.lastIndexOf(":")+1);
		logger.debug(ret);
		System.out.println(ret);
		return ret;
	}
	
	public static String getprojectidfrom_anygetid(String getid)
	{
		String ret =  getid.substring(0,getid.indexOf(":"));
		logger.debug(ret);
		System.out.println(ret);
		return ret;
	}
	
	
	/**
	 * Gets Tables information from all projects
	 * @param Connection
	 * @return List<Table>
	 * @throws SQLException
	 * @throws IOException
	 */
	public static List<Table> GetTables(BQConnection Connection) throws SQLException, IOException
	{
		List<Table> RET = new ArrayList<Table>();
		
		List <Projects> Projects = Connection.getBigquery().projects().list().execute().getProjects();
		
		if(Projects != null && Projects.size()!=0)
		{
			for(Projects proj:Projects)
			{
				DatasetList datasetcontainer = null;
				try {
					datasetcontainer = Connection.getBigquery().datasets().list(proj.getId()).execute();
				} catch (IOException e) {
					throw new SQLException(e);
				}
				List<Datasets> datasetlist = datasetcontainer.getDatasets();
				
				if(datasetlist != null && datasetlist.size()!=0)
				{
					for(Datasets dataset:datasetlist)
					{
						List <Tables> tables = null;
						try {
							System.out.println(dataset.getId());
							tables = Connection.getBigquery().tables().list(proj.getId(), getdatasetidfrom_datasetgetid(dataset.getId())).execute().getTables();				
						} catch (IOException e) {
							throw new SQLException(e);
						}
						if(tables != null && tables.size()!=0)
						{
							for(Tables table:tables)
							{
								try {
									System.out.println(table.getId());
									Table tbl = Connection.getBigquery().tables().get(proj.getId(), getdatasetidfrom_datasetgetid(dataset.getId()), gettableidfrom_tablegetid(table.getId())).execute();
									if(tbl != null)
									RET.add(tbl);
								} catch (IOException e) {
									throw new SQLException(e);
								}
							}
						}
					}		
				}
			}
			if (RET.size() == 0) return null;
			else
			return RET;
		}
		else
			return null;
	}
	
	/**
	 * Gets Tables information from specific projects matching catalog, specific tablenames and dataset ids
	 * @param Connection
	 * @return List<Table>
	 * @throws SQLException
	 * @throws IOException
	 */
	public static List<Table> GetTables(BQConnection Connection, String catalog, String schema, String tablename) throws SQLException, IOException
	{
		List<Table> RET = new ArrayList<Table>();
		
		List <Projects> Projects = getcatalogs(catalog, Connection);
		
		if(Projects != null && Projects.size()!=0)
		{
			for(Projects proj:Projects)
			{
				//ADDED this line to counter false List method not listing public data (for testing only)
					//proj.setId("publicdata");
					List<Datasets> datasetlist = null;
					try {
						datasetlist = getdatasets(schema, proj.getId(), Connection);
					} catch (IOException e) {
						throw new SQLException(e);
					}
					//List<Datasets> datasetlist = datasetcontainer.getDatasets();
					
					if(datasetlist != null && datasetlist.size()!=0)
					{
						for(Datasets dataset:datasetlist)
						{
							List <Tables> tables = null;
							try {
								tables = gettables(tablename, proj.getId(), getdatasetidfrom_datasetgetid(dataset.getId()), Connection); 				
							} catch (IOException e) {
								throw new SQLException(e);
							}
							if(tables != null && tables.size()!=0)
							{
								for(Tables table:tables)
								{
									try {
										Table tbl = Connection.getBigquery().tables().get(proj.getId(), getdatasetidfrom_datasetgetid(dataset.getId()), gettableidfrom_tablegetid(table.getId())).execute();
										if(tbl != null)
										RET.add(tbl);
									} catch (IOException e) {
										throw new SQLException(e);
									}
								}
							}
						}
					}		
			}
			if (RET.size() == 0) return null;
			else
			return RET;
		}
		else
			return null;
	}
	
	private static List <Projects> getcatalogs(String catalogname, BQConnection Connection) throws IOException
	{
		List <Projects> Projects = Connection.getBigquery().projects().list().execute().getProjects();
		
		if(Projects != null && Projects.size()!=0)
		{
			if(catalogname != null){
				List <Projects> ProjectsSearch = new ArrayList<Projects>();
				for(Projects in:Projects)
				{
					if(in.getId().contains(catalogname))
						ProjectsSearch.add(in);
				}
				if(ProjectsSearch.size()==0)
					return null;
				else
					return ProjectsSearch;
			}
			else
			{
				return Projects;
			}
		}
		else 
			return null;
	}
	
	public static List <Tables> gettables (String Tablename, String ProjectId, String DatasetId, BQConnection Connection) throws IOException
	{
		List <Tables> tables = Connection.getBigquery().tables().list(ProjectId, DatasetId).execute().getTables();
		if(tables != null && tables.size()!=0)
		{
			if(Tablename != null){
				List <Tables> tablesSearch = new ArrayList<Tables>();
				for(Tables in:tables)
				{
					if(in.getId().contains(Tablename))
						tablesSearch.add(in);
				}
				if(tablesSearch.size()==0)
					return null;
				else
					return tablesSearch;
			}
			else
			{
				return tables;
			}
		}
		else 
			return null;
	}
	
	
	private static List <Datasets> getdatasets(String datasetname, String ProjectId, BQConnection Connection) throws IOException
	{
		List <Datasets> datasetcontainer = Connection.getBigquery().datasets().list(ProjectId).execute().getDatasets();
		
		if(datasetcontainer != null && datasetcontainer.size()!=0)
		{
			if(datasetname != null){
				List <Datasets> datasetsSearch = new ArrayList<Datasets>();
				for(Datasets in:datasetcontainer)
				{
					if(in.getId().contains(datasetname))
						datasetsSearch.add(in);
				}
				if(datasetsSearch.size()==0)
					return null;
				else
					return datasetsSearch;
			}
			else
			{
				return datasetcontainer;
			}
		}
		else 
			return null;
	}
}
