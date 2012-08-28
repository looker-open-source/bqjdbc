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
 *  
 * This class implements the java.sql.DatabaseMetaData interface
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.ProjectList.Projects;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;


class BQDatabaseMetadata implements DatabaseMetaData
{

	BQConnection Connection;
	
	public BQDatabaseMetadata(BQConnection bqConnection) {
		this.Connection = bqConnection; 
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		try {
			 List<Projects> Projects = this.Connection.getBigquery().projects().list().execute().getProjects();
			 
			 
			 if(Projects != null && Projects.size()!=0)
			 {
			 String[] Data = new String[Projects.size()];
			 
			 for(int i=0;i<Projects.size();i++)
			 {
				 Data[i] = Projects.get(i).getId();
			 }
			 
			 return new DMDResultSet(Data,new String[]{"TABLE_CAT"});
			 }
			 else
				 return new DMDResultSet(new String[]{null},new String[]{"TABLE_CAT"});
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		//TODO JUNIT TEST!!!
		List<Table> Tables = null;
		try {
			Tables = BigQueryApi.GetTables(Connection,catalog,schemaPattern,tableNamePattern);
		} catch (IOException e) {
			throw new SQLException(e);
		}
		
		if(Tables != null)
		{
			List<String[]> data = new ArrayList<String[]>();
			for(int i=0;i<Tables.size();i++)
			{
				String UparsedId = Tables.get(i).getId();
				String ProjectId = BigQueryApi.getprojectidfrom_anygetid(UparsedId);
				String DatasetId = BigQueryApi.getdatasetidfrom_tablegetid(UparsedId);
				String TableId = BigQueryApi.gettableidfrom_tablegetid(UparsedId);
				
				List<TableFieldSchema> tblfldschemas = Tables.get(i).getSchema().getFields();
				if(tblfldschemas!= null && tblfldschemas.size()!=0)
				{
					int index = 1;
					for(TableFieldSchema Column:tblfldschemas)
					{
						if(columnNamePattern == null || Column.getName().contains(columnNamePattern))
						{
						String[] Col = new String[23];
						//TABLE_CAT String => table catalog (may be null)
						Col[0] = ProjectId;
						//TABLE_SCHEM String => table schema (may be null)
						Col[1] = DatasetId;
						//TABLE_NAME String => table name
						Col[2] = TableId;
						//COLUMN_NAME String => column name
						Col[3] = Column.getName();
						//DATA_TYPE int => SQL type from java.sql.Types
						Col[4] =  String.valueOf(BigQueryApi.ParseToSqlFieldType(Column.getType()));
						//TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
						Col[5] =  Column.getType();
						//COLUMN_SIZE int => column size. (In Bigquery max colsize is 64kb and its not specifically determined for fields) 
						Col[6] =  String.valueOf(Column.size());
						//BUFFER_LENGTH is not used.
						Col[7] =  null;
						//DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
						Col[8] =  null;
						//NUM_PREC_RADIX int => Radix (typically either 10 or 2)
						Col[9] =  null;
						//NULLABLE int => is NULL allowed.
						Col[10] = Column.getMode();
						//REMARKS String => comment describing column (may be null) 
						Col[11] = null; 
						//COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
						Col[12] = "String";
						//SQL_DATA_TYPE int => unused
						Col[13] = Column.getType();
						//SQL_DATETIME_SUB int => unused
						Col[14] = null;
						//CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
						Col[15] = String.valueOf(64*1024);
						//ORDINAL_POSITION int => index of column in table (starting at 1) 
						Col[16] = String.valueOf(index);
						//IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
						Col[17] = "";
						//SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
						Col[18] = null;
						//SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
						Col[19] = null;
						//SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)
						Col[20] = null;
						//SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
						Col[21] = null;
						//IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
						Col[22] = "";
						data.add(Col);
						}
						index++;
					}
				}
			}
			if(data.size()== 0)return null;
			else
			{
				String[][] List = new String[data.size()][23];
				for(int i=0;i<data.size();i++)
				{
					List[i] = data.get(i);
				}
				return new DMDResultSet(List, new String[]{"TABLE_CAT",
						"TABLE_SCHEM",
						"TABLE_NAME",
						"COLUMN_NAME",
						"DATA_TYPE",
						"TYPE_NAME",
						"COLUMN_SIZE",
						"BUFFER_LENGTH",
						"DECIMAL_DIGITS",
						"NUM_PREC_RADIX",
						"NULLABLE",
						"REMARKS",
						"COLUMN_DEF",
						"SQL_DATA_TYPE",
						"SQL_DATETIME_SUB",
						"CHAR_OCTET_LENGTH",
						"ORDINAL_POSITION",
						"IS_NULLABLE",
						"SCOPE_CATLOG",
						"SCOPE_SCHEMA",
						"SCOPE_TABLE",
						"SOURCE_DATA_TYPE",
						"IS_AUTOINCREMENT",});
			}
		}
		else
		return new DMDResultSet(new String[][]{{null,null}},new String[]{"TABLE_CAT",
			"TABLE_SCHEM",
			"TABLE_NAME",
			"COLUMN_NAME",
			"DATA_TYPE",
			"TYPE_NAME",
			"COLUMN_SIZE",
			"BUFFER_LENGTH",
			"DECIMAL_DIGITS",
			"NUM_PREC_RADIX",
			"NULLABLE",
			"REMARKS",
			"COLUMN_DEF",
			"SQL_DATA_TYPE",
			"SQL_DATETIME_SUB",
			"CHAR_OCTET_LENGTH",
			"ORDINAL_POSITION",
			"IS_NULLABLE",
			"SCOPE_CATLOG",
			"SCOPE_SCHEMA",
			"SCOPE_TABLE",
			"SOURCE_DATA_TYPE",
			"IS_AUTOINCREMENT",});
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.Connection;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		BigQueryApi.getServicePath();
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "Big Query";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return "google bigquery v2-1.3.3-beta";
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getDriverMajorVersion() {
		return BQDriver.getMajorVersionAsStatic();
	}

	@Override
	public int getDriverMinorVersion() {
		return BQDriver.getMinorVersionAsStatic();
	}

	@Override
	public String getDriverName() throws SQLException {
		return "Starschema.net:BigQuery JDBC driver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return BQDriver.getMajorVersionAsStatic()+"."+BQDriver.getMinorVersionAsStatic();
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "( ) : . , \\ \" / ' ' * < = > + - % & | ^ << >> ~ != <> >= <= ";
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\\\"";
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return BQDriver.getMajorVersionAsStatic();
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return BQDriver.getMinorVersionAsStatic();
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//Couldn't find a way to acces it from the API :(
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 64*1024;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxStatements() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return
				"{ fn  ABS() },"+
				"{ fn  ACOS() },"+
				"{ fn  ACOSH() },"+
				"{ fn  ASIN() },"+
				"{ fn  ASINH() },"+
				"{ fn  ATAN() },"+
				"{ fn  ATANH() },"+
				"{ fn  ATAN2() },"+
				"{ fn  CEIL() },"+
				"{ fn  COS() },"+
				"{ fn  COSH() },"+
				"{ fn  COT() },"+
				"{ fn  DEGREES() },"+
				"{ fn  FLOOR() },"+
				"{ fn  LN() },"+
				"{ fn  LOG() },"+
				"{ fn  LOG2() },"+
				"{ fn  LOG10() },"+
				"{ fn  PI() },"+
				"{ fn  POW() },"+
				"{ fn  RADIANS() },"+
				"{ fn  ROUND() },"+
				"{ fn  SIN() },"+
				"{ fn  SINH() },"+
				"{ fn  SQRT() },"+
				"{ fn  TAN() },"+
				"{ fn  TANH() }";
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO Hopefully we won't need it, anyways:
		//https://developers.google.com/bigquery/docs/reference/v2/
		//https://developers.google.com/bigquery/docs/query-reference
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "PI,POW,RADIANS,ROUND,SIN,SINH,SQRT,TAN,TANH,BOOLEAN," +
			  	"HEX_STRING,STRING,IFNULL,IS_INF,IS_NAN,IS_EXPLICITLY_DEFINED," +
			  	"FORMAT_IP,PARSE_IP,FORMAT_PACKED_IP,PARSE_PACKED_IP," +
			  	"REGEXP_MATCH,REGEXP_EXTRACT,REGEXP_REPLACE,CONCAT," +
			  	"LENGTH,LOWER,LPAD,RIGHT,RPAD,SUBSTR,UPPER," +
			  	"FORMAT_UTC_USEC,NOW,PARSE_UTC_USEC,STRFTIME_UTC_USEC," +
			  	"UTC_USEC_TO_DAY,UTC_USEC_TO_HOUR,UTC_USEC_TO_MONTH," +
			  	"UTC_USEC_TO_WEEK,UTC_USEC_TO_YEAR,POSITION";
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateSQL;
		//TODO I'm might wrong, but bigquery using his own SQL language.
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema"; 
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		List<Table> tables;
		try {
			tables = BigQueryApi.GetTables(this.Connection);
		} catch (IOException e) {
			throw new SQLException(e);
		}
		
		if(tables != null && tables.size() != 0)
		{
			String[][] data = new String[tables.size()][2];
			
			int i =0;
			for(Table tableinstance:tables)
			{
				String UparsedId = tableinstance.getId();
				String ProjectId = BigQueryApi.getprojectidfrom_anygetid(UparsedId);
				String DatasetId = BigQueryApi.getdatasetidfrom_tablegetid(UparsedId);
				//String TableId = BigQueryApi.gettableidfrom_tablegetid(UparsedId);
				
				data[i][0] = DatasetId;
				data[i][1] = ProjectId;	
				i++;
			}
			return new DMDResultSet(data, new String[]{"TABLE_SCHEM","TABLE_CATALOG"});
		}
		else
			return new DMDResultSet(new String[][]{{null,null}}, new String[]{"TABLE_SCHEM","TABLE_CATALOG"});
		
		
		
		
		// TODO Schemas has no ID or Name in Bigquery!!!!
		//This method returns Datasetid as TableCatalog and TableSchema.ToString() as Schema name 
		/*
		List<Datasets> datasetlist = null;
		try {
			datasetlist = this.Connection.getBigquery().datasets().list(this.Connection.getprojectid()).execute().getDatasets();
		} catch (IOException e) {
			throw new SQLException(e);
		}
		
		
		//List<String>
		for(Datasets data:datasetlist)
		{
			BigQueryApi.GetTables(this.Connection, data.getId()).get(1).getSchema();
		}
		
		return null;*/
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return
				"{ fn CONCAT() }," +
				"{ fn LEFT() }," +
				"{ fn LENGTH() }," +
				"{ fn LOWER() }," +
				"{ fn LPAD() }," +
				"{ fn RIGHT() }," +
				"{ fn RPAD() }," +
				"{ fn SUBSTR() }," +
				"{ fn UPPER( },";
		//expr CONTAINS 'str'
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO we might need it
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		List<String> Res = new ArrayList<String>();
		Res.add("TABLE");
		
		return new DMDResultSet(Res.toArray(),new String[]{"TABLE_TYPE"});
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		List<Table> tables = null;
		try {
			tables = BigQueryApi.GetTables(this.Connection,catalog,schemaPattern,tableNamePattern);
		} catch (IOException e) {
			throw new SQLException(e);
		}
		if(tables != null && tables.size()!= 0)
		{
			String[][] data = new String[tables.size()][10];
			for(int i=0;i<tables.size();i++)
			{
				
				/*
				 * TABLE_CAT String => table catalog (may be null) 
				 * TABLE_SCHEM String => table schema (may be null) 
				 * TABLE_NAME String => table name 
				 * TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM". 
				 * REMARKS String => explanatory comment on the table 
				 * TYPE_CAT String => the types catalog (may be null) 
				 * TYPE_SCHEM String => the types schema (may be null) 
				 * TYPE_NAME String => type name (may be null) 
				 * SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null) 
				 * REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null) 
				 */
				String UparsedId = tables.get(i).getId();
				String ProjectId = BigQueryApi.getprojectidfrom_anygetid(UparsedId);
				String DatasetId = BigQueryApi.getdatasetidfrom_tablegetid(UparsedId);
				String TableId = BigQueryApi.gettableidfrom_tablegetid(UparsedId);
				
				data[i][0] = ProjectId;
				data[i][1] = DatasetId;
				data[i][2] = TableId;
				data[i][3] = "TABLE";
				data[i][4] = tables.get(i).getDescription();
				data[i][5] = null;
				data[i][6] = null;
				data[i][7] = null;
				data[i][8] = null;
				data[i][9] = null;
			}
			return new DMDResultSet(data, new String[]{"TABLE_CAT",
					"TABLE_SCHEM",
					"TABLE_NAME",
					"TABLE_TYPE",
					"REMARKS",
					"TYPE_CAT",
					"TYPE_SCHEM",
					"TYPE_NAME",
					"SELF_REFERENCING_COL_NAME",
					"REF_GENERATION"});
		}
		else
			return new DMDResultSet(new String[][]{{null,null}}, new String[]{"TABLE_CAT",
					"TABLE_SCHEM",
					"TABLE_NAME",
					"TABLE_TYPE",
					"REMARKS",
					"TYPE_CAT",
					"TYPE_SCHEM",
					"TYPE_NAME",
					"SELF_REFERENCING_COL_NAME",
					"REF_GENERATION"});
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return 
				"{ fn FORMAT_UTC_USEC() },"+
				"{ fn NOW() },"+
				"{ fn PARSE_UTC_USEC() },"+
				"{ fn STRFTIME_UTC_USEC() },"+
				"{ fn UTC_USEC_TO_DAY() },"+
				"{ fn UTC_USEC_TO_HOUR() },"+
				"{ fn UTC_USEC_TO_MONTH() },"+
				"{ fn UTC_USEC_TO_WEEK() },"+
				"{ fn UTC_USEC_TO_YEAR() }";	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO we might need it
	}

	@Override
	public String getURL() throws SQLException {
		return null;
	}

	@Override
	public String getUserName() throws SQLException {
		String serverdata = this.Connection.getURLPART();
		try {
			return URLDecoder.decode(serverdata.substring(serverdata.lastIndexOf(":")+1),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO we might need it.
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		throw new SQLFeatureNotSupportedException(); //TODO
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return true;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true; //TODO TEST IT!!!!
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
		
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
		//I'm not 100% sure about it.
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		//TODO !! 
		// http://publib.boulder.ibm.com/infocenter/iseries/v5r3/index.jsp?topic=%2Fsqlp%2Frbafyexsub1.htm
		
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
		
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
		
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return false; //TODO since its read only!		
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false; //In the current version.
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return true;		
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;  //Bigquery doesn't support stored jobs.
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;	
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;	//Tested		
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return true;		
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return false; //Only read only functions atm.
		
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		//TODO hopika!
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
		
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false; //only static supported after IN
			/*expr IN(expr1, expr2, ...)	 Returns true if expr matches expr1, expr2, or any value in the parentheses.
			 *  The IN keyword is an efficient shorthand for (expr = expr1 || expr = expr2 || ...). 
			 *  The expressions used with the IN keyword must be constants and they must match the data type of expr.
			 */
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		if(java.sql.Connection.TRANSACTION_NONE == level) return true;
		else return false;
/*		Looks better this way. 		
		if(java.sql.Connection.TRANSACTION_READ_COMMITTED == level) return false;
		if(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED == level) return false;		
		if(java.sql.Connection.TRANSACTION_REPEATABLE_READ == level) return false;
		if(java.sql.Connection.TRANSACTION_SERIALIZABLE == level) return false;
		return false;*/
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;		
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return true;		
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;		
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;		
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;		
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		throw new SQLException("this feature is not yet impelemented.");
		//TODO megírni
		
	}
}


