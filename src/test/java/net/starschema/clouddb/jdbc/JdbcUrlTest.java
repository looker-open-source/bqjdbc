package net.starschema.clouddb.jdbc;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.bigquery.Bigquery.Jobs.Query;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection.JobCreationMode;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

/** Created by steven on 10/21/15. */
public class JdbcUrlTest {

  private BQConnection bq;
  private String URL;
  private Properties properties;
  private final String defaultServiceAccount =
      "src/test/resources/bigquery_credentials_protected.json";

  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void setup() throws SQLException, IOException {
    properties = getProperties("/installedaccount.properties");
    URL = getUrl("/installedaccount.properties", null) + "&useLegacySql=true";

    this.bq = new BQConnection(URL, new Properties());
    this.environmentVariables.set("GOOGLE_APPLICATION_CREDENTIALS", defaultServiceAccount);
  }

  @Test
  public void constructorForBQConnectionWorksWithoutProjectIdAndWithoutDataset()
      throws SQLException {
    // This test demonstrates that the constructor for BQConnection allows for an empty project id
    // (see regex for pathParamsMatcher).
    String url =
        "jdbc:BQDriver:?"
            + "withServiceAccount=true"
            + "&user=697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com"
            + "&password=src/test/resources/bigquery_credentials.p12"
            + "&useLegacySql=true";

    BQConnection connection = new BQConnection(url, new Properties());

    Assert.assertEquals("", connection.getProjectId());
  }

  @Test
  public void constructorForBQConnectionWorksWithEmptyDatasetId() throws SQLException {
    // This test demonstrates that the constructor for BQConnection allows for an empty default
    // dataset id (see regex for projectAndDatasetMatcher).
    String url =
        "jdbc:BQDriver:disco-parsec-659/?"
            + "withServiceAccount=true"
            + "&user=697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com"
            + "&password=src/test/resources/bigquery_credentials.p12"
            + "&useLegacySql=true";

    BQConnection connection = new BQConnection(url, new Properties());

    Assert.assertEquals("disco-parsec-659", connection.getProjectId());
    Assert.assertEquals("disco-parsec-659", connection.getDataSetProjectId());
    Assert.assertEquals("", connection.getDataSet());
  }

  @Test
  public void parseDatasetRefShouldWorkWithFullyQualifiedDataset() {
    DatasetReference datasetRef1 = BQConnection.parseDatasetRef("project1.dataset1");
    DatasetReference datasetRef2 = BQConnection.parseDatasetRef("project2:dataset2");
    DatasetReference datasetRef3 = BQConnection.parseDatasetRef("domain.com:project3:dataset3");
    DatasetReference datasetRef4 = BQConnection.parseDatasetRef("domain.com:project4.dataset4");
    DatasetReference datasetRef5 = BQConnection.parseDatasetRef("project5.dataset-5");
    DatasetReference datasetRef6 = BQConnection.parseDatasetRef("project6:dataset-6");

    Assert.assertEquals(
        new DatasetReference().setProjectId("project1").setDatasetId("dataset1"), datasetRef1);
    Assert.assertEquals(
        new DatasetReference().setProjectId("project2").setDatasetId("dataset2"), datasetRef2);
    Assert.assertEquals(
        new DatasetReference().setProjectId("domain.com:project3").setDatasetId("dataset3"),
        datasetRef3);
    Assert.assertEquals(
        new DatasetReference().setProjectId("domain.com:project4").setDatasetId("dataset4"),
        datasetRef4);
    Assert.assertEquals(
        new DatasetReference().setProjectId("project5").setDatasetId("dataset-5"), datasetRef5);
    Assert.assertEquals(
        new DatasetReference().setProjectId("project6").setDatasetId("dataset-6"), datasetRef6);
  }

  @Test
  public void parseDatasetRefShouldWorkWithoutDatasetId() {
    DatasetReference datasetRef1 = BQConnection.parseDatasetRef("project1.");
    DatasetReference datasetRef2 = BQConnection.parseDatasetRef("project2:");
    DatasetReference datasetRef3 = BQConnection.parseDatasetRef(":");
    DatasetReference datasetRef4 = BQConnection.parseDatasetRef(".");
    DatasetReference datasetRef5 = BQConnection.parseDatasetRef("");
    DatasetReference datasetRef6 = BQConnection.parseDatasetRef(null);

    Assert.assertEquals(
        new DatasetReference().setProjectId("project1").setDatasetId(""), datasetRef1);
    Assert.assertEquals(
        new DatasetReference().setProjectId("project2").setDatasetId(""), datasetRef2);
    Assert.assertEquals(new DatasetReference().setProjectId("").setDatasetId(""), datasetRef3);
    Assert.assertEquals(new DatasetReference().setProjectId("").setDatasetId(""), datasetRef4);
    Assert.assertEquals(new DatasetReference().setProjectId(null).setDatasetId(""), datasetRef5);
    Assert.assertEquals(new DatasetReference().setProjectId(null).setDatasetId(null), datasetRef6);
  }

  @Test
  public void parseDatasetRefShouldWorkWithoutDatasetProjectId() {
    DatasetReference datasetRef1 = BQConnection.parseDatasetRef("dataset1");
    DatasetReference datasetRef2 = BQConnection.parseDatasetRef(".dataset2");
    DatasetReference datasetRef3 = BQConnection.parseDatasetRef(":dataset3");

    Assert.assertEquals(
        new DatasetReference().setProjectId(null).setDatasetId("dataset1"), datasetRef1);
    Assert.assertEquals(
        new DatasetReference().setProjectId("").setDatasetId("dataset2"), datasetRef2);
    Assert.assertEquals(
        new DatasetReference().setProjectId("").setDatasetId("dataset3"), datasetRef3);
  }

  @Test
  public void constructUrlFromPropertiesFileShouldWorkWhenProjectIdAndDatasetProjectIdAreSet()
      throws IOException, SQLException {
    properties.put("projectid", "disco-parsec-659");
    properties.put("dataset", "publicdata.samples");
    File tempFile = File.createTempFile("tmp_installedaccount", ".properties");
    tempFile.deleteOnExit();
    try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
      properties.store(
          outputStream,
          "JdbcUrlTest#constructUrlFromPropertiesFileShouldWorkWhenProjectIdAndDatasetProjectIdAreSet");
    }
    Properties customProperties = BQSupportFuncts.readFromPropFile(tempFile.getAbsolutePath());
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(customProperties, true, null);

    BQConnection connection = new BQConnection(url, new Properties());

    Assert.assertEquals("disco-parsec-659", connection.getProjectId());
    Assert.assertEquals("publicdata", connection.getDataSetProjectId());
    Assert.assertEquals("samples", connection.getDataSet());
  }

  @Test
  public void constructUrlFromPropertiesFileShouldWorkWhenOnlyProjectIdSet()
      throws IOException, SQLException {
    properties.put("projectid", "disco-parsec-659");
    properties.put("dataset", "looker_test");
    File tempFile = File.createTempFile("tmp_installedaccount", ".properties");
    tempFile.deleteOnExit();
    try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
      properties.store(
          outputStream, "JdbcUrlTest#constructUrlFromPropertiesFileShouldWorkWhenOnlyProjectIdSet");
    }
    Properties customProperties = BQSupportFuncts.readFromPropFile(tempFile.getAbsolutePath());
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(customProperties, true, null);

    BQConnection connection = new BQConnection(url, new Properties());

    Assert.assertEquals("disco-parsec-659", connection.getProjectId());
    Assert.assertEquals("disco-parsec-659", connection.getDataSetProjectId());
    Assert.assertEquals("looker_test", connection.getDataSet());
  }

  @Test
  public void urlWithDefaultDatasetShouldWork() throws SQLException {
    Assert.assertEquals(properties.getProperty("dataset"), bq.getDataSet());
  }

  @Test
  public void urlWithoutDatasetProjectIdShouldWorkAndGetDataSetProjectIdShouldReturnProjectId() {
    Assert.assertEquals("disco-parsec-659", bq.getDataSetProjectId());
  }

  @Test
  public void urlWithoutDatasetShouldWorkAndGetDataSetProjectIdShouldReturnProjectId() {
    String urlWithoutDataset = URL.replace("/" + properties.getProperty("dataset"), "");

    try {
      BQConnection bqWithoutDataset = new BQConnection(urlWithoutDataset, new Properties());

      Assert.assertEquals("disco-parsec-659", bqWithoutDataset.getDataSetProjectId());
      Assert.assertEquals(null, bqWithoutDataset.getDataSet());
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void urlWithDatasetProjectIdShouldWorkAndBeReturnedByGetDataSetProjectId() {
    String urlWithDatasetProjectId =
        URL.replace(
            "/" + properties.getProperty("dataset"),
            "/looker-test-db." + properties.getProperty("dataset"));

    try {
      BQConnection bqWithDatasetProjectId =
          new BQConnection(urlWithDatasetProjectId, new Properties());

      Assert.assertEquals("disco-parsec-659", bqWithDatasetProjectId.getProjectId());
      Assert.assertEquals("looker-test-db", bqWithDatasetProjectId.getDataSetProjectId());
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void projectWithColons() throws SQLException {
    String urlWithColonContainingProject = URL.replace(bq.getProjectId(), "example.com:project");
    try {
      BQConnection bqWithColons = new BQConnection(urlWithColonContainingProject, new Properties());
      Assert.assertEquals("example.com:project", bqWithColons.getProjectId());
      Assert.assertEquals("example.com:project", bqWithColons.getCatalog());
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void mungedProjectName() throws SQLException {
    String urlWithUnderscoreContainingProject =
        URL.replace(bq.getProjectId(), "example_com__project");
    try {
      BQConnection bqWithUnderscores =
          new BQConnection(urlWithUnderscoreContainingProject, new Properties());
      Assert.assertEquals("example.com:project", bqWithUnderscores.getProjectId());
      Assert.assertEquals("example.com:project", bqWithUnderscores.getCatalog());
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void mungedDatasetProjectName() throws SQLException {
    String urlWithDatasetProjectIdContainingUnderscores =
        URL.replace(
            "/" + properties.getProperty("dataset"),
            "/example_com__project." + properties.getProperty("dataset"));

    try {
      BQConnection bqWithUnderscores =
          new BQConnection(urlWithDatasetProjectIdContainingUnderscores, new Properties());

      Assert.assertEquals("example.com:project", bqWithUnderscores.getDataSetProjectId());
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void setSchemaWorksWhenDatasetProjectIdIsUnspecified() throws SQLException {
    this.bq.setSchema("tokyo_star");

    Assert.assertEquals("tokyo_star", this.bq.getDataSet());
    Assert.assertEquals("disco-parsec-659", this.bq.getDataSetProjectId());
  }

  @Test
  public void setSchemaWorksWhenDatasetProjectIdIsSpecified() {
    this.bq.setSchema("publicdata.samples");

    Assert.assertEquals("samples", this.bq.getDataSet());
    Assert.assertEquals("publicdata", this.bq.getDataSetProjectId());
  }

  @Test
  public void setSchemaWorksWhenInputIsEmpty() {
    this.bq.setSchema("");

    Assert.assertEquals("", this.bq.getDataSet());
    Assert.assertEquals("disco-parsec-659", this.bq.getDataSetProjectId());
  }

  @Test
  public void setSchemaWorksWhenInputIsNull() {
    this.bq.setSchema(null);

    Assert.assertEquals(null, this.bq.getDataSet());
    Assert.assertEquals("disco-parsec-659", this.bq.getDataSetProjectId());
  }

  @Test
  public void urlWithTimeouts() throws SQLException {
    try {
      String url1 = URL + "&readTimeout=foo";
      new BQConnection(url1, new Properties());
    } catch (SQLException e) {
      Assert.assertEquals(
          "could not parse readTimeout parameter. - java.lang.NumberFormatException: For input"
              + " string: \"foo\"",
          e.getMessage());
    }

    try {
      String url1 = URL + "&connectTimeout=foo";
      new BQConnection(url1, new Properties());
    } catch (SQLException e) {
      Assert.assertEquals(
          "could not parse connectTimeout parameter. - java.lang.NumberFormatException: For input"
              + " string: \"foo\"",
          e.getMessage());
    }

    try {
      String url1 = URL + "&readTimeout=-1000";
      new BQConnection(url1, new Properties());
    } catch (SQLException e) {
      Assert.assertEquals("readTimeout must be positive.", e.getMessage());
    }

    try {
      String url1 = URL + "&connectTimeout=-1000";
      new BQConnection(url1, new Properties());
    } catch (SQLException e) {
      Assert.assertEquals("connectTimeout must be positive.", e.getMessage());
    }
  }

  @Test
  public void canRunQueryWithDefaultDataset() throws SQLException {
    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bq);

    // This should not blow up with a "No dataset specified" exception
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void canRunQueryOnStatementWithEmptyDatasetProjectId()
      throws SQLException, UnsupportedEncodingException {
    properties.put("dataset", ".looker_test");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    Statement stmt = bqConnection.createStatement();

    // Specifying an empty dataset project id results in the billing project id being used instead.
    Assert.assertEquals("", bqConnection.getDataSetProjectId());
    Assert.assertEquals("looker_test", bqConnection.getDataSet());
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void canRunQueryOnPreparedStatementWithEmptyDatasetProjectId()
      throws SQLException, UnsupportedEncodingException {
    properties.put("dataset", ".looker_test");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    PreparedStatement stmt = bqConnection.prepareStatement("SELECT * FROM orders limit 1");

    // Specifying an empty dataset project id results in the billing project id being used instead.
    Assert.assertEquals("", bqConnection.getDataSetProjectId());
    Assert.assertEquals("looker_test", bqConnection.getDataSet());
    stmt.executeQuery();
  }

  @Test
  public void canNotRunQueryOnStatementWithoutDatasetProjectId() throws SQLException, IOException {
    // The query should fail since the billing project 'disco-parsec-659' does not have the dataset
    // 'samples'.
    properties.put("dataset", "samples");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    Statement stmt = bqConnection.createStatement();

    Assert.assertEquals("disco-parsec-659", bqConnection.getDataSetProjectId());
    Assert.assertEquals("samples", bqConnection.getDataSet());
    try {
      stmt.executeQuery("SELECT * FROM shakespeare LIMIT 1");
      Assert.fail("Expected SQLException to be thrown without default dataset project id");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("disco-parsec-659:samples was not found in"));
    }
  }

  @Test
  public void canNotRunQueryOnStatementWithEmptyDatasetProjectId()
      throws SQLException, IOException {
    // Specifying an empty string for the dataset project id should result in the billing project
    // id being used instead. The query should fail since the billing project 'disco-parsec-659'
    // does not have the dataset 'samples'.
    properties.put("dataset", ".samples");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    Statement stmt = bqConnection.createStatement();

    Assert.assertEquals("", bqConnection.getDataSetProjectId());
    Assert.assertEquals("samples", bqConnection.getDataSet());
    try {
      stmt.executeQuery("SELECT * FROM shakespeare LIMIT 1");
      Assert.fail("Expected SQLException to be thrown without default dataset project id");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("disco-parsec-659:samples was not found in"));
    }
  }

  @Test
  public void canNotRunQueryOnStatementWithoutDataset() throws SQLException, IOException {
    // The query should fail since there is no default dataset specified in the connection and the
    // query has an unqualified table reference.
    properties.remove("dataset");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    Statement stmt = bqConnection.createStatement();

    Assert.assertEquals("disco-parsec-659", bqConnection.getDataSetProjectId());
    Assert.assertEquals(null, bqConnection.getDataSet());
    try {
      stmt.executeQuery("SELECT * FROM shakespeare LIMIT 1");
      Assert.fail("Expected SQLException to be thrown without default dataset");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("must be qualified with a dataset"));
    }
  }

  @Test
  public void canNotRunQueryOnStatementWithEmptyDatasetId() throws SQLException, IOException {
    // The query should fail since the default dataset id is empty.
    properties.put("dataset", "publicdata.");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    Statement stmt = bqConnection.createStatement();

    Assert.assertEquals("publicdata", bqConnection.getDataSetProjectId());
    Assert.assertEquals("", bqConnection.getDataSet());
    try {
      stmt.executeQuery("SELECT * FROM shakespeare LIMIT 1");
      Assert.fail("Expected SQLException to be thrown without default dataset id");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("parameter is missing: dataset_id"));
    }
  }

  @Test
  public void canRunQueryOnStatementWithFullyQualifiedDataset() throws SQLException, IOException {
    // Add the dataset project id that houses the 'samples' dataset. We do not have the ability to
    // create BigQuery jobs in 'publicdata', but we can read from the 'samples' dataset in that
    // project. If we fail to respect the dataset project id, then the query will fail, since the
    // billing project 'disco-parsec-659' does not have that dataset.
    properties.put("dataset", "publicdata.samples");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    Statement stmt = bqConnection.createStatement();

    // This should succeed. We use the default dataset and project for the read query, but we charge
    // the computation to 'disco-parsec-659'.
    Assert.assertEquals("publicdata", bqConnection.getDataSetProjectId());
    Assert.assertEquals("samples", bqConnection.getDataSet());
    stmt.executeQuery("SELECT * FROM shakespeare LIMIT 1");
  }

  @Test
  public void canNotRunQueryOnPreparedStatementWithoutDatasetProjectId()
      throws SQLException, IOException {
    // The query should fail since the billing project 'disco-parsec-659' does not have the dataset
    // 'samples'.
    properties.put("dataset", "samples");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    PreparedStatement stmt =
        bqConnection.prepareStatement("SELECT TOP(word, 3), COUNT(*) FROM shakespeare");

    Assert.assertEquals("disco-parsec-659", bqConnection.getDataSetProjectId());
    Assert.assertEquals("samples", bqConnection.getDataSet());
    try {
      stmt.executeQuery();
      Assert.fail("Expected SQLException to be thrown without default dataset project id");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("disco-parsec-659:samples was not found in"));
    }
  }

  @Test
  public void canNotRunQueryOnPreparedStatementWithEmptyDatasetProjectId()
      throws SQLException, IOException {
    // Specifying an empty string for the dataset project id should result in the billing project
    // id being used instead. The query should fail since the billing project 'disco-parsec-659'
    // does not have the dataset 'samples'.
    properties.put("dataset", ".samples");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    PreparedStatement stmt =
        bqConnection.prepareStatement("SELECT TOP(word, 3), COUNT(*) FROM shakespeare");

    Assert.assertEquals("", bqConnection.getDataSetProjectId());
    Assert.assertEquals("samples", bqConnection.getDataSet());
    try {
      stmt.executeQuery();
      Assert.fail("Expected SQLException to be thrown without default dataset project id");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("disco-parsec-659:samples was not found in"));
    }
  }

  @Test
  public void canNotRunQueryOnPreparedStatementWithoutDataset() throws SQLException, IOException {
    // The query should fail since there is no default dataset specified in the connection and the
    // query has an unqualified table reference.
    properties.remove("dataset");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    PreparedStatement stmt =
        bqConnection.prepareStatement("SELECT TOP(word, 3), COUNT(*) FROM shakespeare");

    Assert.assertEquals("disco-parsec-659", bqConnection.getDataSetProjectId());
    Assert.assertEquals(null, bqConnection.getDataSet());
    try {
      stmt.executeQuery();
      Assert.fail("Expected SQLException to be thrown without default dataset");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("must be qualified with a dataset"));
    }
  }

  @Test
  public void canNotRunQueryOnnPreparedStatementWithEmptyDatasetId()
      throws SQLException, IOException {
    // The query should fail since the default dataset id is empty.
    properties.put("dataset", "publicdata.");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    PreparedStatement stmt =
        bqConnection.prepareStatement("SELECT TOP(word, 3), COUNT(*) FROM shakespeare");

    Assert.assertEquals("publicdata", bqConnection.getDataSetProjectId());
    Assert.assertEquals("", bqConnection.getDataSet());
    try {
      stmt.executeQuery();
      Assert.fail("Expected SQLException to be thrown without default dataset id");
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().contains("parameter is missing: dataset_id"));
    }
  }

  @Test
  public void canRunQueryOnPreparedStatementWithFullyQualifiedDataset()
      throws SQLException, IOException {
    // Add the dataset project id that houses the 'samples' dataset. We do not have the ability to
    // create BigQuery jobs in 'publicdata', but we can read from the 'samples' dataset in that
    // project. If we fail to respect the dataset project id, then the query will fail, since the
    // billing project 'disco-parsec-659' does not have that dataset.
    properties.put("dataset", "publicdata.samples");
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(properties, true, null)
            + "&useLegacySql=true";
    BQConnection bqConnection = new BQConnection(url, new Properties());

    PreparedStatement stmt =
        bqConnection.prepareStatement("SELECT TOP(word, 3), COUNT(*) FROM shakespeare");

    // This should succeed. We use the default dataset and project for the read query, but we charge
    // the computation to 'disco-parsec-659'.
    Assert.assertEquals("publicdata", bqConnection.getDataSetProjectId());
    Assert.assertEquals("samples", bqConnection.getDataSet());
    stmt.executeQuery();
  }

  @Test
  public void canConnectWithPasswordProtectedP12File() throws SQLException, IOException {
    String url = getUrl("/protectedaccount.properties", null);
    BQConnection bqConn = new BQConnection(url, new Properties());

    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bqConn);
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void canConnectWithJSONFile() throws SQLException, IOException {
    String url = getUrl("/protectedaccount-json.properties", null);
    properties = getProperties("/protectedaccount-json.properties");
    properties.setProperty("path", "src/test/resources/bigquery_credentials_protected.json");
    BQConnection bqConn = new BQConnection(url, properties);

    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bqConn);
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void canConnectWithJsonAuthFileContentsInProperties() throws SQLException, IOException {
    String url = getUrl("/protectedaccount-json.properties", null);
    properties = getProperties("/protectedaccount-json.properties");
    String jsonContents =
        new String(
            Files.readAllBytes(
                Paths.get("src/test/resources/bigquery_credentials_protected.json")));
    Properties props = new Properties();
    props.setProperty("jsonAuthContents", jsonContents);
    BQConnection bqConn = new BQConnection(url, props);

    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bqConn);
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void canConnectWithOAuthAccessToken()
      throws SQLException, IOException, GeneralSecurityException {
    // generate access token from service account credentials
    Properties serviceProps = getProperties("/protectedaccount.properties");
    String accessToken =
        Oauth2Bigquery.generateAccessToken(
            serviceProps.getProperty("user"),
            serviceProps.getProperty("path"),
            serviceProps.getProperty("password"),
            null);

    Properties oauthProps = getProperties("/oauthaccount.properties");
    oauthProps.setProperty("oauthaccesstoken", accessToken);
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(oauthProps, true, null);
    BQConnection bqConn = new BQConnection(url, new Properties());

    BQStatement stmt = new BQStatement(oauthProps.getProperty("projectid"), bqConn);
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void oAuthAccessTokenOnlyInHeader()
      throws SQLException, IOException, GeneralSecurityException {
    // generate access token from service account credentials
    Properties serviceProps = getProperties("/protectedaccount.properties");
    String accessToken =
        Oauth2Bigquery.generateAccessToken(
            serviceProps.getProperty("user"),
            serviceProps.getProperty("path"),
            serviceProps.getProperty("password"),
            null);

    Properties oauthProps = getProperties("/oauthaccount.properties");
    oauthProps.setProperty("oauthaccesstoken", accessToken);
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(oauthProps, true, null);
    BQConnection bqConn = new BQConnection(url, new Properties());

    Oauth2Bigquery.BigQueryRequestUserAgentInitializer initializer =
        (Oauth2Bigquery.BigQueryRequestUserAgentInitializer)
            bqConn.getBigquery().getGoogleClientRequestInitializer();

    Assertions.assertThat(initializer.getOauthToken())
        .withFailMessage(
            "BigQueryRequestUserAgentInitializer.getOauthToken private API required"
                + " by Looker; token must be present")
        .isNotNull();

    BQStatement stmt = new BQStatement(oauthProps.getProperty("projectid"), bqConn);
    Query query =
        BQSupportFuncts.getSyncQuery(
            bqConn.getBigquery(),
            oauthProps.getProperty("projectid"),
            "SELECT * FROM orders limit 1",
            bqConn.getDataSet(),
            bqConn.getDataSetProjectId(),
            bqConn.getUseLegacySql(),
            null,
            stmt.getSyncTimeoutMillis(),
            (long) stmt.getMaxRows(),
            stmt.getAllLabels(),
            bqConn.getUseQueryCache(),
            JobCreationMode.JOB_CREATION_MODE_UNSPECIFIED);
    String oAuthToken = query.getOauthToken();
    Assertions.assertThat(oAuthToken).isNull();
  }

  @Test
  public void unauthorizedResponseForInvalidOAuthAccessToken()
      throws SQLException, IOException, GeneralSecurityException {
    Properties oauthProps = getProperties("/oauthaccount.properties");
    oauthProps.setProperty("oauthaccesstoken", "invalid_access_token");
    String url = BQSupportFuncts.constructUrlFromPropertiesFile(oauthProps, true, null);
    BQConnection bqConn = new BQConnection(url, new Properties());

    BQStatement stmt = new BQStatement(oauthProps.getProperty("projectid"), bqConn);
    try {
      stmt.executeQuery("SELECT * FROM orders limit 1");
    } catch (BQSQLException e) {
      Assertions.assertThat(e.getCause().getMessage()).contains("Unauthorized");
    }
  }

  @Test
  public void canConnectWithApplicationDefaultCredentials() throws SQLException, IOException {
    // For testing, the `GOOGLE_APPLICATION_ENVIRONMENT` env var is a path to a service account file
    Properties testProps = getProperties("/applicationdefault.properties");
    String url =
        BQDriver.getURLPrefix()
            + testProps.getProperty("projectid")
            + "/"
            + testProps.getProperty("dataset");
    url += "?withApplicationDefaultCredentials=true";
    BQConnection bqConn = new BQConnection(url, new Properties());

    BQStatement stmt = new BQStatement(bqConn.getProjectId(), bqConn);
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void canImpersonateServiceAccountWithApplicationDefaultAsSource()
      throws IOException, SQLException {
    Properties testProps = getProperties("/applicationdefault.properties");
    String url =
        BQDriver.getURLPrefix()
            + testProps.getProperty("projectid")
            + "/"
            + testProps.getProperty("dataset");
    url +=
        "?withApplicationDefaultCredentials=true&targetServiceAccount="
            + testProps.getProperty("targetaccount");
    BQConnection bqConn = new BQConnection(url, new Properties());

    BQStatement stmt = new BQStatement(bqConn.getProjectId(), bqConn);
    stmt.executeQuery("SELECT * FROM orders limit 1");
  }

  @Test
  public void impersonatingServiceAccountWithLimitedPermissionsWorksAsExpected()
      throws IOException, SQLException {
    Properties testProps = getProperties("/applicationdefault.properties");
    String url =
        BQDriver.getURLPrefix()
            + testProps.getProperty("projectid")
            + "/"
            + testProps.getProperty("dataset");
    // dummy service account that has the BigQuery User role but no access to the orders table
    url +=
        "?withApplicationDefaultCredentials=true&targetServiceAccount="
            + testProps.getProperty("limitedpermissiontargetaccount");
    BQConnection bqConn = new BQConnection(url, new Properties());

    BQStatement stmt = new BQStatement(bqConn.getProjectId(), bqConn);
    // Should be fine since it's a public dataset
    stmt.executeQuery("SELECT * FROM bigquery-public-data.baseball.schedules limit 1");
    try {
      stmt.executeQuery("SELECT * FROM orders limit 1");
      Assert.fail("The impersonated service account should not have access to the orders table");
    } catch (SQLException e) {
      Assertions.assertThat(e.getCause().getMessage())
          .contains("User does not have permission to query table");
    }
  }

  @Test
  public void impersonatedServiceAccountWithDelegations() throws IOException, SQLException {
    Properties testProps = getProperties("/applicationdefault.properties");
    String url =
        BQDriver.getURLPrefix()
            + testProps.getProperty("projectid")
            + "/"
            + testProps.getProperty("dataset");
    url +=
        "?withApplicationDefaultCredentials=true&targetServiceAccount="
            + testProps.getProperty("delegates")
            + ","
            + testProps.getProperty("limitedpermissiontargetaccount");
    BQConnection bqConn = new BQConnection(url, new Properties());
    BQStatement stmt = new BQStatement(bqConn.getProjectId(), bqConn);
    // Don't really care about the results here, just need to be able to run a query
    stmt.executeQuery("SELECT * FROM bigquery-public-data.baseball.schedules limit 1");
  }

  @Test
  public void delegationsThrowWithBadDelegationChain() throws IOException, SQLException {
    Properties testProps = getProperties("/applicationdefault.properties");
    String url =
        BQDriver.getURLPrefix()
            + testProps.getProperty("projectid")
            + "/"
            + testProps.getProperty("dataset");
    url +=
        "?withApplicationDefaultCredentials=true&targetServiceAccount="
            + testProps.getProperty("baddelegates")
            + ","
            + testProps.getProperty("limitedpermissiontargetaccount");
    BQConnection bqConn = new BQConnection(url, new Properties());
    BQStatement stmt = new BQStatement(bqConn.getProjectId(), bqConn);
    try {
      stmt.executeQuery("SELECT * FROM bigquery-public-data.baseball.schedules limit 1");
      Assert.fail("I should have failed when trying to execute the query.");
    } catch (SQLException e) {
      Assertions.assertThat(e.getCause().getMessage()).contains("Error requesting access token");
    }
  }

  @Test
  public void gettingUrlComponentsWorks() throws IOException {
    String url = getUrl("/protectedaccount.properties", null);
    Properties protectedProperties = getProperties("/protectedaccount.properties");
    Properties components = BQSupportFuncts.getUrlQueryComponents(url, new Properties());

    Assert.assertEquals(protectedProperties.getProperty("user"), components.getProperty("user"));
    Assert.assertEquals(
        protectedProperties.getProperty("password"), components.getProperty("password"));
    Assert.assertEquals(protectedProperties.getProperty("path"), components.getProperty("path"));
  }

  @Test
  public void connectionUseLegacySqlValueFromProperties() throws IOException, SQLException {
    String url = getUrl("/protectedaccount.properties", null);
    BQConnection bqConn = new BQConnection(url, new Properties());
    // default true
    Assert.assertEquals(bqConn.getUseLegacySql(), false);

    String newUrl = url + "&useLegacySql=false";
    BQConnection bqConn2 = new BQConnection(newUrl, new Properties());
    Assert.assertEquals(bqConn2.getUseLegacySql(), false);
  }

  @Test
  public void connectionMaxBillingBytesFromProperties() throws IOException, SQLException {
    String url = getUrl("/protectedaccount.properties", null);
    BQConnection bqConn = new BQConnection(url, new Properties());
    // default null
    Assert.assertNull(bqConn.getMaxBillingBytes());

    String newUrl = url + "&maxbillingbytes=1000000000";
    BQConnection bqConn2 = new BQConnection(newUrl, new Properties());
    Assert.assertEquals((long) bqConn2.getMaxBillingBytes(), 1000000000);
  }

  @Test
  public void maxBillingBytesOverrideWorks() throws IOException, SQLException {
    String url =
        getUrl("/protectedaccount.properties", null) + "&maxbillingbytes=1" + "&useLegacySql=true";
    BQConnection bqConn = new BQConnection(url, new Properties());
    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bqConn);

    // need to include NOW() otherwise this query will pull from cache & mess up the following
    // ASSERTs
    String sqlStmt = "SELECT word, NOW() from publicdata:samples.shakespeare LIMIT 100";

    boolean didFailAsExpected = false;

    // limited-bytes query should fail
    try {
      stmt.executeQuery(sqlStmt, false);
    } catch (SQLException e) {
      Assert.assertTrue(
          "Expected query to fail because it exceeds maximum billing bytes.",
          e.toString().contains("Query exceeded limit for bytes billed: 1."));
      didFailAsExpected = true;
    }

    Assert.assertTrue("Query did not fail as expected.", didFailAsExpected);

    // unlimited-bytes query should succeed
    stmt.executeQuery(sqlStmt, true);
  }

  @Test
  public void rootUrlOverrideWorks() throws IOException, SQLException {
    properties = getProperties("/vpcaccount.properties");
    // Add URL-encoded `https://restricted.googleapis.com/` to the JDBC URL.
    URL =
        getUrl("/vpcaccount.properties", null)
            + "&rootUrl=https%3A%2F%2Frestricted.googleapis.com%2F";
    // Mock a response similar to
    // https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#response-body
    String mockResponse =
        "{ \"jobComplete\": true, "
            + "\"totalRows\": \"0\", "
            + "\"rows\": [], "
            + "\"totalBytesProcessed\": \"0\", "
            + "\"cacheHit\": false }";
    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(new MockLowLevelHttpResponse().setContent(mockResponse))
            .build();
    bq = new BQConnection(URL, properties, mockTransport);
    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bq);
    String sqlStmt = "SELECT word from publicdata:samples.shakespeare LIMIT 100";

    stmt.executeQuery(sqlStmt);

    MockLowLevelHttpRequest request = mockTransport.getLowLevelHttpRequest();
    Assert.assertTrue(
        request.getUrl().startsWith("https://restricted.googleapis.com/bigquery/v2/"));
  }

  @Test
  public void timeoutMsRejectsBadValues() throws Exception {
    try {
      new BQConnection(URL + "&timeoutMs=-1", new Properties());
    } catch (BQSQLException e) {
      Assert.assertEquals(e.getMessage().contains("timeoutMs must be positive."), true);
    }
    try {
      new BQConnection(URL + "&timeoutMs=NotANumber", new Properties());
    } catch (BQSQLException e) {
      Assert.assertEquals(e.getMessage().contains("could not parse timeoutMs parameter."), true);
    }
  }

  @Test
  public void timeoutMsWorks() throws Exception {
    // should immediately kill the job
    this.URL += "&timeoutMs=1";
    this.bq = new BQConnection(URL, new Properties());
    // ensure that the url string was parsed properly
    Assert.assertEquals(this.bq.getTimeoutMs(), Integer.valueOf(1));
    // this query takes about 50 second to complete normally
    String sqlStmt = "SELECT * from publicdata:samples.wikipedia";
    BQStatement stmt = new BQStatement(this.properties.getProperty("projectid"), this.bq);
    try {
      stmt.executeQuery(sqlStmt);
      Assert.fail("Query job should have timed out");
    } catch (BQSQLException e) {
      Assert.assertEquals(
          e.getMessage().contains("Query run took more than the specified timeout"), true);
    }
  }

  @Test
  public void queryCacheIsOnByDefault() throws Exception {
    // Sanity check to make sure the queryCache param is not set.
    Assert.assertFalse(this.URL.toLowerCase().contains("querycache"));

    // It's possible that this may fail for unforeseeable reasons,
    // but we expect it to be true the vast majority of the time.
    Assert.assertTrue(runSimpleQueryTwiceAndReturnLastCacheHit());
  }

  @Test
  public void queryCacheOverrideWorks() throws Exception {
    this.URL += "&queryCache=false";
    this.bq = new BQConnection(URL, new Properties());

    // This should never fail.
    Assert.assertFalse(runSimpleQueryTwiceAndReturnLastCacheHit());
  }

  private boolean runSimpleQueryTwiceAndReturnLastCacheHit() throws Exception {
    String sqlStmt = "SELECT word from publicdata:samples.shakespeare LIMIT 100";
    boolean lastQueryWasCacheHit = false;

    // Run the same query twice. Expect the second time to be a cache hit.
    for (int i = 0; i < 2; i++) {
      BQStatement stmt = new BQStatement(this.properties.getProperty("projectid"), this.bq);
      ResultSet results = stmt.executeQuery(sqlStmt);
      if (results instanceof BQForwardOnlyResultSet) {
        lastQueryWasCacheHit = ((BQForwardOnlyResultSet) results).getCacheHit();
      } else if (results instanceof BQScrollableResultSet) {
        lastQueryWasCacheHit = ((BQScrollableResultSet) results).getCacheHit();
      } else {
        throw new AssertionError("Unexpected result set: " + results.toString());
      }
    }
    return lastQueryWasCacheHit;
  }

  @Test
  public void setLabelsTest() throws Exception {
    BQStatement stmt = prepareStatementWithLabels();
    Map<String, String> sentLabels = ImmutableMap.of();

    stmt.executeQuery("SELECT * FROM orders LIMIT 1");

    sentLabels = stmt.getLabelsFromMostRecentQuery(this.bq);
    Assert.assertEquals(4, sentLabels.size());
    Assert.assertEquals("connection-label", sentLabels.get("this"));
    Assert.assertEquals("another-connection-label", sentLabels.get("that"));
    Assert.assertEquals("query-label", sentLabels.get("the-other"));
    Assert.assertEquals("another-query-label", sentLabels.get("and-then"));
  }

  @Test
  public void setLabelsTestForStatementRoot() throws Exception {
    BQStatement stmt = prepareStatementWithLabels();
    Map<String, String> sentLabels = ImmutableMap.of();

    // Use executeUpdate instead of executeQuery to ensure the query
    // is executed by `BQStatementRoot` and not its child class `BQStatement`.
    stmt.executeUpdate("SELECT * FROM orders LIMIT 1");

    sentLabels = stmt.getLabelsFromMostRecentQuery(this.bq);
    Assert.assertEquals(4, sentLabels.size());
    Assert.assertEquals("connection-label", sentLabels.get("this"));
    Assert.assertEquals("another-connection-label", sentLabels.get("that"));
    Assert.assertEquals("query-label", sentLabels.get("the-other"));
    Assert.assertEquals("another-query-label", sentLabels.get("and-then"));
  }

  private BQStatement prepareStatementWithLabels() throws Exception {
    this.URL = getUrl("/protectedaccount.properties", null);
    this.URL += "&labels=this%3Dconnection-label,that%3Danother-connection-label";
    this.bq = new BQConnection(this.URL, new Properties());
    BQStatement stmt = new BQStatement(properties.getProperty("projectid"), this.bq);
    stmt.setLabels(
        ImmutableMap.of(
            "the-other", "query-label",
            "and-then", "another-query-label"));
    return stmt;
  }

  private Properties getProperties(String pathToProp) throws IOException {
    return BQSupportFuncts.readFromPropFile(getClass().getResource(pathToProp).getFile());
  }

  private String getUrl(String pathToProp, String dataset) throws IOException {
    return BQSupportFuncts.constructUrlFromPropertiesFile(getProperties(pathToProp), true, dataset);
  }

  @Test
  public void missingJobCreationModeDefaultsToNull() throws Exception {
    final String url = getUrl("/protectedaccount.properties", null);
    Assertions.assertThat(url).doesNotContain("jobcreationmode");
    bq = new BQConnection(url, new Properties());
    final JobCreationMode mode = bq.getJobCreationMode();
    Assertions.assertThat(mode).isNull();
  }

  @Test
  public void jobCreationModeTest() throws Exception {
    final String url = getUrl("/protectedaccount.properties", null);
    Assertions.assertThat(url).doesNotContain("jobcreationmode");
    final JobCreationMode[] modes = JobCreationMode.values();
    for (JobCreationMode mode : modes) {
      final String fullURL = String.format("%s&jobcreationmode=%s", url, mode.name());
      try (BQConnection bq = new BQConnection(fullURL, new Properties())) {
        final JobCreationMode parsedMode = bq.getJobCreationMode();
        Assertions.assertThat(parsedMode).isEqualTo(mode);
      }
    }
  }
}
