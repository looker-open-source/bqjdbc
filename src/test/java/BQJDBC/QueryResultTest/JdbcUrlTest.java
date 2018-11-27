package BQJDBC.QueryResultTest;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.fail;

/**
 * Created by steven on 10/21/15.
 */
public class JdbcUrlTest {

    private BQConnection bq;
    private String URL;
    private Properties properties;

    @Before
    public void setup() throws SQLException, IOException {
        properties = getProperties("/installedaccount.properties");
        URL = getUrl("/installedaccount.properties", null);
        this.bq = new BQConnection(URL, new Properties());
    }

    @Test
    public void urlWithDefaultDatasetShouldWork() throws SQLException {
        Assert.assertEquals(properties.getProperty("dataset"), bq.getDataSet());
    }

    @Test
    public void projectWithColons() throws SQLException {
        String urlWithColonContainingProject = URL.replace(bq.getProjectId(), "before:after");
        try {
            BQConnection bq_with_colons = new BQConnection(urlWithColonContainingProject, new Properties());
            // Some day we'll get rid of the whacky subbing in and out of colons with double underscores, but today is not that day
            Assert.assertEquals("before__after", bq_with_colons.getProjectId());
        } catch (SQLException e){
            fail("failed to get or parse url: " + e.getMessage());
        }
    }

    @Test
    public void urlWithTimeouts() throws SQLException {
        try {
            String url1 = URL + "&readTimeout=foo";
            new BQConnection(url1, new Properties());
        } catch (SQLException e){
            Assert.assertEquals("could not parse readTimeout parameter.", e.getMessage());
        }

        try {
            String url1 = URL + "&connectTimeout=foo";
            new BQConnection(url1, new Properties());
        } catch (SQLException e){
            Assert.assertEquals("could not parse connectTimeout parameter.", e.getMessage());
        }

        try {
            String url1 = URL + "&readTimeout=-1000";
            new BQConnection(url1, new Properties());
        } catch (SQLException e){
            Assert.assertEquals("readTimeout must be positive.", e.getMessage());
        }

        try {
            String url1 = URL + "&connectTimeout=-1000";
            new BQConnection(url1, new Properties());
        } catch (SQLException e){
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
    public void canConnectWithPasswordProtectedP12File() throws SQLException, IOException {
        String url = getUrl("/protectedaccount.properties", null);
        BQConnection bqConn = new BQConnection(url, new Properties());

        BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bqConn);
        stmt.executeQuery("SELECT * FROM orders limit 1");
    }

    @Test
    public void canConnectWithPasswordProtectedJSONFile() throws SQLException, IOException {
        String url = getUrl("/protectedaccountjson.properties", null);
        BQConnection bqConn = new BQConnection(url, new Properties());

        BQStatement stmt = new BQStatement(properties.getProperty("projectid"), bqConn);
        stmt.executeQuery("SELECT * FROM orders limit 1");
    }

    @Test
    public void gettingUrlComponentsWorks() throws IOException {
        String url = getUrl("/protectedaccount.properties", null);
        Properties protectedProperties = getProperties("/protectedaccount.properties");
        Properties components = BQSupportFuncts.getUrlQueryComponents(url, new Properties());

        Assert.assertEquals(protectedProperties.getProperty("user"), components.getProperty("user"));
        Assert.assertEquals(protectedProperties.getProperty("password"), components.getProperty("password"));
        Assert.assertEquals(protectedProperties.getProperty("path"), components.getProperty("path"));
    }

    @Test
    public void connectionUseLegacySqlValueFromProperties() throws IOException, SQLException {
        String url = getUrl("/protectedaccount.properties", null);
        BQConnection bqConn = new BQConnection(url, new Properties());
        // default false
        Assert.assertEquals(bqConn.getUseLegacySql(), true);

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

    private Properties getProperties(String pathToProp) throws IOException {
        return BQSupportFuncts
                .readFromPropFile(getClass().getResource(pathToProp).getFile());
    }

    private String getUrl(String pathToProp, String dataset) throws IOException {
        return BQSupportFuncts.constructUrlFromPropertiesFile(getProperties(pathToProp), true, dataset);
    }

}
