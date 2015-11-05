package BQJDBC.QueryResultTest;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by steven on 10/21/15.
 */
public class JdbcUrlTest {

    static final String URL = "jdbc:BQDriver::disco-parsec-659/looker_test?withServiceAccount=true&user=697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com&password=src%2F%2Ftest%2Fresources%2Fbigquery_credentials.p12";
    private BQConnection bq;

    @Before
    public void setup() throws SQLException {
        this.bq = new BQConnection(URL, new Properties());
    }
    
    @Test
    public void urlWithDefaultDatasetShouldWork() throws SQLException {
        Assert.assertEquals(bq.getDataSet(), "looker_test");
    }

    @Test
    public void canRunQueryWithDefaultDataset() throws SQLException {
        BQStatement stmt = new BQStatement("disco-parsec-659", bq);

        // This should not blow up with a "No dataset specified" exception
        stmt.executeQuery("SELECT * FROM orders limit 1");
    }

    @Test
    public void canConnectWithPasswordProtectedP12File() throws SQLException {
        String url = "jdbc:BQDriver::disco-parsec-659/looker_test?withServiceAccount=true&user=697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com&password=123456&path=src%2Ftest%2Fresources%2Fbigquery_credentials_protected.p12";
        BQConnection bqConn = new BQConnection(url, new Properties());

        BQStatement stmt = new BQStatement("disco-parsec-659", bqConn);
        stmt.executeQuery("SELECT * FROM orders limit 1");
    }

    @Test
    public void gettingUrlComponentsWorks() throws UnsupportedEncodingException {
        String url = "jdbc:BQDriver::disco-parsec-659/looker_test?withServiceAccount=true&user=697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com&password=123456&path=src%2Ftest%2Fresources%2Fbigquery_credentials_protected.p12";
        Map<String, String> components = BQSupportFuncts.getUrlQueryComponents(url);

        Assert.assertEquals("697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com", components.get("user"));
        Assert.assertEquals("123456", components.get("password"));
        Assert.assertEquals("src/test/resources/bigquery_credentials_protected.p12", components.get("path"));
    }

}
