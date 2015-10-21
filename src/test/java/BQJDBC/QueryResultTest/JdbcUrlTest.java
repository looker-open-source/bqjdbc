package BQJDBC.QueryResultTest;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
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

}
