package BQJDBC.QueryResultTest;

import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import static junit.framework.Assert.assertFalse;

/**
 * Created by steven on 11/2/15.
 */
public class CancelTest {

    static final String URL = "jdbc:BQDriver::disco-parsec-659/looker_test?withServiceAccount=true&user=697117590302-76cr6q3217nck6gks0kf4r151j4d9f8e@developer.gserviceaccount.com&password=src%2F%2Ftest%2Fresources%2Fbigquery_credentials.p12";
    private BQConnection bq;

    @Before
    public void setup() throws SQLException {
        this.bq = new BQConnection(URL, new Properties());
    }

    @org.junit.Test
    public void cancelWorks() throws SQLException, InterruptedException, IOException {
        final BQStatement stmt = new BQStatement(bq.getProjectId(), bq);
        stmt.setTestPoint();

        Runnable background = new Runnable() {
            @Override
            public void run() {
                try {
                    stmt.executeQuery("select * from publicdata:samples.shakespeare limit 712");
                } catch (SQLException e) {}
            }
        };
        Thread backgroundThread = new Thread(background);
        backgroundThread.start();

        stmt.waitForTestPoint();
        assertFalse("Statement must have job", stmt.getJob() == null);

        // This will throw error if it tries to cancel a nonexistent job
        stmt.cancel();

        backgroundThread.join();

        // TODO: better checks that cancel worked
        // Right now, there seems to be no reliable indication in the API that a job was cancelled (status is always 'DONE'),
        // and even if the cancel signal was received the result is likely to come back successfully with all its rows.
        // If the Google folks change the API to add a status like 'CANCEL_RECEIVED', we should check for that here
    }
}
