package BQJDBC.QueryResultTest;

import com.google.api.services.bigquery.model.Job;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Created by steven on 11/2/15.
 */
public class CancelTest {

    private BQConnection bq;
    private Throwable diedWith;

    @Before
    public void setup() throws SQLException, IOException {
        String url = BQSupportFuncts.constructUrlFromPropertiesFile(BQSupportFuncts
                .readFromPropFile(getClass().getResource("/installedaccount.properties").getFile()), true, null);
        this.bq = new BQConnection(url, new Properties());
    }

    @After
    public void teardown() throws Throwable {
        if (this.diedWith != null) {
            throw this.diedWith;
        }
    }

    private Thread getAndRunBackgroundQuery(final BQStatement stmt) {
        final CancelTest thisTest = this;
        Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread th, Throwable ex) {
                thisTest.diedWith = ex;
            }
        };

        Runnable background = new Runnable() {
            @Override
            public void run() {
                try {
                    stmt.executeQuery("select * from publicdata:samples.shakespeare limit 712");
                } catch (SQLException e) {}
            }
        };
        Thread backgroundThread = new Thread(background);
        backgroundThread.setUncaughtExceptionHandler(handler);
        backgroundThread.start();
        return backgroundThread;
    }

    @org.junit.Test
    public void cancelWorks() throws SQLException, InterruptedException, IOException {
        TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq);
        stmt.setTestPoint();

        Thread backgroundThread = getAndRunBackgroundQuery(stmt);

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

    @org.junit.Test
    public void connectionCancelWorks() throws InterruptedException {
        final TestableBQStatement stmt1 = new TestableBQStatement(bq.getProjectId(), bq);
        final TestableBQStatement stmt2 = new TestableBQStatement(bq.getProjectId(), bq);

        stmt1.setTestPoint();
        stmt2.setTestPoint();

        Thread backgroundThread1 = getAndRunBackgroundQuery(stmt1);
        Thread backgroundThread2 = getAndRunBackgroundQuery(stmt2);

        stmt1.waitForTestPoint();
        stmt2.waitForTestPoint();

        assertTrue("Must see both running queries", bq.getNumberRunningQueries() == 2);
        assertTrue("Must not fail to cancel queries", bq.cancelRunningQueries() == 0);

        backgroundThread1.join();
        backgroundThread2.join();
    }

    private static class TestableBQStatement extends BQStatement {

        public TestableBQStatement(String projectid, BQConnection bqConnection) {
            super(projectid, bqConnection);
        }

        private Condition testPoint;
        private Lock testLock;
        private boolean conditionHit = false;

        private void signalTestPoint() {
            if (this.testPoint == null) {
                return;
            }
            this.testLock.lock();
            try {
                conditionHit = true;
                this.testPoint.signal();
            } finally {
                this.testLock.unlock();
            }
        }

        public void setTestPoint() {
            Lock lock = new ReentrantLock();
            this.testPoint = lock.newCondition();
            this.testLock = lock;
        }

        public void waitForTestPoint() throws InterruptedException {
            this.testLock.lock();
            try {
                if (!conditionHit) {
                    this.testPoint.await();
                }
            } finally {
                this.testLock.unlock();
            }
        }

        public Job startQuery(String querySql, boolean unlimitedBillingBytes) throws IOException {
            Job result = super.startQuery(querySql, unlimitedBillingBytes);
            signalTestPoint();
            return result;
        }
    }
}
