package BQJDBC.QueryResultTest;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.QueryResponse;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.junit.After;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Created by steven on 11/2/15.
 */
public class CancelTest {

    private Throwable diedWith;

    private BQConnection conn(boolean useQueryApi)  throws SQLException, IOException {
        String url = BQSupportFuncts.constructUrlFromPropertiesFile(BQSupportFuncts
                .readFromPropFile(getClass().getResource("/installedaccount.properties").getFile()), true, null);
        url += "&useLegacySql=false";
        if (!useQueryApi) {
            // this test relies on async to know when to attempt the cancel
            url += "&useQueryApi=false";
        }
        return new BQConnection(url, new Properties());
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
                    Random rand = new Random();
                    int limit = rand.nextInt(1000) + 1;
                    String longQuery = "WITH d as (SELECT * FROM (SELECT 391164 AS num) UNION ALL\n" +
                            "  (SELECT 391165 AS num) UNION ALL\n" +
                            "  (SELECT 391166 AS num) UNION ALL\n" +
                            "  (SELECT 391167 AS num) UNION ALL\n" +
                            "  (SELECT 391168 AS num) UNION ALL\n" +
                            "  (SELECT 391169 AS num) UNION ALL\n" +
                            "  (SELECT 391170 AS num) UNION ALL\n" +
                            "  (SELECT 391171 AS num) UNION ALL\n" +
                            "  (SELECT 391172 AS num) UNION ALL\n" +
                            "  (SELECT 391173 AS num) UNION ALL\n" +
                            "  (SELECT 391174 AS num))\n" +
                            "SELECT count(*) from d d1, d d2, d d3, d d4, d d5, d d6, d d7, d d8, d d9 LIMIT " + limit;
                    stmt.executeQuery(longQuery);
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
        BQConnection bq = conn(false);
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
    public void connectionCancelWorks() throws SQLException, InterruptedException, IOException {
        BQConnection bq = conn(false);
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

    @org.junit.Test
    public void connectionCloseWithSyncQueryApi() throws SQLException, InterruptedException, IOException {
        BQConnection bq = conn(true);
        final TestableBQStatement stmt1 = new TestableBQStatement(bq.getProjectId(), bq);
        stmt1.setTestPoint();
        Thread backgroundThread = getAndRunBackgroundQuery(stmt1);
        stmt1.waitForTestPoint();
        bq.close();
        backgroundThread.join();
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

        @Override
        protected QueryResponse runSyncQuery(String querySql, boolean unlimitedBillingBytes) throws IOException, SQLException {
            Thread signaler = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1); // make absolutely sure we yield to the execution below so it gets through first
                    } catch (InterruptedException e) {
                        throw new RuntimeException();
                    }
                    signalTestPoint();
                }
            });
            signaler.start();
            return super.runSyncQuery(querySql, unlimitedBillingBytes);
        }

        public Job startQuery(String querySql, boolean unlimitedBillingBytes) throws IOException {
            Job result = super.startQuery(querySql, unlimitedBillingBytes);
            signalTestPoint();
            return result;
        }
    }
}
