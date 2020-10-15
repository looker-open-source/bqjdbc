package BQJDBC.QueryResultTest;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryResponse;
import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQStatement;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.junit.After;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static junit.framework.Assert.*;

/**
 * Created by steven on 11/2/15.
 */
public class CancelTest {

    private AtomicReference<Throwable> unexpectedDiedWith = new AtomicReference<>();
    private AtomicReference<SQLException> expectedSqlException = new AtomicReference<>();

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
        if (this.unexpectedDiedWith.get() != null) {
            throw this.unexpectedDiedWith.get();
        }
    }

    private Thread getAndRunBackgroundQuery(final BQStatement stmt) {
        final CancelTest thisTest = this;
        Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread th, Throwable ex) {
                thisTest.unexpectedDiedWith.set(ex);
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
                } catch (SQLException e) {
                    expectedSqlException.set(e);
                }
            }
        };
        Thread backgroundThread = new Thread(background);
        backgroundThread.setUncaughtExceptionHandler(handler);
        backgroundThread.start();
        return backgroundThread;
    }

    @org.junit.Test
    public void syncQueryCancel() throws SQLException, InterruptedException, IOException {
        BQConnection bq = conn(true);
        TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq);
        stmt.setTestPoint();
        Thread backgroundThread = getAndRunBackgroundQuery(stmt);
        stmt.waitForTestPoint();
        assertEquals(1, bq.getNumberRunningQueries());
        stmt.cancel();
        backgroundThread.join();
        SQLException exception = expectedSqlException.get();
        Assert.assertEquals("Job execution was cancelled: User requested cancellation",
                ((com.google.api.client.googleapis.json.GoogleJsonResponseException) exception.getCause()).getDetails().getMessage());
    }

    @org.junit.Test
    public void syncQueryCancel2() throws SQLException, InterruptedException, IOException {
        BQConnection bq = conn(true);
        TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq);
        stmt.setTestPoint();
        Thread backgroundThread = getAndRunBackgroundQuery(stmt);
        stmt.waitForTestPoint();
        Thread.sleep(BQStatement.SYNC_TIMEOUT_MILLIS + 1000); // wait for the sync part of the query to definitely finish
        assertEquals(1, bq.getNumberRunningQueries());
        stmt.cancel();
        backgroundThread.join();
        SQLException exception = expectedSqlException.get();
        Assert.assertEquals("Job execution was cancelled: User requested cancellation",
                ((com.google.api.client.googleapis.json.GoogleJsonResponseException) exception.getCause()).getDetails().getMessage());
    }

    @org.junit.Test
    public void noCancelOnCloseAfterSyncQueryCompletion() throws SQLException, IOException {
        BQConnection bq = conn(true);
        TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq) {
            @Override
            protected void performQueryCancel(JobReference jobRefToCancel) {
                throw new RuntimeException("don't cancel in this test");
            }
        };
        stmt.executeQuery("SELECT 1");
        stmt.close();
        assertTrue(stmt.isClosed());
    }

    @org.junit.Test
    public void testAsyncQueryCancel() throws SQLException, InterruptedException, IOException {
        BQConnection bq = conn(false);
        TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq);
        stmt.setTestPoint();
        Thread backgroundThread = getAndRunBackgroundQuery(stmt);
        stmt.waitForTestPoint();
        assertFalse("Statement must have job", stmt.getJob() == null);
        // This will throw error if it tries to cancel a nonexistent job
        stmt.cancel();
        backgroundThread.join();
        SQLException exception = expectedSqlException.get();
        Assert.assertEquals("Job execution was cancelled: User requested cancellation",
                ((com.google.api.client.googleapis.json.GoogleJsonResponseException) exception.getCause()).getDetails().getMessage());
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
                        Thread.sleep(1000); // make absolutely sure we yield to the execution below so it gets through first
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
