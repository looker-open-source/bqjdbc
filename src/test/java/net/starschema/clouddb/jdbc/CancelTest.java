package net.starschema.clouddb.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.QueryResponse;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Created by steven on 11/2/15. */
public class CancelTest {

  private AtomicReference<Throwable> unexpectedDiedWith = new AtomicReference<>();
  private AtomicReference<SQLException> expectedSqlException = new AtomicReference<>();

  private BQConnection conn() throws SQLException, IOException {
    String url =
        BQSupportFuncts.constructUrlFromPropertiesFile(
            BQSupportFuncts.readFromPropFile(
                getClass().getResource("/installedaccount.properties").getFile()),
            true,
            null);
    url += "&useLegacySql=false";
    return new BQConnection(url, new Properties());
  }

  @AfterEach
  public void teardown() throws Throwable {
    if (this.unexpectedDiedWith.get() != null) {
      throw this.unexpectedDiedWith.get();
    }
  }

  private Thread getAndRunBackgroundQuery(final BQStatement stmt) {
    final CancelTest thisTest = this;
    Thread.UncaughtExceptionHandler handler =
        new Thread.UncaughtExceptionHandler() {
          public void uncaughtException(Thread th, Throwable ex) {
            thisTest.unexpectedDiedWith.set(ex);
          }
        };
    Runnable background =
        new Runnable() {
          @Override
          public void run() {
            try {
              Random rand = new Random();
              int limit = rand.nextInt(1000) + 1;
              String longQuery =
                  "WITH d as (SELECT * FROM (SELECT 391164 AS num) UNION ALL\n"
                      + "  (SELECT 391165 AS num) UNION ALL\n"
                      + "  (SELECT 391166 AS num) UNION ALL\n"
                      + "  (SELECT 391167 AS num) UNION ALL\n"
                      + "  (SELECT 391168 AS num) UNION ALL\n"
                      + "  (SELECT 391169 AS num) UNION ALL\n"
                      + "  (SELECT 391170 AS num) UNION ALL\n"
                      + "  (SELECT 391171 AS num) UNION ALL\n"
                      + "  (SELECT 391172 AS num) UNION ALL\n"
                      + "  (SELECT 391173 AS num) UNION ALL\n"
                      + "  (SELECT 391174 AS num) UNION ALL\n"
                      + "  (SELECT 391175 AS num) UNION ALL\n"
                      + "  (SELECT 391176 AS num) UNION ALL\n"
                      + "  (SELECT 391177 AS num) UNION ALL\n"
                      + "  (SELECT 391178 AS num) UNION ALL\n"
                      + "  (SELECT 391179 AS num) UNION ALL\n"
                      + "  (SELECT 391180 AS num) UNION ALL\n"
                      + "  (SELECT 391181 AS num) UNION ALL\n"
                      + "  (SELECT 391182 AS num) UNION ALL\n"
                      + "  (SELECT 391183 AS num) UNION ALL\n"
                      + "  (SELECT 391184 AS num) UNION ALL\n"
                      + "  (SELECT 391185 AS num) UNION ALL\n"
                      + "  (SELECT 391186 AS num) UNION ALL\n"
                      + "  (SELECT 391187 AS num) UNION ALL\n"
                      + "  (SELECT 391188 AS num) UNION ALL\n"
                      + "  (SELECT 391189 AS num))\n"
                      + "SELECT count(*) from d d1, d d2, d d3, d d4, d d5, d d6, d d7, d d8, d d9"
                      + " LIMIT "
                      + limit;
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

  @Test
  public void syncQueryCancel() throws SQLException, InterruptedException, IOException {
    BQConnection bq = conn();
    TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq);
    stmt.setTestPoint();
    Thread backgroundThread = getAndRunBackgroundQuery(stmt);
    stmt.waitForTestPoint();
    assertEquals(1, bq.getNumberRunningQueries());
    stmt.cancel();
    backgroundThread.join();
    SQLException exception = expectedSqlException.get();
    Assertions.assertEquals(
        "Job execution was cancelled: User requested cancellation",
        ((com.google.api.client.googleapis.json.GoogleJsonResponseException) exception.getCause())
            .getDetails()
            .getMessage());
  }

  @Test
  public void syncQueryCancel2() throws SQLException, InterruptedException, IOException {
    BQConnection bq = conn();
    TestableBQStatement stmt = new TestableBQStatement(bq.getProjectId(), bq);
    stmt.setTestPoint();
    Thread backgroundThread = getAndRunBackgroundQuery(stmt);
    stmt.waitForTestPoint();
    Thread.sleep(
        BQStatement.SYNC_TIMEOUT_MILLIS
            + 1000); // wait for the sync part of the query to definitely finish
    assertEquals(1, bq.getNumberRunningQueries());
    stmt.cancel();
    backgroundThread.join();
    SQLException exception = expectedSqlException.get();
    Assertions.assertEquals(
        "Job execution was cancelled: User requested cancellation",
        ((com.google.api.client.googleapis.json.GoogleJsonResponseException) exception.getCause())
            .getDetails()
            .getMessage());
  }

  @Test
  public void noCancelOnCloseAfterSyncQueryCompletion() throws SQLException, IOException {
    BQConnection bq = conn();
    TestableBQStatement stmt =
        new TestableBQStatement(bq.getProjectId(), bq) {
          @Override
          protected void performQueryCancel(JobReference jobRefToCancel) {
            throw new RuntimeException("don't cancel in this test");
          }
        };
    stmt.executeQuery("SELECT 1");
    stmt.close();
    assertTrue(stmt.isClosed());
  }

  @Test
  public void connectionCancelWorks() throws SQLException, InterruptedException, IOException {
    BQConnection bq = conn();
    final TestableBQStatement stmt1 = new TestableBQStatement(bq.getProjectId(), bq);
    final TestableBQStatement stmt2 = new TestableBQStatement(bq.getProjectId(), bq);
    stmt1.setTestPoint();
    stmt2.setTestPoint();
    Thread backgroundThread1 = getAndRunBackgroundQuery(stmt1);
    Thread backgroundThread2 = getAndRunBackgroundQuery(stmt2);
    stmt1.waitForTestPoint();
    stmt2.waitForTestPoint();
    assertTrue(bq.getNumberRunningQueries() == 2, "Must see both running queries");
    assertTrue(bq.cancelRunningQueries() == 0, "Must not fail to cancel queries");
    backgroundThread1.join();
    backgroundThread2.join();
  }

  @Test
  public void connectionCloseWithSyncQueryApi()
      throws SQLException, InterruptedException, IOException {
    BQConnection bq = conn();
    final TestableBQStatement stmt1 = new TestableBQStatement(bq.getProjectId(), bq);
    stmt1.setTestPoint();
    Thread backgroundThread = getAndRunBackgroundQuery(stmt1);
    stmt1.waitForTestPoint();
    bq.close();
    backgroundThread.join();
  }

  @Test
  public void handlesNullResultsWhilePollingJob()
      throws SQLException, IOException, InterruptedException {
    // We want to check a few possibilites if BQ kills the query job prematurely due to reservation
    // quotas. We're not guaranteed a complete job object when we're polling for completion in the
    // `BQStatement.runSync` loop.
    Job[] jobs =
        new Job[] {
          new Job(),
          new Job().setStatus(new JobStatus()),
          new Job()
              .setStatus(new JobStatus().setState("PENDING"))
              .setStatistics(new JobStatistics())
        };
    BQConnection bq = conn();
    for (Job j : jobs) {
      final TestableBQStatementWithBadPollJobs stmt1 =
          new TestableBQStatementWithBadPollJobs(bq.getProjectId(), bq);
      stmt1.pollJob = j;
      stmt1.setTestPoint();
      Thread backgroundThread = getAndRunBackgroundQuery(stmt1);
      stmt1.waitForTestPoint();
      backgroundThread.join();
      SQLException exception = expectedSqlException.get();
      assertPollJobException(exception);
    }
  }

  @Test
  public void handlesLoggingBQJobStatus() throws IOException {
    // Check that the job status is correctly retrieved if query job has all
    // required attributes (status, statistics, and job reference). Previous
    // test verifies that exception is thrown if one or more of these is missing.
    Job testJob =
        new Job()
            .setStatus(new JobStatus().setState("PENDING"))
            .setStatistics(new JobStatistics().setCreationTime(12345567789L))
            .setJobReference(new JobReference().setJobId("Job Id"));
    String result = BQSupportFuncts.logAndGetQueryState(testJob);
    Assertions.assertEquals(result, "PENDING");
  }

  @Test
  public void handlesNullCreationTime() throws IOException {
    // Check that proper exception is thrown if creation time is null while
    // logging job status.
    Job testJob =
        new Job()
            .setStatus(new JobStatus().setState("PENDING"))
            .setStatistics(new JobStatistics().setEndTime(0L))
            .setJobReference(new JobReference().setJobId("Job Id"));
    Throwable exception =
        assertThrows(
            IllegalStateException.class, () -> BQSupportFuncts.logAndGetQueryState(testJob));
    assertEquals("Failed to fetch start or creation time.", exception.getMessage());
  }

  private void assertPollJobException(Exception exception) {
    Assertions.assertTrue(
        exception.getMessage().contains("Something went wrong getting results for the job"));
    Assertions.assertEquals(exception.getClass(), BQSQLException.class);
    Assertions.assertEquals(exception.getCause().getMessage(), "Failed to fetch query state.");
    Assertions.assertEquals(exception.getCause().getClass(), IOException.class);
  }

  private static class TestableBQStatementWithBadPollJobs extends TestableBQStatement {
    public Job pollJob;

    public TestableBQStatementWithBadPollJobs(String projectId, BQConnection bqConnection) {
      super(projectId, bqConnection);
    }

    @Override
    protected Job getPollJob(Job jobToPoll) throws IOException {
      return pollJob;
    }
  }

  private static class TestableBQStatement extends BQStatement {
    public TestableBQStatement(String projectId, BQConnection bqConnection) {
      super(projectId, bqConnection);
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
    protected QueryResponse runSyncQuery(String querySql, boolean unlimitedBillingBytes)
        throws IOException, SQLException {
      Thread signaler =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    Thread.sleep(
                        1000); // make absolutely sure we yield to the execution below so it gets
                    // through first
                  } catch (InterruptedException e) {
                    throw new RuntimeException();
                  }
                  signalTestPoint();
                }
              });
      signaler.start();
      return super.runSyncQuery(querySql, unlimitedBillingBytes);
    }
  }
}
