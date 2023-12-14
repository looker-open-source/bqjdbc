package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.assertj.core.api.Assertions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * Performance microbenchmark for stateless queries. This uses the example query from {@link
 * StatelessQuery}, same as the tests.
 *
 * <p>Run with mvn exec:exec@benchmark-stateless - note that mvn install must have been run at least
 * once before.
 *
 * <p>Representative summary as of 2023-12-14: <code>
 * Benchmark                                                     Mode  Cnt   Score    Error  Units
 * StatelessSmallQueryBenchmark.benchmarkSmallQueryOptionalJob  thrpt    5  67.994 ± 10.326  ops/s
 * StatelessSmallQueryBenchmark.benchmarkSmallQueryRequiredJob  thrpt    5  37.171 ±  3.041  ops/s
 * </code>
 */
@Fork(1)
@Threads(10)
@BenchmarkMode(Mode.Throughput)
public class StatelessSmallQueryBenchmark {
  private static final String CONNECTION_PROPERTIES = "installedaccount1.properties";

  @State(Scope.Thread)
  public static class RequiredJob {
    private BQConnection connection;

    @Setup(Level.Trial)
    public void connect() throws SQLException, IOException {
      connection = ConnectionFromResources.connect(CONNECTION_PROPERTIES, null);
    }

    @TearDown
    public void disconnect() throws SQLException {
      connection.close();
    }
  }

  @State(Scope.Thread)
  public static class OptionalJob {
    private BQConnection connection;

    @Setup(Level.Trial)
    public void connect() throws SQLException, IOException {
      connection =
          ConnectionFromResources.connect(
              CONNECTION_PROPERTIES, "&jobcreationmode=JOB_CREATION_OPTIONAL");
    }

    @TearDown
    public void disconnect() throws SQLException {
      connection.close();
    }
  }

  private String[][] benchmarkSmallQuery(final Connection connection) throws SQLException {
    final Statement statement = connection.createStatement();
    final ResultSet results = statement.executeQuery(StatelessQuery.exampleQuery());
    final String[][] rows = BQSupportMethods.GetQueryResult(results);
    Assertions.assertThat(rows).isEqualTo(StatelessQuery.exampleValues());
    return rows;
  }

  @Benchmark
  public String[][] benchmarkSmallQueryRequiredJob(final RequiredJob requiredJob)
      throws SQLException {
    return benchmarkSmallQuery(requiredJob.connection);
  }

  @Benchmark
  public String[][] benchmarkSmallQueryOptionalJob(final OptionalJob optionalJob)
      throws SQLException {
    return benchmarkSmallQuery(optionalJob.connection);
  }
}
