package net.starschema.clouddb.jdbc;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;

/** Helpers for tests that require projects with stateless queries enabled */
public final class StatelessQuery {

  private StatelessQuery() {}

  private static final Set<String> ENABLED_PROJECTS =
      ImmutableSet.of("disco-parsec-659", "looker-db-test");

  /**
   * Raise an {@link org.junit.AssumptionViolatedException} if the provided project isn't one that's
   * known to have stateless queries enabled
   *
   * @param project the project to check - get it from {@link BQConnection#getCatalog() }
   */
  public static void assumeStatelessQueriesEnabled(String project) {
    Assumptions.assumeTrue(ENABLED_PROJECTS.contains(project));
  }

  /**
   * A small query that should run statelessly (that is, without a job).
   *
   * @return the query
   */
  public static String exampleQuery() {
    return "SELECT 9876";
  }

  /**
   * The values returned by {@link StatelessQuery#exampleQuery()}
   *
   * @return An array of strings representing the returned values
   */
  public static String[][] exampleValues() {
    return new String[][] {new String[] {"9876"}};
  }
}
