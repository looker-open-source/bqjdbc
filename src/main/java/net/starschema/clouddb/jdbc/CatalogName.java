package net.starschema.clouddb.jdbc;

/**
 * Utility class to translate between JDBC Catalog names and BigQuery Project Ids.
 *
 * <p>This is primarily here to maintain compatibility with older versions of this driver which
 * accepted an encoded version of the ProjectId as the Catalog name. So as not to break existing
 * clients, we will still accept this encoding as part of the JDBC URL identifier.
 *
 * <p>The encoding affects Projects which are prefixed with a domain, such as
 * "looker.com:sample-project". These may be selected with a Catalog name of
 * "looker_com__sample-project", where the period (`.`) is replaced wih a single underscore (`_`)
 * and the colon (`:`) is replaced with two underscores (`__`).
 */
public final class CatalogName {
  public static String toProjectId(String encodedCatalogName) {
    if (encodedCatalogName == null) {
      return null;
    } else {
      return encodedCatalogName.replace("__", ":").replace("_", ".");
    }
  }
}
