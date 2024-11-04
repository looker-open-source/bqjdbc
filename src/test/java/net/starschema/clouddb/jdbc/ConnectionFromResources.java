package net.starschema.clouddb.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to enable BigQuery connections from properties resources. */
public class ConnectionFromResources {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionFromResources.class);

  /**
   * Connect to a BigQuery project as described in a properties file
   *
   * @param propertiesFilePath the path to the properties in file in src/test/resources
   * @param extraUrl extra URL arguments to add; must start with &
   * @return A {@link BQConnection} connected to the given database
   * @throws IOException if the properties file cannot be read
   * @throws SQLException if the configuration can't connect to BigQuery
   */
  public static BQConnection connect(String propertiesFilePath, String extraUrl)
      throws IOException, SQLException {
    final Properties properties = new Properties();
    final ClassLoader loader = ConnectionFromResources.class.getClassLoader();
    try (InputStream stream = loader.getResourceAsStream(propertiesFilePath)) {
      properties.load(stream);
    } catch (NullPointerException e) {
      File file = new File(propertiesFilePath);
      properties.load(new FileInputStream(file));
    }
    final StringBuilder jdcbUrlBuilder =
        new StringBuilder(BQSupportFuncts.constructUrlFromPropertiesFile(properties));
    if (extraUrl != null) {
      jdcbUrlBuilder.append(extraUrl);
    }
    final String jdbcUrl = jdcbUrlBuilder.toString();

    final Connection connection = DriverManager.getConnection(jdbcUrl, properties);
    final BQConnection bqConnection = (BQConnection) connection;
    logger.info("Created connection from {} to {}", propertiesFilePath, bqConnection.getURLPART());
    return bqConnection;
  }
}
