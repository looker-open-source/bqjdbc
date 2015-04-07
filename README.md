# starschema-bigquery-jdbc
Fork of Starschema's JDBC driver for BigQuery. https://code.google.com/p/starschema-bigquery-jdbc/

Various fixes to have a production ready driver:

- Type fixing (using BigInt/Long and Double as per BigQuery specifications)
- Timestamp support
- Handle null as required by JDBC spec

Improvements done by jsiessataccess
