Demonstration for a bug in Postgresql's JDBC driver.

When several repeated queries for empty array are made in a single connection, the `ResultSetMetadata` of the
array is incorrectly populated by `readBinaryResultSet` of `org.postgresql.jdbc2.AbstractJdbc2Array`.
This causes `NullPointerException`s to be thrown by `ResultSetMetadata`s methods.
