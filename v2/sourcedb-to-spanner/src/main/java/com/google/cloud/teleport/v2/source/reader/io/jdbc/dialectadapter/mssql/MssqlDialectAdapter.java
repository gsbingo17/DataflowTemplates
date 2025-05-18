/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mssql;

import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link DialectAdapter} for MSSQL. */
public class MssqlDialectAdapter implements DialectAdapter {

  private static final Logger logger = LoggerFactory.getLogger(MssqlDialectAdapter.class);

  @Override
  public ImmutableList<String> discoverTables(
      javax.sql.DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    List<String> tables = new ArrayList<>();
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs = metaData.getTables(null, null, "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
    return ImmutableList.copyOf(tables);
  }

  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference schemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();

    try (Connection connection = dataSource.getConnection()) {
      for (String tableName : tables) {
        ImmutableMap.Builder<String, SourceColumnType> tableSchema = ImmutableMap.builder();
        List<SourceColumnType> columnTypes = getColumnTypes(connection, tableName);

        try (Statement stmt = connection.createStatement()) {
          // MSSQL-specific query to get column information
          String query =
              String.format(
                  "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
                      + "FROM INFORMATION_SCHEMA.COLUMNS "
                      + "WHERE TABLE_NAME = '%s' "
                      + "ORDER BY ORDINAL_POSITION",
                  tableName);

          try (ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
              String columnName = rs.getString("COLUMN_NAME");
              String dataType = rs.getString("DATA_TYPE");

              // Create mods array with precision and scale for numeric types
              Long[] mods = null;
              if (dataType.equals("decimal")
                  || dataType.equals("numeric")
                  || dataType.equals("money")
                  || dataType.equals("smallmoney")) {
                int precision = rs.getInt("NUMERIC_PRECISION");
                int scale = rs.getInt("NUMERIC_SCALE");
                mods = new Long[] {(long) precision, (long) scale};
              } else if (dataType.equals("varchar")
                  || dataType.equals("nvarchar")
                  || dataType.equals("char")
                  || dataType.equals("nchar")
                  || dataType.equals("binary")
                  || dataType.equals("varbinary")) {
                // For string and binary types, get the max length
                long maxLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                if (!rs.wasNull()) {
                  mods = new Long[] {maxLength};
                }
              }

              tableSchema.put(columnName, new SourceColumnType(dataType.toUpperCase(), mods, null));
            }
          }
        }

        result.put(tableName, tableSchema.build());
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }

    return result.build();
  }

  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result =
        ImmutableMap.builder();

    try (Connection connection = dataSource.getConnection()) {
      for (String tableName : tables) {
        List<SourceColumnIndexInfo> indexes = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        // First, get column type information for this table
        Map<String, String> columnTypes = new HashMap<>();
        Map<String, Integer> columnMaxLengths = new HashMap<>();

        try (Statement stmt = connection.createStatement()) {
          String columnQuery =
              String.format(
                  "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
                      + "FROM INFORMATION_SCHEMA.COLUMNS "
                      + "WHERE TABLE_NAME = '%s'",
                  tableName);

          try (ResultSet colRs = stmt.executeQuery(columnQuery)) {
            while (colRs.next()) {
              String colName = colRs.getString("COLUMN_NAME");
              String dataType = colRs.getString("DATA_TYPE");
              columnTypes.put(colName, dataType);

              // For string types, get the max length
              if (isStringType(dataType)) {
                int maxLength = colRs.getInt("CHARACTER_MAXIMUM_LENGTH");
                if (!colRs.wasNull()) {
                  columnMaxLengths.put(colName, maxLength);
                }
              }
            }
          }
        }

        // Get collation information for the database
        String dbCollation = null;
        try (Statement stmt = connection.createStatement()) {
          try (ResultSet colRs =
              stmt.executeQuery("SELECT SERVERPROPERTY('Collation') AS Collation")) {
            if (colRs.next()) {
              dbCollation = colRs.getString("Collation");
            }
          }
        }

        // Create a collation reference if we have a valid collation
        com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper
                .CollationReference
            collationRef = null;
        if (dbCollation != null) {
          collationRef =
              com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper
                  .CollationReference.builder()
                  .setDbCharacterSet("UTF-8") // Default character set for SQL Server
                  .setDbCollation(dbCollation)
                  .setPadSpace(false) // Default for SQL Server
                  .build();
        }

        try (ResultSet rs = metaData.getIndexInfo(null, null, tableName, false, false)) {
          while (rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            String columnName = rs.getString("COLUMN_NAME");
            boolean nonUnique = rs.getBoolean("NON_UNIQUE");
            long ordinalPosition = rs.getInt("ORDINAL_POSITION");

            if (indexName != null && columnName != null) {
              SourceColumnIndexInfo.Builder builder =
                  SourceColumnIndexInfo.builder()
                      .setColumnName(columnName)
                      .setIndexName(indexName)
                      .setIsUnique(!nonUnique)
                      .setIsPrimary(false) // Will be set to true for primary keys below
                      .setOrdinalPosition(ordinalPosition)
                      .setCardinality(0); // Default cardinality

              // Set the appropriate index type based on the column data type
              String dataType = columnTypes.get(columnName);
              if (dataType != null) {
                if (isStringType(dataType)) {
                  builder.setIndexType(IndexType.STRING);
                  // For string columns, set collation and max length
                  if (collationRef != null) {
                    builder.setCollationReference(collationRef);
                  }
                  Integer maxLength = columnMaxLengths.get(columnName);
                  if (maxLength != null) {
                    builder.setStringMaxLength(maxLength);
                  } else {
                    // Default max length for string columns if not specified
                    builder.setStringMaxLength(255);
                  }
                } else if (isNumericType(dataType)) {
                  builder.setIndexType(IndexType.NUMERIC);
                } else if (isBinaryType(dataType)) {
                  builder.setIndexType(IndexType.BINARY);
                } else {
                  // Default to OTHER for unsupported types
                  builder.setIndexType(IndexType.OTHER);
                }
              } else {
                // If we couldn't determine the type, default to OTHER
                builder.setIndexType(IndexType.OTHER);
              }

              indexes.add(builder.build());
            }
          }
        }

        // Add primary key information
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, tableName)) {
          while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            String pkName = rs.getString("PK_NAME");
            long keySeq = rs.getInt("KEY_SEQ");

            SourceColumnIndexInfo.Builder builder =
                SourceColumnIndexInfo.builder()
                    .setColumnName(columnName)
                    .setIndexName(pkName != null ? pkName : "PRIMARY")
                    .setIsUnique(true)
                    .setIsPrimary(true)
                    .setOrdinalPosition(keySeq)
                    .setCardinality(0);

            // Set the appropriate index type based on the column data type
            String dataType = columnTypes.get(columnName);
            if (dataType != null) {
              if (isStringType(dataType)) {
                builder.setIndexType(IndexType.STRING);
                // For string columns, set collation and max length
                if (collationRef != null) {
                  builder.setCollationReference(collationRef);
                }
                Integer maxLength = columnMaxLengths.get(columnName);
                if (maxLength != null) {
                  builder.setStringMaxLength(maxLength);
                } else {
                  // Default max length for string columns if not specified
                  builder.setStringMaxLength(255);
                }
              } else if (isNumericType(dataType)) {
                builder.setIndexType(IndexType.NUMERIC);
              } else if (isBinaryType(dataType)) {
                builder.setIndexType(IndexType.BINARY);
              } else {
                // Default to OTHER for unsupported types
                builder.setIndexType(IndexType.OTHER);
              }
            } else {
              // If we couldn't determine the type, default to OTHER
              builder.setIndexType(IndexType.OTHER);
            }

            indexes.add(builder.build());
          }
        }

        result.put(tableName, ImmutableList.copyOf(indexes));
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }

    return result.build();
  }

  /**
   * Determines if the given SQL Server data type is a string type.
   *
   * @param dataType The SQL Server data type
   * @return true if the data type is a string type, false otherwise
   */
  private boolean isStringType(String dataType) {
    return dataType != null
        && (dataType.equalsIgnoreCase("varchar")
            || dataType.equalsIgnoreCase("nvarchar")
            || dataType.equalsIgnoreCase("char")
            || dataType.equalsIgnoreCase("nchar")
            || dataType.equalsIgnoreCase("text")
            || dataType.equalsIgnoreCase("ntext"));
  }

  /**
   * Determines if the given SQL Server data type is a numeric type.
   *
   * @param dataType The SQL Server data type
   * @return true if the data type is a numeric type, false otherwise
   */
  private boolean isNumericType(String dataType) {
    return dataType != null
        && (dataType.equalsIgnoreCase("int")
            || dataType.equalsIgnoreCase("bigint")
            || dataType.equalsIgnoreCase("smallint")
            || dataType.equalsIgnoreCase("tinyint")
            || dataType.equalsIgnoreCase("decimal")
            || dataType.equalsIgnoreCase("numeric")
            || dataType.equalsIgnoreCase("money")
            || dataType.equalsIgnoreCase("smallmoney")
            || dataType.equalsIgnoreCase("float")
            || dataType.equalsIgnoreCase("real"));
  }

  /**
   * Determines if the given SQL Server data type is a binary type.
   *
   * @param dataType The SQL Server data type
   * @return true if the data type is a binary type, false otherwise
   */
  private boolean isBinaryType(String dataType) {
    return dataType != null
        && (dataType.equalsIgnoreCase("binary")
            || dataType.equalsIgnoreCase("varbinary")
            || dataType.equalsIgnoreCase("image"));
  }

  /**
   * Helper method to add a WHERE clause to a query based on partition columns. This method follows
   * the MySQL approach but uses MS SQL specific syntax.
   *
   * @param query The base query to add the WHERE clause to
   * @param partitionColumns The partition columns to include in the WHERE clause
   * @return The query with the WHERE clause added
   */
  private String addWhereClause(String query, ImmutableList<String> partitionColumns) {
    StringBuilder queryBuilder = new StringBuilder(query);
    boolean firstDone = false;

    for (String partitionColumn : partitionColumns) {
      if (firstDone) {
        // Add AND for iteration after first
        queryBuilder.append(" AND ");
      } else {
        // Add WHERE only for first iteration
        queryBuilder.append(" WHERE ");
      }

      // Clean the partition column name (MS SQL specific)
      String cleanPartitionCol = partitionColumn;
      if (cleanPartitionCol.startsWith("\"") && cleanPartitionCol.endsWith("\"")) {
        cleanPartitionCol =
            cleanPartitionCol.substring(1, cleanPartitionCol.length() - 1).replace("\"\"", "\"");
      }

      // Include the column?
      // Using MS SQL syntax: ((? = 0) OR ...) instead of MySQL's ((? = FALSE) OR ...)
      queryBuilder.append("((? = 0) OR ");
      // Range to define the WHERE clause
      queryBuilder.append(
          String.format(
              "([%1$s] >= ? AND ([%1$s] < ? OR (? = 1 AND [%1$s] = ?)))", cleanPartitionCol));
      queryBuilder.append(")");

      firstDone = true;
    }

    return queryBuilder.toString();
  }

  @Override
  public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
    // Remove any quotes from tableName
    String cleanTableName = tableName;
    if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
      cleanTableName = tableName.substring(1, tableName.length() - 1).replace("\"\"", "\"");
    }

    String baseQuery = "SELECT * FROM [" + cleanTableName + "]";
    return addWhereClause(baseQuery, partitionColumns);
  }

  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    // Remove any quotes from tableName
    String cleanTableName = tableName;
    if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
      cleanTableName = tableName.substring(1, tableName.length() - 1).replace("\"\"", "\"");
    }

    String baseQuery = "SELECT COUNT(*) FROM [" + cleanTableName + "]";

    // We're using the JDBC driver's setQueryTimeout instead of SQL Server's OPTION (QUERY TIMEOUT)
    // to avoid syntax errors with certain query structures
    // The timeoutMillis parameter is handled at the JDBC connection level

    return addWhereClause(baseQuery, partitionColumns);
  }

  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    // Convert double quotes to SQL Server square brackets
    // (double quotes are added by JdbcIoWrapper.delimitIdentifier)
    String cleanColName;
    if (colName.startsWith("\"") && colName.endsWith("\"")) {
      // Extract the column name without quotes and handle escaped quotes
      cleanColName = colName.substring(1, colName.length() - 1).replace("\"\"", "\"");
    } else {
      cleanColName = colName;
    }

    // Remove any quotes from tableName as well
    String cleanTableName = tableName;
    if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
      cleanTableName = tableName.substring(1, tableName.length() - 1).replace("\"\"", "\"");
    }

    String baseQuery =
        String.format(
            "SELECT MIN([%s]) as min_value, MAX([%s]) as max_value FROM [%s]",
            cleanColName, cleanColName, cleanTableName);

    String finalQuery = addWhereClause(baseQuery, partitionColumns);
    logger.debug(
        "Generated boundary query for table [{}], column [{}]: {}", tableName, colName, finalQuery);

    return finalQuery;
  }

  @Override
  public boolean checkForTimeout(SQLException exception) {
    // MSSQL timeout error codes
    // Error code -2146232060 (0x80131904) is for query timeout
    // SQLState "HYT00" is for timeout expired
    return exception.getErrorCode() == -2146232060
        || "HYT00".equals(exception.getSQLState())
        || exception.getMessage().toLowerCase().contains("timeout")
        || exception.getMessage().toLowerCase().contains("timed out");
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    // MSSQL-specific query to get collation order
    // This is a simplified implementation - in a real scenario, you would need to
    // generate a query that returns all characters in the character set with their collation order
    return String.format(
        "SELECT CHAR(number) AS character, UNICODE(CHAR(number)) AS unicode_value, "
            + "ASCII(CHAR(number)) AS ascii_value "
            + "FROM master.dbo.spt_values "
            + "WHERE type = 'P' AND number BETWEEN 1 AND 255 "
            + "ORDER BY CHAR(number) COLLATE %s",
        dbCollation);
  }

  private List<SourceColumnType> getColumnTypes(Connection connection, String tableName)
      throws SQLException {
    List<SourceColumnType> columnTypes = new ArrayList<>();

    try (Statement stmt = connection.createStatement()) {
      // MSSQL-specific query to get column information
      String query =
          String.format(
              "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
                  + "FROM INFORMATION_SCHEMA.COLUMNS "
                  + "WHERE TABLE_NAME = '%s' "
                  + "ORDER BY ORDINAL_POSITION",
              tableName);

      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          String dataType = rs.getString("DATA_TYPE");

          // Create mods array with precision and scale for numeric types
          Long[] mods = null;
          if (dataType.equals("decimal")
              || dataType.equals("numeric")
              || dataType.equals("money")
              || dataType.equals("smallmoney")) {
            int precision = rs.getInt("NUMERIC_PRECISION");
            int scale = rs.getInt("NUMERIC_SCALE");
            mods = new Long[] {(long) precision, (long) scale};
          } else if (dataType.equals("varchar")
              || dataType.equals("nvarchar")
              || dataType.equals("char")
              || dataType.equals("nchar")
              || dataType.equals("binary")
              || dataType.equals("varbinary")) {
            // For string and binary types, get the max length
            long maxLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
            if (!rs.wasNull()) {
              mods = new Long[] {maxLength};
            }
          }

          columnTypes.add(new SourceColumnType(dataType.toUpperCase(), mods, null));
        }
      }
    }

    return columnTypes;
  }

  public String getPartitionRangeQuery(
      String tableName, String partitionColumn, Object lowerBound, Object upperBound) {
    // MSSQL-specific range query
    return String.format(
        "SELECT * FROM [%s] WHERE [%s] >= ? AND [%s] < ?",
        tableName, partitionColumn, partitionColumn);
  }

  public String getPartitionColumnBoundaryQuery(String tableName, String partitionColumn) {
    // MSSQL-specific query to get min and max values for a column
    return String.format(
        "SELECT MIN([%s]) as min_value, MAX([%s]) as max_value FROM [%s]",
        partitionColumn, partitionColumn, tableName);
  }
}
