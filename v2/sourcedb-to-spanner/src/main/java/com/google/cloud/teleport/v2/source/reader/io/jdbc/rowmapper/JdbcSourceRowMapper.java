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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.source.reader.io.exception.ValueMappingException;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implement the {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper} interface. */
public final class JdbcSourceRowMapper implements JdbcIO.RowMapper<SourceRow> {
  private final JdbcValueMappingsProvider mappingsProvider;

  private final SourceSchemaReference sourceSchemaReference;

  private final SourceTableSchema sourceTableSchema;

  @Nullable private final String shardId;

  private static final Logger logger = LoggerFactory.getLogger(JdbcSourceRowMapper.class);

  private final Counter mapperErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_MAPPING_ERRORS);

  /**
   * Construct {@link JdbcSourceRowMapper}.
   *
   * @param mappingsProvider Mapping Provider based on the type of database.
   * @param sourceTableSchema Schema of source table.
   */
  public JdbcSourceRowMapper(
      JdbcValueMappingsProvider mappingsProvider,
      SourceSchemaReference sourceSchemaReference,
      SourceTableSchema sourceTableSchema,
      String shardId) {
    this.mappingsProvider = mappingsProvider;
    this.sourceSchemaReference = sourceSchemaReference;
    this.sourceTableSchema = sourceTableSchema;
    this.shardId = shardId;
  }

  long getCurrentTimeMicros() {
    Instant now = Instant.now();
    long nanos = TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    return TimeUnit.NANOSECONDS.toMicros(nanos);
  }

  /**
   * Map {@link java.sql.ResultSet} to {@link SourceRow}.
   *
   * @param resultSet the resultSet for a read record.
   * @return SourceRow
   */
  @Override
  public @UnknownKeyFor @Nullable @Initialized SourceRow mapRow(
      @UnknownKeyFor @NonNull @Initialized ResultSet resultSet) {
    var builder =
        SourceRow.builder(
            sourceSchemaReference, sourceTableSchema, shardId, getCurrentTimeMicros());

    // Log the metadata about the ResultSet
    try {
      java.sql.ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      logger.info("ResultSet has {} columns", columnCount);
      for (int i = 1; i <= columnCount; i++) {
        logger.info(
            "Column {}: Name={}, Type={}, TypeName={}",
            i,
            metaData.getColumnName(i),
            metaData.getColumnType(i),
            metaData.getColumnTypeName(i));
      }
    } catch (SQLException e) {
      logger.warn("Failed to get ResultSet metadata", e);
    }

    this.sourceTableSchema
        .sourceColumnNameToSourceColumnType()
        .entrySet()
        .forEach(
            entry -> {
              try {
                String columnName = entry.getKey();
                String columnType = entry.getValue().getName().toUpperCase();

                logger.info("Processing column: Name={}, Type={}", columnName, columnType);

                Schema schema =
                    this.sourceTableSchema.getAvroPayload().getField(columnName).schema();
                // The Unified avro mapping produces a union of the mapped type with null type
                // except for "Unsupported" case.
                if (schema.isUnion()) {
                  schema = schema.getTypes().get(1);
                }

                // Get the appropriate mapper for this column type
                JdbcValueMapper<?> mapper =
                    this.mappingsProvider
                        .getMappings()
                        .getOrDefault(columnType, JdbcValueMapper.UNSUPPORTED);

                logger.info(
                    "Using mapper for column {}: {}",
                    columnName,
                    mapper == JdbcValueMapper.UNSUPPORTED ? "UNSUPPORTED" : "SUPPORTED");

                // Extract and map the value
                Object value = mapper.mapValue(resultSet, columnName, schema);

                logger.info(
                    "Mapped value for column {}: {}",
                    columnName,
                    value == null ? "NULL" : value.toString());

                builder.setField(columnName, value);
              } catch (SQLException e) {
                mapperErrors.inc();
                logger.error(
                    "Exception while mapping jdbc ResultSet to avro. Check for potential schema changes or unexpected inaccuracy in schema discovery logs. SourceSchemaReference: {},  SourceTableSchema: {}. Exception: {}",
                    sourceSchemaReference,
                    sourceTableSchema,
                    e);
                throw new ValueMappingException(e);
              }
            });
    return builder.build();
  }
}
