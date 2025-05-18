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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MSSQL data type mapping to AVRO types.
 *
 * <p>This class provides mappings from SQL Server data types to Avro types, with specialized
 * handling for various SQL Server-specific types.
 */
public class MssqlJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Logger logger = LoggerFactory.getLogger(MssqlJdbcValueMappings.class);

  private static final Calendar UTC_CALENDAR =
      Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

  /**
   * Pass the value extracted from {@link ResultSet} to {@link GenericRecordBuilder#set}. Most basic
   * types don't need any marshalling and can be directly passed through.
   */
  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  /** Helper method to convert an Instant to microseconds since epoch. */
  private static long instantToMicros(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  /**
   * Wrap all extractors with LoggingResultSetValueExtractor for detailed logging. This helps
   * diagnose issues with data extraction from the ResultSet.
   */
  private static final ResultSetValueExtractor<ByteBuffer> bytesExtractor =
      LoggingResultSetValueExtractor.wrap(
          (rs, fieldName) -> {
            byte[] bytes = rs.getBytes(fieldName);
            if (bytes == null) {
              return null;
            }
            return ByteBuffer.wrap(bytes);
          },
          "bytes");

  private static final ResultSetValueExtractor<java.sql.Date> dateExtractor =
      LoggingResultSetValueExtractor.wrap(
          (rs, fieldName) -> rs.getDate(fieldName, UTC_CALENDAR), "date");

  private static final ResultSetValueExtractor<java.sql.Timestamp> timestampExtractor =
      LoggingResultSetValueExtractor.wrap(
          (rs, fieldName) -> rs.getTimestamp(fieldName, UTC_CALENDAR), "timestamp");

  private static final ResultSetValueExtractor<String> stringExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getString, "string");

  private static final ResultSetValueExtractor<Integer> intExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getInt, "int");

  private static final ResultSetValueExtractor<Long> longExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getLong, "long");

  private static final ResultSetValueExtractor<Boolean> booleanExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getBoolean, "boolean");

  private static final ResultSetValueExtractor<java.math.BigDecimal> bigDecimalExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getBigDecimal, "bigDecimal");

  private static final ResultSetValueExtractor<Double> doubleExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getDouble, "double");

  private static final ResultSetValueExtractor<Float> floatExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getFloat, "float");

  private static final ResultSetValueExtractor<java.sql.Time> timeExtractor =
      LoggingResultSetValueExtractor.wrap(ResultSet::getTime, "time");

  /** Map BigDecimal type to Byte array for Avro decimal logical type. */
  private static final ResultSetValueMapper<BigDecimal> bigDecimalToByteArray =
      (value, schema) -> {
        logger.info("Converting BigDecimal {} to byte array with schema {}", value, schema);
        // Get the scale from the schema
        int scale = (int) schema.getObjectProp("scale");
        // Set the scale of the BigDecimal to match the schema
        BigDecimal scaledValue = value.setScale(scale, RoundingMode.HALF_DOWN);
        // Convert to byte array and wrap in ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(scaledValue.unscaledValue().toByteArray());
        logger.info("Converted to ByteBuffer: {}", byteBuffer);
        return byteBuffer;
      };

  /** Map BigDecimal to string for non-decimal types. */
  private static final ResultSetValueMapper<Number> numericToString =
      (value, schema) -> value.toString();

  private static final ResultSetValueMapper<java.sql.Date> dateToAvro =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  /** Map {@link java.sql.Timestamp} to {@link DateTime#SCHEMA} generic record. */
  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroDateTime =
      (value, schema) ->
          new GenericRecordBuilder(DateTime.SCHEMA)
              .set(
                  DateTime.DATE_FIELD_NAME,
                  (int) value.toLocalDateTime().toLocalDate().toEpochDay())
              .set(
                  DateTime.TIME_FIELD_NAME,
                  TimeUnit.NANOSECONDS.toMicros(
                      value.toLocalDateTime().toLocalTime().toNanoOfDay()))
              .build();

  /** Map {@link java.sql.Timestamp} to Avro timestamp-micros logical type. */
  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroTimestampMicros =
      (value, schema) -> {
        logger.info("Converting Timestamp {} to microseconds since epoch", value);
        long micros = instantToMicros(value.toInstant());
        logger.info("Converted to microseconds since epoch: {}", micros);
        return micros;
      };

  /** Map {@link java.sql.Time} to Avro time-micros logical type. */
  private static final ResultSetValueMapper<java.sql.Time> sqlTimeToAvroTimeMicros =
      (value, schema) -> {
        logger.info("Converting Time {} to microseconds since midnight", value);
        long micros = TimeUnit.NANOSECONDS.toMicros(value.toLocalTime().toNanoOfDay());
        logger.info("Converted to microseconds since midnight: {}", micros);
        return micros;
      };

  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(longExtractor, valuePassThrough))
          .put("BINARY", Pair.of(bytesExtractor, valuePassThrough))
          .put("BIT", Pair.of(booleanExtractor, valuePassThrough))
          .put("CHAR", Pair.of(stringExtractor, valuePassThrough))
          .put("DATE", Pair.of(dateExtractor, dateToAvro))
          .put("DATETIME", Pair.of(timestampExtractor, sqlTimestampToAvroDateTime))
          .put("DATETIME2", Pair.of(timestampExtractor, sqlTimestampToAvroDateTime))
          .put("DATETIMEOFFSET", Pair.of(timestampExtractor, sqlTimestampToAvroDateTime))
          .put("DECIMAL", Pair.of(bigDecimalExtractor, bigDecimalToByteArray))
          .put("FLOAT", Pair.of(doubleExtractor, valuePassThrough))
          .put("IMAGE", Pair.of(bytesExtractor, valuePassThrough))
          .put("INT", Pair.of(intExtractor, valuePassThrough))
          .put("MONEY", Pair.of(bigDecimalExtractor, bigDecimalToByteArray))
          .put("NCHAR", Pair.of(stringExtractor, valuePassThrough))
          .put("NTEXT", Pair.of(stringExtractor, valuePassThrough))
          .put("NUMERIC", Pair.of(bigDecimalExtractor, bigDecimalToByteArray))
          .put("NVARCHAR", Pair.of(stringExtractor, valuePassThrough))
          .put("REAL", Pair.of(floatExtractor, valuePassThrough))
          .put("SMALLDATETIME", Pair.of(timestampExtractor, sqlTimestampToAvroDateTime))
          .put("SMALLINT", Pair.of(intExtractor, valuePassThrough))
          .put("SMALLMONEY", Pair.of(bigDecimalExtractor, bigDecimalToByteArray))
          .put("TEXT", Pair.of(stringExtractor, valuePassThrough))
          .put("TIME", Pair.of(timeExtractor, sqlTimeToAvroTimeMicros))
          .put("TIMESTAMP", Pair.of(timestampExtractor, sqlTimestampToAvroTimestampMicros))
          .put("TINYINT", Pair.of(intExtractor, valuePassThrough))
          .put("UNIQUEIDENTIFIER", Pair.of(stringExtractor, valuePassThrough))
          .put("VARBINARY", Pair.of(bytesExtractor, valuePassThrough))
          .put("VARCHAR", Pair.of(stringExtractor, valuePassThrough))
          .put("XML", Pair.of(stringExtractor, valuePassThrough))
          .build()
          .entrySet()
          .stream()
          .map(
              entry ->
                  Map.entry(
                      entry.getKey(),
                      new JdbcValueMapper<>(
                          entry.getValue().getLeft(), entry.getValue().getRight())))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

  /** Get static mapping of SourceColumnType to {@link JdbcValueMapper}. */
  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return SCHEMA_MAPPINGS;
  }
}
