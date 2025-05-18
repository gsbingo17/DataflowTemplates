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
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang3.tuple.Pair;

/** MSSQL data type mapping to AVRO types. */
public class MssqlJdbcValueMappingsProvider implements JdbcValueMappingsProvider {

  private static final Calendar UTC_CALENDAR =
      Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueExtractor<ByteBuffer> bytesExtractor =
      (rs, fieldName) -> {
        byte[] bytes = rs.getBytes(fieldName);
        if (bytes == null) {
          return null;
        }
        return ByteBuffer.wrap(bytes);
      };

  private static final ResultSetValueExtractor<java.sql.Date> dateExtractor =
      (rs, fieldName) -> rs.getDate(fieldName, UTC_CALENDAR);

  private static final ResultSetValueExtractor<java.sql.Timestamp> timestampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName, UTC_CALENDAR);

  // Value might be a Double.NaN or a valid BigDecimal
  private static final ResultSetValueMapper<Number> numericToAvro =
      (value, schema) -> value.toString();

  private static final ResultSetValueMapper<java.sql.Date> dateToAvro =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  private static final ResultSetValueMapper<java.sql.Timestamp> timestampToAvro =
      (value, schema) -> {
        long epochSeconds = value.getTime() / 1000;
        int nanos = value.getNanos();
        return epochSeconds * 1_000_000L + nanos / 1000;
      };

  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BINARY", Pair.of(bytesExtractor, valuePassThrough))
          .put("BIT", Pair.of(ResultSet::getBoolean, valuePassThrough))
          .put("CHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DATE", Pair.of(dateExtractor, dateToAvro))
          .put("DATETIME", Pair.of(timestampExtractor, timestampToAvro))
          .put("DATETIME2", Pair.of(timestampExtractor, timestampToAvro))
          .put("DATETIMEOFFSET", Pair.of(timestampExtractor, timestampToAvro))
          .put("DECIMAL", Pair.of(ResultSet::getBigDecimal, numericToAvro))
          .put("FLOAT", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("IMAGE", Pair.of(bytesExtractor, valuePassThrough))
          .put("INT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("MONEY", Pair.of(ResultSet::getBigDecimal, numericToAvro))
          .put("NCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("NTEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("NUMERIC", Pair.of(ResultSet::getBigDecimal, numericToAvro))
          .put("NVARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("REAL", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("SMALLDATETIME", Pair.of(timestampExtractor, timestampToAvro))
          .put("SMALLINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SMALLMONEY", Pair.of(ResultSet::getBigDecimal, numericToAvro))
          .put("TEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TIME", Pair.of(ResultSet::getTime, valuePassThrough))
          .put("TIMESTAMP", Pair.of(bytesExtractor, valuePassThrough))
          .put("TINYINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("UNIQUEIDENTIFIER", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARBINARY", Pair.of(bytesExtractor, valuePassThrough))
          .put("VARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("XML", Pair.of(ResultSet::getString, valuePassThrough))
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
