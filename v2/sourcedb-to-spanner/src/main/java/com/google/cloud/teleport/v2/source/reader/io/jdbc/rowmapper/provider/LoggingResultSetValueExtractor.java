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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for {@link ResultSetValueExtractor} that adds detailed logging to help diagnose
 * extraction issues.
 *
 * @param <T> type of the value extracted.
 */
public class LoggingResultSetValueExtractor<T extends Object>
    implements ResultSetValueExtractor<T> {

  private static final Logger logger =
      LoggerFactory.getLogger(LoggingResultSetValueExtractor.class);

  private final ResultSetValueExtractor<T> delegate;
  private final String extractorType;

  /**
   * Construct a new {@link LoggingResultSetValueExtractor}.
   *
   * @param delegate The underlying extractor to delegate to.
   * @param extractorType A descriptive name for the type of extractor.
   */
  public LoggingResultSetValueExtractor(ResultSetValueExtractor<T> delegate, String extractorType) {
    this.delegate = delegate;
    this.extractorType = extractorType;
  }

  /**
   * Extract the requested field from the result set with detailed logging.
   *
   * @param rs resultSet.
   * @param fieldName name of the field to extract.
   * @return extracted value.
   * @throws SQLException Any exception thrown by ResultSet API.
   */
  @Nullable
  @Override
  public T extract(ResultSet rs, String fieldName) throws SQLException {
    logger.info("Extracting field '{}' using {} extractor", fieldName, extractorType);

    try {
      // Check if the column exists in the ResultSet
      int columnIndex = rs.findColumn(fieldName);
      logger.info("Field '{}' found at index {} in ResultSet", fieldName, columnIndex);

      // Try to get the column type
      try {
        int columnType = rs.getMetaData().getColumnType(columnIndex);
        String columnTypeName = rs.getMetaData().getColumnTypeName(columnIndex);
        logger.info("Field '{}' has SQL type {} ({})", fieldName, columnTypeName, columnType);
      } catch (SQLException e) {
        logger.warn("Could not get type information for field '{}': {}", fieldName, e.getMessage());
      }

      // Extract the value using the delegate
      T value;
      try {
        value = delegate.extract(rs, fieldName);
        logger.info(
            "Successfully extracted value for field '{}': {}",
            fieldName,
            value == null ? "NULL" : value.toString());

        // Check if the field was NULL
        boolean wasNull = rs.wasNull();
        logger.info("Field '{}' wasNull={}", fieldName, wasNull);

        return value;
      } catch (SQLException e) {
        logger.error("Error extracting value for field '{}': {}", fieldName, e.getMessage());
        throw e;
      }
    } catch (SQLException e) {
      logger.error("Field '{}' not found in ResultSet: {}", fieldName, e.getMessage());
      throw e;
    }
  }

  /**
   * Create a new logging extractor that wraps the given delegate.
   *
   * @param delegate The extractor to wrap.
   * @param extractorType A descriptive name for the type of extractor.
   * @return A new logging extractor.
   */
  public static <T> LoggingResultSetValueExtractor<T> wrap(
      ResultSetValueExtractor<T> delegate, String extractorType) {
    return new LoggingResultSetValueExtractor<>(delegate, extractorType);
  }
}
