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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQueryPreparedStatementSetter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryExtractorFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to find boundary (min, max) for a column (optionally for a given parent range). */
final class RangeBoundaryDoFn extends DoFn<ColumnForBoundaryQuery, Range> implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(RangeBoundaryDoFn.class);
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;

  private final UniformSplitterDBAdapter dbAdapter;

  private String tableName;
  private ImmutableList<String> partitionColumns;

  @Nullable private BoundaryTypeMapper boundaryTypeMapper;

  @JsonIgnore private transient @Nullable DataSource dataSource;

  RangeBoundaryDoFn(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      UniformSplitterDBAdapter dbAdapter,
      String tableName,
      ImmutableList<String> partitionColumns,
      BoundaryTypeMapper boundaryTypeMapper) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.dbAdapter = dbAdapter;
    this.tableName = tableName;
    this.partitionColumns = partitionColumns;
    this.dataSource = null;
    this.boundaryTypeMapper = boundaryTypeMapper;
  }

  @Setup
  public void setup() throws Exception {
    dataSource = dataSourceProviderFn.apply(null);
  }

  private Connection acquireConnection() throws SQLException {
    return checkStateNotNull(this.dataSource).getConnection();
  }

  /**
   * DoFn to find boundary (min, max) for a column (optionally for a given parent range).
   *
   * @param input Details for the column and parent range for which a boundary is requested.
   * @param out new Range with column boundary.
   * @param c process context.
   * @throws SQLException - since this is in the run time, beam will auto retry the exception.
   */
  @ProcessElement
  public void processElement(
      @Element ColumnForBoundaryQuery input, OutputReceiver<Range> out, ProcessContext c)
      throws SQLException {
    String boundaryQuery =
        dbAdapter.getBoundaryQuery(tableName, partitionColumns, input.columnName());

    logger.info(
        "Executing boundary query for column: {}, query: {}", input.columnName(), boundaryQuery);

    try (Connection conn = acquireConnection()) {
      // Set query timeout to avoid hanging
      PreparedStatement stmt =
          conn.prepareStatement(
              boundaryQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout(30); // 30 seconds timeout

      logger.info("Setting parameters for boundary query");
      new ColumnForBoundaryQueryPreparedStatementSetter(partitionColumns)
          .setParameters(input, stmt);

      logger.info("Executing boundary query for column: {}", input.columnName());
      ResultSet rs = stmt.executeQuery();

      logger.info("Processing boundary query results for column: {}", input.columnName());
      Range output =
          BoundaryExtractorFactory.create(input.columnClass())
              .getBoundary(input.partitionColumn(), rs, boundaryTypeMapper)
              .toRange(input.parentRange(), c);

      logger.info(
          "Got Boundary for column: {}, Range: {}, Min: {}, Max: {}",
          input.columnName(),
          output,
          output.start(),
          output.end());

      out.output(output); // Output the counted Range.
    } catch (SQLException e) {
      logger.error(
          "SQL Exception while getting boundary of column: {}, Error: {}, Query: {}, Message: {}",
          input.columnName(),
          e.getClass().getName(),
          boundaryQuery,
          e.getMessage());

      // Additional error details for MSSQL
      if (e.getErrorCode() != 0) {
        logger.error("SQL Error code: {}, SQL State: {}", e.getErrorCode(), e.getSQLState());
      }

      throw e;
    } catch (Exception e) {
      // This exception is triggered from nullness checks of checker framework for the input to
      // statement preparator and hence should
      // indicate a programming error. Any other exception in the code flow returns a SQL Exception.
      // It's hard to trigger this exception for UT as the checks are not running by default in the
      // UT.
      logger.error(
          "Exception while getting boundary of column: {}, Error: {}, Message: {}",
          input.columnName(),
          e.getClass().getName(),
          e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
