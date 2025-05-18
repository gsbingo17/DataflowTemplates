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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider;

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Provides a set of {@link org.apache.avro.Schema Avro Schemas} that each of the MSSQL database's
 * type must map into.
 *
 * @see <a href = https://cloud.google.com/datastream/docs/unified-types> Mappings of unified types
 *     to source and destination data types</a>
 */
public final class MssqlMappingProvider {
  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("BIGINT", UnifiedMappingProvider.Type.LONG)
          .put("BINARY", UnifiedMappingProvider.Type.STRING)
          .put("BIT", UnifiedMappingProvider.Type.BOOLEAN)
          .put("CHAR", UnifiedMappingProvider.Type.STRING)
          .put("DATE", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("DATETIME", UnifiedMappingProvider.Type.DATETIME)
          .put("DATETIME2", UnifiedMappingProvider.Type.DATETIME)
          .put("DATETIMEOFFSET", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("DECIMAL", UnifiedMappingProvider.Type.DECIMAL)
          .put("FLOAT", UnifiedMappingProvider.Type.DOUBLE)
          .put("IMAGE", UnifiedMappingProvider.Type.STRING)
          .put("INT", UnifiedMappingProvider.Type.INTEGER)
          .put("MONEY", UnifiedMappingProvider.Type.DECIMAL)
          .put("NCHAR", UnifiedMappingProvider.Type.STRING)
          .put("NTEXT", UnifiedMappingProvider.Type.STRING)
          .put("NUMERIC", UnifiedMappingProvider.Type.DECIMAL)
          .put("NVARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("REAL", UnifiedMappingProvider.Type.FLOAT)
          .put("SMALLDATETIME", UnifiedMappingProvider.Type.DATETIME)
          .put("SMALLINT", UnifiedMappingProvider.Type.INTEGER)
          .put("SMALLMONEY", UnifiedMappingProvider.Type.DECIMAL)
          .put("TEXT", UnifiedMappingProvider.Type.STRING)
          .put("TIME", UnifiedMappingProvider.Type.TIME_INTERVAL)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.BYTES)
          .put("TINYINT", UnifiedMappingProvider.Type.INTEGER)
          .put("UNIQUEIDENTIFIER", UnifiedMappingProvider.Type.STRING)
          .put("VARBINARY", UnifiedMappingProvider.Type.STRING)
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("XML", UnifiedMappingProvider.Type.STRING)
          .put("UNSUPPORTED", UnifiedMappingProvider.Type.UNSUPPORTED)
          .build()
          .entrySet()
          .stream()
          .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  /**
   * Returns the map of Source Schema to {@link UnifiedTypeMapping} for all supported MSSQL types.
   *
   * @return MSSQL mapping.
   */
  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return MAPPING;
  }

  /** Static final class. * */
  private MssqlMappingProvider() {}
}
