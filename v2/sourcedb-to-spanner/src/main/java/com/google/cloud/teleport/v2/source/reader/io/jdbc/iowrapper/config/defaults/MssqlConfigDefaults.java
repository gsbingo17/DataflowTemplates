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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.defaults;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mssql.MssqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.MssqlJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;

/** Default configuration values for MSSQL. */
public class MssqlConfigDefaults {
  public static final MapperType DEFAULT_MSSQL_SCHEMA_MAPPER_TYPE = MapperType.SQLSERVER;

  public static final DialectAdapter DEFAULT_MSSQL_DIALECT_ADAPTER = new MssqlDialectAdapter();

  public static final JdbcValueMappingsProvider DEFAULT_MSSQL_VALUE_MAPPING_PROVIDER =
      new MssqlJdbcValueMappings();

  public static final Long DEFAULT_MSSQL_MAX_CONNECTIONS = 20L;

  public static final FluentBackoff DEFAULT_MSSQL_SCHEMA_DISCOVERY_BACKOFF =
      FluentBackoff.DEFAULT.withMaxCumulativeBackoff(Duration.standardMinutes(5L));

  /** Default Initialization Sequence for the JDBC connection. */
  public static final ImmutableList<String> DEFAULT_MSSQL_INIT_SEQ = ImmutableList.of();

  private MssqlConfigDefaults() {}
}
