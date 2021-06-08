/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.flink.core.table;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

public class LogDynamicTableSourceFactory implements DynamicTableSourceFactory {

  static final String FACTORY_IDENTIFIER = "proxima";

  public LogDynamicTableSourceFactory() {}

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final LogCatalogTable<?> catalogTable =
        (LogCatalogTable<?>) context.getCatalogTable().getOrigin();
    if (catalogTable instanceof LogCatalogTable.CommitLogCatalogTable) {
      final LogCatalogTable.CommitLogCatalogTable cast =
          (LogCatalogTable.CommitLogCatalogTable) catalogTable;
      return new CommitLogDynamicTableSource(catalogTable.getRepository(), cast);
    }
    if (catalogTable instanceof LogCatalogTable.BatchLogCatalogTable) {
      final LogCatalogTable.BatchLogCatalogTable cast =
          (LogCatalogTable.BatchLogCatalogTable) catalogTable;
      return new BatchLogDynamicTableSource(catalogTable.getRepository(), cast);
    }
    throw new UnsupportedOperationException(
        String.format(
            "Unknown %s implementation %s.", LogCatalogTable.class, catalogTable.getClass()));
  }

  @Override
  public String factoryIdentifier() {
    return FACTORY_IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
