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

import cz.o2.proxima.flink.core.BatchLogSourceFunction;
import cz.o2.proxima.repository.Repository;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.types.RowKind;

public class BatchLogDynamicTableSource implements ScanTableSource {

  private final Repository repository;
  private final LogCatalogTable.BatchLogCatalogTable catalogTable;

  public BatchLogDynamicTableSource(
      Repository repository, LogCatalogTable.BatchLogCatalogTable catalogTable) {
    this.repository = repository;
    this.catalogTable = catalogTable;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .build();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return SourceFunctionProvider.of(
        new BatchLogSourceFunction<>(
            repository.asFactory(),
            catalogTable.getAttributeDescriptors(),
            catalogTable.getLogOptions(),
            new RowDataResultExtractor(catalogTable.getAttributeDescriptors())),
        true);
  }

  @Override
  public DynamicTableSource copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String asSummaryString() {
    return "BatchLog table source";
  }
}
