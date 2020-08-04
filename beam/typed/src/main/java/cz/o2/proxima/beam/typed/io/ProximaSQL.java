/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.typed.io;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ProximaSQL {

  private static class ProximaTableProvider extends InMemoryMetaTableProvider {

    private final Repository repository;

    private ProximaTableProvider(Repository repository) {
      this.repository = repository;
    }

    @Override
    public String getTableType() {
      return "proxima";
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
      return null;
    }

    @Override
    public Map<String, Table> getTables() {
      return Collections.emptyMap();
    }
  }

  public static Read query(Repository repository, String query) {
    return new Read();
  }

  public static class Read extends PTransform<PBegin, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PBegin input) {
      final BeamSqlEnv.BeamSqlEnvBuilder sqlEnvBuilder =
          BeamSqlEnv.builder(new ProximaTableProvider(null));
      return null;
    }

    public <T> Read withTable(String tableName, AttributeDescriptor<T> attributeDescriptor) {
      return this;
    }
  }
}
