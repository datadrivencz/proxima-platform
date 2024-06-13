/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.sql;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Pair;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public class RepositorySchema extends AbstractSchema {

  private final Repository repo;

  public RepositorySchema(Repository repo) {
    this.repo = repo;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return repo.getAllEntities()
        .map(e -> Pair.of(e.getName().toUpperCase(), new EntityTable(repo, e)))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }
}
