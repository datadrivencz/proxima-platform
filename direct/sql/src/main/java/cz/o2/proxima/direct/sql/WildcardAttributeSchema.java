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

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Pair;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public class WildcardAttributeSchema extends AbstractSchema {

  private final Repository repo;
  private final EntityDescriptor entity;
  private final AttributeDescriptor<?> attribute;

  public WildcardAttributeSchema(
      Repository repo, EntityDescriptor entity, AttributeDescriptor<?> attribute) {

    this.repo = repo;
    this.entity = entity;
    this.attribute = attribute;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return entity.getAllAttributes().stream()
        .filter(AttributeDescriptor::isWildcard)
        .map(a -> Pair.of(a.getName().toUpperCase(), new WildcardAttributeTable(repo, entity, a)))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }
}
