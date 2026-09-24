/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
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
    List<EntityDescriptor> randomAccessEntities = getAllRandomAccessEntities();
    // table with regular attributes
    Map<String, Table> regularMap =
        randomAccessEntities.stream()
            .filter(e -> e.getAllAttributes().stream().anyMatch(a -> !a.isWildcard()))
            .filter(randomAccessOnly(repo))
            .map(e -> Pair.of(e.getName().toUpperCase(), new EntityTable(repo, e)))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    Map<String, Table> wildcardMap =
        randomAccessEntities.stream()
            .filter(randomAccessOnly(repo))
            .flatMap(
                e ->
                    e.getAllAttributes().stream()
                        .filter(AttributeDescriptor::isWildcard)
                        .map(a -> Pair.of(e, a)))
            .filter(
                p ->
                    repo.getFamiliesForAttribute(p.getSecond()).stream()
                        .anyMatch(af -> af.getAccess().canRandomRead()))
            .map(
                p ->
                    Pair.of(
                        p.getFirst().getName().toUpperCase()
                            + "."
                            + p.getSecond().toAttributePrefix(false).toUpperCase(),
                        new WildcardAttributeTable(repo, p.getFirst(), p.getSecond())))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    regularMap.putAll(wildcardMap);
    return regularMap;
  }

  private List<EntityDescriptor> getAllRandomAccessEntities() {
    return repo.getAllFamilies()
        .filter(af -> af.getAccess().canRandomRead())
        .map(AttributeFamilyDescriptor::getEntity)
        .distinct()
        .collect(Collectors.toList());
  }

  private static Predicate<EntityDescriptor> randomAccessOnly(Repository repo) {
    return e ->
        e.getAllAttributes().stream()
            .filter(a -> !a.isWildcard())
            .anyMatch(
                a ->
                    repo.getFamiliesForAttribute(a).stream()
                        .anyMatch(af -> af.getAccess().canRandomRead()));
  }
}
