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

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FieldSchema implements Schema {

  private final Repository repo;
  private final EntityDescriptor entityDesc;
  private final String field;

  public FieldSchema(Repository repo, EntityDescriptor entityDesc, String field) {
    this.repo = repo;
    this.entityDesc = entityDesc;
    this.field = field;
  }

  @Override
  public @Nullable Table getTable(String name) {
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return Set.of();
  }

  @Override
  public @Nullable RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Set.of();
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return List.of();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Set.of();
  }

  @Override
  public @Nullable Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.of();
  }

  @Override
  public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return null;
  }
}
