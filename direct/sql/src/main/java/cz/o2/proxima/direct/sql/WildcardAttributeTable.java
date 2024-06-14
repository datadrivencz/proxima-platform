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

import static cz.o2.proxima.direct.sql.FilterUtil.extractKeysFromFilters;

import com.google.common.base.Preconditions;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WildcardAttributeTable extends AbstractTable implements FilterableTable {

  private final Repository repo;
  private final DirectDataOperator direct;
  private final EntityDescriptor entity;
  private final AttributeDescriptor<?> attribute;
  private final RandomAccessReader reader;

  public WildcardAttributeTable(
      Repository repo, EntityDescriptor entity, AttributeDescriptor<?> attribute) {

    this.repo = repo;
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
    this.entity = entity;
    this.attribute = attribute;
    this.reader = Optionals.get(direct.getRandomAccess(attribute));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType valueType =
        TypeUtil.getRelDataType(attribute.getSchemaTypeDescriptor(), typeFactory);
    FieldInfoBuilder builder = typeFactory.builder();
    builder.add("KEY", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    valueType.getFieldList().forEach(f -> builder.add(f.getName(), f.getType()));
    return builder.build();
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root, @Nullable List<RexNode> filters) {
    List<String> keys = extractKeysFromFilters(filters);
    Preconditions.checkArgument(
        keys != null && !keys.isEmpty(), "Wildcard scans must contain keys, got %s.", filters);
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          @Override
          public Object[] current() {
            return new Object[0];
          }

          @Override
          public boolean moveNext() {
            return false;
          }

          @Override
          public void reset() {}

          @Override
          public void close() {}
        };
      }
    };
  }
}
