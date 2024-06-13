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
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.randomaccess.MultiAccessBuilder;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class EntityTable extends AbstractTable implements ScannableTable {

  @Getter private final Repository repo;
  @Getter private final DirectDataOperator direct;
  private final EntityDescriptor entity;

  public EntityTable(Repository repo, EntityDescriptor entity) {
    this.repo = repo;
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
    this.entity = entity;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    // FIXME
    MultiAccessBuilder builder = RandomAccessReader.newBuilder(repo, direct.getContext());
    List<AttributeDescriptor<?>> attributes = entity.getAllAttributes();
    attributes.forEach(a -> builder.addAttributes(Optionals.get(direct.getRandomAccess(a)), a));
    RandomAccessReader reader = Optionals.get(direct.getRandomAccess(attributes.get(0)));
    Deque<Pair<RandomOffset, String>> entities = new LinkedList<>();
    reader.listEntities(entities::add);
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          String currentKey = null;

          @Override
          public Object[] current() {
            return IntStream.range(0, attributes.size() + 1)
                .mapToObj(a -> currentKey)
                .collect(Collectors.toList())
                .toArray(new Object[] {});
          }

          @Override
          public boolean moveNext() {
            currentKey = null;
            if (!entities.isEmpty()) {
              currentKey = entities.pollFirst().getSecond();
            }
            return currentKey != null;
          }

          @Override
          public void reset() {}

          @Override
          public void close() {
            ExceptionUtils.unchecked(reader::close);
          }
        };
      }
    };
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    FieldInfoBuilder builder = typeFactory.builder();
    entity
        .getAllAttributes()
        .forEach(a -> builder.add(a.getName().toUpperCase(), TypeUtil.intoSqlType(a, typeFactory)));
    builder.add("KEY", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    return builder.build();
  }
}
