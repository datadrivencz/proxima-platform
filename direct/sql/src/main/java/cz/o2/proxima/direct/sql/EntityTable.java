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
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.MultiAccessBuilder;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class EntityTable extends AbstractTable implements ScannableTable, FilterableTable {

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
    return scanKeys(null);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    return scanKeys(extractKeysFromFilters(filters));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    FieldInfoBuilder builder = typeFactory.builder();
    builder.add("KEY", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    entity
        .getAllAttributes()
        .forEach(a -> builder.add(a.getName().toUpperCase(), TypeUtil.intoSqlType(a, typeFactory)));
    return builder.build();
  }

  private Enumerable<Object[]> scanKeys(@Nullable List<String> keys) {
    // FIXME: closing?
    MultiAccessBuilder builder = RandomAccessReader.newBuilder(repo, direct.getContext());
    List<AttributeDescriptor<?>> attributes = entity.getAllAttributes();
    attributes.forEach(a -> builder.addAttributes(Optionals.get(direct.getRandomAccess(a)), a));
    RandomAccessReader reader = Optionals.get(direct.getRandomAccess(attributes.get(0)));
    Deque<String> entities = new LinkedList<>();
    if (keys == null) {
      reader.listEntities(p -> entities.add(p.getSecond()));
    } else {
      entities.addAll(keys);
    }
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          String currentKey = null;

          @Override
          public Object[] current() {
            Object[] res = new Object[attributes.size() + 1];
            // first is key
            res[0] = currentKey;
            int pos = 1;
            for (AttributeDescriptor<?> a : attributes) {
              Optional<? extends KeyValue<?>> kv = reader.get(currentKey, a);
              if (kv.isPresent()) {
                res[pos] = TypeUtil.convertKv(kv.get());
              }
              pos++;
            }
            return res;
          }

          @Override
          public boolean moveNext() {
            currentKey = null;
            if (!entities.isEmpty()) {
              currentKey = entities.pollFirst();
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

  private List<String> extractKeysFromFilters(List<RexNode> filters) {
    List<String> keys = new ArrayList<>();
    for (RexNode filter : filters) {
      if (filter.isA(SqlKind.EQUALS)) {
        RexCall call = (RexCall) filter;
        if (call.operands.get(0) instanceof RexInputRef
            && call.operands.get(1) instanceof RexLiteral) {
          RexInputRef inputRef = (RexInputRef) call.operands.get(0);
          RexLiteral literal = (RexLiteral) call.operands.get(1);
          if (inputRef.getIndex() == 0) {
            keys.add(literal.getValueAs(String.class));
          }
        }
      }
    }
    return keys.isEmpty() ? null : keys;
  }
}
