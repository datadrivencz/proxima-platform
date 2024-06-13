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
import cz.o2.proxima.core.scheme.SchemaDescriptors;
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.core.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WildcardAttributeTable extends AbstractTable
    implements ScannableTable, FilterableTable, ProjectableFilterableTable {

  private final Repository repo;
  private final DirectDataOperator direct;
  private final EntityDescriptor entity;
  private final AttributeDescriptor<?> attribute;
  private final RandomAccessReader reader;
  private final Map<String, SchemaTypeDescriptor<?>> fields;
  private final boolean primitiveOrArrayType;

  public WildcardAttributeTable(
      Repository repo, EntityDescriptor entity, AttributeDescriptor<?> attribute) {

    this.repo = repo;
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
    this.entity = entity;
    this.attribute = attribute;
    this.reader = Optionals.get(direct.getRandomAccess(attribute));
    this.fields = getValueFields(attribute);
    this.primitiveOrArrayType =
        attribute.getSchemaTypeDescriptor().isPrimitiveType()
            || attribute.getSchemaTypeDescriptor().isArrayType();
  }

  private Map<String, SchemaTypeDescriptor<?>> getValueFields(AttributeDescriptor<?> attribute) {
    LinkedHashMap<String, SchemaTypeDescriptor<?>> ret = new LinkedHashMap<>();
    // key
    ret.put("KEY", SchemaDescriptors.strings());
    // suffix
    ret.put(attribute.toAttributePrefix(false).toUpperCase(), SchemaDescriptors.strings());
    SchemaTypeDescriptor<?> schema = attribute.getSchemaTypeDescriptor();
    if (schema.isPrimitiveType() || schema.isArrayType()) {
      ret.put("VALUE", schema);
    } else if (schema.isStructureType()) {
      StructureTypeDescriptor<?> struct = schema.asStructureTypeDescriptor();
      struct.getFields().forEach((n, s) -> ret.put(n.toUpperCase(), s));
    } else {
      throw new UnsupportedOperationException("Currently unsupported: " + schema);
    }
    return ret;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    FieldInfoBuilder builder = typeFactory.builder();
    fields.forEach((n, s) -> builder.add(n, TypeUtil.getRelDataType(s, typeFactory)));
    return builder.build();
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return scan(root, null, null);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root, @Nullable List<RexNode> filters) {
    return scan(root, filters, null);
  }

  @Override
  public Enumerable<Object[]> scan(
      DataContext root, @Nullable List<RexNode> filters, int[] projects) {
    List<String> keys = extractKeysFromFilters(filters);
    Preconditions.checkArgument(
        keys != null && !keys.isEmpty(), "Wildcard scans must contain keys, got %s.", filters);
    Map<String, List<KeyValue<?>>> scanned = new HashMap<>();
    keys.forEach(
        k ->
            reader.scanWildcard(
                k, attribute, kv -> scanned.computeIfAbsent(k, ign -> new ArrayList<>()).add(kv)));
    int suffixLength = attribute.toAttributePrefix().length();
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          Iterator<Entry<String, List<KeyValue<?>>>> keysIterator = scanned.entrySet().iterator();
          String currentKey = null;
          List<KeyValue<?>> currentList = null;
          int pos = -1;

          @Override
          public Object[] current() {
            KeyValue<?> kv = currentList.get(pos);
            String suffix = kv.getAttribute().substring(suffixLength);
            Object value = TypeUtil.convertKv(kv);

            Object[] fullRow;
            if (primitiveOrArrayType) {
              fullRow = new Object[] {currentKey, suffix, value};
            } else {
              fullRow = new Object[fields.size()];
              fullRow[0] = currentKey;
              fullRow[1] = suffix;
              System.arraycopy(value, 0, fullRow, 2, fields.size() - 2);
            }

            if (projects == null) {
              return fullRow;
            }

            Object[] res = new Object[projects.length];
            for (int i = 0; i < projects.length; i++) {
              res[i] = fullRow[projects[i]];
            }
            return res;
          }

          @Override
          public boolean moveNext() {
            if (currentKey == null || pos == scanned.get(currentKey).size() - 1) {
              if (keysIterator.hasNext()) {
                Entry<String, List<KeyValue<?>>> next = keysIterator.next();
                pos = 0;
                currentKey = next.getKey();
                currentList = next.getValue();
                return true;
              }
              return false;
            }
            pos++;
            return true;
          }

          @Override
          public void reset() {
            keysIterator = scanned.entrySet().iterator();
            pos = -1;
            currentKey = null;
            currentList = null;
          }

          @Override
          public void close() {}
        };
      }
    };
  }
}
