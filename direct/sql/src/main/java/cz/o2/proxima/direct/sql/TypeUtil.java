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
import cz.o2.proxima.core.scheme.AttributeValueType;
import cz.o2.proxima.core.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.core.scheme.SchemaDescriptors.StructureTypeDescriptor;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

class TypeUtil {

  static RelDataType intoSqlType(AttributeDescriptor<?> attribute, RelDataTypeFactory typeFactory) {
    SchemaTypeDescriptor<?> schemaDescriptor =
        attribute.getValueSerializer().getValueSchemaDescriptor();
    return getRelDataType(schemaDescriptor, typeFactory);
  }

  private static RelDataType getRelDataType(
      SchemaTypeDescriptor<?> schemaDescriptor, RelDataTypeFactory typeFactory) {

    AttributeValueType type = schemaDescriptor.getType();
    switch (type) {
      case ARRAY:
        ArrayTypeDescriptor<?> arraySchema = schemaDescriptor.asArrayTypeDescriptor();
        return typeFactory.createArrayType(
            getRelDataType(arraySchema.getValueDescriptor(), typeFactory), -1);
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case BYTE:
        return typeFactory.createSqlType(SqlTypeName.SMALLINT);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case ENUM:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);
      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case STRUCTURE:
        StructureTypeDescriptor<?> structureSchema = schemaDescriptor.asStructureTypeDescriptor();
        List<SimpleImmutableEntry<String, RelDataType>> fields =
            structureSchema.getFields().entrySet().stream()
                .map(
                    e ->
                        new SimpleImmutableEntry<>(
                            e.getKey(), getRelDataType(e.getValue(), typeFactory)))
                .collect(Collectors.toList());
        return typeFactory.createStructType(fields);
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  private TypeUtil() {}

  public static RelDataType intoSqlType(EntityDescriptor entity, RelDataTypeFactory typeFactory) {
    List<SimpleImmutableEntry<String, RelDataType>> fields =
        entity.getAllAttributes().stream()
            .map(a -> new SimpleImmutableEntry<>(a.getName(), intoSqlType(a, typeFactory)))
            .collect(Collectors.toList());
    return typeFactory.createStructType(fields);
  }
}
