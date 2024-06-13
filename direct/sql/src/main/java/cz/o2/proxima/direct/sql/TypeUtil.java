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
import cz.o2.proxima.core.scheme.AttributeValueType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

class TypeUtil {

  static RelProtoDataType intoSqlType(AttributeDescriptor<?> attribute) {
    AttributeValueType type = attribute.getValueSerializer().getValueSchemaDescriptor().getType();
    switch (type) {
      case ARRAY:
        return RelDataTypeImpl.proto(SqlTypeName.ARRAY, true);
      case BOOLEAN:
        return RelDataTypeImpl.proto(SqlTypeName.BOOLEAN, true);
      case BYTE:
        return RelDataTypeImpl.proto(SqlTypeName.SMALLINT, true);
      case DOUBLE:
        return RelDataTypeImpl.proto(SqlTypeName.DOUBLE, true);
      case ENUM:
        return RelDataTypeImpl.proto(SqlTypeName.VARCHAR, true);
      case FLOAT:
        return RelDataTypeImpl.proto(SqlTypeName.FLOAT, true);
      case INT:
        return RelDataTypeImpl.proto(SqlTypeName.INTEGER, true);
      case LONG:
        return RelDataTypeImpl.proto(SqlTypeName.BIGINT, true);
      case STRING:
        return RelDataTypeImpl.proto(SqlTypeName.VARCHAR, true);
      case STRUCTURE:
        return RelDataTypeImpl.proto(SqlTypeName.STRUCTURED, true);
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  private TypeUtil() {}
}
