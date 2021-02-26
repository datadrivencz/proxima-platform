/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.bulk.fs.parquet;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;

@Slf4j
public class ParquetUtils {

  private ParquetUtils() {
    // no-op
  }

  public static MessageType createParquetSchema(
      AttributeFamilyDescriptor family, String attributeNamesPrefix) {
    return ParquetUtils.createMessageWithFields(
        family
            .getAttributes()
            .stream()
            .map(a -> createParquetSchema(a, attributeNamesPrefix))
            .collect(Collectors.toList()));
  }

  public static <T> Type createParquetSchema(
      AttributeDescriptor<T> attribute, String attributeNamesPrefix) {
    final SchemaTypeDescriptor<T> schema = attribute.getSchemaTypeDescriptor();
    String attributeName = attributeNamesPrefix + attribute.getName();
    if (attribute.isWildcard()) {
      attributeName = attributeNamesPrefix + attribute.toAttributePrefix(false);
    }
    Type parquet;
    if (schema.isStructureType()) {
      parquet =
          Types.optionalGroup()
              .named(attributeName)
              .withNewFields(
                  schema
                      .getStructureTypeDescriptor()
                      .getFields()
                      .entrySet()
                      .stream()
                      .map(e -> mapSchemaTypeToParquet(e.getValue().toTypeDescriptor(), e.getKey()))
                      .collect(Collectors.toList()));
    } else {
      parquet = mapSchemaTypeToParquet(schema.toTypeDescriptor(), attributeName);
    }
    return parquet;
  }

  public static Type mapSchemaTypeToParquet(SchemaTypeDescriptor<?> descriptor, String name) {
    switch (descriptor.getType()) {
      case INT:
        return Types.optional(PrimitiveTypeName.INT32).named(name);
      case LONG:
        return Types.optional(PrimitiveTypeName.INT64).named(name);
      case DOUBLE:
        return Types.optional(PrimitiveTypeName.DOUBLE).named(name);
      case FLOAT:
        return Types.optional(PrimitiveTypeName.FLOAT).named(name);
      case BOOLEAN:
        return Types.optional(PrimitiveTypeName.BOOLEAN).named(name);
      case STRING:
        return Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(name);
      case ENUM:
        return Types.optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.enumType())
            .named(name);
      case BYTE:
        return Types.optional(PrimitiveTypeName.BINARY).named(name);
      case ARRAY:
        SchemaTypeDescriptor<?> valueTypeDescriptor =
            descriptor.getArrayTypeDescriptor().getValueDescriptor();
        Type valueType = mapSchemaTypeToParquet(valueTypeDescriptor, name);

        // proxima byte array should be encoded as binary
        if (valueTypeDescriptor.isPrimitiveType()
            && valueTypeDescriptor.getType().equals(AttributeValueType.BYTE)) {
          return valueType;
        }

        if (valueTypeDescriptor.isPrimitiveType() || valueTypeDescriptor.isEnumType()) {
          return Types.repeated(valueType.asPrimitiveType().getPrimitiveTypeName())
              .as(valueType.getLogicalTypeAnnotation())
              .named(name);
        } else {
          return Types.repeatedGroup().named(name).withNewFields(valueType).asGroupType();
        }
      case STRUCTURE:
        GroupBuilder<GroupType> structure = Types.optionalGroup();
        descriptor
            .getStructureTypeDescriptor()
            .getFields()
            .forEach(
                (fieldName, des) ->
                    structure.addField(mapSchemaTypeToParquet(des.toTypeDescriptor(), fieldName)));

        return structure.named(name);
      default:
        throw new IllegalStateException("Unexpected value: " + descriptor.getType());
    }
  }

  static MessageType createMessageWithFields(List<Type> fieldsToAdd) {

    List<Type> fields =
        new ArrayList<>(
            Arrays.asList(
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY),
                Types.required(PrimitiveTypeName.BINARY)
                    // @TODO when parquet 1.12 is released we should be able to use logicalType UUID
                    // More info: https://issues.apache.org/jira/browse/PARQUET-1827
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID),
                Types.required(PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP),
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.enumType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION),
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE),
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX)));
    fields.addAll(fieldsToAdd);
    return new MessageType("proxima-bulk", fields);
  }
}
