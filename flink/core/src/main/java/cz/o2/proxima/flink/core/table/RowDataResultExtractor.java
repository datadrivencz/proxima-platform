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
package cz.o2.proxima.flink.core.table;

import cz.o2.proxima.flink.core.ResultExtractor;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/** Convert {@link StreamElement} into {@link RowData}. */
public class RowDataResultExtractor implements ResultExtractor<RowData> {

  private final List<AttributeDescriptor<?>> attributeDescriptors;

  public RowDataResultExtractor(List<AttributeDescriptor<?>> attributeDescriptors) {
    this.attributeDescriptors = attributeDescriptors;
  }

  @Override
  public RowData toResult(StreamElement element) {
    final AttributeDescriptor<?>[] attributeDescriptors =
        this.attributeDescriptors.toArray(new AttributeDescriptor<?>[0]);
    final GenericRowData data = new GenericRowData(5 + attributeDescriptors.length);
    // key
    data.setField(0, StringData.fromString(element.getKey()));
    // uuid
    data.setField(1, StringData.fromString(element.getUuid()));
    // attribute
    data.setField(2, StringData.fromString(element.getAttribute()));
    // attribute prefix
    data.setField(
        3, StringData.fromString(element.getAttributeDescriptor().toAttributePrefix(false)));
    // timestamp
    data.setField(4, TimestampData.fromEpochMillis(element.getStamp()));
    // value
    for (int i = 0; i < attributeDescriptors.length; i++) {
      if (element.getAttributeDescriptor().equals(attributeDescriptors[i])) {
        data.setField(5 + i, toFlinkObject(element));
      }
    }
    return data;
  }

  @VisibleForTesting
  static <ValueT> Object toFlinkObject(StreamElement element) {
    @SuppressWarnings("unchecked")
    final AttributeDescriptor<ValueT> attributeDescriptor =
        (AttributeDescriptor<ValueT>) element.getAttributeDescriptor();
    final ValueT value =
        Optionals.get(attributeDescriptor.getValueSerializer().deserialize(element.getValue()));
    final SchemaDescriptors.SchemaTypeDescriptor<ValueT> schemaDescriptor =
        attributeDescriptor.getSchemaTypeDescriptor();
    final AttributeValueAccessor<ValueT, Object> valueAccessor =
        attributeDescriptor.getValueSerializer().getValueAccessor();
    return toFlinkObject(schemaDescriptor, valueAccessor, value);
  }

  private static <ValueT, OutputT> Object toFlinkObject(
      SchemaDescriptors.SchemaTypeDescriptor<ValueT> descriptor,
      AttributeValueAccessor<ValueT, OutputT> accessor,
      ValueT value) {
    switch (descriptor.getType()) {
      case STRUCTURE:
        final AttributeValueAccessors.StructureValueAccessor<ValueT> structureAccessor =
            (AttributeValueAccessors.StructureValueAccessor<ValueT>) accessor;
        return toFlinkStructure(descriptor.asStructureTypeDescriptor(), structureAccessor, value);
      case STRING:
      case ENUM:
        return StringData.fromString((String) value);
      case BOOLEAN:
      case LONG:
      case DOUBLE:
      case FLOAT:
      case INT:
      case BYTE:
        return value;
      case ARRAY:
        final AttributeValueAccessors.ArrayValueAccessor<ValueT, OutputT> arrayAccessor =
            (AttributeValueAccessors.ArrayValueAccessor<ValueT, OutputT>) accessor;
        @SuppressWarnings("unchecked")
        final List<ValueT> values = (List<ValueT>) value;
        return toFlinkArray(descriptor.asArrayTypeDescriptor(), arrayAccessor, values);
      default:
        throw new UnsupportedOperationException(
            String.format("Type [%s] is not supported.", descriptor.getType()));
    }
  }

  private static <ValueT> RowData toFlinkStructure(
      SchemaDescriptors.StructureTypeDescriptor<ValueT> descriptor,
      AttributeValueAccessors.StructureValueAccessor<ValueT> accessor,
      ValueT value) {
    final GenericRowData rowData = new GenericRowData(descriptor.getFields().size());
    final AtomicInteger idx = new AtomicInteger(0);
    descriptor
        .getFields()
        .forEach(
            (fieldName, fieldType) -> {
              @SuppressWarnings("unchecked")
              final SchemaDescriptors.SchemaTypeDescriptor<Object> cast =
                  (SchemaDescriptors.SchemaTypeDescriptor<Object>) fieldType;
              final Object fieldValue = accessor.getRawFieldValue(fieldName, value);
              @SuppressWarnings("unchecked")
              final AttributeValueAccessor<Object, Object> fieldAccessor =
                  (AttributeValueAccessor<Object, Object>) accessor.getFieldAccessor(fieldName);
              rowData.setField(
                  idx.getAndIncrement(), toFlinkObject(cast, fieldAccessor, fieldValue));
            });
    return rowData;
  }

  private static <ValueT, OutputT> ArrayData toFlinkArray(
      SchemaDescriptors.ArrayTypeDescriptor<ValueT> descriptor,
      AttributeValueAccessors.ArrayValueAccessor<ValueT, OutputT> accessor,
      List<ValueT> values) {
    final SchemaDescriptors.SchemaTypeDescriptor<ValueT> valueDescriptor =
        descriptor.getValueDescriptor();
    final AttributeValueAccessor<ValueT, OutputT> valueAccessor = accessor.getValueAccessor();
    return new GenericArrayData(
        values.stream().map(item -> toFlinkObject(valueDescriptor, valueAccessor, item)).toArray());
  }
}
