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

import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.OPERATION;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.StreamElement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

@Slf4j
class InternalProximaRecordMaterializer extends RecordMaterializer<StreamElement> {

  private final Map<String, Object> recordData = new HashMap<>();
  private final ProximaParquetRecordConverter root;
  private final EntityDescriptor entity;
  private final String attributeNamesPrefix;

  InternalProximaRecordMaterializer(
      MessageType schema, EntityDescriptor entity, String attributeNamesPrefix) {
    ParentValueContainer parentValueContainer =
        new ParentValueContainer() {
          @Override
          public void add(String name, Object value) {
            recordData.put(name, value);
          }

          @Override
          public Object get(String name) {
            return recordData.get(name);
          }
        };
    this.root = new ProximaMessageConverter(parentValueContainer, schema);
    this.entity = entity;
    this.attributeNamesPrefix = attributeNamesPrefix;
  }

  @Override
  public StreamElement getCurrentRecord() {
    final String key =
        (String) getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY);
    final String operation =
        (String)
            getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION);
    final String attributeName =
        (String)
            getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE);
    Optional<AttributeDescriptor<Object>> attribute = entity.findAttribute(attributeName);
    if (!attribute.isPresent()) {
      // current attribute is not in entity -> skip
      log.info("Skipping attribute [{}] which is not in current attribute family.", attributeName);
      return null;
    }
    final String uuid =
        (String) getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID);
    final long timestamp =
        (long) getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP);
    switch (OPERATION.of(operation)) {
      case DELETE:
        return StreamElement.delete(entity, attribute.get(), uuid, key, attributeName, timestamp);
      case DELETE_WILDCARD:
        return StreamElement.deleteWildcard(entity, attribute.get(), uuid, key, timestamp);
      case UPSERT:
        return StreamElement.upsert(
            entity,
            attribute.get(),
            uuid,
            key,
            attributeName,
            timestamp,
            getValueFromCurrentRowData(attribute.get()));
      default:
        throw new RecordMaterializationException("Unknown operation " + operation);
    }
  }

  @Override
  public GroupConverter getRootConverter() {
    return this.root;
  }

  private byte[] getValueFromCurrentRowData(AttributeDescriptor<?> attribute) {

    final String attributeKeyName = attributeNamesPrefix + attribute.toAttributePrefix(false);

    final SchemaTypeDescriptor<?> attributeSchema = attribute.getSchemaTypeDescriptor();
    Object value;
    if (attributeSchema.isStructureType()) {
      @SuppressWarnings("unchecked")
      final StructureValueAccessor<Object> valueAccessor =
          (StructureValueAccessor<Object>)
              attributeSchema.asStructureTypeDescriptor().getValueAccessor();
      value = valueAccessor.createFrom(recordData.get(attributeKeyName));
    } else if (attributeSchema.isArrayType()) {
      @SuppressWarnings("unchecked")
      final ArrayValueAccessor<Object> valueAccessor =
          (ArrayValueAccessor<Object>) attributeSchema.asArrayTypeDescriptor().getValueAccessor();
      // FIXME after bytes is resolved
      value = recordData.get(attributeKeyName);
      // value = valueAccessor.createFrom(record.get(attributeKeyName));
    } else {
      throw new UnsupportedOperationException("Fixme");
    }
    @SuppressWarnings("unchecked")
    final ValueSerializer<Object> serializer =
        (ValueSerializer<Object>) attribute.getValueSerializer();
    return serializer.serialize(value);
  }

  private Object getRequiredValueFromCurrentRowData(String column) {
    return Optional.ofNullable(recordData.getOrDefault(column, null))
        .orElseThrow(
            () -> new IllegalStateException("Unable to read required value for column " + column));
  }

  interface ParentValueContainer {
    void add(String name, Object value);

    Object get(String name);
  }

  static class ProximaMessageConverter extends ProximaParquetRecordConverter {

    ProximaMessageConverter(ParentValueContainer parent, GroupType schema) {
      super(parent, schema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return super.getConverter(fieldIndex);
    }

    @Override
    public void start() {
      super.start();
    }

    @Override
    public void end() {
      // do nothing, don't call ParentValueContainer at top level.
    }
  }
}
