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
package cz.o2.proxima.scheme.proto;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.scheme.AttributeValueAccessors.EnumValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.GenericTypeDescriptor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoMessageValueAccessor<T extends Message> implements StructureValueAccessor<T> {

  private final Map<String, GenericTypeDescriptor<?>> fields;
  private final Factory<Descriptor> protoDescriptorFactory;
  private final Factory<T> defaultValueFactory;
  private transient Descriptor proto;
  private transient T defaultValue;

  public ProtoMessageValueAccessor(
      Map<String, GenericTypeDescriptor<?>> fields,
      Factory<Descriptor> protoDescriptorFactory,
      Factory<T> defaultValueFactory) {
    this.fields = fields;
    this.protoDescriptorFactory = protoDescriptorFactory;
    this.defaultValueFactory = defaultValueFactory;
  }

  @Override
  public Map<String, Object> valuesOf(T value) {
    final Map<String, Object> result = new HashMap<>();
    fields.forEach((field, type) -> result.put(field, readField(field, value)));
    return result;
  }

  @Override
  public <V> V readField(String name, T value) {
    @SuppressWarnings("unchecked")
    final GenericTypeDescriptor<Object> valueSchema =
        (GenericTypeDescriptor<Object>) getFieldSchemaTypeDescriptor(fields, name);
    return readField(name, value, valueSchema, value.getDescriptorForType());
  }

  @Override
  @SuppressWarnings("unchecked")
  public T createFrom(Map<String, Object> map) {
    return (T)
        buildMessage(map, fields, getProtoDescriptor(), getDefaultValue().newBuilderForType());
  }

  @SuppressWarnings("unchecked")
  private <V> V readField(
      String name,
      Message value,
      GenericTypeDescriptor<Object> valueSchema,
      Descriptor protoDescriptor) {
    final FieldDescriptor fieldProtoDescriptor = protoDescriptor.findFieldByName(name);
    final Object fieldValue = value.getField(fieldProtoDescriptor);

    if (valueSchema.isPrimitiveType()) {
      return (V) valueSchema.asPrimitiveTypeDescriptor().getValueAccessor().valueOf(fieldValue);
    } else if (valueSchema.isArrayType()) {
      return (V) valueSchema.asArrayTypeDescriptor().getValueAccessor().values(fieldValue);
    } else if (valueSchema.isStructureType()) {
      final Map<String, Object> messageValue = new HashMap<>();
      valueSchema
          .asStructureTypeDescriptor()
          .getFields()
          .forEach(
              (innerFieldName, innerType) -> {
                final Object innerValue =
                    readField(
                        innerFieldName,
                        (Message) fieldValue,
                        (GenericTypeDescriptor<Object>) innerType,
                        fieldProtoDescriptor.getMessageType());
                messageValue.put(innerFieldName, innerValue);
              });
      return (V) messageValue;
    } else if (valueSchema.isEnumType()) {
      EnumValueAccessor<Object> accessor = valueSchema.asEnumTypeDescriptor().getValueAccessor();
      return (V) accessor.createFrom(fieldValue.toString());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported field conversion for type [%s]", valueSchema.getType()));
    }
  }

  @SuppressWarnings("unchecked")
  private Message buildMessage(
      Map<String, Object> map,
      Map<String, GenericTypeDescriptor<?>> fieldsDescriptors,
      Descriptor protoDescriptor,
      Builder builder) {
    for (Entry<String, Object> entry : map.entrySet()) {
      final String field = entry.getKey();
      final Object value = entry.getValue();
      @SuppressWarnings("unchecked")
      final GenericTypeDescriptor<Object> valueSchema =
          (GenericTypeDescriptor<Object>) getFieldSchemaTypeDescriptor(fieldsDescriptors, field);
      final FieldDescriptor protoFieldDescriptor = protoDescriptor.findFieldByName(field);
      Preconditions.checkNotNull(
          protoFieldDescriptor,
          "Unable to find field [%s] in descriptor [%s]",
          field,
          protoDescriptor.getName());

      if (valueSchema.isPrimitiveType()) {
        builder.setField(
            protoFieldDescriptor,
            valueSchema.asPrimitiveTypeDescriptor().getValueAccessor().createFrom(value));
      } else if (valueSchema.isArrayType()) {
        final GenericTypeDescriptor<Object> arrayValueDescriptor =
            valueSchema.asArrayTypeDescriptor().getValueDescriptor();
        if (valueSchema.asArrayTypeDescriptor().getValueType().equals(AttributeValueType.BYTE)) {
          // Bytes needs to be converted as PrimitiveValue of String
          builder.setField(
              protoFieldDescriptor,
              arrayValueDescriptor.asPrimitiveTypeDescriptor().getValueAccessor().valueOf(value));

        } else {
          final List<Object> values =
              valueSchema.asArrayTypeDescriptor().getValueAccessor().values(value);
          values.forEach(
              v -> {
                Object arrayValue = v;
                if (arrayValueDescriptor.isStructureType()) {
                  // Array<Structure> needs to be created via builder
                  arrayValue =
                      buildMessage(
                          (Map<String, Object>) v,
                          arrayValueDescriptor.asStructureTypeDescriptor().getFields(),
                          protoFieldDescriptor.getMessageType(),
                          builder.newBuilderForField(protoFieldDescriptor));
                }
                builder.addRepeatedField(protoFieldDescriptor, arrayValue);
              });
        }
      } else if (valueSchema.isStructureType()) {
        final Builder fieldBuilder = builder.getFieldBuilder(protoFieldDescriptor);
        final Message message =
            buildMessage(
                (Map<String, Object>) value,
                valueSchema.asStructureTypeDescriptor().getFields(),
                fieldBuilder.getDescriptorForType(),
                fieldBuilder);
        builder.setField(protoFieldDescriptor, message);
      } else if (valueSchema.isEnumType()) {
        final EnumValueAccessor<Object> enumValueAccessor =
            valueSchema.asEnumTypeDescriptor().getValueAccessor();
        builder.setField(protoFieldDescriptor, enumValueAccessor.valueOf(value));
      } else {
        throw new UnsupportedOperationException(
            String.format("Unknown value type [%s]", valueSchema.getType()));
      }
    }
    return builder.build();
  }

  private GenericTypeDescriptor<?> getFieldSchemaTypeDescriptor(
      Map<String, GenericTypeDescriptor<?>> from, String name) {
    return Optional.ofNullable(from.getOrDefault(name, null))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Unknown field [%s] in fields: %s", name, from.keySet())));
  }

  private Descriptor getProtoDescriptor() {
    if (proto == null) {
      proto = protoDescriptorFactory.apply();
    }
    return proto;
  }

  private T getDefaultValue() {
    if (defaultValue == null) {
      defaultValue = defaultValueFactory.apply();
    }
    return defaultValue;
  }
}
