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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.scheme.AttributeValueAccessors.EnumValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.PrimitiveTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoMessageValueAccessor<T extends Message> implements StructureValueAccessor<T> {

  private final Map<String, SchemaTypeDescriptor<?>> fields;
  private final Factory<Descriptor> protoDescriptorFactory;
  private final Factory<T> defaultValueFactory;
  @Nullable private transient Descriptor proto;
  @Nullable private transient T defaultValue;

  public ProtoMessageValueAccessor(
      Map<String, SchemaTypeDescriptor<?>> fields,
      Factory<Descriptor> protoDescriptorFactory,
      Factory<T> defaultValueFactory) {
    this.fields = fields;
    this.protoDescriptorFactory = protoDescriptorFactory;
    this.defaultValueFactory = defaultValueFactory;
  }

  @Override
  public Map<String, Object> valuesOf(T value) {
    final Map<String, Object> result = new HashMap<>();
    fields.forEach((field, type) -> result.put(field, valueOf(field, value)));
    return result;
  }

  @Override
  public <V> V valueOf(String name, T value) {
    @SuppressWarnings("unchecked")
    final SchemaTypeDescriptor<Object> valueSchema =
        (SchemaTypeDescriptor<Object>) getFieldSchemaTypeDescriptor(fields, name);
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
      SchemaTypeDescriptor<Object> valueSchema,
      Descriptor protoDescriptor) {
    final FieldDescriptor fieldProtoDescriptor = protoDescriptor.findFieldByName(name);
    final Object fieldValue = value.getField(fieldProtoDescriptor);

    if (valueSchema.isArrayType()) {
      if (valueSchema
          .asArrayTypeDescriptor()
          .getValueDescriptor()
          .getType()
          .equals(AttributeValueType.BYTE)) {
        final Object val =
            valueSchema
                .asArrayTypeDescriptor()
                .getValueDescriptor()
                .asPrimitiveTypeDescriptor()
                .getValueAccessor()
                .valueOf(fieldValue);
        if (val instanceof List) {
          return (V) ((List<Object>) val).toArray();
        } else {
          return (V) val;
        }
      } else {
        return (V)
            valueSchema
                .asArrayTypeDescriptor()
                .getValueAccessor()
                .valuesOf(((List<Object>) fieldValue).toArray());
      }
    } else if (valueSchema.isPrimitiveType()) {
      return valueSchema.asPrimitiveTypeDescriptor().getValueAccessor().valueOf(fieldValue);
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
                        (SchemaTypeDescriptor<Object>) innerType,
                        fieldProtoDescriptor.getMessageType());
                messageValue.put(innerFieldName, innerValue);
              });
      return (V) messageValue;
    } else if (valueSchema.isEnumType()) {
      EnumValueAccessor<Object> accessor = valueSchema.asEnumTypeDescriptor().getValueAccessor();
      return (V) accessor.valueOf(fieldValue);
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported field conversion for type [%s]", valueSchema.getType()));
    }
  }

  @SuppressWarnings("unchecked")
  private Message buildMessage(
      Map<String, Object> map,
      Map<String, SchemaTypeDescriptor<?>> fieldsDescriptors,
      Descriptor protoDescriptor,
      Builder builder) {
    for (Entry<String, Object> entry : map.entrySet()) {
      final String field = entry.getKey();
      final Object value = entry.getValue();
      final SchemaTypeDescriptor<Object> valueSchema =
          (SchemaTypeDescriptor<Object>) getFieldSchemaTypeDescriptor(fieldsDescriptors, field);
      final FieldDescriptor protoFieldDescriptor = getProtoFieldDescriptor(protoDescriptor, field);

      if (valueSchema.isArrayType()) {
        setArrayValue(valueSchema.asArrayTypeDescriptor(), value, protoFieldDescriptor, builder);
      } else if (valueSchema.isPrimitiveType()) {
        builder.setField(
            protoFieldDescriptor,
            buildPrimitiveValue(valueSchema.asPrimitiveTypeDescriptor(), value));
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
        builder.setField(
            protoFieldDescriptor,
            valueSchema.asEnumTypeDescriptor().getValueAccessor().createFrom(value.toString()));
      } else {
        throw new UnsupportedOperationException(
            String.format("Unknown value type [%s] for field [%s]", valueSchema.getType(), field));
      }
    }
    return builder.build();
  }

  private Object buildPrimitiveValue(PrimitiveTypeDescriptor<Object> type, Object value) {
    return type.getValueAccessor().createFrom(value);
  }

  private void setArrayValue(
      ArrayTypeDescriptor<Object> type,
      Object values,
      FieldDescriptor protoFieldDescriptor,
      Builder builder) {
    final SchemaTypeDescriptor<Object> valueType = type.getValueDescriptor();
    if (valueType.isPrimitiveType()
        && valueType.asPrimitiveTypeDescriptor().getType().equals(AttributeValueType.BYTE)) {
      // Bytes needs to be converted as PrimitiveValue of String
      builder.setField(
          protoFieldDescriptor,
          buildPrimitiveValue(type.getValueDescriptor().asPrimitiveTypeDescriptor(), values));
    } else {
      for (Object value : (Object[]) values) {
        builder.addRepeatedField(
            protoFieldDescriptor, buildArrayValue(type, protoFieldDescriptor, value, builder));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Object buildArrayValue(
      ArrayTypeDescriptor<Object> type,
      FieldDescriptor protoFieldDescriptor,
      Object value,
      Builder builder) {
    if (type.getValueDescriptor().isStructureType()) {
      // Array<Structure> needs to be created via builder
      return buildMessage(
          (Map<String, Object>) value,
          type.getValueDescriptor().asStructureTypeDescriptor().getFields(),
          protoFieldDescriptor.getMessageType(),
          builder.newBuilderForField(protoFieldDescriptor));
    } else if (type.getValueDescriptor().isPrimitiveType()) {
      return buildPrimitiveValue(type.getValueDescriptor().asPrimitiveTypeDescriptor(), value);
    } else {
      throw new UnsupportedOperationException(
          String.format("Unknown Array value type %s", type.getValueDescriptor().getType()));
    }
  }

  private FieldDescriptor getProtoFieldDescriptor(Descriptor proto, String name) {
    return Optional.ofNullable(proto.findFieldByName(name))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Unknown field [%s] in structure [%s]", name, proto.getName())));
  }

  private SchemaTypeDescriptor<?> getFieldSchemaTypeDescriptor(
      Map<String, SchemaTypeDescriptor<?>> from, String name) {
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
