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
package cz.o2.proxima.scheme.proto.utils;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ProtoUtils {

  private static Map<String, SchemaTypeDescriptor<?>> structCache = new ConcurrentHashMap<>();

  private ProtoUtils() {
    // no-op
  }

  public static <T extends AbstractMessage> Map<String, Object> convertProtoObjectToMap(T value) {
    return value
        .getAllFields()
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().getName(),
                entry -> mapFieldToObject(entry.getKey(), entry.getValue())));
  }

  public static Message createProtoObject(Map<String, Object> value, Builder builder) {
    value.forEach(
        (key, val) -> {
          FieldDescriptor field = builder.getDescriptorForType().findFieldByName(key);
          if (field == null) {
            throw new IllegalArgumentException(
                "Message "
                    + builder.getDescriptorForType().toProto().getName()
                    + " doesn't have field "
                    + key);
          }
          if (value.containsKey(field.getName())) {
            if (field.isRepeated()) {
              ((Iterable<?>) val)
                  .forEach(
                      v ->
                          builder.addRepeatedField(
                              field, mapFieldValueToProtoValue(field, v, builder)));
            } else {
              builder.setField(field, mapFieldValueToProtoValue(field, val, builder));
            }
          }
        });
    return builder.build();
  }

  protected static Object mapFieldToObject(FieldDescriptor field, Object value) {
    switch (field.getJavaType()) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case INT:
      case LONG:
      case STRING:
        // for primitive types we can return value directly
        return value;
      case ENUM:
        // enums is converted to string
        return field.getEnumType().findValueByName(value.toString()).getName();
      case BYTE_STRING:
        return ((ByteString) value).toByteArray();
      case MESSAGE:
        if (value instanceof Iterable) {
          // repeated message
          return ((List<?>) value)
              .stream()
              .map(v -> convertProtoObjectToMap((AbstractMessage) v))
              .collect(Collectors.toList());
        } else {
          return convertProtoObjectToMap((AbstractMessage) value);
        }
      default:
        throw new IllegalStateException(
            "Unable to convert proto field "
                + field.getName()
                + " with type "
                + field.getJavaType());
    }
  }

  @SuppressWarnings("unchecked")
  protected static Object mapFieldValueToProtoValue(
      FieldDescriptor field, Object value, @Nullable Builder builder) {
    switch (field.getJavaType()) {
      case ENUM:
        return field.getEnumType().findValueByName(value.toString());
      case MESSAGE:
        Objects.requireNonNull(builder, "Builder can not be null.");
        return createProtoObject((Map<String, Object>) value, builder.newBuilderForField(field));
      case BYTE_STRING:
        if (value instanceof byte[]) {
          return ByteString.copyFrom((byte[]) value);
        } else {
          return value;
        }
      default:
        return value;
    }
  }

  public static <T> SchemaTypeDescriptor<T> convertProtoToSchema(Descriptor proto) {
    StructureTypeDescriptor<T> schema = SchemaDescriptors.structures(proto.getName());
    proto.getFields().forEach(f -> schema.addField(f.getName(), convertField(f)));
    return schema.toTypeDescriptor();
  }

  @SuppressWarnings("unchecked")
  protected static <T> SchemaTypeDescriptor<T> convertField(FieldDescriptor proto) {
    SchemaTypeDescriptor<T> descriptor;

    switch (proto.getJavaType()) {
      case STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.strings().toTypeDescriptor();
        break;
      case BOOLEAN:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.booleans().toTypeDescriptor();
        break;
      case BYTE_STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.bytes().toTypeDescriptor();
        break;
      case FLOAT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.floats().toTypeDescriptor();
        break;
      case DOUBLE:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.doubles().toTypeDescriptor();
        break;
      case LONG:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.longs().toTypeDescriptor();
        break;
      case INT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.integers().toTypeDescriptor();
        break;
      case ENUM:
        descriptor =
            (SchemaTypeDescriptor<T>)
                SchemaDescriptors.enums(
                        proto
                            .getEnumType()
                            .getValues()
                            .stream()
                            .map(EnumValueDescriptor::getName)
                            .collect(Collectors.toList()))
                    .toTypeDescriptor();
        break;
      case MESSAGE:
        final String messageTypeName = proto.getMessageType().toProto().getName();
        structCache.computeIfAbsent(
            messageTypeName, name -> convertProtoToSchema(proto.getMessageType()));
        descriptor = (SchemaTypeDescriptor<T>) structCache.get(messageTypeName).toTypeDescriptor();

        break;
      default:
        throw new IllegalStateException(
            "Unable to convert type " + proto.getJavaType() + " to any proxima type");
    }

    if (proto.isRepeated()) {
      return SchemaDescriptors.arrays(descriptor).toTypeDescriptor();
    } else {
      return descriptor;
    }
  }
}
