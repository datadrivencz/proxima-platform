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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.ValueBuilder;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoUtils {

  private static final Map<String, SchemaTypeDescriptor<?>> structCache = new ConcurrentHashMap<>();

  private ProtoUtils() {
    // no-op
  }

  public static <T extends Message> SchemaTypeDescriptor<T> convertProtoToSchema(
      Descriptor proto, @Nullable Builder builder) {
    StructureTypeDescriptor<T> schema =
        SchemaDescriptors.structures(
            proto.getName(),
            Collections.emptyMap(),
            new ProtoFieldReader<>(),
            new ProtoValueBuilder<>(builder));
    proto
        .getFields()
        .forEach(
            field -> {
              Builder fieldBuilder = null;
              if (builder != null) {
                if (field.getJavaType().equals(JavaType.MESSAGE) && !field.isRepeated()) {
                  fieldBuilder = builder.getFieldBuilder(field);
                } else if (field.isRepeated()) {
                  // throw new UnsupportedOperationException("FIXME - builder");
                }
              }
              schema.addField(field.getName(), convertField(field, fieldBuilder));
            });
    return schema.toTypeDescriptor();
  }

  @SuppressWarnings("unchecked")
  protected static <T> SchemaTypeDescriptor<T> convertField(
      FieldDescriptor proto, Builder builder) {
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
            messageTypeName, name -> convertProtoToSchema(proto.getMessageType(), builder));
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

  public static class ProtoValueBuilder<T extends Message> implements ValueBuilder<T> {

    private final Builder delegate;

    public ProtoValueBuilder(Builder delegate) {
      this.delegate = delegate;
    }

    @Override
    public <V> ValueBuilder<T> setField(String name, V value) {
      final FieldDescriptor protoFieldDescriptor =
          delegate.getDescriptorForType().findFieldByName(name);
      delegate.setField(protoFieldDescriptor, value);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T build() {
      return (T) delegate.build();
    }
  }

  private static class ProtoFieldReader<T extends Message>
      implements SchemaDescriptors.FieldReader<T> {

    @Override
    @SuppressWarnings("unchecked")
    public <V> V readField(
        String field, SchemaDescriptors.SchemaTypeDescriptor<V> fieldDescriptor, T value) {
      final FieldDescriptor protoFieldDescriptor =
          value.getDescriptorForType().findFieldByName(field);
      Preconditions.checkNotNull(
          protoFieldDescriptor,
          "Field {} not exists in {}",
          field,
          value.getDescriptorForType().getName());
      if (protoFieldDescriptor.getJavaType().equals(JavaType.BYTE_STRING)) {
        // ByteString is an special case and needs to be handled as ByteString
        return (V) ((ByteString) value.getField(protoFieldDescriptor)).toByteArray();
      } else if (protoFieldDescriptor.getJavaType().equals(JavaType.ENUM)) {
        // Enums is encoded as String
        return (V) (((EnumValueDescriptor) value.getField(protoFieldDescriptor)).getName());
      } else {
        return (V) value.getField(protoFieldDescriptor);
      }
    }
  }
}
