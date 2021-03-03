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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.EnumValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.GenericTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.ProtoMessageValueAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoUtils {

  private static final Map<String, GenericTypeDescriptor<?>> structCache =
      new ConcurrentHashMap<>();

  private ProtoUtils() {
    // no-op
  }

  public static <T extends Message> StructureTypeDescriptor<T> convertProtoToSchema(
      Descriptor proto, T defaultValue) {
    final Map<String, GenericTypeDescriptor<?>> fields =
        proto
            .getFields()
            .stream()
            .collect(
                Collectors.toMap(
                    FieldDescriptor::getName,
                    f -> convertField(f, defaultValue.newBuilderForType())));

    StructureTypeDescriptor<T> schema =
        SchemaDescriptors.structures(
            proto.getName(),
            fields,
            new ProtoMessageValueAccessor<>(fields, () -> proto, () -> defaultValue));
    // fields.forEach(schema::addField);
    return schema;
  }

  @SuppressWarnings("unchecked")
  protected static <T> GenericTypeDescriptor<T> convertField(
      FieldDescriptor proto, Builder builder) {
    GenericTypeDescriptor<T> descriptor;

    switch (proto.getJavaType()) {
      case STRING:
        descriptor = (GenericTypeDescriptor<T>) SchemaDescriptors.strings();
        break;
      case BOOLEAN:
        descriptor = (GenericTypeDescriptor<T>) SchemaDescriptors.booleans();
        break;
      case BYTE_STRING:
        descriptor =
            (GenericTypeDescriptor<T>)
                SchemaDescriptors.bytes(
                    new PrimitiveValueAccessor<byte[]>() {
                      @Override
                      public byte[] createFrom(Object object) {
                        return ((ByteString) object).toByteArray();
                      }

                      @Override
                      public Object valueOf(byte[] value) {
                        return ByteString.copyFrom(value);
                      }

                      @Override
                      public byte[] asBytes(Object object) {
                        return ((ByteString) object).toByteArray();
                      }

                      @Override
                      public Object fromBytes(byte[] bytes) {
                        return ByteString.copyFrom(bytes);
                      }
                    });
        break;
      case FLOAT:
        descriptor = (GenericTypeDescriptor<T>) SchemaDescriptors.floats();
        break;
      case DOUBLE:
        descriptor = (GenericTypeDescriptor<T>) SchemaDescriptors.doubles();
        break;
      case LONG:
        descriptor = (GenericTypeDescriptor<T>) SchemaDescriptors.longs();
        break;
      case INT:
        descriptor = (GenericTypeDescriptor<T>) SchemaDescriptors.integers();
        break;
      case ENUM:
        final Map<String, EnumValueDescriptor> enumValues =
            proto
                .getEnumType()
                .getValues()
                .stream()
                .collect(Collectors.toMap(EnumValueDescriptor::getName, Function.identity()));

        descriptor =
            (GenericTypeDescriptor<T>)
                SchemaDescriptors.enums(
                    new ArrayList<>(enumValues.keySet()),
                    new EnumValueAccessor<String>() {
                      @Override
                      public String createFrom(Object object) {
                        return enumValues
                            .keySet()
                            .stream()
                            .filter(e -> e.equals(object.toString()))
                            .findFirst()
                            .orElseThrow(
                                () ->
                                    new IllegalArgumentException(
                                        String.format(
                                            "Unknown value [%s] for enum type. Values: %s",
                                            object, enumValues.keySet())));
                      }

                      @Override
                      public Object valueOf(String value) {
                        return enumValues
                            .values()
                            .stream()
                            .filter(v -> v.getName().equals(value))
                            .findFirst()
                            .orElseThrow(
                                () ->
                                    new IllegalArgumentException(
                                        String.format(
                                            "Unknown value [%s] for enum type. Values: %s",
                                            value, enumValues.keySet())));
                      }
                    });
        break;
      case MESSAGE:
        final String messageTypeName = proto.getMessageType().toProto().getName();
        log.debug("Converting message {} with descriptor {}", messageTypeName, proto);
        structCache.computeIfAbsent(
            messageTypeName,
            name ->
                convertProtoToSchema(
                    proto.getMessageType(),
                    proto.getMessageType().toProto().getDefaultInstanceForType()));
        descriptor = (GenericTypeDescriptor<T>) structCache.get(messageTypeName);

        break;
      default:
        throw new IllegalStateException(
            "Unable to convert type " + proto.getJavaType() + " to any proxima type");
    }

    if (proto.isRepeated()) {

      return SchemaDescriptors.arrays(
          descriptor,
          new ArrayValueAccessor<T>() {
            @Override
            public <V> List<T> values(V object) {
              @SuppressWarnings({"unchecked", "rawtypes"})
              List<T> cast = (List) object;
              return cast;
            }

            @Override
            public T createFrom(Object object) {
              @SuppressWarnings({"unchecked", "rawtypes"})
              List<T> cast = (List) object;
              return (T) cast; // @TODO: WTF?
            }
          });
    } else {
      return descriptor;
    }
  }
}
