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
import cz.o2.proxima.scheme.AttributeValueAccessors.DefaultArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.EnumValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.GenericValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.ProtoMessageValueAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoUtils {

  private ProtoUtils() {
    // no-op
  }

  /**
   * Covert proto object to proxima schema.
   *
   * @param proto proto message descriptor
   * @param defaultValue default value
   * @param <T> message type
   * @return structure type descriptor
   */
  public static <T extends Message> StructureTypeDescriptor<T> convertProtoToSchema(
      Descriptor proto, T defaultValue) {
    return convertProtoMessage(proto, defaultValue, new HashMap<>());
  }

  static <T extends Message> StructureTypeDescriptor<T> convertProtoMessage(
      Descriptor proto, T defaultValue, Map<String, SchemaTypeDescriptor<?>> structCache) {
    final Map<String, SchemaTypeDescriptor<?>> fields =
        proto
            .getFields()
            .stream()
            .collect(
                Collectors.toMap(
                    FieldDescriptor::getName, field -> convertField(field, structCache)));

    return SchemaDescriptors.structures(
        proto.getName(),
        fields,
        new ProtoMessageValueAccessor<>(fields, () -> proto, () -> defaultValue));
  }

  /**
   * Convert field of proto message to type descriptor
   *
   * @param proto field proto descriptor
   * @param <T> field type
   * @return schema type descriptor
   */
  @SuppressWarnings("unchecked")
  static <T> SchemaTypeDescriptor<T> convertField(
      FieldDescriptor proto, Map<String, SchemaTypeDescriptor<?>> structCache) {
    SchemaTypeDescriptor<T> descriptor;

    switch (proto.getJavaType()) {
      case STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.strings();
        break;
      case BOOLEAN:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.booleans();
        break;
      case BYTE_STRING:
        descriptor =
            (SchemaTypeDescriptor<T>)
                SchemaDescriptors.bytes(
                    new PrimitiveValueAccessor<ByteString>() {
                      @Override
                      public ByteString createFrom(Object object) {
                        return ByteString.copyFrom((byte[]) object);
                      }

                      @Override
                      public Object valueOf(ByteString value) {
                        return value.toByteArray();
                      }
                    });
        break;
      case FLOAT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.floats();
        break;
      case DOUBLE:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.doubles();
        break;
      case LONG:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.longs();
        break;
      case INT:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.integers();
        break;
      case ENUM:
        descriptor =
            SchemaDescriptors.enums(
                proto
                    .getEnumType()
                    .getValues()
                    .stream()
                    .map(EnumValueDescriptor::getName)
                    .collect(Collectors.toList()),
                new EnumValueAccessor<EnumValueDescriptor>() {
                  private EnumValueDescriptor getEnumDescriptor(String name) {
                    @Nullable
                    final EnumValueDescriptor enumValue = proto.getEnumType().findValueByName(name);
                    if (enumValue == null) {
                      throw new IllegalArgumentException(
                          String.format(
                              "Unknown value [%s] for enum type. Values: %s",
                              name, proto.getEnumType().getValues()));
                    }
                    return enumValue;
                  }

                  @Override
                  public EnumValueDescriptor createFrom(String object) {
                    return getEnumDescriptor(object);
                  }

                  @Override
                  public String valueOf(EnumValueDescriptor value) {
                    return getEnumDescriptor(value.getName()).getName();
                  }
                });
        break;
      case MESSAGE:
        final String messageTypeName = proto.getMessageType().toProto().getName();
        structCache.computeIfAbsent(
            messageTypeName,
            name ->
                convertProtoMessage(
                    proto.getMessageType(),
                    proto.getMessageType().toProto().getDefaultInstanceForType(),
                    structCache));
        descriptor = (SchemaTypeDescriptor<T>) structCache.get(messageTypeName);

        break;
      default:
        throw new IllegalStateException(
            "Unable to convert type " + proto.getJavaType() + " to any proxima type");
    }

    if (proto.isRepeated()) {
      GenericValueAccessor<T> valueAccessor;
      if (descriptor.isPrimitiveType()) {
        valueAccessor = descriptor.asPrimitiveTypeDescriptor().getValueAccessor();
      } else {
        valueAccessor = descriptor.asStructureTypeDescriptor().getValueAccessor();
      }
      return SchemaDescriptors.arrays(
          descriptor,
          new DefaultArrayValueAccessor<T>(valueAccessor) {
            @Override
            public <V> V[] valuesOf(T object) {
              return valuesOf((T[]) object);
            }

            @Override
            public <V> T[] createFrom(V[] object) {
              return (T[]) Arrays.stream(object).map(valueAccessor::createFrom).toArray();
            }
          });
    } else {
      return descriptor;
    }
  }
}
