/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import cz.o2.proxima.core.scheme.SchemaDescriptors;
import cz.o2.proxima.core.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.core.scheme.SchemaDescriptors.StructureTypeDescriptor;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
   * @param <T> message type
   * @return structure type descriptor
   */
  public static <T extends Message> StructureTypeDescriptor<T> convertProtoToSchema(
      Descriptor proto) {
    return convertProtoMessage(proto, new ConcurrentHashMap<>(), Collections.singleton(proto));
  }

  static <T extends Message> StructureTypeDescriptor<T> convertProtoMessage(
      Descriptor proto,
      Map<String, SchemaTypeDescriptor<?>> structCache,
      Set<Descriptor> seenMessages) {
    final Map<String, SchemaTypeDescriptor<?>> fields =
        proto.getFields().stream()
            .collect(
                Collectors.toMap(
                    FieldDescriptor::getName,
                    field -> {
                      if (field.getJavaType().equals(JavaType.MESSAGE)
                          && seenMessages.contains(field.getMessageType())) {
                        throw new UnsupportedOperationException(
                            "Recursion in field [" + field.getName() + "] is not supported");
                      }
                      return convertField(field, structCache, seenMessages);
                    }));

    return SchemaDescriptors.structures(proto.getName(), fields);
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
      FieldDescriptor proto,
      Map<String, SchemaTypeDescriptor<?>> structCache,
      Set<Descriptor> seenMessages) {
    SchemaTypeDescriptor<T> descriptor;
    switch (proto.getJavaType()) {
      case STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.strings();
        break;
      case BOOLEAN:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.booleans();
        break;
      case BYTE_STRING:
        descriptor = (SchemaTypeDescriptor<T>) SchemaDescriptors.bytes();
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
            (SchemaTypeDescriptor<T>)
                SchemaDescriptors.enums(
                    proto.getEnumType().getValues().stream()
                        .map(EnumValueDescriptor::getName)
                        .collect(Collectors.toList()));
        break;
      case MESSAGE:
        final String messageTypeName = proto.getMessageType().toProto().getName();
        descriptor = (SchemaTypeDescriptor<T>) structCache.get(messageTypeName);
        if (descriptor == null) {
          final Set<Descriptor> newSeenMessages = new HashSet<>(seenMessages);
          newSeenMessages.add(proto.getMessageType());
          @Nullable
          final SchemaTypeDescriptor<T> known =
              (SchemaTypeDescriptor<T>) mapKnownWrapperTypes(proto);
          if (known != null) {
            descriptor = known;
          } else {
            descriptor =
                (SchemaTypeDescriptor<T>)
                    convertProtoMessage(
                        proto.getMessageType(),
                        structCache,
                        Collections.unmodifiableSet(newSeenMessages));
          }
          structCache.put(messageTypeName, descriptor);
        }
        break;
      default:
        throw new IllegalStateException(
            "Unable to convert type " + proto.getJavaType() + " to any proxima type");
    }

    if (proto.isRepeated()) {
      return SchemaDescriptors.arrays(descriptor);
    } else {
      return descriptor;
    }
  }

  @Nullable
  private static SchemaTypeDescriptor<?> mapKnownWrapperTypes(FieldDescriptor descriptor) {
    switch (descriptor.getMessageType().getFullName()) {
      case "google.protobuf.StringValue":
        return SchemaDescriptors.strings();
      case "google.protobuf.BoolValue":
        return SchemaDescriptors.booleans();
      case "google.protobuf.Int32Value":
      case "google.protobuf.UInt32Value":
        return SchemaDescriptors.integers();
      case "google.protobuf.Int64Value":
      case "google.protobuf.UInt64Value":
        return SchemaDescriptors.longs();
      case "google.protobuf.DoubleValue":
        return SchemaDescriptors.doubles();
      case "google.protobuf.BytesValue":
        return SchemaDescriptors.bytes();
      case "google.protobuf.FloatValue":
        return SchemaDescriptors.floats();
      default:
        return null;
    }
  }
}
