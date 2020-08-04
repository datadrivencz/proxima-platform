/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.typed;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TypedElementSchemaProvider implements SchemaProvider {

  private final ProtoMessageSchema protoSchemaProvider = new ProtoMessageSchema();

  private final Map<TypeDescriptor<?>, Set<? extends AttributeDescriptor<?>>> attributes =
      new HashMap<>();

  @VisibleForTesting
  TypedElementSchemaProvider(Repository repository) {
    Objects.requireNonNull(repository)
        .getAllEntities()
        .flatMap(e -> e.getAllAttributes(true).stream())
        .forEach(this::registerAttribute);
  }

  @VisibleForTesting
  final <T> TypedElementSchemaProvider registerAttribute(
      AttributeDescriptor<T> attributeDescriptor) {
    @SuppressWarnings("unchecked")
    final Set<AttributeDescriptor<T>> attributeDescriptors =
        (Set<AttributeDescriptor<T>>)
            attributes.computeIfAbsent(
                TypedElement.typeDescriptor(attributeDescriptor), ignore -> new HashSet<>());
    attributeDescriptors.add(attributeDescriptor);
    return this;
  }

  @Nullable
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    @SuppressWarnings("unchecked")
    final Set<AttributeDescriptor<T>> attributeDescriptors =
        (Set<AttributeDescriptor<T>>) attributes.get(typeDescriptor);
    if (attributeDescriptors != null) {
      final AttributeDescriptor<T> first =
          Objects.requireNonNull(Iterables.getFirst(attributeDescriptors, null));
      final T defaultValue = first.getValueSerializer().getDefault();
      if (defaultValue instanceof Message) {
        return Schema.builder()
            .addStringField("key")
            .addStringField("attribute")
            .addRowField(
                "value", protoSchemaProvider.schemaFor(TypeDescriptor.of(defaultValue.getClass())))
            .build();
      }
    }
    return null;
  }

  @Nullable
  @Override
  public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
    @SuppressWarnings("unchecked")
    final TypeDescriptor<TypedElement<T>> typedElementDescriptor =
        (TypeDescriptor<TypedElement<T>>) typeDescriptor;
    final TypeDescriptor<T> messageDescriptor =
        TypeDescriptors.extractFromTypeParameters(
            typedElementDescriptor,
            TypedElement.class,
            new TypeDescriptors.TypeVariableExtractor<TypedElement<T>, T>() {});

    final Schema schema = schemaFor(typeDescriptor);
    final SerializableFunction<T, Row> delegate =
        Objects.requireNonNull(protoSchemaProvider.toRowFunction(messageDescriptor));
    return element -> {
      @SuppressWarnings("unchecked")
      final TypedElement<T> cast = (TypedElement<T>) element;
      return Row.withSchema(schema)
          .withFieldValue("key", cast.getKey())
          .withFieldValue("attribute", cast.getAttribute())
          .withFieldValue("value", delegate.apply(cast.getValue()))
          .build();
    };
  }

  @Nullable
  @Override
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    @SuppressWarnings("unchecked")
    final TypeDescriptor<TypedElement<T>> typedElementDescriptor =
        (TypeDescriptor<TypedElement<T>>) typeDescriptor;
    final TypeDescriptor<T> messageDescriptor =
        TypeDescriptors.extractFromTypeParameters(
            typedElementDescriptor,
            TypedElement.class,
            new TypeDescriptors.TypeVariableExtractor<TypedElement<T>, T>() {});
    @SuppressWarnings("unchecked")
    final Map<String, AttributeDescriptor<T>> candidates =
        (Map<String, AttributeDescriptor<T>>)
            attributes
                .get(typeDescriptor)
                .stream()
                .collect(Collectors.toMap(desc -> desc.getName(), Function.identity()));
    final SerializableFunction<Row, T> delegate =
        Objects.requireNonNull(protoSchemaProvider.fromRowFunction(messageDescriptor));
    return row -> {
      final AttributeDescriptor<T> attributeDescriptor = candidates.get(row.getString("attribute"));
      return (T)
          TypedElement.upsert(
              attributeDescriptor, row.getString("key"), delegate.apply(row.getRow("value")));
    };
  }

  public Set<TypeDescriptor<?>> getDescriptors() {
    return Collections.unmodifiableSet(attributes.keySet());
  }
}
