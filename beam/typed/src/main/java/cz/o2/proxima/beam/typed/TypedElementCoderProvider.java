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
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Coder provider for {@link TypedElement typed elements}. */
public class TypedElementCoderProvider extends CoderProvider {

  /**
   * Creates a new coder provider for a given repository. All attributes within a provided
   * repository will be automatically registered with this provider. See {@link
   * TypedElement#typeDescriptor(AttributeDescriptor)} to create a {@link TypeDescriptor} this
   * provider can handle.
   *
   * @param repository Repository to use for constructing the provider.
   * @return Coder provider.
   */
  public static TypedElementCoderProvider of(Repository repository) {
    return new TypedElementCoderProvider(repository);
  }

  private final Map<TypeDescriptor<?>, Set<? extends AttributeDescriptor<?>>> attributes =
      new HashMap<>();

  private final @Nullable StreamElementCoder streamElementCoder;

  @VisibleForTesting
  TypedElementCoderProvider() {
    this.streamElementCoder = null;
  }

  @VisibleForTesting
  TypedElementCoderProvider(Repository repository) {
    Objects.requireNonNull(repository)
        .getAllEntities()
        .flatMap(e -> e.getAllAttributes(true).stream())
        .forEach(this::registerAttribute);
    this.streamElementCoder = StreamElementCoder.of(repository);
  }

  @VisibleForTesting
  final <T> TypedElementCoderProvider registerAttribute(
      AttributeDescriptor<T> attributeDescriptor) {
    @SuppressWarnings("unchecked")
    final Set<AttributeDescriptor<T>> attributeDescriptors =
        (Set<AttributeDescriptor<T>>)
            attributes.computeIfAbsent(
                TypedElement.typeDescriptor(attributeDescriptor), ignore -> new HashSet<>());
    attributeDescriptors.add(attributeDescriptor);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
      throws CannotProvideCoderException {
    if (typeDescriptor.equals(TypeDescriptor.of(StreamElementCoder.class))
        && streamElementCoder != null) {
      return (Coder<T>) streamElementCoder;
    }
    if (!attributes.containsKey(typeDescriptor)) {
      throw new CannotProvideCoderException(
          String.format(
              "Type descriptor [%s] is not registered with this provider.", typeDescriptor));
    }
    final TypedElementCoder<Object> coder =
        TypedElementCoder.of((Set<AttributeDescriptor<Object>>) attributes.get(typeDescriptor));
    return (Coder<T>) coder;
  }
}
