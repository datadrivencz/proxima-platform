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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import cz.o2.proxima.repository.AttributeDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * {@link org.apache.beam.sdk.coders.Coder} for {@link TypedElement typed elements}. This coder can
 * effectively serialize multiple attributes by serializing unique hash code of (entity,attribute)
 * tuple into the serialized payload.
 *
 * @param <T> Payload type.
 */
@EqualsAndHashCode(callSuper = false)
public class TypedElementCoder<T> extends CustomCoder<TypedElement<T>> {

  /**
   * Construct {@link TypedElementCoder} that can handle single attribute descriptor.
   *
   * @param attributeDescriptor Attribute descriptor to handle.
   * @param <T> Type of attribute values.
   * @return Coder.
   */
  public static <T> TypedElementCoder<T> of(AttributeDescriptor<T> attributeDescriptor) {
    return new TypedElementCoder<>(Collections.singleton(attributeDescriptor));
  }

  /**
   * Construct {@link TypedElementCoder} that can handle multiple attribute descriptors.
   *
   * @param attributeDescriptor Attribute descriptors to handle.
   * @param <T> Type of attribute values.
   * @return Coder.
   */
  public static <T> TypedElementCoder<T> of(Set<AttributeDescriptor<T>> attributeDescriptor) {
    return new TypedElementCoder<>(attributeDescriptor);
  }

  final Map<Integer, AttributeDescriptor<T>> indexedAttributeDescriptors;

  private TypedElementCoder(Set<AttributeDescriptor<T>> attributeDescriptors) {
    this.indexedAttributeDescriptors =
        attributeDescriptors
            .stream()
            .collect(
                Collectors.toMap(
                    attr -> Objects.hash(attr.getEntity(), attr.getName()), Function.identity()));
  }

  @Override
  public void encode(TypedElement<T> value, OutputStream outStream) throws IOException {
    final AttributeDescriptor<T> attributeDescriptor = value.getAttributeDescriptor();
    final int idx = Objects.hash(attributeDescriptor.getEntity(), attributeDescriptor.getName());
    if (!indexedAttributeDescriptors.containsKey(idx)) {
      throw new IOException(
          String.format(
              "Attribute [%s.%s] is not registered with this coder.",
              attributeDescriptor.getEntity(), attributeDescriptor.getName()));
    }
    VarIntCoder.of().encode(idx, outStream);
    VarIntCoder.of().encode(value.getOperation().ordinal(), outStream);
    StringUtf8Coder.of().encode(value.getKey(), outStream);
    if (attributeDescriptor.isWildcard()) {
      StringUtf8Coder.of()
          .encode(
              value
                  .getAttributeSuffix()
                  .orElseThrow(() -> new CoderException("Wildcard attribute must have a suffix.")),
              outStream);
    }
    if (value.isUpsert()) {
      ByteArrayCoder.of().encode(value.getPayload(), outStream);
    }
  }

  @Override
  public TypedElement<T> decode(InputStream inStream) throws IOException {
    final int idx = VarIntCoder.of().decode(inStream);
    final AttributeDescriptor<T> attributeDescriptor = indexedAttributeDescriptors.get(idx);
    if (attributeDescriptor == null) {
      throw new IOException(
          String.format("Attribute with index [%d] is not registered with this coder.", idx));
    }
    final TypedElement.Operation operation =
        TypedElement.Operation.values()[VarIntCoder.of().decode(inStream)];
    final String key = StringUtf8Coder.of().decode(inStream);
    final String attributeSuffix =
        attributeDescriptor.isWildcard() ? StringUtf8Coder.of().decode(inStream) : null;
    final byte[] payload;
    if (operation == TypedElement.Operation.UPSERT) {
      payload = ByteArrayCoder.of().decode(inStream);
    } else {
      payload = new byte[] {0};
    }
    return TypedElement.of(attributeDescriptor, operation, key, attributeSuffix, payload);
  }

  @Override
  public void verifyDeterministic() {
    VarIntCoder.of().verifyDeterministic();
    StringUtf8Coder.of().verifyDeterministic();
    ByteArrayCoder.of().verifyDeterministic();
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  @Override
  public Object structuralValue(TypedElement<T> value) {
    return super.structuralValue(value);
  }

  /**
   * Get descriptor of attribute this {@link org.apache.beam.sdk.coders.Coder} is supposed to
   * handle.
   *
   * @return Attribute descriptor.
   */
  public List<AttributeDescriptor<T>> getAttributeDescriptor() {
    return ImmutableList.copyOf(indexedAttributeDescriptors.values());
  }

  /**
   * Get {@link TypeDescriptor} of the {@link TypedElement} payload.
   *
   * @return Type descriptor.
   */
  @SuppressWarnings("unchecked")
  public TypeDescriptor<T> getValueTypeDescriptor() {
    // TODO remove this method.
    final AttributeDescriptor<T> attributeDescriptor =
        Objects.requireNonNull(Iterables.getFirst(indexedAttributeDescriptors.values(), null));
    return TypeDescriptor.of(
        (Class<T>) attributeDescriptor.getValueSerializer().getDefault().getClass());
  }
}
