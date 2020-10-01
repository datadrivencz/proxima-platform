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
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import cz.o2.proxima.repository.AttributeDescriptor;
import java.lang.ref.SoftReference;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * Typed version of {@link cz.o2.proxima.storage.StreamElement} for convenient usage in Beam
 * pipelines.
 *
 * @param <T> Type of element value.
 */
@EqualsAndHashCode
public class TypedElement<T> {

  public enum Operation {

    /** Operation representing delete of a single attribute. */
    DELETE,

    /** Operation representing delete of a wildcard attribute. */
    DELETE_ALL,

    /** Operation representing update or insert of a single attribute */
    UPSERT
  }

  /**
   * Return new {@link TypedElement} {@link TypeDescriptor} for a given attribute.
   *
   * @param attributeDescriptor Attribute descriptor to get {@link TypeDescriptor} for.
   * @param <T> Type of attribute values.
   * @return Type descriptor.
   */
  public static <T> TypeDescriptor<TypedElement<T>> typeDescriptor(
      AttributeDescriptor<T> attributeDescriptor) {
    @SuppressWarnings("unchecked")
    final Class<T> clazz =
        (Class<T>) attributeDescriptor.getValueSerializer().getDefault().getClass();
    return new TypeDescriptor<TypedElement<T>>() {}.where(
        new TypeParameter<T>() {}, TypeDescriptor.of(clazz));
  }

  /**
   * Construct a new element for an attribute.
   *
   * @param attributeDescriptor Descriptor of an attribute.
   * @param operation - Operation type
   * @param key Entity key.
   * @param attributeSuffix Suffix of an attribute (the part replacing wildcard).
   * @param payload Byte representation of element value.
   * @param <T> Type of element value.
   * @return Typed element.
   */
  public static <T> TypedElement<T> of(
      AttributeDescriptor<T> attributeDescriptor,
      Operation operation,
      String key,
      @Nullable String attributeSuffix,
      @Nullable byte[] payload) {
    if (attributeDescriptor.isWildcard()) {
      Preconditions.checkArgument(attributeSuffix != null, "Null attribute suffix.");
      Preconditions.checkArgument(
          !attributeSuffix.startsWith(attributeDescriptor.toAttributePrefix()),
          "Fully qualified attribute name.");
    } else {
      Preconditions.checkArgument(attributeSuffix == null, "Non-empty attribute suffix.");
    }
    return new TypedElement<>(attributeDescriptor, operation, key, attributeSuffix, payload, null);
  }

  /**
   * Construct a new {@link Operation#UPSERT} element for an attribute.
   *
   * @param attributeDescriptor Descriptor of an attribute.
   * @param key Entity key.
   * @param value A new value for the attribute.
   * @param <T> Type of element value.
   * @return Typed element.
   */
  public static <T> TypedElement<T> upsert(
      AttributeDescriptor<T> attributeDescriptor, String key, T value) {
    return new TypedElement<>(
        attributeDescriptor,
        Operation.UPSERT,
        key,
        null,
        attributeDescriptor.getValueSerializer().serialize(value),
        new SoftReference<>(value));
  }

  /**
   * Construct a new {@link Operation#UPSERT} element for a wildcard attribute.
   *
   * @param attributeDescriptor Descriptor of a wildcard attribute.
   * @param key Entity key.
   * @param attributeSuffix Suffix of an attribute (the part replacing wildcard).
   * @param value A new value for the attribute.
   * @param <T> Type of element value.
   * @return Typed element.
   */
  public static <T> TypedElement<T> upsertWildcard(
      AttributeDescriptor<T> attributeDescriptor, String key, String attributeSuffix, T value) {
    return new TypedElement<>(
        attributeDescriptor,
        Operation.UPSERT,
        key,
        Objects.requireNonNull(attributeSuffix),
        attributeDescriptor.getValueSerializer().serialize(value),
        new SoftReference<>(value));
  }

  /**
   * Construct a new {@link Operation#DELETE} element for non-wildcard attribute.
   *
   * @param attributeDescriptor Descriptor of a non-wildcard attribute.
   * @param key Entity key.
   * @param <T> Type of element value.
   * @return Typed element.
   */
  public static <T> TypedElement<T> delete(AttributeDescriptor<T> attributeDescriptor, String key) {
    checkNotWildcard(attributeDescriptor);
    return new TypedElement<>(attributeDescriptor, Operation.DELETE, key, null, null, null);
  }

  /**
   * Construct a new {@link Operation#DELETE} element for given instance of wildcard attribute.
   *
   * @param attributeDescriptor Descriptor of a non-wildcard attribute.
   * @param key Entity key.
   * @param attributeSuffix Suffix of an attribute (the part replacing wildcard).
   * @param <T> Type of element value.
   * @return Typed element.
   */
  public static <T> TypedElement<T> delete(
      AttributeDescriptor<T> attributeDescriptor, String key, String attributeSuffix) {
    checkWildcard(attributeDescriptor);
    return new TypedElement<>(
        attributeDescriptor,
        Operation.DELETE,
        key,
        Objects.requireNonNull(attributeSuffix),
        null,
        null);
  }

  /**
   * Construct a new {@link Operation#DELETE} element for non-wildcard attribute.
   *
   * @param attributeDescriptor Descriptor of a non-wildcard attribute.
   * @param key Entity key.
   * @param <T> Type of element value.
   * @return Typed element.
   */
  public static <T> TypedElement<T> deleteWildcard(
      AttributeDescriptor<T> attributeDescriptor, String key) {
    checkWildcard(attributeDescriptor);
    return new TypedElement<>(attributeDescriptor, Operation.DELETE_ALL, key, null, null, null);
  }

  @Getter private final AttributeDescriptor<T> attributeDescriptor;
  @Getter private final Operation operation;
  @Getter private final String key;
  @Getter private final byte[] payload;
  private final String attributeSuffix;

  /**
   * De-serialized value form, wrapped in {@link SoftReference}, which allows gc to free the value
   * when JVM runs out of memory. Please note that behaviour of soft reference may vary depending on
   * GC implementation.
   */
  @Nullable transient SoftReference<T> value;

  private TypedElement(
      AttributeDescriptor<T> attributeDescriptor,
      Operation operation,
      String key,
      String attributeSuffix,
      byte[] payload,
      @Nullable SoftReference<T> value) {
    this.attributeDescriptor = attributeDescriptor;
    this.operation = operation;
    this.key = key;
    this.attributeSuffix = attributeSuffix;
    this.payload = payload;
    this.value = value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("attributeDescriptor", attributeDescriptor)
        .add("operation", operation)
        .add("key", key)
        .add("attributeSuffix", attributeSuffix)
        .add("payload.length", payload == null ? -1 : payload.length)
        .toString();
  }

  /**
   * Get full attribute name of this element.
   *
   * @return Attribute name.
   */
  public String getAttribute() {
    if (attributeDescriptor.isWildcard()) {
      return attributeDescriptor.toAttributePrefix() + attributeSuffix;
    }
    return attributeDescriptor.getName();
  }

  /**
   * Get attribute suffix of wildcard attribute.
   *
   * @return Maybe attribute suffix.
   */
  public Optional<String> getAttributeSuffix() {
    return Optional.ofNullable(attributeSuffix);
  }

  /**
   * Find value of this element. Values can be empty if {@link #operation} in {@link
   * Operation#DELETE} or {@link Operation#DELETE_ALL}.
   *
   * @param cache If value is expected to be accessed multiple times, cache the de-serialized form.
   * @return Maybe value.
   */
  public Optional<T> findValue(boolean cache) {
    final T cachedValue = value != null ? value.get() : null;
    if (cachedValue == null) {
      final Optional<T> deserialize = attributeDescriptor.getValueSerializer().deserialize(payload);
      if (cache && deserialize.isPresent()) {
        value = new SoftReference<>(deserialize.get());
      }
      return deserialize;
    }
    return Optional.of(cachedValue);
  }

  /**
   * Find value of this element. Values can be empty if {@link #operation} in {@link
   * Operation#DELETE} or {@link Operation#DELETE_ALL}.
   *
   * @return Maybe value.
   */
  public Optional<T> findValue() {
    return findValue(false);
  }

  /**
   * Get value of this element. This method can be used for {@link Operation#UPSERT} only and throws
   * an {@link IllegalStateException} otherwise.
   *
   * @param cache If value is expected to be accessed multiple times, cache the de-serialized form.
   * @return Value.
   */
  public T getValue(boolean cache) {
    return findValue(cache)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("Unable to deserialize value of [%s].", attributeDescriptor)));
  }
  /**
   * Get value of this element. This method can be used for {@link Operation#UPSERT} only and throws
   * an {@link IllegalStateException} otherwise.
   *
   * @return Value.
   */
  public T getValue() {
    return getValue(false);
  }

  /**
   * Checks if {@link #operation} is {@link Operation#DELETE}.
   *
   * @return True if operation is delete.
   */
  public boolean isDelete() {
    return getOperation() == Operation.DELETE;
  }

  /**
   * Checks if {@link #operation} is {@link Operation#DELETE_ALL} (wildcard delete).
   *
   * @return True if operation is delete all.
   */
  public boolean isDeleteAll() {
    return getOperation() == Operation.DELETE_ALL;
  }

  /**
   * Checks if {@link #operation} is {@link Operation#UPSERT}.
   *
   * @return True if operation is upsert.
   */
  public boolean isUpsert() {
    return getOperation() == Operation.UPSERT;
  }

  private static void checkWildcard(AttributeDescriptor<?> attributeDescriptor) {
    if (!attributeDescriptor.isWildcard()) {
      throw new UnsupportedOperationException(
          String.format(
              "This operation is not supported for non-wildcard attribute [%s:%s].",
              attributeDescriptor.getEntity(), attributeDescriptor.getName()));
    }
  }

  private static void checkNotWildcard(AttributeDescriptor<?> attributeDescriptor) {
    if (attributeDescriptor.isWildcard()) {
      throw new UnsupportedOperationException(
          String.format(
              "This operation is not supported for wildcard attribute [%s:%s].",
              attributeDescriptor.getEntity(), attributeDescriptor.getName()));
    }
  }

  @VisibleForTesting
  void clearCachedValue() {
    if (value != null) {
      value.clear();
    }
  }
}
