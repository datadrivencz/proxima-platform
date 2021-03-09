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
package cz.o2.proxima.scheme;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Experimental;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Classes providing access to Attribute values */
@Experimental
public class AttributeValueAccessors {
  public enum ValueAccessorType {
    PRIMITIVE,
    STRUCTURE,
    ARRAY,
    ENUM
  }

  private AttributeValueAccessors() {}

  /** Generic value accessor */
  public interface ValueAccessor extends Serializable {

    ValueAccessorType getType();
  }

  /**
   * Primitive value accessor
   *
   * @param <T> primitive type
   */
  public interface PrimitiveValueAccessor<T> extends ValueAccessor {

    T createFrom(Object object);

    @SuppressWarnings("unchecked")
    default <V> V valueOf(T value) {
      return (V) value;
    }

    @Override
    default ValueAccessorType getType() {
      return ValueAccessorType.PRIMITIVE;
    }
  }

  /**
   * Array value accessor
   *
   * @param <T>
   */
  public interface ArrayValueAccessor<T> extends ValueAccessor {

    <V> T[] createFrom(V[] object);

    <V> V[] valuesOf(T[] object);

    @Override
    default ValueAccessorType getType() {
      return ValueAccessorType.ARRAY;
    }
  }

  /**
   * Value accessor for {@link AttributeValueType#STRUCTURE} provides access for fields.
   *
   * @param <T> structure type
   */
  public interface StructureValueAccessor<T> extends ValueAccessor {

    /**
     * Return structure as {@link Map} with fields as keys
     *
     * @param value input structure
     * @return map with fields
     */
    Map<String, Object> valuesOf(T value);

    /**
     * Return value of specific field from structure
     *
     * @param name field name
     * @param value structure
     * @param <V> value type
     * @return field value
     */
    <V> V valueOf(String name, T value);

    /**
     * Create structure from {@link Map} with fields as keys
     *
     * @param map create from
     * @return structure
     */
    T createFrom(Map<String, Object> map);

    @Override
    default ValueAccessorType getType() {
      return ValueAccessorType.STRUCTURE;
    }
  }

  /**
   * Enum value accessor. Enum values is stored as string and default implementation works also with
   * string as input type.
   *
   * @param <T> value input type
   */
  public interface EnumValueAccessor<T> extends ValueAccessor {

    /**
     * Return string representation of input value type.
     *
     * @param value input value
     * @return value as string
     */
    String valueOf(T value);

    /**
     * Return enum value from string
     *
     * @param value string representation of enum value.
     * @return enum value
     */
    T createFrom(String value);

    @Override
    default ValueAccessorType getType() {
      return ValueAccessorType.ENUM;
    }
  }

  /**
   * Default implementation of {@link ArrayValueAccessor}.
   *
   * <p>This implementation delegate {@link #createFrom(Object[])} and {@link #valuesOf(Object[])}
   * to value accessor.
   *
   * @param <T> array value type
   */
  public static class DefaultArrayValueAccessor<T> implements ArrayValueAccessor<T> {

    private final DelegateToAccessor<T> delegateToAccessor;

    public DefaultArrayValueAccessor(ValueAccessor valueAccessor) {
      delegateToAccessor = createDelegates(valueAccessor);
    }

    private DelegateToAccessor<T> createDelegates(ValueAccessor accessor) {
      switch (accessor.getType()) {
        case PRIMITIVE:
          return new DelegateToAccessor<T>() {
            @SuppressWarnings("unchecked")
            final PrimitiveValueAccessor<T> cast = (PrimitiveValueAccessor<T>) accessor;

            @Override
            public T createFrom(Object object) {
              return cast.createFrom(object);
            }

            @Override
            public <V> V valueOf(T object) {
              return cast.valueOf(object);
            }
          };
        case STRUCTURE:
          return new DelegateToAccessor<T>() {
            @SuppressWarnings("unchecked")
            final StructureValueAccessor<T> cast = (StructureValueAccessor<T>) accessor;

            @Override
            @SuppressWarnings("unchecked")
            public T createFrom(Object object) {
              return cast.createFrom((Map<String, Object>) object);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <V> V valueOf(T object) {
              return (V) cast.valuesOf(object);
            }
          };
        case ENUM:
          return new DelegateToAccessor<T>() {
            @SuppressWarnings("unchecked")
            final EnumValueAccessor<T> cast = (EnumValueAccessor<T>) accessor;

            @Override
            public T createFrom(Object object) {
              return cast.createFrom(object.toString());
            }

            @Override
            @SuppressWarnings("unchecked")
            public <V> V valueOf(T object) {
              return (V) cast.valueOf(object);
            }
          };
        default:
          throw new UnsupportedOperationException("Unknown accessor type " + accessor.getType());
      }
    }

    /**
     * Delegation interface
     *
     * @param <T>
     */
    private interface DelegateToAccessor<T> extends Serializable {
      T createFrom(Object object);

      <V> V valueOf(T object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> T[] createFrom(V[] object) {
      return (T[]) Arrays.stream(object).map(delegateToAccessor::createFrom).toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V[] valuesOf(T[] object) {
      return (V[]) Arrays.stream(object).map(delegateToAccessor::valueOf).toArray();
    }
  }

  /**
   * Default structure accessor.
   *
   * @param <T> structure type
   */
  public static class DefaultStructureValueAccessor<T> implements StructureValueAccessor<T> {

    @SuppressWarnings("unchecked")
    public Map<String, Object> valuesOf(T value) {
      checkInputValue(value);
      return (Map<String, Object>) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V valueOf(String name, T value) {
      checkInputValue(value);
      return ((Map<String, V>) value).get(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T createFrom(Map<String, Object> map) {
      return (T) map;
    }

    private void checkInputValue(Object object) {
      Preconditions.checkArgument(
          object instanceof Map,
          "Input value must be instance of Map. Given " + object.getClass().getName());
    }
  }

  /** Default implementation of {@link EnumValueAccessor} with string representation. */
  public static class DefaultEnumValueAccessor implements EnumValueAccessor<String> {
    private final List<String> values;

    public DefaultEnumValueAccessor(List<String> values) {
      this.values = values;
    }

    @Override
    public String valueOf(String value) {
      Preconditions.checkArgument(
          values.contains(value), "Illegal value [%s]. Available values %s.", values);
      return value;
    }

    @Override
    public String createFrom(String value) {
      return valueOf(value);
    }
  }
}
