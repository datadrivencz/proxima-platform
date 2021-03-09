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
import java.util.Map;

/** Classes providing access to Attribute values */
@Experimental
public class AttributeValueAccessors {

  private AttributeValueAccessors() {}

  /**
   * Generic value accessor
   *
   * @param <T> value type
   */
  public interface GenericValueAccessor<T> extends Serializable {

    /**
     * Create value from object
     *
     * @param object create from
     * @return value
     */
    default T createFrom(Object object) {
      throw new UnsupportedOperationException("Method createFrom() is not implemented.");
    }

    /**
     * Get value @TODO: this is a little bit weird
     *
     * @param value value
     * @param <V> expected type
     * @return value
     */
    @SuppressWarnings("unchecked")
    default <V> V valueOf(T value) {
      return (V) value;
    }
  }

  public interface PrimitiveValueAccessor<T> extends GenericValueAccessor<T> {}

  /**
   * Array value accessor
   *
   * @param <T>
   */
  public interface ArrayValueAccessor<T> extends GenericValueAccessor<T> {

    default <V> T[] createFrom(V[] object) {
      throw new UnsupportedOperationException("Method createFrom() is not implemented.");
    }

    default <V> V[] valuesOf(T[] object) {
      throw new UnsupportedOperationException("Method valuesOf() is not implemented.");
    }

    default <V> V[] valuesOf(T object) {
      throw new UnsupportedOperationException("Method valuesOf() is not implemented.");
    }
  }

  public static class DefaultArrayValueAccessor<T> implements ArrayValueAccessor<T> {

    private final GenericValueAccessor<T> valueAccessor;

    public DefaultArrayValueAccessor(GenericValueAccessor<T> valueAccessor) {
      this.valueAccessor = valueAccessor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> T[] createFrom(V[] object) {
      return (T[]) Arrays.stream(object).map(valueAccessor::createFrom).toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V[] valuesOf(T[] object) {
      return (V[]) Arrays.stream(object).map(valueAccessor::valueOf).toArray();
    }
  }

  public interface StructureValueAccessor<T> extends GenericValueAccessor<T> {

    Map<String, Object> valuesOf(T value);

    <V> V readField(String name, T value);

    T createFrom(Map<String, Object> map);
  }

  public interface EnumValueAccessor<T> extends Serializable {
    default String valueOf(T value) {
      return value.toString();
    }

    @SuppressWarnings("unchecked")
    default T createFrom(String value) {
      return (T) value;
    }
  }

  public static class DefaultStructureValueAccessor<T> implements StructureValueAccessor<T> {

    @SuppressWarnings("unchecked")
    public Map<String, Object> valuesOf(T value) {
      checkInputValue(value);
      return (Map<String, Object>) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V readField(String name, T value) {
      checkInputValue(value);
      return ((Map<String, V>) value).get(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T createFrom(Map<String, Object> map) {
      return (T) map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T createFrom(Object object) {
      checkInputValue(object);
      return createFrom((Map<String, Object>) object);
    }

    private void checkInputValue(Object object) {
      Preconditions.checkArgument(
          object instanceof Map,
          "Input value must be instance of Map. Given " + object.getClass().getName());
    }
  }
}
