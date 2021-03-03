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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public class AttributeValueAccessors {

  private AttributeValueAccessors() {}

  public interface GenericValueAccessor<T> extends Serializable {

    default T createFrom(Object object) {
      throw new UnsupportedOperationException("Method createFrom() is not implemented.");
    }

    default Object valueOf(T value) {
      return value;
    }
  }

  public interface PrimitiveValueAccessor<T> extends GenericValueAccessor<T> {}

  public static class PrimitiveValueAccessorImpl<T> implements PrimitiveValueAccessor<T> {}

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

  public static class ArrayValueAccessorImpl<T> implements ArrayValueAccessor<T> {

    private final GenericValueAccessor<T> valueAccessor;

    public ArrayValueAccessorImpl(GenericValueAccessor<T> valueAccessor) {
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

    default Map<String, Object> valuesOf(T value) {
      throw new UnsupportedOperationException("Method valuesOf is not implemented.");
    }

    default <V> V readField(String name, T value) {
      throw new UnsupportedOperationException("Method readField is not implemented.");
    }

    default T createFrom(Map<String, Object> map) {
      throw new UnsupportedOperationException("Method createFrom is not implemented.");
    }
  }

  public static class StructureValueAccessorImpl<T> implements StructureValueAccessor<T> {}

  public interface EnumValueAccessor<T> extends GenericValueAccessor<T> {}
}
