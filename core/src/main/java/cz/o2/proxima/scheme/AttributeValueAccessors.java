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
import java.util.List;
import java.util.Map;

public class AttributeValueAccessors {

  private AttributeValueAccessors() {}

  private interface GenericValueAccessor<T> extends Serializable {

    default T createFrom(Object object) {
      throw new UnsupportedOperationException("Method createFrom() is not implemented.");
    }

    default Object valueOf(T value) {
      return value;
    }

    default byte[] asBytes(Object object) {
      throw new UnsupportedOperationException("Method asBytes() is not implemented.");
    }

    default Object fromBytes(byte[] bytes) {
      throw new UnsupportedOperationException("Method fromBytes() is not implemented.");
    }
  }

  public interface PrimitiveValueAccessor<T> extends GenericValueAccessor<T> {}

  public static class PrimitiveValueAccessorImpl<T> implements PrimitiveValueAccessor<T> {}

  public interface ArrayValueAccessor<T> extends GenericValueAccessor<T> {
    default <V> List<T> values(V object) {
      throw new UnsupportedOperationException("Method values() is not implemented.");
    }

    default <V> List<V> valuesOf(T object) {
      throw new UnsupportedOperationException("Method values() is not implemented.");
    }
  }

  public static class ArrayValueAccessorImpl<T> implements ArrayValueAccessor<T> {}

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
