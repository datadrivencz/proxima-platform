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
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessorImpl;
import cz.o2.proxima.scheme.AttributeValueAccessors.EnumValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessorImpl;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessorImpl;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;

/** SchemaDescriptors for types. */
public class SchemaDescriptors {

  private SchemaDescriptors() {
    // no-op
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for specific {@link AttributeValueType}.
   *
   * @param type primitive type
   * @param <T> descriptor type
   * @return primitive type descriptor.
   */
  public static <T> PrimitiveTypeDescriptor<T> primitives(
      AttributeValueType type, PrimitiveValueAccessor<T> valueProvider) {
    return new PrimitiveTypeDescriptor<>(type, valueProvider);
  }

  /**
   * Create {@link EnumTypeDescriptor} for {@link AttributeValueType#ENUM} type.
   *
   * @param values possible values
   * @param <T> type of descriptor
   * @return enum type descriptor
   */
  public static <T extends Serializable> EnumTypeDescriptor<T> enums(List<T> values) {
    return enums(values, new EnumValueAccessor<T>() {});
  }

  public static <T> EnumTypeDescriptor<T> enums(
      List<T> values, EnumValueAccessor<T> valueAccessor) {
    return new EnumTypeDescriptor<>(values, valueAccessor);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code String}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<String> strings() {
    return strings(
        new PrimitiveValueAccessor<String>() {
          @Override
          public String createFrom(Object object) {
            return Objects.toString(object);
          }
        });
  }

  public static PrimitiveTypeDescriptor<String> strings(
      PrimitiveValueAccessor<String> valueProvider) {
    return primitives(AttributeValueType.STRING, valueProvider);
  }

  /**
   * Create {@link ArrayTypeDescriptor} for byte array.
   *
   * @return Array type descriptor
   */
  public static ArrayTypeDescriptor<byte[]> bytes() {
    return arrays(
        primitives(AttributeValueType.BYTE, new PrimitiveValueAccessor<byte[]>() {}),
        new ArrayValueAccessorImpl<>());
  }

  public static ArrayTypeDescriptor<byte[]> bytes(PrimitiveValueAccessor<byte[]> valueProvider) {
    return arrays(
        primitives(AttributeValueType.BYTE, valueProvider), new ArrayValueAccessorImpl<>());
  }

  public static ArrayTypeDescriptor<byte[]> bytes(ArrayValueAccessor<byte[]> valueProvider) {
    return arrays(
        primitives(AttributeValueType.BYTE, new PrimitiveValueAccessorImpl<>()), valueProvider);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Integer}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Integer> integers() {
    return integers(
        new PrimitiveValueAccessor<Integer>() {
          @Override
          public Integer createFrom(Object object) {
            return Integer.parseInt(object.toString());
          }
        });
  }

  public static PrimitiveTypeDescriptor<Integer> integers(
      PrimitiveValueAccessor<Integer> valueProvider) {
    return primitives(AttributeValueType.INT, valueProvider);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Long}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Long> longs() {
    return longs(
        new PrimitiveValueAccessor<Long>() {
          @Override
          public Long createFrom(Object object) {
            return Long.parseLong(object.toString());
          }
        });
  }

  public static PrimitiveTypeDescriptor<Long> longs(PrimitiveValueAccessor<Long> valueProvider) {
    return primitives(AttributeValueType.LONG, valueProvider);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Double}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Double> doubles() {
    return doubles(
        new PrimitiveValueAccessor<Double>() {
          @Override
          public Double createFrom(Object object) {
            return Double.parseDouble(object.toString());
          }
        });
  }

  public static PrimitiveTypeDescriptor<Double> doubles(
      PrimitiveValueAccessor<Double> valueProvider) {
    return primitives(AttributeValueType.DOUBLE, valueProvider);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Float}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Float> floats() {
    return floats(
        new PrimitiveValueAccessor<Float>() {
          @Override
          public Float createFrom(Object object) {
            return Float.parseFloat(object.toString());
          }
        });
  }

  public static PrimitiveTypeDescriptor<Float> floats(PrimitiveValueAccessor<Float> valueProvider) {
    return primitives(AttributeValueType.FLOAT, valueProvider);
  }

  /**
   * Create {@link PrimitiveTypeDescriptor} for {@code Boolean}.
   *
   * @return Primitive type descriptor
   */
  public static PrimitiveTypeDescriptor<Boolean> booleans() {
    return booleans(
        new PrimitiveValueAccessor<Boolean>() {
          @Override
          public Boolean createFrom(Object object) {
            return Boolean.parseBoolean(object.toString());
          }
        });
  }

  public static PrimitiveTypeDescriptor<Boolean> booleans(
      PrimitiveValueAccessor<Boolean> valueProvider) {
    return primitives(AttributeValueType.BOOLEAN, valueProvider);
  }

  /**
   * Create {@link ArrayTypeDescriptor}.
   *
   * @param valueDescriptor primitive type
   * @param <T> value type
   * @return Array type descriptor
   */
  public static <T> ArrayTypeDescriptor<T> arrays(GenericTypeDescriptor<T> valueDescriptor) {
    return arrays(valueDescriptor, new ArrayValueAccessorImpl<>());
  }

  public static <T> ArrayTypeDescriptor<T> arrays(
      GenericTypeDescriptor<T> valueDescriptor, ArrayValueAccessor<T> valueAccessor) {
    return new ArrayTypeDescriptor<>(valueDescriptor, valueAccessor);
  }

  /**
   * Create {@link StructureTypeDescriptor} with name
   *
   * @param name structure name
   * @param <T> structure type
   * @return Structure type descriptor
   */
  public static <T> StructureTypeDescriptor<T> structures(String name) {
    return structures(name, Collections.emptyMap());
  }

  public static <T> StructureTypeDescriptor<T> structures(
      String name, Map<String, GenericTypeDescriptor<?>> fields) {
    return structures(name, fields, new StructureValueAccessorImpl<>());
  }

  public static <T> StructureTypeDescriptor<T> structures(
      String name,
      Map<String, GenericTypeDescriptor<?>> fields,
      StructureValueAccessor<T> valueProvider) {
    return new StructureTypeDescriptor<>(name, fields, valueProvider);
  }

  /**
   * Generic type descriptor. Parent class for other types
   *
   * @param <T> value type
   */
  public abstract static class GenericTypeDescriptor<T> implements Serializable {

    private static final String TYPE_CHECK_ERROR_MESSAGE_TEMPLATE =
        "TYPE_CHECK_ERROR_MESSAGE_TEMPLATE";

    /** Value type */
    @Getter protected final AttributeValueType type;

    protected GenericTypeDescriptor(AttributeValueType type) {
      this.type = type;
    }

    public boolean isPrimitiveType() {
      return this instanceof PrimitiveTypeDescriptor;
    }

    public PrimitiveTypeDescriptor<T> asPrimitiveTypeDescriptor() {
      Preconditions.checkState(
          isPrimitiveType(),
          TYPE_CHECK_ERROR_MESSAGE_TEMPLATE,
          getClass(),
          AttributeValueType.ENUM,
          getType());
      return (PrimitiveTypeDescriptor<T>) this;
    }

    public boolean isArrayType() {
      return getType().equals(AttributeValueType.ARRAY);
    }

    public ArrayTypeDescriptor<T> asArrayTypeDescriptor() {
      Preconditions.checkState(
          isArrayType(),
          TYPE_CHECK_ERROR_MESSAGE_TEMPLATE,
          getClass(),
          AttributeValueType.ENUM,
          getType());
      return (ArrayTypeDescriptor<T>) this;
    }

    public boolean isStructureType() {
      return getType().equals(AttributeValueType.STRUCTURE);
    }

    public StructureTypeDescriptor<T> asStructureTypeDescriptor() {
      Preconditions.checkState(
          isStructureType(),
          TYPE_CHECK_ERROR_MESSAGE_TEMPLATE,
          getClass(),
          AttributeValueType.STRUCTURE,
          getType());
      return (StructureTypeDescriptor<T>) this;
    }

    public boolean isEnumType() {
      return getType().equals(AttributeValueType.ENUM);
    }

    public EnumTypeDescriptor<T> asEnumTypeDescriptor() {
      Preconditions.checkState(
          isEnumType(),
          TYPE_CHECK_ERROR_MESSAGE_TEMPLATE,
          getClass(),
          AttributeValueType.ENUM,
          getType());
      return (EnumTypeDescriptor<T>) this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof GenericTypeDescriptor) {
        return type.equals(((GenericTypeDescriptor<?>) o).getType());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type);
    }
  }

  /**
   * Primitive type descriptor with simple type (eq String, Long, Integer, etc).
   *
   * @param <T> value type
   */
  public static class PrimitiveTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    @Getter private final PrimitiveValueAccessor<T> valueAccessor;

    public PrimitiveTypeDescriptor(
        AttributeValueType type, PrimitiveValueAccessor<T> valueAccessor) {
      super(type);
      Preconditions.checkNotNull(valueAccessor, "ValueProvider is not provided.");
      this.valueAccessor = valueAccessor;
    }

    @Override
    public PrimitiveTypeDescriptor<T> asPrimitiveTypeDescriptor() {
      return this;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type.name());
    }

    @Override
    public String toString() {
      return getType().name();
    }
  }

  /**
   * Array type descriptor allows to use other descriptor as value.
   *
   * @param <T> value type
   */
  public static class ArrayTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    @Getter final GenericTypeDescriptor<T> valueDescriptor;
    @Getter final ArrayValueAccessor<T> valueAccessor;

    public ArrayTypeDescriptor(
        GenericTypeDescriptor<T> valueDescriptor, ArrayValueAccessor<T> valueAccessor) {
      super(AttributeValueType.ARRAY);
      this.valueDescriptor = valueDescriptor;
      this.valueAccessor = valueAccessor;
    }

    /**
     * Get value type in Array.
     *
     * @return value type
     */
    public AttributeValueType getValueType() {
      return valueDescriptor.getType();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof ArrayTypeDescriptor) {
        return super.equals(o)
            && getValueType().equals(((ArrayTypeDescriptor<?>) o).getValueType());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 31 + valueDescriptor.hashCode();
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getType().name(), getValueType().toString());
    }
  }

  /**
   * Structure type descriptor allows to have fields with type as another descriptor.
   *
   * @param <T> structure type
   */
  public static class StructureTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    @Getter final String name;
    @Getter private final Map<String, GenericTypeDescriptor<?>> fields = new HashMap<>();
    @Getter private final StructureValueAccessor<T> valueAccessor;

    public StructureTypeDescriptor(
        String name,
        Map<String, GenericTypeDescriptor<?>> fields,
        StructureValueAccessor<T> valueAccessor) {
      super(AttributeValueType.STRUCTURE);
      this.name = name;
      Preconditions.checkNotNull(valueAccessor, "ValueProvider is not provided.");
      this.valueAccessor = valueAccessor;
      fields.forEach(this::addField);
    }

    @Override
    public StructureTypeDescriptor<T> asStructureTypeDescriptor() {
      return this;
    }

    /**
     * Add Field into structure type.
     *
     * @param name field name
     * @param descriptor value descriptor
     * @return this
     */
    public StructureTypeDescriptor<T> addField(String name, GenericTypeDescriptor<?> descriptor) {
      Preconditions.checkArgument(!fields.containsKey(name), "Duplicate field " + name);
      fields.put(name, descriptor);
      return this;
    }

    /**
     * Return {@code true} if field with given name exists in structure.
     *
     * @param name field name
     * @return boolean
     */
    public boolean hasField(String name) {
      return fields.containsKey(name);
    }

    @SuppressWarnings("squid:S1452")
    public GenericTypeDescriptor<?> getField(String name) {
      return Optional.ofNullable(fields.getOrDefault(name, null))
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "Field " + name + " not found in structure " + getName()));
    }

    @Override
    public boolean isPrimitiveType() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      StructureTypeDescriptor<?> that = (StructureTypeDescriptor<?>) o;
      return name.equals(that.name) && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), name, fields);
    }

    @Override
    public String toString() {
      StringBuilder builder =
          new StringBuilder(String.format("%s %s", getType().name(), getName()));
      getFields()
          .forEach(
              (field, type) ->
                  builder.append(
                      String.format("\n\t%s: %s", field, type.toString().replace("\t", "\t\t"))));
      return builder.toString();
    }
  }

  /**
   * Enum type descriptor.
   *
   * @param <T> value type
   */
  public static class EnumTypeDescriptor<T> extends GenericTypeDescriptor<T> {

    @Getter private final List<T> values;
    @Getter private final EnumValueAccessor<T> valueAccessor;

    public EnumTypeDescriptor(List<T> values, EnumValueAccessor<T> valueAccessor) {
      super(AttributeValueType.ENUM);
      this.values = values;
      this.valueAccessor = valueAccessor;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      EnumTypeDescriptor<?> that = (EnumTypeDescriptor<?>) o;
      return values.equals(that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), values);
    }

    @Override
    public String toString() {
      return getType().name() + getValues();
    }
  }
}
