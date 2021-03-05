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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.EnumTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.PrimitiveTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SchemaDescriptorsTest {

  @Test
  public void assertSerializablePrimitiveType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(SchemaDescriptors.strings());
  }

  @Test
  public void assertSerializableArrayType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(SchemaDescriptors.bytes());
  }

  @Test
  public void assertSerializableStructType() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(
        SchemaDescriptors.structures("struct")
            .addField("field1", SchemaDescriptors.longs())
            .addField("field2", SchemaDescriptors.bytes()));
  }

  @Test
  public void checkEquals() {
    assertNotEquals(SchemaDescriptors.strings(), SchemaDescriptors.longs());
    assertEquals(SchemaDescriptors.strings(), SchemaDescriptors.strings());
    assertEquals(
        SchemaDescriptors.arrays(SchemaDescriptors.strings()),
        SchemaDescriptors.arrays(SchemaDescriptors.strings()));
    assertNotEquals(
        SchemaDescriptors.arrays(SchemaDescriptors.strings()),
        SchemaDescriptors.arrays(SchemaDescriptors.integers()));
    assertEquals(SchemaDescriptors.structures("test"), SchemaDescriptors.structures("test"));
    assertNotEquals(SchemaDescriptors.structures("test"), SchemaDescriptors.structures("another"));
    assertNotEquals(
        SchemaDescriptors.structures("test").addField("field", SchemaDescriptors.integers()),
        SchemaDescriptors.structures("test").addField("field", SchemaDescriptors.longs()));
    assertEquals(
        SchemaDescriptors.structures(
            "test", Collections.singletonMap("field", SchemaDescriptors.integers())),
        SchemaDescriptors.structures(
            "test", Collections.singletonMap("field", SchemaDescriptors.integers())));
  }

  @Test
  public void testStringDescriptor() {
    PrimitiveTypeDescriptor<String> string = SchemaDescriptors.strings();
    assertEquals(AttributeValueType.STRING, string.getType());
    assertEquals("STRING", string.toString());
    assertTrue(string.isPrimitiveType());
    assertThrows(UnsupportedOperationException.class, string::asArrayTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, string::asStructureTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, string::asEnumTypeDescriptor);
    assertEquals("10", string.getValueAccessor().createFrom(10));
    assertEquals("20", string.getValueAccessor().valueOf("20"));
  }

  @Test
  public void testArrayDescriptor() {
    ArrayTypeDescriptor<String> desc = SchemaDescriptors.arrays(SchemaDescriptors.strings());
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.STRING, desc.getValueType());
    assertEquals("ARRAY[STRING]", desc.toString());
    assertThrows(UnsupportedOperationException.class, desc::asStructureTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, desc::asEnumTypeDescriptor);
  }

  @Test
  public void testStructureDescriptorWithoutFields() {
    StructureTypeDescriptor<Object> desc = SchemaDescriptors.structures("structure");
    assertEquals("structure", desc.getName());
    assertTrue(desc.getFields().isEmpty());
    assertEquals("STRUCTURE structure", desc.toString());
    assertThrows(IllegalArgumentException.class, () -> desc.getField("something"));
    assertFalse(desc.hasField("something"));
    assertFalse(desc.isArrayType());
    assertFalse(desc.isPrimitiveType());
    assertTrue(desc.isStructureType());
    assertThrows(UnsupportedOperationException.class, desc::asArrayTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, desc::asPrimitiveTypeDescriptor);
    assertThrows(UnsupportedOperationException.class, desc::asEnumTypeDescriptor);
  }

  @Test
  public void testStructureDescriptorWithFields() {
    StructureTypeDescriptor<Object> s =
        SchemaDescriptors.structures("structure")
            .addField("string_field", SchemaDescriptors.strings())
            .addField(
                "array_of_string_field", SchemaDescriptors.arrays(SchemaDescriptors.strings()))
            .addField(
                "inner_structure",
                SchemaDescriptors.structures("inner_message")
                    .addField("byte_array", SchemaDescriptors.bytes())
                    .addField("int", SchemaDescriptors.integers())
                    .addField("long", SchemaDescriptors.longs()));
    assertEquals(AttributeValueType.STRUCTURE, s.getType());
    assertFalse(s.getFields().isEmpty());
    assertThrows(IllegalArgumentException.class, () -> s.getField("not-exists-field"));
    PrimitiveTypeDescriptor<String> stringDescriptor = SchemaDescriptors.strings();
    assertThrows(
        IllegalArgumentException.class, () -> s.addField("string_field", stringDescriptor));
    assertEquals(AttributeValueType.STRING, s.getField("string_field").getType());
    assertEquals(AttributeValueType.ARRAY, s.getField("array_of_string_field").getType());
    assertEquals(
        AttributeValueType.STRING,
        s.getField("array_of_string_field").asArrayTypeDescriptor().getValueType());
    assertEquals(AttributeValueType.STRUCTURE, s.getField("inner_structure").getType());
    assertEquals(
        AttributeValueType.STRUCTURE,
        s.getField("inner_structure").asStructureTypeDescriptor().getType());

    assertEquals(
        "inner_message", s.getField("inner_structure").asStructureTypeDescriptor().getName());

    assertEquals(
        AttributeValueType.ARRAY,
        s.getField("inner_structure").asStructureTypeDescriptor().getField("byte_array").getType());
    assertEquals(
        AttributeValueType.BYTE,
        s.getField("inner_structure")
            .asStructureTypeDescriptor()
            .getField("byte_array")
            .asArrayTypeDescriptor()
            .getValueType());
  }

  @Test
  public void testArrayTypeWithPrimitiveValue() {
    final ArrayTypeDescriptor<Long> desc = SchemaDescriptors.arrays(SchemaDescriptors.longs());
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.LONG, desc.getValueType());
    final ArrayValueAccessor<Long> accessor = desc.getValueAccessor();
    final Long[] expected = new Long[] {22L, 33L};
    assertArrayEquals(expected, accessor.createFrom(expected));
    assertArrayEquals(expected, accessor.createFrom(new String[] {"22", "33"}));
    assertArrayEquals(expected, accessor.valuesOf(expected));
  }

  @Test
  public void testBytes() {
    final ArrayTypeDescriptor<byte[]> bytes = SchemaDescriptors.bytes();
    assertEquals(AttributeValueType.ARRAY, bytes.getType());
    assertEquals(AttributeValueType.BYTE, bytes.getValueType());
    final ArrayValueAccessor<byte[]> accessor = bytes.getValueAccessor();
    final byte[] testBytes = new byte[] {1, 2, 3};
    byte[] valuesOf = accessor.valueOf(testBytes);
    assertArrayEquals(testBytes, valuesOf);
    // @TODO: FIX later
    // byte[] from = accessor.createFrom(valuesOf);
    // assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), accessor.createFrom("foo".getBytes(
    //    StandardCharsets.UTF_8)));

  }

  @Test
  public void testArrayTypeWithStructValue() {
    ArrayTypeDescriptor<Object> desc =
        SchemaDescriptors.arrays(
            SchemaDescriptors.structures("structure")
                .addField("field1", SchemaDescriptors.strings())
                .addField("field2", SchemaDescriptors.longs()));
    assertEquals(AttributeValueType.ARRAY, desc.getType());
    assertEquals(AttributeValueType.STRUCTURE, desc.getValueType());
    assertEquals(AttributeValueType.STRUCTURE, desc.getValueDescriptor().getType());
    final StructureTypeDescriptor<Object> structureTypeDescriptor =
        desc.getValueDescriptor().asStructureTypeDescriptor();
    assertEquals("structure", structureTypeDescriptor.getName());
    final Map<String, SchemaTypeDescriptor<?>> fields = structureTypeDescriptor.getFields();
    assertEquals(2, fields.size());
    final PrimitiveTypeDescriptor<String> string = SchemaDescriptors.strings();
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          // fields should be always immutable
          fields.put("foo", string);
        });

    final ArrayValueAccessor<Object> valueAccessor = desc.getValueAccessor();
    // Object[] expected = Arrays.asList()

    // Object[] arrayValues = valueAccessor.valuesOf()
  }

  @Test
  public void testLongType() {
    PrimitiveTypeDescriptor<Long> desc = SchemaDescriptors.longs();
    assertEquals(AttributeValueType.LONG, desc.getType());
    PrimitiveValueAccessor<Long> accessor = desc.getValueAccessor();
    assertEquals((Long) 33L, accessor.createFrom(33L));
    assertEquals((Long) 33L, accessor.valueOf(33L));
    assertEquals((Long) 33L, accessor.createFrom("33"));
  }

  @Test
  public void testDoubleType() {
    PrimitiveTypeDescriptor<Double> descriptor = SchemaDescriptors.doubles();
    assertEquals(AttributeValueType.DOUBLE, descriptor.getType());
    final PrimitiveValueAccessor<Double> valueProvider = descriptor.getValueAccessor();
    assertEquals(20.08D, valueProvider.valueOf(20.08), 0.0001);
    assertEquals(20.08D, valueProvider.createFrom("20.08"), 0.0001);
  }

  @Test
  public void testFloatType() {
    PrimitiveTypeDescriptor<Float> descriptor = SchemaDescriptors.floats();
    assertEquals(AttributeValueType.FLOAT, descriptor.getType());
    final PrimitiveValueAccessor<Float> valueProvider = descriptor.getValueAccessor();
    assertEquals(13.37F, valueProvider.valueOf(13.37F), 0.0001F);
    assertEquals(13.37F, valueProvider.createFrom("13.37"), 0.0001);
  }

  @Test
  public void testBooleanType() {
    PrimitiveTypeDescriptor<Boolean> descriptor = SchemaDescriptors.booleans();
    assertEquals(AttributeValueType.BOOLEAN, descriptor.getType());
    final PrimitiveValueAccessor<Boolean> valueProvider = descriptor.getValueAccessor();
    assertEquals(true, valueProvider.valueOf(true));
    assertEquals(false, valueProvider.createFrom("false"));
  }

  @Test
  public void testEnumType() {
    List<String> values = Arrays.asList("LEFT", "RIGHT");
    EnumTypeDescriptor<String> desc = SchemaDescriptors.enums(values);
    assertEquals(AttributeValueType.ENUM, desc.getType());
    assertEquals(values, desc.getValues());
    assertEquals("ENUM[LEFT, RIGHT]", desc.toString());

    assertEquals(desc, SchemaDescriptors.enums(values));
  }
}
