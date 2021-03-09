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
package cz.o2.proxima.scheme.proto.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.EnumValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.EnumTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme.Device;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.SecondEnum;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ProtoUtilsTest {

  @Test
  public void testConvertSimpleProtoToSchema() {
    StructureTypeDescriptor<Device> schema =
        ProtoUtils.convertProtoToSchema(Device.getDescriptor(), Device.getDefaultInstance());
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    assertEquals(2, schema.getFields().size());
    assertEquals(AttributeValueType.STRING, schema.getField("type").getType());
    assertEquals(AttributeValueType.ARRAY, schema.getField("payload").getType());
    assertEquals(
        AttributeValueType.BYTE, schema.getField("payload").asArrayTypeDescriptor().getValueType());

    Device value =
        Device.newBuilder()
            .setType("test-type-value")
            .setPayload(ByteString.copyFromUtf8("test-payload-value"))
            .build();

    StructureValueAccessor<Device> valueProvider = schema.getValueAccessor();
    assertThrows(
        IllegalArgumentException.class, () -> valueProvider.valueOf("unknown-field", value));
    assertEquals("test-type-value", valueProvider.valueOf("type", value));
    assertArrayEquals(
        "test-payload-value".getBytes(StandardCharsets.UTF_8),
        valueProvider.valueOf("payload", value));
    Map<String, Object> createFrom =
        new HashMap<String, Object>() {
          {
            put("type", "test-type-value");
            put("payload", "test-payload-value".getBytes());
          }
        };

    Device created = valueProvider.createFrom(createFrom);
    log.info("Proto object created from hashmap {}", created);
    assertEquals(value, created);
  }

  @Test
  public void testConvertComplexProtoToSchema() {
    StructureTypeDescriptor<ValueSchemeMessage> descriptor =
        ProtoUtils.convertProtoToSchema(
            ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.getDefaultInstance());
    log.debug("Schema: {}", descriptor);
    assertEquals(AttributeValueType.STRUCTURE, descriptor.getType());
    assertEquals(ValueSchemeMessage.getDescriptor().getName(), descriptor.getName());
    assertTrue(descriptor.hasField("repeated_inner_message"));
    assertEquals(AttributeValueType.ARRAY, descriptor.getField("repeated_inner_message").getType());
    assertEquals(
        AttributeValueType.ENUM,
        descriptor
            .getField("inner_message")
            .asStructureTypeDescriptor()
            .getField("inner_enum")
            .getType());

    assertEquals(
        AttributeValueType.STRUCTURE,
        descriptor.getField("repeated_inner_message").asArrayTypeDescriptor().getValueType());

    final ValueSchemeMessage object =
        ValueSchemeMessage.newBuilder()
            .addRepeatedString("repeated_string_value_1")
            .addRepeatedString("repeated_string_value_2")
            .setInnerMessage(
                InnerMessage.newBuilder()
                    .addRepeatedInnerString("repeated_inner_string_value1")
                    .addRepeatedInnerString("repeated_inner_string_value2")
                    .setInnerDoubleType(38)
                    .setInnerEnum(Directions.LEFT)
                    .build())
            .setStringType("string_type_value")
            .setBooleanType(true)
            .setLongType(100)
            .setIntType(69)
            .build();

    final StructureValueAccessor<ValueSchemeMessage> valueProvider = descriptor.getValueAccessor();

    Object[] repeatedStringValue = valueProvider.valueOf("repeated_string", object);
    assertArrayEquals(
        Arrays.asList("repeated_string_value_1", "repeated_string_value_2").toArray(new String[0]),
        repeatedStringValue);

    Map<String, Object> innerMessage = valueProvider.valueOf("inner_message", object);

    assertArrayEquals(
        Arrays.asList("repeated_inner_string_value1", "repeated_inner_string_value2")
            .toArray(new Object[0]),
        (Object[]) innerMessage.get("repeated_inner_string"));
    assertEquals(38D, innerMessage.get("inner_double_type"));

    assertEquals(Directions.LEFT.name(), innerMessage.get("inner_enum"));

    assertEquals("string_type_value", valueProvider.valueOf("string_type", object));
    assertEquals(true, valueProvider.valueOf("boolean_type", object));
    assertEquals(100L, Optional.ofNullable(valueProvider.valueOf("long_type", object)).orElse(-1));
    assertEquals(69, Optional.ofNullable(valueProvider.valueOf("int_type", object)).orElse(-1));

    final Map<String, Object> createFrom =
        new HashMap<String, Object>() {
          {
            put(
                "repeated_string",
                Arrays.asList("repeated_string_value_1", "repeated_string_value_2").toArray());
            put(
                "inner_message",
                new HashMap<String, Object>() {
                  {
                    put(
                        "repeated_inner_string",
                        Arrays.asList(
                                "repeated_inner_string_value1", "repeated_inner_string_value2")
                            .toArray());
                    put("inner_double_type", 38);
                    put("inner_enum", Directions.LEFT.name());
                  }
                });
            put("string_type", "string_type_value");
            put("boolean_type", true);
            put("long_type", 100);
            put("int_type", 69);
          }
        };
    ValueSchemeMessage created = valueProvider.createFrom(createFrom);
    log.debug("New created message {}", created);
    assertEquals(object, created);
  }

  @Test
  public void testEnumProtoDescriptor() {
    StructureTypeDescriptor<ValueSchemeMessage> descriptor =
        ProtoUtils.convertProtoToSchema(
            ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.getDefaultInstance());

    @SuppressWarnings("unchecked")
    EnumTypeDescriptor<EnumValueDescriptor> enumTypeDescriptor =
        descriptor
            .getField("inner_message")
            .asStructureTypeDescriptor()
            .getField("inner_enum")
            .asEnumTypeDescriptor();

    EnumValueAccessor<EnumValueDescriptor> accessor = enumTypeDescriptor.getValueAccessor();
    // proto enum fields contains +1 value UNRECOGNIZED
    assertEquals(Directions.values().length - 1, enumTypeDescriptor.getValues().size());
    assertEquals(
        Directions.getDescriptor()
            .getValues()
            .stream()
            .map(EnumValueDescriptor::getName)
            .collect(Collectors.toList()),
        enumTypeDescriptor.getValues());
    assertEquals(Directions.LEFT.name(), accessor.valueOf(Directions.LEFT.getValueDescriptor()));
    assertEquals(Directions.LEFT.getValueDescriptor(), accessor.createFrom("LEFT"));
    final EnumValueDescriptor illegalEnumValue = SecondEnum.VALUE1.getValueDescriptor();
    assertThrows(IllegalArgumentException.class, () -> accessor.valueOf(illegalEnumValue));
    assertThrows(IllegalArgumentException.class, () -> accessor.createFrom("NOT_IN_VALUES"));
  }

  @Test
  public void testArrayDescriptor() {
    StructureTypeDescriptor<ValueSchemeMessage> descriptor =
        ProtoUtils.convertProtoToSchema(
            ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.getDefaultInstance());
    @SuppressWarnings("unchecked")
    ArrayTypeDescriptor<String> arrayOfString =
        ((ArrayTypeDescriptor<String>) descriptor.getField("repeated_string"))
            .asArrayTypeDescriptor();
    ArrayValueAccessor<String> accessor = arrayOfString.getValueAccessor();
    assertArrayEquals(
        Arrays.asList("foo", "bar").toArray(new String[0]),
        accessor.valuesOf(new String[] {"foo", "bar"}));
    assertArrayEquals(
        Arrays.asList("foo", "bar").toArray(new String[0]),
        accessor.createFrom(new String[] {"foo", "bar"}));
  }
}
