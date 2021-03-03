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
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme.Device;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
        IllegalArgumentException.class, () -> valueProvider.readField("unknown-field", value));
    assertEquals("test-type-value", valueProvider.readField("type", value));
    assertArrayEquals(
        "test-payload-value".getBytes(StandardCharsets.UTF_8),
        valueProvider.readField("payload", value));
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

    Object[] repeatedStringValue = valueProvider.readField("repeated_string", object);
    assertArrayEquals(
        Arrays.asList("repeated_string_value_1", "repeated_string_value_2").toArray(new String[0]),
        repeatedStringValue);

    Map<String, Object> innerMessage = valueProvider.readField("inner_message", object);

    assertArrayEquals(
        Arrays.asList("repeated_inner_string_value1", "repeated_inner_string_value2")
            .toArray(new Object[0]),
        (Object[]) innerMessage.get("repeated_inner_string"));
    assertEquals(38D, innerMessage.get("inner_double_type"));

    assertEquals(Directions.LEFT.name(), innerMessage.get("inner_enum"));

    assertEquals("string_type_value", valueProvider.readField("string_type", object));
    assertEquals(true, valueProvider.readField("boolean_type", object));
    assertEquals(
        100L, Optional.ofNullable(valueProvider.readField("long_type", object)).orElse(-1));
    assertEquals(69, Optional.ofNullable(valueProvider.readField("int_type", object)).orElse(-1));

    final Map<String, Object> createFrom =
        new HashMap<String, Object>() {
          {
            put(
                "repeated_string",
                Arrays.asList("repeated_string_value_1", "repeated_string_value_2"));
            put(
                "inner_message",
                new HashMap<String, Object>() {
                  {
                    put(
                        "repeated_inner_string",
                        Arrays.asList(
                            "repeated_inner_string_value1", "repeated_inner_string_value2"));
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
}
