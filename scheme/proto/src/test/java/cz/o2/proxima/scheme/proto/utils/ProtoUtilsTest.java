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
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme.Device;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.SecondInnerMessage;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

public class ProtoUtilsTest {

  @Test
  public void testConvertSimpleProtoToSchema() {
    SchemaTypeDescriptor<Device> schema =
        ProtoUtils.convertProtoToSchema(Device.getDescriptor(), Device.newBuilder());
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    assertEquals(2, schema.getStructureTypeDescriptor().getFields().size());
    assertEquals(
        AttributeValueType.STRING, schema.getStructureTypeDescriptor().getField("type").getType());
    assertEquals(
        AttributeValueType.ARRAY,
        schema.getStructureTypeDescriptor().getField("payload").getType());
    assertEquals(
        AttributeValueType.BYTE,
        schema
            .getStructureTypeDescriptor()
            .getField("payload")
            .getArrayTypeDescriptor()
            .getValueType());
  }

  @Test
  public void testConvertComplexProtoToSchema() {
    SchemaTypeDescriptor<ValueSchemeMessage> schema =
        ProtoUtils.convertProtoToSchema(
            ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.newBuilder());
    assertEquals(AttributeValueType.STRUCTURE, schema.getType());
    StructureTypeDescriptor<ValueSchemeMessage> descriptor = schema.getStructureTypeDescriptor();
    assertEquals(ValueSchemeMessage.getDescriptor().getName(), descriptor.getName());
    assertTrue(descriptor.hasField("repeated_inner_message"));
    assertEquals(AttributeValueType.ARRAY, descriptor.getField("repeated_inner_message").getType());
    assertEquals(
        AttributeValueType.ENUM,
        descriptor
            .getField("inner_message")
            .getStructureTypeDescriptor()
            .getField("inner_enum")
            .getType());

    assertEquals(
        AttributeValueType.STRUCTURE,
        descriptor.getField("repeated_inner_message").getArrayTypeDescriptor().getValueType());
  }

  @Test
  public void testReadFieldFromSimpleValue() {
    final StructureTypeDescriptor<Message> descriptor =
        ProtoUtils.convertProtoToSchema(Event.getDescriptor(), Event.newBuilder())
            .getStructureTypeDescriptor();
    assertEquals(AttributeValueType.STRUCTURE, descriptor.getType());

    final Event event =
        Event.newBuilder()
            .setGatewayId("test-id")
            .setPayload(ByteString.copyFromUtf8("test-payload"))
            .build();

    assertEquals(
        "test-id", descriptor.readField("gatewayId", descriptor.getField("gatewayId"), event));
    assertArrayEquals(
        "test-payload".getBytes(StandardCharsets.UTF_8),
        (byte[]) descriptor.readField("payload", descriptor.getField("payload"), event));
  }

  @Test
  public void testReadValuesFromComplexObject() {
    final StructureTypeDescriptor<Message> scheme =
        ProtoUtils.convertProtoToSchema(
                ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.newBuilder())
            .getStructureTypeDescriptor();

    final ValueSchemeMessage value =
        ValueSchemeMessage.newBuilder()
            .addRepeatedString("repeated_string_1")
            .addRepeatedString("repeated_string_2")
            .addRepeatedInnerMessage(
                InnerMessage.newBuilder()
                    .addRepeatedInnerString("inner_repeated_string_1")
                    .addRepeatedInnerString("inner_repeated_string_2")
                    .setInnerEnum(Directions.LEFT)
                    .setInnerDoubleType(100.0)
                    .setInnerInnerMessage(
                        SecondInnerMessage.newBuilder().setInnerFloatType(333.33F).buildPartial())
                    .build())
            .setInnerMessage(
                InnerMessage.newBuilder()
                    .addRepeatedInnerString("inner_repeated_string_A")
                    .addRepeatedInnerString("inner_repeated_string_B")
                    .setInnerEnum(Directions.RIGHT)
                    .setInnerDoubleType(200.0)
                    .setInnerInnerMessage(
                        SecondInnerMessage.newBuilder().setInnerFloatType(666.66F).buildPartial())
                    .build())
            .setStringType("string_value")
            .setBooleanType(true)
            .setLongType(55)
            .setIntType(69)
            .build();

    Object repeated =
        scheme.readField("repeated_string", scheme.getField("repeated_string"), value);

    ArrayTypeDescriptor<Object> valueDescriptor =
        (ArrayTypeDescriptor<Object>) scheme.getField("repeated_string").getArrayTypeDescriptor();
    List<Object> values =
        valueDescriptor.readValues(repeated, valueDescriptor.getValueDescriptor());

    assertEquals(Arrays.asList("repeated_string_1", "repeated_string_2"), values);

    assertEquals(
        "string_value", scheme.readField("string_type", scheme.getField("string_type"), value));
    assertEquals(true, scheme.readField("boolean_type", scheme.getField("boolean_type"), value));
    assertEquals(55L, scheme.readField("long_type", scheme.getField("long_type"), value));
    assertEquals(69, scheme.readField("int_type", scheme.getField("int_type"), value));
    //    assertEquals(Arrays.asList("repeated_string_1", "repeated_string_2").toArray(new
    // String[0]), scheme.readField("repeated_string", s));

  }

  @Test
  @Ignore(value = "not finished")
  public void testBuildSimpleValue() {
    final StructureTypeDescriptor<Message> descriptor =
        ProtoUtils.convertProtoToSchema(Event.getDescriptor(), Event.newBuilder())
            .getStructureTypeDescriptor();

    final Event event =
        Event.newBuilder()
            .setGatewayId("test-id")
            .setPayload(ByteString.copyFromUtf8("test-payload"))
            .build();
    final Message message =
        descriptor
            .getValueBuilder()
            .setField("gatewayId", "test-id")
            .setField("payload", "test-payload")
            .build();

    assertEquals(event, message);
  }

  @Test
  @Ignore(value = "not finished")
  public void testBuildComplexValue() {
    final StructureTypeDescriptor<Message> schema =
        ProtoUtils.convertProtoToSchema(
                ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.newBuilder())
            .getStructureTypeDescriptor();
    final Message result =
        schema
            .getValueBuilder()
            .setField("repeated_string", Arrays.asList("value1", "value1"))
            .build();
    assertTrue(true);
  }

  /*
  @Test
  public void testReadFieldFromComplexValue() {
    final StructureTypeDescriptor<Message> schema = ProtoUtils
        .convertProtoToSchema(ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.newBuilder())
        .getStructureTypeDescriptor();

    final ValueSchemeMessage value = ValueSchemeMessage.newBuilder()
        .addRepeatedString("repeated_string_1")
        .addRepeatedString("repeated_string_2")
        .addRepeatedInnerMessage(InnerMessage.newBuilder()
            .addRepeatedInnerString("inner_repeated_string_1")
            .addRepeatedInnerString("inner_repeated_string_2")
            .setInnerEnum(Directions.LEFT)
            .setInnerDoubleType(100.0)
            .setInnerInnerMessage(SecondInnerMessage.newBuilder()
                .setInnerFloatType(333.33F)
                .buildPartial())
            .build())
        .setInnerMessage(InnerMessage.newBuilder()
            .addRepeatedInnerString("inner_repeated_string_A")
            .addRepeatedInnerString("inner_repeated_string_B")
            .setInnerEnum(Directions.RIGHT)
            .setInnerDoubleType(200.0)
            .setInnerInnerMessage(SecondInnerMessage.newBuilder()
                .setInnerFloatType(666.66F)
                .buildPartial())
            .build())
        .setStringType("string_value")
        .setBooleanType(true)
        .setLongType(55)
        .setIntType(69)
        .build();
    assertEquals(
        Arrays.asList("repeated_string_1", "repeated_string_2"),
        schema.readField("repeated_string", value));
    assertEquals("string_value", schema.readField("string_type", value));
    assertEquals(true, schema.readField("boolean_type", value));
    assertEquals(55L, Optional.ofNullable(schema.readField("long_type", value)).orElse(-1));
    assertEquals(69, Optional.ofNullable(schema.readField("int_type", value)).orElse(-1));

    InnerMessage inner_message = schema.readField("inner_message", value);
    assertNotNull(inner_message);
    @SuppressWarnings("unchecked")
    StructureTypeDescriptor<Object> inner_schema = (StructureTypeDescriptor<Object>) schema
        .getField("inner_message")
        .getStructureTypeDescriptor();

    assertEquals(200.0D, Optional
        .ofNullable(inner_schema.readField("inner_double_type", inner_message))
        .orElse(-1));
    assertEquals(Directions.RIGHT.name(), inner_schema.readField("inner_enum", inner_message));
  }

   */
}
