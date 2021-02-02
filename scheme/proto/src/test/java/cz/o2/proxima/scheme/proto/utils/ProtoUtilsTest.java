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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme.Device;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ProtoUtilsTest {

  @Test
  public void testConvertSimpleProtoToSchema() {
    SchemaTypeDescriptor<Device> schema = ProtoUtils.convertProtoToSchema(Device.getDescriptor());
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
  @SuppressWarnings("unchecked")
  public void testConvertComplexProtoToSchema() {
    SchemaTypeDescriptor<ValueSchemeMessage> schema =
        ProtoUtils.convertProtoToSchema(ValueSchemeMessage.getDescriptor());
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

  @SuppressWarnings("unchecked")
  @Test
  public void simpleProtoToObject() {
    Event event =
        Event.newBuilder()
            .setGatewayId("gateway value")
            .setPayload(ByteString.copyFromUtf8("payload value"))
            .build();
    SchemaTypeDescriptor<Event> schema =
        ProtoUtils.convertProtoToSchema(event.getDescriptorForType());
    Map<String, Object> result = ProtoUtils.convertProtoObjectToMap(event);
    assertEquals(schema.getStructureTypeDescriptor().getFields().size(), result.size());
    assertTrue(result.containsKey("gatewayId"));
    assertEquals("gateway value", result.get("gatewayId"));
    assertArrayEquals("payload value".getBytes(), (byte[]) result.get("payload"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void complexProtoToObject() {
    ValueSchemeMessage message =
        ValueSchemeMessage.newBuilder()
            .addAllRepeatedString(Arrays.asList("repeated value 1", "repeated value 2"))
            .addRepeatedInnerMessage(
                InnerMessage.newBuilder()
                    .addAllRepeatedInnerString(
                        Arrays.asList("inner string value 1", "inner string value 2"))
                    .setInnerEnum(Directions.LEFT)
                    .setInnerDoubleType(100.0)
                    .build())
            .setInnerMessage(
                InnerMessage.newBuilder()
                    .addAllRepeatedInnerString(Arrays.asList("inner1", "inner2"))
                    .build())
            .setStringType("string value")
            .setBooleanType(true)
            .setLongType(100L)
            .setIntType(10)
            .build();

    Map<String, Object> result = ProtoUtils.convertProtoObjectToMap(message);
    log.info("result: {}", result);
    assertEquals(
        Arrays.asList("repeated value 1", "repeated value 2"), result.get("repeated_string"));
    List<Map<String, Object>> repeated_inner_message_value =
        (List<Map<String, Object>>) result.get("repeated_inner_message");
    assertEquals(1, repeated_inner_message_value.size());
    assertEquals(
        Arrays.asList("inner string value 1", "inner string value 2"),
        repeated_inner_message_value.get(0).get("repeated_inner_string"));
    assertEquals(Directions.LEFT.name(), repeated_inner_message_value.get(0).get("inner_enum"));
    assertEquals(100.0, repeated_inner_message_value.get(0).get("inner_double_type"));
    assertEquals(
        Arrays.asList("inner1", "inner2"),
        ((Map<String, Object>) result.get("inner_message")).get("repeated_inner_string"));
    assertTrue((Boolean) result.get("boolean_type"));
    assertEquals(100L, result.get("long_type"));
    assertEquals(10, result.get("int_type"));
  }

  @Test
  public void createSimpleProtoTest() {
    Map<String, Object> value =
        new HashMap<String, Object>() {
          {
            put("gatewayId", "my-gatewayId");
            put("payload", "my-payload".getBytes());
          }
        };
    Event event = (Event) ProtoUtils.createProtoObject(value, Event.newBuilder());
    log.debug("result: {}", event);
    assertEquals("my-gatewayId", event.getGatewayId());
    assertArrayEquals("my-payload".getBytes(), event.getPayload().toByteArray());
  }

  @Test
  public void createComplexProtoTest() {

    Map<String, Object> value =
        new HashMap<String, Object>() {
          {
            put(
                "repeated_string",
                Arrays.asList("repeated_string value1", "repeated_string value2"));
            put(
                "repeated_inner_message",
                Collections.singletonList(
                    new HashMap<String, Object>() {
                      {
                        put("repeated_inner_string", Arrays.asList("inner value1", "inner value2"));
                        put("inner_enum", "LEFT");
                        put("inner_double_type", 100.0);
                        put(
                            "inner_inner_message",
                            new HashMap<String, Object>() {
                              {
                                put("inner_float_type", 666.666F);
                              }
                            });
                      }
                    }));
            put(
                "inner_message",
                new HashMap<String, Object>() {
                  {
                    put("inner_double_type", 3333);
                  }
                });

            put("string_type", "test string");

            put("boolean_type", false);
            put("long_type", 20L);
            put("int_type", 8);
          }
        };

    log.debug("Data {}", value);
    ValueSchemeMessage proto =
        (ValueSchemeMessage) ProtoUtils.createProtoObject(value, ValueSchemeMessage.newBuilder());
    log.debug("Created proto: {}", proto);
    assertEquals(
        Arrays.asList("repeated_string value1", "repeated_string value2"),
        proto.getRepeatedStringList());
    assertEquals(1, proto.getRepeatedInnerMessageCount());
    assertEquals(
        Arrays.asList("inner value1", "inner value2"),
        proto.getRepeatedInnerMessageList().get(0).getRepeatedInnerStringList());
    assertEquals(Directions.LEFT, proto.getRepeatedInnerMessageList().get(0).getInnerEnum());
    assertEquals(100.0, proto.getRepeatedInnerMessageList().get(0).getInnerDoubleType(), 0.0001);
    assertEquals(
        666.666F,
        proto.getRepeatedInnerMessageList().get(0).getInnerInnerMessage().getInnerFloatType(),
        0.0001);
    assertEquals(3333, proto.getInnerMessage().getInnerDoubleType(), 0.0001);
    assertEquals("test string", proto.getStringType());
    assertFalse(proto.getBooleanType());
    assertEquals(20L, proto.getLongType());
    assertEquals(8, proto.getIntType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryToSetNotExistedField() {
    ProtoUtils.createProtoObject(
        new HashMap<String, Object>() {
          {
            put("not-existed-field", "value");
          }
        },
        Event.newBuilder());
  }
}
