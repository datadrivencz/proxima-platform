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
package cz.o2.proxima.scheme.proto;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.SecondInnerMessage;
import cz.o2.proxima.scheme.proto.utils.ProtoUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ProtoMessageValueAccessorTest {

  private final StructureTypeDescriptor<ValueSchemeMessage> schema =
      ProtoUtils.convertProtoToSchema(
          ValueSchemeMessage.getDescriptor(), ValueSchemeMessage.getDefaultInstance());
  private final StructureValueAccessor<ValueSchemeMessage> valueAccessor =
      schema.getValueAccessor();

  private final ValueSchemeMessage referenceValue =
      ValueSchemeMessage.newBuilder()
          .addAllRepeatedString(
              Arrays.asList(
                  "top_level_repeated_string_value_1", "top_level_repeated_string_value_2"))
          .setStringType("top_level_string_type_value")
          .addAllRepeatedInnerMessage(
              Arrays.asList(
                  InnerMessage.newBuilder()
                      .setInnerEnum(Directions.LEFT)
                      .setInnerDoubleType(40)
                      .setInnerInnerMessage(
                          SecondInnerMessage.newBuilder().setInnerFloatType(20).buildPartial())
                      .build(),
                  InnerMessage.newBuilder()
                      .setInnerEnum(Directions.RIGHT)
                      .setInnerDoubleType(20)
                      .setInnerInnerMessage(
                          SecondInnerMessage.newBuilder().setInnerFloatType(40).buildPartial())
                      .build()))
          .setInnerMessage(
              InnerMessage.newBuilder()
                  .addAllRepeatedInnerString(
                      Arrays.asList("inner_repeated_string_value1", "inner_repeated_string_value2"))
                  .setInnerEnum(Directions.LEFT)
                  .setInnerDoubleType(100)
                  .setInnerInnerMessage(
                      SecondInnerMessage.newBuilder().setInnerFloatType(33).buildPartial())
                  .build())
          .setBooleanType(true)
          .setLongType(20L)
          .setIntType(8)
          .addRepeatedBytes(ByteString.copyFromUtf8("repeated_bytes_value_1"))
          .addRepeatedBytes(ByteString.copyFromUtf8("repeated_bytes_value_2"))
          .build();

  @Test
  public void testReadIllegalFieldShouldThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> valueAccessor.valueOf("not-existed-field", referenceValue));
  }

  @Test
  public void testReadTopLevelPrimitiveTypes() {
    assertEquals(
        "top_level_string_type_value", valueAccessor.valueOf("string_type", referenceValue));
    assertEquals(true, valueAccessor.valueOf("boolean_type", referenceValue));
    assertEquals(
        20L, Optional.ofNullable(valueAccessor.valueOf("long_type", referenceValue)).orElse(-1));
    assertEquals(
        8, Optional.ofNullable(valueAccessor.valueOf("int_type", referenceValue)).orElse(-1));
  }

  @Test
  public void testReadTopLevelArrayOfString() {
    assertArrayEquals(
        Arrays.asList("top_level_repeated_string_value_1", "top_level_repeated_string_value_2")
            .toArray(new Object[0]),
        valueAccessor.valueOf("repeated_string", referenceValue));
  }

  @Test
  public void testReadInnerMessage() {
    final Map<String, Object> innerValue = valueAccessor.valueOf("inner_message", referenceValue);
    assertArrayEquals(
        Arrays.asList("inner_repeated_string_value1", "inner_repeated_string_value2")
            .toArray(new Object[0]),
        (Object[]) innerValue.get("repeated_inner_string"));
    assertEquals(Directions.LEFT.name(), innerValue.get("inner_enum"));
    assertEquals(100D, innerValue.get("inner_double_type"));
    @SuppressWarnings("unchecked")
    Map<String, Object> innerInnerMessage =
        (Map<String, Object>) innerValue.get("inner_inner_message");
    assertEquals(33F, innerInnerMessage.get("inner_float_type"));
  }

  @Test
  public void testReadRepeatedMessage() {
    final Map<String, Object> value = valueAccessor.valuesOf(referenceValue);

    @SuppressWarnings("unchecked")
    final ArrayValueAccessor<Object> repeatedMessageAccessor =
        (ArrayValueAccessor<Object>)
            schema.getField("repeated_inner_message").asArrayTypeDescriptor().getValueAccessor();

    @SuppressWarnings("unchecked")
    StructureValueAccessor<Object> arrayValueAccessor =
        (StructureValueAccessor<Object>)
            schema
                .getField("repeated_inner_message")
                .asArrayTypeDescriptor()
                .getValueDescriptor()
                .asStructureTypeDescriptor()
                .getValueAccessor();

    Object[] values = (Object[]) value.get("repeated_inner_message");
    assertEquals(2, values.length);

    @SuppressWarnings("unchecked")
    final Map<String, Object> first = (Map<String, Object>) values[0];
    @SuppressWarnings("unchecked")
    final Map<String, Object> second = (Map<String, Object>) values[1];

    assertEquals(40.0, first.get("inner_double_type"));
    assertEquals(Directions.LEFT.name(), first.get("inner_enum"));
    assertEquals(20.0, second.get("inner_double_type"));
    assertEquals(Directions.RIGHT.name(), second.get("inner_enum"));
  }

  @Test
  public void testCreateObjectFromMap() {
    final Map<String, Object> createFrom =
        new HashMap<String, Object>() {
          {
            put(
                "repeated_string",
                Arrays.asList(
                        "top_level_repeated_string_value_1", "top_level_repeated_string_value_2")
                    .toArray());

            put(
                "repeated_inner_message",
                Arrays.asList(
                        new HashMap<String, Object>() {
                          {
                            put("inner_double_type", 40);
                            put("inner_enum", Directions.LEFT.name());
                            put(
                                "inner_inner_message",
                                new HashMap<String, Object>() {
                                  {
                                    put("inner_float_type", 20);
                                  }
                                });
                          }
                        },
                        new HashMap<String, Object>() {
                          {
                            put("inner_double_type", 20);
                            put("inner_enum", Directions.RIGHT.name());
                            put(
                                "inner_inner_message",
                                new HashMap<String, Object>() {
                                  {
                                    put("inner_float_type", 40);
                                  }
                                });
                          }
                        })
                    .toArray());
            put(
                "inner_message",
                new HashMap<String, Object>() {
                  {
                    put(
                        "repeated_inner_string",
                        Arrays.asList(
                                "inner_repeated_string_value1", "inner_repeated_string_value2")
                            .toArray());
                    put("inner_enum", Directions.LEFT.name());
                    put("inner_double_type", 100);
                    put(
                        "inner_inner_message",
                        new HashMap<String, Object>() {
                          {
                            put("inner_float_type", 33);
                          }
                        });
                  }
                });
            put("string_type", "top_level_string_type_value");
            put("boolean_type", true);
            put("long_type", 20);
            put("int_type", 8);
            put(
                "repeated_bytes",
                Arrays.asList(
                        "repeated_bytes_value_1".getBytes(StandardCharsets.UTF_8),
                        "repeated_bytes_value_2".getBytes(StandardCharsets.UTF_8))
                    .toArray());
          }
        };
    final Object created = valueAccessor.createFrom(createFrom);
    assertNotNull(created);
    log.info("New created object from map {}", created);
    assertEquals(referenceValue, created);
  }
}
