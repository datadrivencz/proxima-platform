/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.typed;

import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

public class TypedElementCoderTest {

  private static AttributeDescriptor<byte[]> createAttributeDescriptor(String name) {
    return AttributeDescriptor.newBuilder(Repository.ofTest(ConfigFactory.empty()))
        .setEntity("entity")
        .setName(name)
        .setSchemeUri(URI.create("bytes://whatever"))
        .build();
  }

  @Test
  public void testEncodeDecode() throws IOException {
    final AttributeDescriptor<byte[]> attributeDescriptor = createAttributeDescriptor("first");
    final TypedElementCoder<byte[]> coder =
        TypedElementCoder.of(Collections.singleton(attributeDescriptor));
    Assertions.assertEquals(
        attributeDescriptor, Iterables.getOnlyElement(coder.getAttributeDescriptor()));
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(
          TypedElement.upsert(attributeDescriptor, "first", new byte[] {0x00, 0x01, 0x02}), baos);
      try (final ByteArrayInputStream bios = new ByteArrayInputStream(baos.toByteArray())) {
        final TypedElement<byte[]> decoded = coder.decode(bios);
        Assertions.assertEquals(attributeDescriptor, decoded.getAttributeDescriptor());
        Assertions.assertEquals("first", decoded.getKey());
        Assertions.assertFalse(decoded.getAttributeSuffix().isPresent());
        Assertions.assertArrayEquals(new byte[] {0x00, 0x01, 0x02}, decoded.getValue());
      }
    }
  }

  @Test
  public void testEncodeDecodeWildcard() throws IOException {
    final AttributeDescriptor<byte[]> attributeDescriptor = createAttributeDescriptor("bar.*");
    final TypedElementCoder<byte[]> coder =
        TypedElementCoder.of(Collections.singleton(attributeDescriptor));
    Assertions.assertEquals(
        attributeDescriptor, Iterables.getOnlyElement(coder.getAttributeDescriptor()));
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(
          TypedElement.upsertWildcard(
              attributeDescriptor, "first", "suffix", new byte[] {0x00, 0x01, 0x02}),
          baos);
      try (final ByteArrayInputStream bios = new ByteArrayInputStream(baos.toByteArray())) {
        final TypedElement<byte[]> decoded = coder.decode(bios);
        Assertions.assertEquals(attributeDescriptor, decoded.getAttributeDescriptor());
        Assertions.assertEquals("first", decoded.getKey());
        Assertions.assertTrue(decoded.getAttributeSuffix().isPresent());
        Assertions.assertEquals("suffix", decoded.getAttributeSuffix().get());
        Assertions.assertEquals("bar.suffix", decoded.getAttribute());
        Assertions.assertArrayEquals(new byte[] {0x00, 0x01, 0x02}, decoded.getValue());
      }
    }
  }

  @Test
  public void testEncodeDecodeWithMultipleAttributes() throws IOException {
    final AttributeDescriptor<byte[]> fooDescriptor = createAttributeDescriptor("foo");
    final AttributeDescriptor<byte[]> barDescriptor = createAttributeDescriptor("bar");
    final Set<AttributeDescriptor<byte[]>> set = new HashSet<>();
    set.add(fooDescriptor);
    set.add(barDescriptor);
    final TypedElementCoder<byte[]> coder = TypedElementCoder.of(set);
    Assertions.assertEquals(2, coder.getAttributeDescriptor().size());
    // Encode / decode foo attribute.
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(
          TypedElement.upsert(fooDescriptor, "first", new byte[] {0x00, 0x01, 0x02}), baos);
      try (final ByteArrayInputStream bios = new ByteArrayInputStream(baos.toByteArray())) {
        final TypedElement<byte[]> decoded = coder.decode(bios);
        Assertions.assertEquals(fooDescriptor, decoded.getAttributeDescriptor());
        Assertions.assertEquals("first", decoded.getKey());
        Assertions.assertFalse(decoded.getAttributeSuffix().isPresent());
        Assertions.assertArrayEquals(new byte[] {0x00, 0x01, 0x02}, decoded.getValue());
      }
    }
    // Encode / decode bar attribute.
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(
          TypedElement.upsert(barDescriptor, "first", new byte[] {0x00, 0x01, 0x02}), baos);
      try (final ByteArrayInputStream bios = new ByteArrayInputStream(baos.toByteArray())) {
        final TypedElement<byte[]> decoded = coder.decode(bios);
        Assertions.assertEquals(barDescriptor, decoded.getAttributeDescriptor());
        Assertions.assertEquals("first", decoded.getKey());
        Assertions.assertFalse(decoded.getAttributeSuffix().isPresent());
        Assertions.assertArrayEquals(new byte[] {0x00, 0x01, 0x02}, decoded.getValue());
      }
    }
  }

  @Test
  public void testConflictingAttributes() {
    @SuppressWarnings("unchecked")
    final AttributeDescriptor<String> mock1 =
        (AttributeDescriptor<String>) Mockito.mock(AttributeDescriptor.class);
    Mockito.when(mock1.getEntity()).thenReturn("entity");
    Mockito.when(mock1.getName()).thenReturn("bar");
    @SuppressWarnings("unchecked")
    final AttributeDescriptor<String> mock2 =
        (AttributeDescriptor<String>) Mockito.mock(AttributeDescriptor.class);
    Mockito.when(mock2.getEntity()).thenReturn("entity");
    Mockito.when(mock2.getName()).thenReturn("bar");
    final Set<AttributeDescriptor<String>> set = new HashSet<>();
    set.add(mock1);
    set.add(mock2);
    Assertions.assertEquals(2, set.size());
    Assertions.assertThrows(IllegalStateException.class, () -> TypedElementCoder.of(set));
  }

  @Test
  public void testDeterministic() {
    final AttributeDescriptor<byte[]> attributeDescriptor = createAttributeDescriptor("foo");
    final TypedElementCoder<byte[]> coder =
        TypedElementCoder.of(Collections.singleton(attributeDescriptor));
    Assertions.assertDoesNotThrow((Executable) coder::verifyDeterministic);
  }
}
