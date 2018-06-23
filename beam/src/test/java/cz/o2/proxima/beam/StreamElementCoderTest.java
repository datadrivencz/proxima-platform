/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.*;

public class StreamElementCoderTest {

  private static final String ENTITY = "event";
  private static final String ATTRIBUTE = "data";
  private static final String KEY = "key";
  private static final long STAMP = 123L;
  private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);

  private Repository repository;
  private EntityDescriptor entityDescriptor;
  private AttributeDescriptor attributeDescriptor;
  private StreamElementCoder coder;

  public StreamElementCoderTest() {
    repository = Repository.of(ConfigFactory.load("test-reference.conf"));
    entityDescriptor = Optionals.get(repository.findEntity(ENTITY));
    attributeDescriptor = Optionals.get(entityDescriptor.findAttribute(ATTRIBUTE));
    coder = StreamElementCoder.of(repository);
  }

  @Test
  public void testDelete() throws IOException {
    final String uuid = UUID.randomUUID().toString();
    final StreamElement delete = StreamElement.delete(
            entityDescriptor, attributeDescriptor, uuid, KEY, ATTRIBUTE, STAMP);
    // encode
    final byte[] buf = encode(delete);

    // decode
    final StreamElement decoded = decode(buf);


    assertNotNull(decoded);
    assertTrue(decoded.isDelete());
    assertNull(decoded.getValue());
    assertEquals(entityDescriptor, decoded.getEntityDescriptor());
    assertEquals(attributeDescriptor, decoded.getAttributeDescriptor());
    assertEquals(uuid, decoded.getUuid());
    assertEquals(KEY, decoded.getKey());
    assertEquals(ATTRIBUTE, decoded.getAttribute());
    assertEquals(STAMP, decoded.getStamp());

  }

  @Test
  public void testDeleteWildcard() throws IOException {
    final String uuid = UUID.randomUUID().toString();
    final StreamElement delete = StreamElement.deleteWildcard(entityDescriptor, attributeDescriptor, uuid, KEY, STAMP);

    final byte[] buf = encode(delete);
    final StreamElement decoded = decode(buf);

    assertNotNull(decoded);
    assertTrue(decoded.isDeleteWildcard());
    assertTrue(decoded.isDelete());
    assertEquals(entityDescriptor, decoded.getEntityDescriptor());
    assertEquals(attributeDescriptor, decoded.getAttributeDescriptor());
    assertEquals(uuid, decoded.getUuid());
    assertEquals(KEY, decoded.getKey());
    assertEquals(ATTRIBUTE + "*", decoded.getAttribute());
    assertEquals(STAMP, decoded.getStamp());
    assertNull(decoded.getValue());
  }

  @Test
  public void testUpdate() throws IOException {

    final String uuid = UUID.randomUUID().toString();

    final StreamElement update = StreamElement.update(
            entityDescriptor, attributeDescriptor, uuid, KEY, ATTRIBUTE, STAMP, VALUE);

    // encode
    final byte[] buf = encode(update);

    // decode
    final StreamElement decoded = decode(buf);

    assertNotNull(decoded);
    assertEquals(entityDescriptor, decoded.getEntityDescriptor());
    assertEquals(attributeDescriptor, decoded.getAttributeDescriptor());
    assertEquals(uuid, decoded.getUuid());
    assertEquals(KEY, decoded.getKey());
    assertEquals(ATTRIBUTE, decoded.getAttribute());
    assertEquals(STAMP, decoded.getStamp());
    assertArrayEquals(VALUE, decoded.getValue());
  }

  private StreamElement decode(byte[] buf) throws IOException {
    // decode
    final StreamElement decoded;
    try (final InputStream inputStream = new ByteArrayInputStream(buf)) {
      decoded = coder.decode(inputStream);
    } catch (Error ex) {
      ex.printStackTrace(System.err);
      throw new RuntimeException(ex);
    }
    return decoded;
  }

  private byte[] encode(StreamElement element) throws IOException {
    // encode
    final byte[] buf;
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      coder.encode(element, outputStream);
      buf = outputStream.toByteArray();
    }
    return buf;
  }
}
