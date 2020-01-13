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
package cz.o2.proxima.direct.jdbc;

import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

@Slf4j
public class JdbcOnlineAttributeReaderTest extends JdbcBaseTest {

  public JdbcOnlineAttributeReaderTest() throws URISyntaxException {
    super();
  }

  @Test
  public void listEntitiesTest() throws IOException {
    assertTrue(
        writeElement(
                accessor,
                StreamElement.update(
                    entity,
                    attr,
                    UUID.randomUUID().toString(),
                    "1",
                    attr.getName(),
                    System.currentTimeMillis(),
                    "value".getBytes()))
            .get());
    assertTrue(
        writeElement(
                accessor,
                StreamElement.update(
                    entity,
                    attr,
                    UUID.randomUUID().toString(),
                    "2",
                    attr.getName(),
                    System.currentTimeMillis(),
                    "value".getBytes()))
            .get());
    try (RandomAccessReader reader = accessor.newRandomAccessReader()) {
      List<String> keys = new ArrayList<>();
      reader.listEntities(x -> keys.add(x.getSecond()));
      assertEquals(Arrays.asList("1", "2"), keys);
    }
  }

  @Test
  public void writeAndReadSuccessfullyTest() {
    StreamElement element =
        StreamElement.update(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "12345",
            attr.getName(),
            System.currentTimeMillis(),
            "value".getBytes());
    assertTrue(writeElement(accessor, element).get());

    Optional<KeyValue<Byte[]>> keyValue = accessor.newRandomAccessReader().get("12345", attr);
    assertTrue(keyValue.isPresent());
    log.debug("KV: {}", keyValue.get());
    assertEquals(attr, keyValue.get().getAttrDescriptor());
    assertEquals("value", new String(Objects.requireNonNull(keyValue.get().getValueBytes())));
  }

  @Test
  public void getNotExistsTest() {
    Optional<KeyValue<Byte[]>> keyValue = accessor.newRandomAccessReader().get("12345", attr);
    assertFalse(keyValue.isPresent());
  }

  @Test
  public void getInvalidAttributeTest() throws URISyntaxException {
    AttributeDescriptor<byte[]> missing = AttributeDescriptor
        .newBuilder(repository)
        .setEntity(entity.getName())
        .setName("missing")
        .setSchemeUri(new URI("bytes:///"))
        .build();
    assertFalse(accessor.newRandomAccessReader().get("key", missing).isPresent());
  }
}
