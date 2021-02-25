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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public abstract class AbstractFileFormatTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  protected final Repository repo = Repository.of(ConfigFactory.load().resolve());
  protected final AttributeDescriptor<?> attribute;
  protected final AttributeDescriptor<?> wildcard;
  protected final EntityDescriptor entity;
  protected final long now = System.currentTimeMillis();
  protected Path file;

  protected AbstractFileFormatTest() throws URISyntaxException {
    this.wildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .setName("wildcard.*")
            .build();
    this.attribute =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .setName("attr")
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("dummy")
            .addAttribute(attribute)
            .addAttribute(wildcard)
            .build();
  }

  protected abstract FileFormat getFileFormat();

  @Before
  public void setUp() throws IOException {
    folder.create();
    File file = folder.newFile();
    this.file =
        Path.local(
            FileSystem.local(
                file.getParentFile(),
                NamingConvention.defaultConvention(
                    Duration.ofHours(1), "prefix", getFileFormat().fileSuffix())),
            file);
  }

  @After
  public void tearDown() {
    ExceptionUtils.unchecked(folder::delete);
  }

  @Test
  public void testWriteAndReadAllTypes() throws IOException {
    assertWriteAndReadElements(
        getFileFormat(), entity, Arrays.asList(deleteWildcard(), delete(), upsert()));
  }

  @Test
  public void testWriteAndReadUpsert() throws IOException {
    assertWriteAndReadElements(getFileFormat(), entity, Collections.singletonList(upsert()));
  }

  @Test
  public void testWriteAndReadDelete() throws IOException {
    assertWriteAndReadElements(getFileFormat(), entity, Collections.singletonList(delete()));
  }

  @Test
  public void testWriteAndReadDeleteWildcard() throws IOException {
    assertWriteAndReadElements(
        getFileFormat(), entity, Collections.singletonList(deleteWildcard()));
  }

  protected void writeElements(
      Path path, FileFormat format, EntityDescriptor entityDescriptor, List<StreamElement> elements)
      throws IOException {
    try (Writer writer = format.openWriter(path, entityDescriptor)) {
      elements.forEach(
          e -> {
            try {
              writer.write(e);
            } catch (IOException ex) {
              log.error(ex.getMessage(), ex);
              fail("Error during write.");
            }
          });
    }
  }

  protected List<StreamElement> readElements(
      Path path, FileFormat format, EntityDescriptor entityDescriptor) throws IOException {
    List<StreamElement> data = new ArrayList<>();
    try (Reader reader = format.openReader(path, entityDescriptor)) {
      for (StreamElement e : reader) {
        data.add(e);
      }
    }
    return data;
  }

  protected void assertWriteAndReadElements(
      FileFormat format, EntityDescriptor entityDescriptor, List<StreamElement> elements)
      throws IOException {
    writeElements(file, format, entityDescriptor, elements);
    List<StreamElement> read = readElements(file, format, entityDescriptor);
    assertTrue(elements.containsAll(read));
    assertEquals(elements.size(), read.size());
    Map<String, StreamElement> elementsByUuid =
        elements
            .stream()
            .map(e -> new SimpleEntry<>(e.getUuid(), e))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    read.forEach(
        e -> {
          assertTrue(elementsByUuid.containsKey(e.getUuid()));
          StreamElement gets = elementsByUuid.get(e.getUuid());
          assertEquals(e, gets);
          assertEquals(e.getStamp(), gets.getStamp());
          assertArrayEquals(e.getValue(), gets.getValue());
        });
  }

  protected StreamElement upsert() {
    return StreamElement.upsert(
        entity,
        wildcard,
        UUID.randomUUID().toString(),
        "key",
        wildcard.toAttributePrefix() + "1",
        now,
        new byte[] {1});
  }

  protected StreamElement delete() {
    return StreamElement.delete(
        entity,
        wildcard,
        UUID.randomUUID().toString(),
        "key",
        wildcard.toAttributePrefix() + "1",
        now);
  }

  protected StreamElement deleteWildcard() {
    return StreamElement.deleteWildcard(entity, wildcard, UUID.randomUUID().toString(), "key", now);
  }
}
