/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.http;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

/** Verify that all accessors are serializable. */
public class SerializableTest implements Serializable {

  Repository repo = Repository.ofTest(ConfigFactory.load());
  AttributeDescriptor<byte[]> attr =
      AttributeDescriptor.newBuilder(repo)
          .setName("attr")
          .setEntity("entity")
          .setSchemeUri(new URI("bytes:///"))
          .build();
  EntityDescriptor entity =
      EntityDescriptor.newBuilder().setName("entity").addAttribute(attr).build();

  public SerializableTest() throws Exception {}

  @Test
  public void testHttpWriter() throws Exception {
    HttpWriter writer = new HttpWriter(entity, new URI("http://test/"), Collections.emptyMap());
    HttpWriter.Factory<?> deserialized =
        TestUtils.deserializeObject(TestUtils.serializeObject(writer.asFactory()));
    assertEquals(writer, deserialized.apply(repo));
  }

  @Test
  public void testWebsocketReader() throws Exception {
    WebsocketReader reader =
        new WebsocketReader(
            entity,
            new URI("ws://test"),
            ImmutableMap.of("hello", "hi", "attributes", Lists.newArrayList("*")));
    WebsocketReader.Factory<?> deserialized =
        TestUtils.deserializeObject(TestUtils.serializeObject(reader.asFactory()));
    assertEquals(reader, deserialized.apply(repo));
  }
}
