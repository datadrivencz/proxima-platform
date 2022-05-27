/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.flink.core;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.jupiter.api.Test;

class OnlineAttributeWriterSinkTest {

  private static final String MODEL =
      "{\n"
          + "  entities: {\n"
          + "    test {\n"
          + "      attributes {\n"
          + "        data: { scheme: \"string\" }\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "  attributeFamilies: {\n"
          + "    test_storage_stream {\n"
          + "      entity: test\n"
          + "      attributes: [ data ]\n"
          + "      storage: \"inmem:///test_inmem\"\n"
          + "      type: primary\n"
          + "      access: commit-log\n"
          + "      num-partitions: 3\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  @Test
  void testWrite() throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final EntityDescriptor entity = repository.getEntity("test");
    final AttributeDescriptor<String> attribute = entity.getAttribute("data");
    final List<StreamElement> elements =
        Arrays.asList(
            createElement(entity, attribute, "key-1", "first"),
            createElement(entity, attribute, "key-2", "second"));
    ListTestSource<StreamElement> source = new ListTestSource<>(elements);

    final Configuration configuration = new Configuration();
    final StreamExecutionEnvironment env =
        TestStreamEnvironment.createLocalEnvironment(1, configuration).setBufferTimeout(0);

    final OnlineAttributeWriterSink sink = new OnlineAttributeWriterSink(repository.asFactory());

    env.addSource(source).returns(new TypeHint<StreamElement>() {}).addSink(sink);
    env.execute();
    CommitLogReader reader =
        sink.getOrCreateDirect()
            .getCommitLogReader(attribute)
            .orElseThrow(() -> new IllegalStateException("Unable to find reader."));

    final List<StreamElement> read = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(elements.size());
    reader.observe(
        "test",
        Position.OLDEST,
        (element, context) -> {
          read.add(element);
          context.confirm();
          latch.countDown();
          return true;
        });
    latch.await();
    assertEquals(elements.size(), read.size());
    assertEquals(elements, read);
  }

  <T> StreamElement createElement(
      EntityDescriptor entity, AttributeDescriptor<T> attribute, String key, T data) {
    return StreamElement.upsert(
        entity,
        attribute,
        UUID.randomUUID().toString(),
        key,
        attribute.getName(),
        System.currentTimeMillis(),
        attribute.getValueSerializer().serialize(data));
  }
}
