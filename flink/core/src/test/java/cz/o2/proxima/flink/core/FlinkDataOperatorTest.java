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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Collections;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FlinkDataOperatorTest {
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
          + "      storage: \"inmem:///test_inmem_stream\"\n"
          + "      type: primary\n"
          + "      access: commit-log\n"
          + "      num-partitions: 3\n"
          + "    }\n"
          + "    test_storage_batch {\n"
          + "      entity: test\n"
          + "      attributes: [ data ]\n"
          + "      storage: \"inmem:///test_inmem_batch\"\n"
          + "      type: replica\n"
          + "      access: batch-updates\n"
          + "      num-partitions: 3\n"
          + "    }\n"
          + "  }\n"
          + "}\n";
  private final Repository repository =
      ConfigRepository.ofTest(ConfigFactory.parseString(MODEL).resolve());
  private final EntityDescriptor entity = repository.getEntity("test");
  private final AttributeDescriptor<String> attribute = entity.getAttribute("data");
  private final FlinkDataOperator operator =
      repository.getOrCreateOperator(FlinkDataOperator.class);

  @Test
  void testReadStreamFromCommitLog() {
    DataStream<StreamElement> stream =
        operator.createCommitLogStream(Collections.singletonList(attribute));
    Assertions.assertTrue(true);
  }
}
