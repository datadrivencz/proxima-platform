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
package cz.o2.proxima.beam.io;

import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PDone;
import org.junit.Rule;
import org.junit.Test;

public class ProximaIOTest {

  private final RepositoryFactory repositoryFactory =
      RepositoryFactory.compressed(ConfigFactory.load("test-reference.conf").resolve());
  private final Repository repository = repositoryFactory.apply();

  private final EntityDescriptor gateway = repository.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void writeTest() {
    PDone done =
        pipeline
            .apply(Create.of(Arrays.asList(write("key1"), write("key2"), write("key3"))))
            .apply(ProximaIO.write(repositoryFactory));
    assertNotNull(pipeline.run());
  }

  private StreamElement write(String key) {
    return StreamElement.upsert(
        gateway,
        status,
        UUID.randomUUID().toString(),
        key,
        status.getName(),
        System.currentTimeMillis(),
        new byte[] {1});
  }
}
