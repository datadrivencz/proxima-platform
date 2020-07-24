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
package cz.o2.proxima.direct.s3;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.BulkAttributeWriter.Factory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import org.junit.Test;

public class BulkS3WriterTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    S3Accessor accessor =
        new S3Accessor(
            entity,
            URI.create("s3://bucket/path"),
            ImmutableMap.<String, Object>builder()
                .put("access-key", "key")
                .put("secret-key", "secret")
                .build());
    BulkS3Writer writer = new BulkS3Writer(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(writer.asFactory(repo.asFactory()));
    Factory factory = TestUtils.deserializeObject(bytes);
    assertEquals(
        writer.getAccessor().getUri(), ((BulkS3Writer) factory.create()).getAccessor().getUri());
  }
}
