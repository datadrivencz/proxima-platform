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
package cz.o2.proxima.direct.io.hbase;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

public class HBaseDataAccessorTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testAccessorSerializable() throws IOException, ClassNotFoundException {
    HBaseDataAccessor accessor =
        new HBaseDataAccessor(entity, URI.create("hbase://test"), Collections.emptyMap());
    HBaseDataAccessor cloned = TestUtils.assertSerializable(accessor);
    assertEquals(accessor.getUri(), cloned.getUri());
  }
}
