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
package cz.o2.proxima.beam.direct.io;

import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.direct.io.DirectUnboundedSource.Checkpoint;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.util.Optional;
import org.junit.Test;

/** Test for {@link DirectUnboundedSource}. */
public class DirectUnboundedSourceTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor event = repo.getEntity("event");
  private final AttributeDescriptor<byte[]> data = event.getAttribute("data");
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);

  @Test
  public void testCheckpointSerializableAndEquals() throws IOException, ClassNotFoundException {
    Optional<CommitLogReader> reader = direct.getCommitLogReader(data);
    assertTrue(reader.isPresent());
    BeamCommitLogReader beamReader =
        new BeamCommitLogReader(
            null,
            reader.get(),
            Position.CURRENT,
            true,
            Partition.of(0),
            null,
            Long.MAX_VALUE,
            false);
    Checkpoint checkpoint = new Checkpoint(beamReader);
    TestUtils.assertSerializable(checkpoint);
  }
}
