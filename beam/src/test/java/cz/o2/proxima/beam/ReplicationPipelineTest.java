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
import static cz.o2.proxima.beam.Utils.startPipeline;
import static cz.o2.proxima.beam.Utils.update;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test suite for {@link ReplicationPipeline}.
 */
public class ReplicationPipelineTest implements Serializable {

  private final ConfigRepository repo;
  private final EntityDescriptor gateway;
  private final AttributeDescriptor<byte[]> status;

  @SuppressWarnings("unchecked")
  public ReplicationPipelineTest() {
    this.repo = ConfigRepository.of(
        ConfigFactory.load()
            .withFallback(ConfigFactory.load("test-stateful-persist.conf"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    this.gateway = repo.findEntity("gateway")
        .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    this.status = (AttributeDescriptor) gateway
        .findAttribute("status")
        .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
  }


  @Test
  public void testReplication() throws InterruptedException, ExecutionException {
    // write some update
    OnlineAttributeWriter writer = repo.getWriter(status)
        .orElseThrow(() -> new IllegalArgumentException("Missing writer for status"));

    CompletableFuture<Throwable> result = startPipeline(ReplicationPipeline.from(repo, 1));

    // wait till pipeline starts
    TimeUnit.SECONDS.sleep(1);

    writer.write(update(gateway, status), (succ, exc) -> { });

    // wait till change propagates
    TimeUnit.SECONDS.sleep(1);

    RandomAccessReader reader = repo.getAllFamilies()
        .filter(af -> af.getName().equals("gateway-bulk-writer-stateful"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "Missing family gateway-bulk-writer-stateful"))
        .getRandomAccessReader()
        .get();

    Optional<KeyValue<byte[]>> get = reader.get("key", status);
    assertTrue(get.isPresent());
    assertEquals("key", get.get().getKey());
    assertNull(result.get());
  }

  @Test
  public void testReadWrite() throws InterruptedException, ExecutionException {
    // write some update
    OnlineAttributeWriter writer = repo.getWriter(status)
        .orElseThrow(() -> new IllegalArgumentException("Missing writer for status"));

    StreamElement update = update(gateway, status);
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input = pipeline
        .apply(ProximaIO.from(repo).read(Position.CURRENT, status))
        .apply(MapElements.via(
            new SimpleFunction<StreamElement, StreamElement>(e -> {
              System.err.println(" *** " + e);
              return e;
            }) { }));
    input.apply(ProximaIO.from(repo).writeBulk(100, TimeUnit.MILLISECONDS, 1));
    PAssert.that(input).containsInAnyOrder(update);
    CompletableFuture<Throwable> result = startPipeline(pipeline);

    // wait till pipeline starts
    TimeUnit.SECONDS.sleep(1);

    writer.write(update, (succ, exc) -> { });

    // wait till pipeline starts
    TimeUnit.SECONDS.sleep(1);

    RandomAccessReader reader = repo.getFamiliesForAttribute(status)
        .stream()
        .filter(af -> af.getType() == StorageType.PRIMARY)
        .filter(af -> af.getAccess().canRandomRead())
        .map(af -> af.getRandomAccessReader().get())
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            "Cannot get random access reader for status"));
    Optional<KeyValue<byte[]>> get = reader.get("key", status);
    assertTrue(get.isPresent());
    assertEquals("key", get.get().getKey());
    assertNull(result.get());
  }

}
