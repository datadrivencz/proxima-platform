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

import com.typesafe.config.Config;
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
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.CountByKey;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.util.Pair;
import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test suite for {@link ProximaIO}.
 */
public class ProximaIOTest implements Serializable {

  private final ConfigRepository repo;
  private final EntityDescriptor gateway;
  private final AttributeDescriptor<byte[]> status;

  @SuppressWarnings("unchecked")
  public ProximaIOTest() {
    this.repo = ConfigRepository.of(
        ConfigFactory.load()
            .withFallback(ConfigFactory.parseString(
                "attributeFamilies.gateway-storage-stream.storage = \"kafka-test://dummy/proxima_gateway\""))
            .withFallback(ConfigFactory.parseString(
                "attributeFamilies.gateway-storage-stream.access = commit-log"))
            .withFallback(ConfigFactory.load("test-reference.conf"))
            .resolve());
    this.gateway = repo.findEntity("gateway")
        .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
    this.status = (AttributeDescriptor) gateway
        .findAttribute("status")
        .orElseThrow(() -> new IllegalStateException("Missing attribute status"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUnboundedRead() throws InterruptedException, ExecutionException {
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input = pipeline.apply(
        ProximaIO.from(repo).withMaxReadTime(Duration.ofSeconds(2))
            .read(Position.OLDEST, status));
    BeamFlow bf = BeamFlow.create(pipeline);
    Dataset<StreamElement> ds = bf.wrapped(input);
    ds = FlatMap.of(ds)
        .using((StreamElement in, Collector<StreamElement> out) -> {
          System.err.println(" *** in: " + in);
          out.collect(in);
        })
        .output();
    Dataset<Pair<Integer, Long>> output = CountByKey.of(ds)
        .keyBy(e -> { System.err.println(" **** " + e); return 0; })
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();
    PAssert.that(bf.unwrapped(output))
        .containsInAnyOrder(Pair.of(0, 1L));
    repo.getWriter(status)
        .orElseThrow(() -> new IllegalStateException("status has no writer"))
        .write(update(gateway, status),
            (succ, exc) -> {
              // nop
            });
    pipeline.run();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadSingleFamily()
      throws InterruptedException, ExecutionException {

    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input = pipeline.apply(
        ProximaIO.from(repo).withMaxReadTime(Duration.ofSeconds(1))
            .streamFrom(
                repo.getFamiliesForAttribute(status)
                    .stream()
                    .filter(af -> af.getType() == StorageType.PRIMARY)
                    .findFirst()
                    .get(),
                Position.NEWEST));
    BeamFlow bf = BeamFlow.create(pipeline);
    Dataset<Pair<Integer, Long>> output = CountByKey.of(bf.wrapped(input))
        .keyBy(e -> 0)
        .windowBy(Time.of(Duration.ofMillis(100)))
        .output();
    PAssert.that(bf.unwrapped(output))
        .containsInAnyOrder(Pair.of(0, 1L));
    CompletableFuture<Throwable> result = startPipeline(pipeline);
    repo.getWriter(status)
        .orElseThrow(() -> new IllegalStateException("status has no writer"))
        .write(update(gateway, status),
            (succ, exc) -> {
              // nop
            });
    assertNull(result.get());
  }

  @Test
  public void testPersistOnline() {
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input = pipeline.apply(
        Create.of(update(gateway, status)));
    input.apply(ProximaIO.from(repo).write());
    pipeline.run();
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
  }

  @Test
  public void testPersistBulk() {
    Config config = ConfigFactory.load()
        .withFallback(ConfigFactory.parseString(
            "attributeFamilies.gateway-storage-stream.storage = \"inmem-bulk:///proxima_gateway/bulk\""))
        .withFallback(ConfigFactory.parseString(
            "attributeFamilies.gateway-storage-stream.access = \"random-access\""))
        .withFallback(ConfigFactory.load("test-reference.conf"))
        .resolve();
    repo.reloadConfig(true, config);
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input = pipeline.apply(
        Create.of(update(gateway, status)));
    input.apply(ProximaIO.from(repo).writeBulk(1, TimeUnit.SECONDS, 1));
    pipeline.run();
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
  }

  @Test
  public void testReadFromWriter()
      throws InterruptedException, ExecutionException {

    // write some update
    OnlineAttributeWriter writer = repo.getWriter(status)
        .orElseThrow(() -> new IllegalArgumentException(
            "Missing writer for status"));

    StreamElement update = update(gateway, status);
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input = pipeline
        .apply(
            ProximaIO.from(repo).withMaxReadTime(Duration.ofSeconds(1))
                .streamFrom(
                    repo.getFamiliesForAttribute(status)
                        .stream()
                        .filter(af -> af.getType() == StorageType.PRIMARY)
                        .findFirst()
                        .get(), Position.CURRENT));
    PAssert.that(input)
        .containsInAnyOrder(update);
    CompletableFuture<Throwable> result = startPipeline(pipeline);

    // wait till pipeline starts
    TimeUnit.SECONDS.sleep(1);

    writer.write(update, (succ, exc) -> { });
    assertNull(result.get());
  }

  @Test
  public void testReadFromWriterGroupping()
      throws InterruptedException, ExecutionException {

    // write some update
    OnlineAttributeWriter writer = repo.getWriter(status)
        .orElseThrow(() -> new IllegalArgumentException("Missing writer for status"));

    Pipeline pipeline = Pipeline.create();
    PCollection<Integer> output = pipeline
        .apply(ProximaIO.from(repo).withMaxReadTime(Duration.ofSeconds(1))
            .read(Position.CURRENT, status))
        .apply(MapElements.via(
            new SimpleFunction<StreamElement, KV<Integer, StreamElement>>(
                e -> KV.of(0, e)) { }))
        .apply(Window.into(FixedWindows.of(org.joda.time.Duration.millis(100))))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new DoFn<KV<Integer, Iterable<StreamElement>>, Integer>() {
          @SuppressWarnings("unused")
          @ProcessElement
          public void process(ProcessContext context) {
            int c = 0;
            for (StreamElement e : context.element().getValue()) {
              c++;
            }
            context.output(c);
          }
        }));
    PAssert.that(output)
        .containsInAnyOrder(1);
    CompletableFuture<Throwable> result = startPipeline(pipeline);

    // wait till pipeline starts
    TimeUnit.SECONDS.sleep(1);

    writer.write(update(gateway, status), (succ, exc) -> { });
    assertNull(result.get());
  }


}
