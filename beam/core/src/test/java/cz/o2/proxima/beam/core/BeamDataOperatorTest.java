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
package cz.o2.proxima.beam.core;

import static org.junit.Assert.*;

import cz.o2.proxima.beam.core.direct.io.DirectDataAccessorWrapper;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.ReplicationRunner;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link BeamDataOperator}. */
public class BeamDataOperatorTest {

  final Repository repo =
      Repository.ofTest(
          ConfigFactory.load("test-config-parsing.conf")
              .withFallback(ConfigFactory.load("test-reference.conf")));
  final EntityDescriptor gateway = repo.getEntity("gateway");
  final AttributeDescriptor<?> armed = gateway.getAttribute("armed");
  final AttributeDescriptor<?> status = gateway.getAttribute("status");
  final EntityDescriptor proxied = repo.getEntity("proxied");
  final AttributeDescriptor<?> event = proxied.getAttribute("event.*");

  BeamDataOperator beam;
  DirectDataOperator direct;
  Pipeline pipeline;
  long now;

  @Before
  public synchronized void setUp() {
    beam = repo.getOrCreateOperator(BeamDataOperator.class);
    direct = beam.getDirect();
    pipeline = Pipeline.create();
    now = System.currentTimeMillis();
    ReplicationRunner.runAttributeReplicas(direct);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumption() {
    direct
        .getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer for armed"))
        .write(
            StreamElement.upsert(
                gateway, armed, "uuid", "key", armed.getName(), now, new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);
    PCollection<KV<String, Long>> counted = stream.apply(WithKeys.of("")).apply(Count.perKey());
    PAssert.that(counted).containsInAnyOrder(KV.of("", 1L));
    runPipeline(pipeline);
  }

  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumptionWithWindow() {
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key1", armed.getName(), now - 5000, new byte[] {1, 2, 3}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key2", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);

    PCollection<Long> counted =
        stream
            .apply(
                Window.<StreamElement>into(FixedWindows.of(Duration.millis(1000)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(Combine.globally(Count.<StreamElement>combineFn()).withoutDefaults());

    PAssert.that(counted).containsInAnyOrder(1L, 1L);
    runPipeline(pipeline);
  }

  @Test(timeout = 60000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindow() {
    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key1", armed.getName(), now - 5000, new byte[] {1, 2, 3}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key2", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<StreamElement> stream =
        beam.getStream("", pipeline, Position.OLDEST, false, true, 2, armed);

    PCollection<Long> counted =
        stream
            .apply(
                Window.<StreamElement>into(FixedWindows.of(Duration.millis(1000)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(Combine.globally(Count.<StreamElement>combineFn()).withoutDefaults());
    PAssert.that(counted).containsInAnyOrder(1L, 1L);
    runPipeline(pipeline);
  }

  @Test(timeout = 30000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindowMany() {
    final long elements = 99L;
    validatePCollectionWindowedRead(
        () -> beam.getStream("", pipeline, Position.OLDEST, false, true, elements + 1, armed),
        elements);
  }

  @Test(timeout = 180000)
  public synchronized void testUnboundedCommitLogConsumptionWithWindowManyMany() {
    final long elements = 1000L;
    validatePCollectionWindowedRead(
        () -> beam.getStream("", pipeline, Position.OLDEST, false, true, elements + 1, armed),
        elements);
  }

  @Test /* (timeout = 5000) */
  public synchronized void testBatchUpdatesConsumptionWithWindowMany() {
    validatePCollectionWindowedRead(() -> beam.getBatchUpdates(pipeline, armed), 99L);
  }

  @Test(timeout = 5000)
  public synchronized void testBatchSnapshotConsumptionWithWindowMany() {
    validatePCollectionWindowedRead(() -> beam.getBatchSnapshot(pipeline, armed), 99L);
  }

  @Test(timeout = 5000)
  public void testReadingFromSpecificFamily() {
    validatePCollectionWindowedRead(
        () ->
            beam.getAccessorFor(
                    beam.getRepository().getFamiliesForAttribute(armed).stream()
                        .filter(af -> af.getAccess().canReadBatchSnapshot())
                        .findFirst()
                        .orElseThrow(
                            () -> new IllegalStateException("Missing batch snapshot for " + armed)))
                .createBatch(
                    pipeline, Collections.singletonList(armed), Long.MIN_VALUE, Long.MAX_VALUE),
        99L);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public synchronized void testBoundedCommitLogConsumptionFromProxy() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway,
                event,
                "uuid",
                "key",
                event.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    PCollection<StreamElement> stream =
        beam.getStream(pipeline, Position.OLDEST, true, true, event);
    PCollection<Long> counted =
        stream
            .apply(
                Filter.by(e -> e.getAttributeDescriptor().toAttributePrefix().startsWith("event")))
            .apply(Count.globally());
    PAssert.that(counted).containsInAnyOrder(1L);
    runPipeline(pipeline);
  }

  @Test
  public void testTwoPipelines() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway,
                event,
                "uuid",
                "key",
                event.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});

    PCollection<Long> result =
        beam.getStream(pipeline, Position.OLDEST, true, true, event).apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    runPipeline(pipeline);

    pipeline = Pipeline.create();
    result = beam.getStream(pipeline, Position.OLDEST, true, true, event).apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    runPipeline(pipeline);
  }

  @Test
  public void testUnion() {
    direct
        .getWriter(event)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway,
                event,
                "uuid",
                "key",
                event.toAttributePrefix() + "1",
                now,
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    direct
        .getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer for event"))
        .write(
            StreamElement.upsert(
                gateway, armed, "uuid", "key2", armed.getName(), now + 1, new byte[] {1, 2, 3, 4}),
            (succ, exc) -> {});

    PCollection<StreamElement> events =
        beam.getStream(pipeline, Position.OLDEST, true, true, event);
    PCollection<StreamElement> armedStream =
        beam.getStream(pipeline, Position.OLDEST, true, true, armed);
    PCollection<Long> result =
        PCollectionList.of(events)
            .and(armedStream)
            .apply(Flatten.pCollections())
            .apply(Count.globally());
    PAssert.that(result).containsInAnyOrder(2L);
    runPipeline(pipeline);
  }

  @Test
  public void testStreamFromOldestWithKafkaTest() {
    Config config =
        ConfigFactory.parseMap(
                Collections.singletonMap(
                    "attributeFamilies.event-storage-stream.storage", "kafka-test://dummy/events"))
            .withFallback(ConfigFactory.load("test-reference.conf"));
    Repository repo = Repository.ofTest(config);
    EntityDescriptor event = repo.getEntity("event");
    AttributeDescriptor<?> data = event.getAttribute("data");
    int numElements = 10000;
    long now = System.currentTimeMillis();
    try (DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
        BeamDataOperator operator = repo.getOrCreateOperator(BeamDataOperator.class)) {

      for (int i = 0; i < numElements; i++) {
        direct
            .getWriter(data)
            .orElseThrow(() -> new IllegalStateException("Missing writer for data"))
            .write(
                StreamElement.upsert(
                    event,
                    data,
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    data.getName(),
                    now + i,
                    new byte[] {}),
                (succ, exc) -> {});
      }
      Pipeline p = Pipeline.create();
      PCollection<StreamElement> input = operator.getStream(p, Position.OLDEST, true, true, data);
      PCollection<Long> count = input.apply(Count.globally());
      PAssert.that(count).containsInAnyOrder(Collections.singletonList((long) numElements));
      assertNotNull(p.run());
    }
  }

  @Test
  public void testMultipleConsumptionsFromSingleAttribute() {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> first = beam.getStream(p, Position.OLDEST, true, true, armed);
    PCollection<StreamElement> second = beam.getStream(p, Position.OLDEST, true, true, armed);
    // validate that we have used cached PCollection
    terminatePipeline(first, second);
    checkHasSingleInput(p);

    p = Pipeline.create();
    first = beam.getBatchUpdates(p, armed);
    second = beam.getBatchUpdates(p, armed);
    terminatePipeline(first, second);
    checkHasSingleInput(p);

    p = Pipeline.create();
    first = beam.getBatchSnapshot(p, armed);
    second = beam.getBatchSnapshot(p, armed);
    terminatePipeline(first, second);
    checkHasSingleInput(p);
  }

  @Test
  public void testDifferentAttributesFromSameFamily() {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> first = beam.getStream(p, Position.OLDEST, true, true, armed);
    PCollection<StreamElement> second = beam.getStream(p, Position.OLDEST, true, true, status);
    // validate that we have used cached PCollection
    terminatePipeline(first, second);
    checkHasSingleInput(p);

    p = Pipeline.create();
    first = beam.getBatchUpdates(p, armed);
    second = beam.getBatchUpdates(p, status);
    terminatePipeline(first, second);
    checkHasSingleInput(p);

    p = Pipeline.create();
    first = beam.getBatchSnapshot(p, armed);
    second = beam.getBatchSnapshot(p, status);
    terminatePipeline(first, second);
    checkHasSingleInput(p);
  }

  @Test
  public void testConsumptionFromDifferentAttributesFromSameFamily() {
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(armed));
    long now = System.currentTimeMillis();
    writer.write(
        StreamElement.upsert(
            gateway,
            armed,
            UUID.randomUUID().toString(),
            "key",
            armed.getName(),
            now,
            new byte[] {}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key",
            status.getName(),
            now,
            new byte[] {}),
        (succ, exc) -> {});
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> first = beam.getBatchSnapshot(p, armed);
    PCollection<StreamElement> second = beam.getBatchSnapshot(p, status);
    PCollectionList<StreamElement> input = PCollectionList.of(first).and(second);
    PCollection<KV<String, Long>> counted =
        input
            .apply(Flatten.pCollections())
            .apply(WithKeys.of(StreamElement::getKey).withKeyType(TypeDescriptors.strings()))
            .apply(Count.perKey());
    PAssert.that(counted).containsInAnyOrder(KV.of("key", 2L));
  }

  @Test
  public void testReadFromProxy() {
    EntityDescriptor entity = repo.getEntity("proxied");
    AttributeDescriptor<byte[]> event = entity.getAttribute("event.*");
    direct
        .getWriter(event)
        .get()
        .write(
            StreamElement.upsert(
                entity,
                event,
                UUID.randomUUID().toString(),
                "key",
                event.toAttributePrefix() + 1,
                System.currentTimeMillis(),
                new byte[] {1}),
            (succ, exc) -> {});
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> input = beam.getStream(p, Position.OLDEST, true, true, event);
    PAssert.that(input.apply(Count.globally())).containsInAnyOrder(1L);
    assertNotNull(p.run());
  }

  @Test
  public void testBatchLogReadConfigParsing() {
    Pipeline p = Pipeline.create();
    beam.getBatchUpdates(p, armed);
    DataAccessor accessor = beam.getAccessorFor(repo.getFamilyByName("gateway-storage-batch"));
    Map<String, Object> cfg = ((DirectDataAccessorWrapper) accessor).getCfg();
    assertEquals(1, cfg.get("batch.max-initial-splits"));
  }

  private void terminatePipeline(
      PCollection<StreamElement> first, PCollection<StreamElement> second) {
    PCollectionList.of(first).and(second).apply(Flatten.pCollections()).apply(done());
  }

  private static <T> PTransform<PCollection<T>, PDone> done() {
    return new PTransform<PCollection<T>, PDone>() {
      @Override
      public PDone expand(PCollection<T> input) {
        return PDone.in(input.getPipeline());
      }
    };
  }

  private void checkHasSingleInput(Pipeline p) {
    AtomicBoolean terminated = new AtomicBoolean();
    p.traverseTopologically(checkHasSingleInputVisitor(terminated));
    assertTrue(terminated.get());
  }

  private PipelineVisitor checkHasSingleInputVisitor(AtomicBoolean terminated) {
    Set<Node> roots = new HashSet<>();
    return new PipelineVisitor() {

      @Override
      public void enterPipeline(Pipeline p) {}

      @Override
      public CompositeBehavior enterCompositeTransform(Node node) {
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      @Override
      public void leaveCompositeTransform(Node node) {}

      @Override
      public void visitPrimitiveTransform(Node node) {
        visitNode(node);
      }

      @Override
      public void visitValue(PValue value, Node producer) {}

      @Override
      public void leavePipeline(Pipeline pipeline) {
        assertEquals(String.format("Expected single root, got %s", roots), 1, roots.size());
        terminated.set(true);
      }

      void visitNode(Node node) {
        if (node.getTransform() != null && node.getInputs().isEmpty()) {
          roots.add(node);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private synchronized void validatePCollectionWindowedRead(
      Factory<PCollection<StreamElement>> input, long elements) {

    OnlineAttributeWriter writer =
        direct
            .getWriter(armed)
            .orElseThrow(() -> new IllegalStateException("Missing writer for armed"));

    for (int i = 0; i < elements; i++) {
      writer.write(
          StreamElement.upsert(
              gateway,
              armed,
              "uuid",
              "key" + i,
              armed.getName(),
              now - 20000 - i,
              new byte[] {1, 2, 3}),
          (succ, exc) -> {});
    }

    writer.write(
        StreamElement.upsert(
            gateway, armed, "uuid", "key-last", armed.getName(), now, new byte[] {1, 2, 3}),
        (succ, exc) -> {});

    PCollection<Long> counted =
        input
            .apply()
            .apply(
                Window.<StreamElement>into(Sessions.withGapDuration(Duration.millis(1000)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.millis(10000))
                    .discardingFiredPanes())
            .apply(Combine.globally(Count.<StreamElement>combineFn()).withoutDefaults());

    PAssert.that(counted).containsInAnyOrder(elements, 1L);
    runPipeline(pipeline);
  }

  private void runPipeline(Pipeline pipeline) {
    try {
      assertNotNull(pipeline.run());
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
  }
}
