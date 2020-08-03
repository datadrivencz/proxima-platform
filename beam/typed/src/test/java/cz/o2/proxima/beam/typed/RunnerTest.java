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
package cz.o2.proxima.beam.typed;

import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.storage.InMemStorage;
import cz.o2.proxima.direct.time.BoundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.ReplicationController;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.Watermarks;
import cz.seznam.profile.utils.Instances;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class RunnerTest {

  private static final String TEST_CONFIG =
      "\n"
          + "entities {\n"
          + "  foo {\n"
          + "    attributes {\n"
          + "      first: { scheme: string }\n"
          + "      second: { scheme: string }\n"
          + "      third: { scheme: string }\n"
          + "    }\n"
          + "  }\n"
          + "}\n"
          + "attributeFamilies {\n"
          + "  foo-first {\n"
          + "    type: primary\n"
          + "    storage: \"inmem:///foo-first\"\n"
          + "    entity: foo\n"
          + "    attributes: [\"first\"]\n"
          + "    access: commit-log\n"
          + "  }\n"
          + "  foo-first-batch {\n"
          + "    type: replica \n"
          + "    storage: \"inmem:///foo-first-batch\"\n"
          + "    entity: foo\n"
          + "    attributes: [\"first\"]\n"
          + "    access: batch-updates\n"
          + "  }\n"
          + "  foo-second {\n"
          + "    type: primary\n"
          + "    storage: \"inmem:///foo-second\"\n"
          + "    entity: foo\n"
          + "    attributes: [\"second\"]\n"
          + "    access: commit-log\n"
          + "  }\n"
          + "  foo-third {\n"
          + "    type: primary\n"
          + "    storage: \"inmem:///foo-third\"\n"
          + "    entity: foo\n"
          + "    attributes: [\"third\"]\n"
          + "    access: commit-log\n"
          + "  }\n"
          + "}";

  public static class FirstMaterializer
      implements SingleOutputPipelineMaterializer<String>, Serializable {

    private final AttributeDescriptor<String> firstDescriptor;
    private final AttributeDescriptor<String> secondDescriptor;

    public FirstMaterializer(Repository repository) {
      this.firstDescriptor = repository.getEntity("foo").getAttribute("first");
      this.secondDescriptor = repository.getEntity("foo").getAttribute("second");
    }

    @Override
    public void registerInputs(RegisterInputsContext ctx) {
      ctx.registerInput(firstDescriptor);
    }

    @Override
    public void registerOutputs(RegisterOutputsContext ctx) {
      ctx.registerOutput(secondDescriptor);
    }

    @Override
    public PCollection<TypedElement<String>> materialize(MaterializeContext ctx) {
      return ctx.getInput(firstDescriptor)
          .apply(
              MapElements.into(TypedElement.typeDescriptor(secondDescriptor))
                  .via(
                      input ->
                          TypedElement.upsert(secondDescriptor, input.getKey(), input.getValue())));
    }
  }

  public static class SecondMaterializer implements MultiOutputPipelineMaterializer, Serializable {

    private static final TupleTag<TypedElement<String>> OUTPUT_FOO_SECOND = new TupleTag<>();
    private static final TupleTag<TypedElement<String>> OUTPUT_FOO_THIRD = new TupleTag<>();

    private final AttributeDescriptor<String> firstDescriptor;
    private final AttributeDescriptor<String> secondDescriptor;
    private final AttributeDescriptor<String> thirdDescriptor;

    public SecondMaterializer(Repository repository) {
      this.firstDescriptor = repository.getEntity("foo").getAttribute("first");
      this.secondDescriptor = repository.getEntity("foo").getAttribute("second");
      this.thirdDescriptor = repository.getEntity("foo").getAttribute("third");
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }

    @Override
    public void registerInputs(RegisterInputsContext ctx) {
      ctx.registerInput(firstDescriptor);
    }

    @Override
    public void registerOutputs(RegisterOutputsContext ctx) {
      ctx.registerOutput(OUTPUT_FOO_SECOND, secondDescriptor);
      ctx.registerOutput(OUTPUT_FOO_THIRD, thirdDescriptor);
    }

    @Override
    public PCollectionTuple materialize(MaterializeContext ctx) {
      final PCollection<TypedElement<String>> first = ctx.getInput(firstDescriptor);
      return PCollectionTuple.empty(ctx.getPipeline())
          .and(
              OUTPUT_FOO_SECOND,
              first.apply(
                  MapElements.into(TypedElement.typeDescriptor(secondDescriptor))
                      .via(
                          input ->
                              TypedElement.upsert(
                                  secondDescriptor, input.getKey(), input.getValue()))))
          .and(
              OUTPUT_FOO_THIRD,
              first.apply(
                  MapElements.into(TypedElement.typeDescriptor(thirdDescriptor))
                      .via(
                          input ->
                              TypedElement.upsert(
                                  thirdDescriptor, input.getKey(), input.getValue()))));
    }
  }

  private static class FinishingWatermarkIdlePolicy implements WatermarkIdlePolicy {

    private long lastIdle = -1;

    @Override
    public long getIdleWatermark() {
      if (lastIdle > 0 && System.currentTimeMillis() - lastIdle > 1_000) {
        // Wait for 1s after the last idle call to avoid race conditions.
        return Watermarks.MAX_WATERMARK;
      }
      return Watermarks.MIN_WATERMARK;
    }

    @Override
    public void update(StreamElement element) {
      // No-op.

    }

    @Override
    public void idle(long currentWatermark) {
      lastIdle = System.currentTimeMillis();
    }
  }

  private static class TestLogObserver implements LogObserver {

    private final CountDownLatch doneReading = new CountDownLatch(1);
    private final List<String> results = new ArrayList<>();

    @Override
    public void onCompleted() {
      doneReading.countDown();
    }

    @Override
    public boolean onError(Throwable throwable) {
      throw new IllegalStateException(throwable);
    }

    @Override
    public boolean onNext(StreamElement streamElement, LogObserver.OnNextContext onNextContext) {
      results.add(
          String.format(
              "%s.%s: %s",
              streamElement.getEntityDescriptor().getName(),
              streamElement.getAttribute(),
              streamElement
                  .getAttributeDescriptor()
                  .valueOf(streamElement)
                  .orElseThrow(() -> new IllegalStateException("Unable to deserialize element."))));
      return true;
    }
  }

  @Test
  public void testSingleOutput() throws InterruptedException {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(TEST_CONFIG));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final EntityDescriptor entityDescriptor = repository.getEntity("foo");
    final AttributeDescriptor<String> firstDescriptor = entityDescriptor.getAttribute("first");
    final AttributeDescriptor<String> secondDescriptor = entityDescriptor.getAttribute("second");

    // Write elements to foo.first.
    final OnlineAttributeWriter firstWriter =
        direct
            .getWriter(firstDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct writer."));
    final CountDownLatch doneWriting = new CountDownLatch(1);
    firstWriter.write(
        StreamElement.upsert(
            entityDescriptor,
            firstDescriptor,
            UUID.randomUUID().toString(),
            "key",
            firstDescriptor.getName(),
            0L,
            "value".getBytes(StandardCharsets.UTF_8)),
        (success, error) -> doneWriting.countDown());

    Assertions.assertTrue(doneWriting.await(100, TimeUnit.MILLISECONDS));

    // Run pipeline.
    final Runner runner =
        new Runner(
            repository,
            Collections.singletonList(new Instances.Parameter<>(Repository.class, repository)));
    final PipelineOptions options = PipelineOptionsFactory.create();
    final RunnerPipelineOptions runnerOptions = options.as(RunnerPipelineOptions.class);
    runnerOptions.setMaterializers(Collections.singletonList(FirstMaterializer.class));
    runnerOptions.setInputMode(InputMode.STREAM_FROM_OLDEST_BATCH);
    runner.runBlocking(options);

    // Read from foo.second.
    final CommitLogReader secondReader =
        direct
            .getCommitLogReader(secondDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct reader."));
    final TestLogObserver logObserver = new TestLogObserver();
    secondReader.observePartitions(
        secondReader.getPartitions(), Position.OLDEST, true, logObserver);
    Assertions.assertTrue(logObserver.doneReading.await(100, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(Collections.singletonList("foo.second: value"), logObserver.results);
  }

  @Test
  public void testMultiOutput() throws InterruptedException {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(TEST_CONFIG));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final EntityDescriptor entityDescriptor = repository.getEntity("foo");
    final AttributeDescriptor<String> firstDescriptor = entityDescriptor.getAttribute("first");
    final AttributeDescriptor<String> secondDescriptor = entityDescriptor.getAttribute("second");
    final AttributeDescriptor<String> thirdDescriptor = entityDescriptor.getAttribute("third");

    // Write elements to foo.first.
    final OnlineAttributeWriter firstWriter =
        direct
            .getWriter(firstDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct writer."));
    final CountDownLatch doneWriting = new CountDownLatch(1);
    firstWriter.write(
        StreamElement.upsert(
            entityDescriptor,
            firstDescriptor,
            UUID.randomUUID().toString(),
            "key",
            firstDescriptor.getName(),
            0L,
            "value".getBytes(StandardCharsets.UTF_8)),
        (success, error) -> doneWriting.countDown());

    Assertions.assertTrue(doneWriting.await(100, TimeUnit.MILLISECONDS));

    // Run pipeline.
    final Runner runner =
        new Runner(
            repository,
            Collections.singletonList(new Instances.Parameter<>(Repository.class, repository)));
    final PipelineOptions options = PipelineOptionsFactory.create();
    final RunnerPipelineOptions runnerOptions = options.as(RunnerPipelineOptions.class);
    runnerOptions.setMaterializers(Collections.singletonList(SecondMaterializer.class));
    runnerOptions.setInputMode(InputMode.STREAM_FROM_OLDEST_BATCH);
    runner.runBlocking(options);

    // Read from foo.second.
    final CommitLogReader secondReader =
        direct
            .getCommitLogReader(secondDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct reader."));
    final TestLogObserver secondLogObserver = new TestLogObserver();
    secondReader.observePartitions(
        secondReader.getPartitions(), Position.OLDEST, true, secondLogObserver);
    Assertions.assertTrue(secondLogObserver.doneReading.await(100, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(
        Collections.singletonList("foo.second: value"), secondLogObserver.results);

    // Read from foo.third.
    final CommitLogReader thirdReader =
        direct
            .getCommitLogReader(thirdDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct reader."));
    final TestLogObserver thirdLogObserver = new TestLogObserver();
    thirdReader.observePartitions(
        thirdReader.getPartitions(), Position.OLDEST, true, thirdLogObserver);
    Assertions.assertTrue(thirdLogObserver.doneReading.await(100, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(
        Collections.singletonList("foo.third: value"), thirdLogObserver.results);
  }

  @Test
  @Disabled("UPRZ-133")
  public void testBootstrapInputMode() throws InterruptedException {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(TEST_CONFIG));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final EntityDescriptor entityDescriptor = repository.getEntity("foo");
    final AttributeDescriptor<String> firstDescriptor = entityDescriptor.getAttribute("first");
    final AttributeDescriptor<String> secondDescriptor = entityDescriptor.getAttribute("second");

    // Run replications for batch storage.
    final CompletableFuture<Void> controllerRunning =
        ReplicationController.of(repository).runReplicationThreads();
    Assertions.assertFalse(controllerRunning.isDone());

    // Write elements to foo.first.
    final OnlineAttributeWriter firstWriter =
        direct
            .getWriter(firstDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct writer."));

    final int numElements = 100;
    final CountDownLatch doneWriting = new CountDownLatch(numElements);
    for (int i = 0; i < numElements; i++) {
      final String value = "value-" + i;
      firstWriter.write(
          StreamElement.upsert(
              entityDescriptor,
              firstDescriptor,
              UUID.randomUUID().toString(),
              "key-" + i,
              firstDescriptor.getName(),
              i,
              value.getBytes(StandardCharsets.UTF_8)),
          (success, error) -> doneWriting.countDown());
    }

    Assertions.assertTrue(doneWriting.await(100, TimeUnit.MILLISECONDS));

    // Override watermark factory, so the pipeline can finish.
    repository
        .getAllFamilies()
        .filter(af -> af.getStorageUri().getScheme().equals("inmem"))
        .forEach(
            af ->
                InMemStorage.setWatermarkEstimatorFactory(
                    af.getStorageUri(),
                    (InMemStorage.WatermarkEstimatorFactory)
                        (l, s, consumedOffset) ->
                            BoundedOutOfOrdernessWatermarkEstimator.newBuilder()
                                .withWatermarkIdlePolicy(new FinishingWatermarkIdlePolicy())
                                .build()));

    // Run pipeline.
    final Runner runner =
        new Runner(
            repository,
            Collections.singletonList(new Instances.Parameter<>(Repository.class, repository)));
    final PipelineOptions options = PipelineOptionsFactory.create();
    options.as(StreamingOptions.class).setStreaming(true);
    final RunnerPipelineOptions runnerOptions = options.as(RunnerPipelineOptions.class);
    runnerOptions.setMaterializers(Collections.singletonList(FirstMaterializer.class));
    runnerOptions.setInputMode(InputMode.BOOTSTRAP);
    runnerOptions.setBootstrapBarrierTimestamp((long) numElements / 2);
    final PipelineResult pipelineResult = runner.runInternal(options);
    pipelineResult.waitUntilFinish();

    // Assert beam metrics.
    MetricResult<Long> filterUntil =
        Iterables.getOnlyElement(
            pipelineResult
                .metrics()
                .queryMetrics(
                    MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named("filter-timestamp", "until"))
                        .build())
                .getCounters());
    MetricResult<Long> filterFrom =
        Iterables.getOnlyElement(
            pipelineResult
                .metrics()
                .queryMetrics(
                    MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named("filter-timestamp", "from"))
                        .build())
                .getCounters());
    Assertions.assertEquals(50, filterUntil.getCommitted());
    Assertions.assertEquals(50, filterFrom.getCommitted());

    // Read from foo.second.
    final CommitLogReader secondReader =
        direct
            .getCommitLogReader(secondDescriptor)
            .orElseThrow(() -> new IllegalStateException("Unable to construct reader."));
    final TestLogObserver logObserver = new TestLogObserver();
    secondReader.observePartitions(
        secondReader.getPartitions(), Position.OLDEST, true, logObserver);
    Assertions.assertTrue(logObserver.doneReading.await(100, TimeUnit.MILLISECONDS));
    final List<String> result = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      result.add("foo.second: value-" + i);
    }
    Assertions.assertEquals(result, logObserver.results);
  }
}
