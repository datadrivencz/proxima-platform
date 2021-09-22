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
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.KeyAttributePartitioner;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.storage.commitlog.Partitioners;
import cz.o2.proxima.util.Optionals;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class CommitLogSourceFunctionTest extends AbstractLogSourceFunctionTest {

  private static final String MODEL =
      "{\n"
          + "  entities: {\n"
          + "    test {\n"
          + "      attributes {\n"
          + "        first: { scheme: \"string\" }\n"
          + "        second: { scheme: \"string\" }\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "  attributeFamilies: {\n"
          + "    test_storage_stream {\n"
          + "      entity: test\n"
          + "      attributes: [ first, second ]\n"
          + "      storage: \"inmem:///test_inmem\"\n"
          + "      type: primary\n"
          + "      access: commit-log\n"
          + "      num-partitions: 3\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  private static <T> AbstractStreamOperatorTestHarness<T> createTestHarness(
      SourceFunction<T> source, int numSubtasks, int subtaskIndex) throws Exception {
    final int maxParallelism = 12;
    Preconditions.checkArgument(numSubtasks <= maxParallelism);
    return new AbstractStreamOperatorTestHarness<>(
        new StreamSource<>(source), maxParallelism, numSubtasks, subtaskIndex);
  }

  @Test
  void testRunAndClose() throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final AttributeDescriptor<?> attribute = repository.getEntity("test").getAttribute("first");
    final CommitLogSourceFunction<StreamElement> sourceFunction =
        new CommitLogSourceFunction<>(
            "test-consumer",
            repository.asFactory(),
            Collections.singletonList(attribute),
            FlinkDataOperator.newCommitLogOptions().build(),
            ResultExtractor.identity());
    final AbstractStreamOperatorTestHarness<StreamElement> testHarness =
        createTestHarness(sourceFunction, 1, 0);
    testHarness.initializeEmptyState();
    testHarness.open();

    final CheckedThread runThread =
        new CheckedThread("run") {

          @Override
          public void go() throws Exception {
            sourceFunction.run(
                new TestSourceContext<StreamElement>() {

                  @Override
                  public void collect(StreamElement element) {
                    // No-op.
                  }
                });
          }
        };

    runThread.start();
    sourceFunction.awaitRunning();
    sourceFunction.cancel();
    testHarness.close();

    // Make sure run thread finishes normally.
    runThread.sync();
  }

  @Test
  void testObserverErrorPropagatesToTheMainThread() throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final EntityDescriptor entity = repository.getEntity("test");
    final AttributeDescriptor<String> attributeDescriptor = entity.getAttribute("first");
    final CommitLogSourceFunction<StreamElement> sourceFunction =
        new CommitLogSourceFunction<>(
            "test-consumer",
            repository.asFactory(),
            Collections.singletonList(attributeDescriptor),
            FlinkDataOperator.newCommitLogOptions().build(),
            element -> {
              throw new IllegalStateException("Test failure.");
            });
    final AbstractStreamOperatorTestHarness<StreamElement> testHarness =
        createTestHarness(sourceFunction, 1, 0);
    testHarness.initializeEmptyState();
    testHarness.open();

    final CheckedThread runThread =
        new CheckedThread("run") {

          @Override
          public void go() throws Exception {
            sourceFunction.run(
                new TestSourceContext<StreamElement>() {

                  @Override
                  public void collect(StreamElement element) {
                    // No-op.
                  }
                });
          }
        };

    final StreamElement element =
        createUpsertElement(entity, attributeDescriptor, "key", Instant.now(), "value");
    final OnlineAttributeWriter writer = Optionals.get(direct.getWriter(attributeDescriptor));
    writer.write(element, CommitCallback.noop());

    runThread.start();
    sourceFunction.awaitRunning();

    final IllegalStateException exception =
        Assertions.assertThrows(IllegalStateException.class, runThread::sync);
    Assertions.assertEquals("Test failure.", exception.getCause().getMessage());
    testHarness.close();
  }

  @Test
  void testRestore() throws Exception {
    testSnapshotAndRestore(6, 6);
  }

  @Test
  void testDownScale() throws Exception {
    testSnapshotAndRestore(3, 1);
  }

  @Test
  void testUpScale() throws Exception {
    testSnapshotAndRestore(1, 3);
  }

  @Test
  void testReadFilteredAttributesFromSource() throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final EntityDescriptor testEntity = repository.getEntity("test");
    final AttributeDescriptor<String> first = testEntity.getAttribute("first");
    final AttributeDescriptor<String> second = testEntity.getAttribute("second");

    final OnlineAttributeWriter writerForFirst = Optionals.get(direct.getWriter(first));
    final OnlineAttributeWriter writerForSecond = Optionals.get(direct.getWriter(second));
    final Instant now = Instant.now();
    final int numElements = 1000;
    CommitCallback callback =
        (success, error) -> {
          if (error != null) {
            log.error("Error during write {}.", error.toString());
          }
          Assertions.assertTrue(success);
        };
    // let's write numElements of first and second into same commitlog
    for (int i = 0; i < numElements; i++) {
      writerForFirst.write(
          createUpsertElement(testEntity, first, "key_" + i, now, "value_" + i),
          CommitCallback.afterNumCommits(numElements, callback));
      writerForSecond.write(
          createUpsertElement(testEntity, second, "key_" + i, now, "value_" + i),
          CommitCallback.afterNumCommits(numElements, callback));
    }

    final List<StreamElement> result = Collections.synchronizedList(new ArrayList<>());
    final RunReadTestSubtask runTest =
        (List<AttributeDescriptor<?>> attributes, int expectedElements) -> {
          result.clear();
          runSubtask(repository, attributes, null, result::add, 1, 0, expectedElements);
          Assertions.assertEquals(expectedElements, result.size());
          Assertions.assertFalse(
              result.stream().anyMatch(e -> !attributes.contains(e.getAttributeDescriptor())));
        };
    // try read just first
    runTest.run(Collections.singletonList(first), numElements);
    // try read just second
    runTest.run(Collections.singletonList(second), numElements);
    // try read both
    runTest.run(Arrays.asList(first, second), 2 * numElements);
  }

  private void testSnapshotAndRestore(int numSubtasks, int numRestoredSubtasks) throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class);
    final EntityDescriptor entity = repository.getEntity("test");
    final AttributeDescriptor<String> attributeDescriptor = entity.getAttribute("first");
    final Instant now = Instant.now();

    final OnlineAttributeWriter writer = Optionals.get(direct.getWriter(attributeDescriptor));

    final int numCommitLogPartitions = 3;
    final int numElements = 1000;
    final Partitioner partitioner = new KeyAttributePartitioner();
    final Map<Integer, Integer> partitionElements = new HashMap<>();
    final List<StreamElement> emittedElements = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      final StreamElement element =
          createUpsertElement(entity, attributeDescriptor, "key_" + i, now, "value_" + i);
      emittedElements.add(element);
      partitionElements.merge(
          Partitioners.getTruncatedPartitionId(
              partitioner, element, Math.min(numCommitLogPartitions, numSubtasks)),
          1,
          Integer::sum);
      writer.write(element, CommitCallback.noop());
    }

    final List<StreamElement> result = Collections.synchronizedList(new ArrayList<>());
    final List<OperatorSubtaskState> snapshots = new ArrayList<>();

    // Run first iteration - clean state.
    for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
      final int expectedElements = partitionElements.getOrDefault(subtaskIndex, 0);
      snapshots.add(
          runSubtask(
              repository,
              Collections.singletonList(attributeDescriptor),
              null,
              result::add,
              numSubtasks,
              subtaskIndex,
              expectedElements));
    }

    final OperatorSubtaskState mergedState =
        AbstractStreamOperatorTestHarness.repackageState(
            snapshots.toArray(new OperatorSubtaskState[0]));

    // Run second iteration - restored from snapshot.
    partitionElements.clear();
    for (int i = 0; i < numElements; i++) {
      final StreamElement element =
          createUpsertElement(entity, attributeDescriptor, "second_key_" + i, now, "value_" + i);
      emittedElements.add(element);
      partitionElements.merge(
          Partitioners.getTruncatedPartitionId(
              partitioner, element, Math.min(numCommitLogPartitions, numRestoredSubtasks)),
          1,
          Integer::sum);
      writer.write(element, CommitCallback.noop());
    }

    Assertions.assertEquals(1000, result.size());
    for (int subtaskIndex = 0; subtaskIndex < numRestoredSubtasks; subtaskIndex++) {
      final int expectedElements = partitionElements.getOrDefault(subtaskIndex, 0);
      runSubtask(
          repository,
          Collections.singletonList(attributeDescriptor),
          mergedState,
          result::add,
          numRestoredSubtasks,
          subtaskIndex,
          expectedElements);
    }

    final List<String> expectedKeys =
        emittedElements.stream().map(StreamElement::getKey).sorted().collect(Collectors.toList());
    final List<String> receivedKeys =
        result.stream().map(StreamElement::getKey).sorted().collect(Collectors.toList());
    Assertions.assertEquals(expectedKeys, receivedKeys);
  }

  private OperatorSubtaskState runSubtask(
      Repository repository,
      List<AttributeDescriptor<?>> attributeDescriptors,
      @Nullable OperatorSubtaskState state,
      Consumer<StreamElement> outputConsumer,
      int numSubtasks,
      int subtaskIndex,
      int expectedElements)
      throws Exception {
    final CommitLogSourceFunction<StreamElement> sourceFunction =
        new CommitLogSourceFunction<>(
            "test-consumer",
            repository.asFactory(),
            attributeDescriptors,
            FlinkDataOperator.newCommitLogOptions().build(),
            ResultExtractor.identity());
    final AbstractStreamOperatorTestHarness<StreamElement> testHarness =
        createTestHarness(sourceFunction, numSubtasks, subtaskIndex);
    if (state == null) {
      testHarness.initializeEmptyState();
    } else {
      testHarness.initializeState(state);
    }
    testHarness.open();
    Assertions.assertEquals("test-consumer", sourceFunction.getConsumerName());
    final CountDownLatch elementsReceived = new CountDownLatch(expectedElements);
    final CheckedThread runThread =
        new CheckedThread("run") {

          @Override
          public void go() throws Exception {
            sourceFunction.run(
                new TestSourceContext<StreamElement>() {

                  @Override
                  public void collect(StreamElement element) {
                    outputConsumer.accept(element);
                    elementsReceived.countDown();
                  }

                  @Override
                  public void collectWithTimestamp(StreamElement element, long timestamp) {
                    collect(element);
                  }
                });
          }
        };
    runThread.start();
    sourceFunction.awaitRunning();
    elementsReceived.await();

    final OperatorSubtaskState snapshot = testHarness.snapshot(0, 0L);

    sourceFunction.cancel();
    testHarness.close();

    // Make sure run thread finishes normally.
    runThread.sync();
    return snapshot;
  }
}
