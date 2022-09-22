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
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.BatchLogReaders;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.storage.ListBatchReader;
import cz.o2.proxima.flink.core.batch.OffsetTrackingBatchLogReader;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
class BatchLogSourceFunctionTest extends AbstractLogSourceFunctionTest {

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
          + "    test_storage_batch {\n"
          + "      entity: test\n"
          + "      attributes: [ first, second ]\n"
          + "      storage: \"inmem:///test_storage_batch\"\n"
          + "      type: primary\n"
          + "      access: batch-updates\n"
          + "      num-partitions: 3\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  private static final Random RANDOM = new Random();

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
    final BatchLogSourceFunction<StreamElement> sourceFunction =
        new BatchLogSourceFunction<StreamElement>(
            repository.asFactory(),
            Collections.singletonList(attribute),
            FlinkDataOperator.newBatchLogOptions().build(),
            ResultExtractor.identity()) {

          @Override
          BatchLogReader createLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
            final DirectDataOperator direct =
                repository.getOrCreateOperator(DirectDataOperator.class);
            final ListBatchReader reader = ListBatchReader.ofPartitioned(direct.getContext());
            return OffsetTrackingBatchLogReader.of(reader);
          }
        };
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
    final EntityDescriptor entity = repository.getEntity("test");
    final AttributeDescriptor<String> first = entity.getAttribute("first");
    final AttributeDescriptor<String> second = entity.getAttribute("second");
    final OnlineAttributeWriter writerForFirst = Optionals.get(direct.getWriter(first));
    final OnlineAttributeWriter writerForSecond = Optionals.get(direct.getWriter(second));
    final Instant now = Instant.now();

    final int numCommitLogPartitions = 30;
    final Partitioner partitioner = new KeyAttributePartitioner();
    final Map<Integer, List<StreamElement>> partitionElements = new HashMap<>();
    final int numElements = 1000;
    for (int i = 0; i < numElements; i++) {
      final StreamElement firstElement =
          createUpsertElement(entity, first, "key_" + i, now, "value_" + i);
      final StreamElement secondElement =
          createUpsertElement(entity, second, "key_" + i, now, "value_" + i);
      int firstPartitionId =
          Partitioners.getTruncatedPartitionId(partitioner, firstElement, numCommitLogPartitions);
      partitionElements.computeIfAbsent(firstPartitionId, ArrayList::new).add(firstElement);
      final int secondPartitionId =
          Partitioners.getTruncatedPartitionId(partitioner, secondElement, numCommitLogPartitions);
      partitionElements.computeIfAbsent(secondPartitionId, ArrayList::new).add(secondElement);
    }
    final List<StreamElement> result = Collections.synchronizedList(new ArrayList<>());

    final RunReadTestSubtask runTest =
        (List<AttributeDescriptor<?>> attributes, int expectedElements) -> {
          result.clear();
          runSubtask(
              repository,
              attributes,
              null,
              result::add,
              1,
              0,
              expectedElements,
              partitionElements
                  .entrySet()
                  .stream()
                  .sorted(Comparator.comparingInt(Map.Entry::getKey))
                  .map(Map.Entry::getValue)
                  .collect(Collectors.toList()));
          Assertions.assertEquals(expectedElements, result.size());
          Assertions.assertFalse(
              result.stream().anyMatch(e -> !attributes.contains(e.getAttributeDescriptor())));
        };
    // try read just first
    runTest.run(Collections.singletonList(first), numElements);
    // try read just second
    runTest.run(Collections.singletonList(second), numElements);
    // try read both attributes
    runTest.run(Arrays.asList(first, second), 2 * numElements);
  }

  private void testSnapshotAndRestore(int numSubtasks, int numRestoredSubtasks) throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    final EntityDescriptor entity = repository.getEntity("test");
    final AttributeDescriptor<String> attribute = entity.getAttribute("first");
    final Instant now = Instant.now();

    final int numCommitLogPartitions = 30;
    final int numElements = 10_000;
    final Partitioner partitioner = new KeyAttributePartitioner();
    final Map<Integer, Integer> expectedElements = new HashMap<>();
    final Map<Integer, List<StreamElement>> partitionElements = new HashMap<>();
    final List<StreamElement> emittedElements = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      final StreamElement element =
          createUpsertElement(entity, attribute, "key_" + i, now, "value_" + i);
      emittedElements.add(element);
      final int partitionId =
          Partitioners.getTruncatedPartitionId(partitioner, element, numCommitLogPartitions);
      final int subtaskId = partitionId % numSubtasks;
      partitionElements.computeIfAbsent(partitionId, ArrayList::new).add(element);
      expectedElements.merge(subtaskId, 1, Integer::sum);
    }

    final List<StreamElement> result = Collections.synchronizedList(new ArrayList<>());
    final List<OperatorSubtaskState> snapshots = new ArrayList<>();

    // Run the first iteration - clean state. We subtract random number of elements from each
    // subTask, that we'll process in the second iteration.
    int subtractTotal = 0;
    for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
      int numExpectedElements = expectedElements.getOrDefault(subtaskIndex, 0);
      if (numExpectedElements > 0) {
        final int subtractCurrent = RANDOM.nextInt(numExpectedElements);
        numExpectedElements -= subtractCurrent;
        subtractTotal += subtractCurrent;
      }
      snapshots.add(
          runSubtask(
              repository,
              Collections.singletonList(attribute),
              null,
              result::add,
              numSubtasks,
              subtaskIndex,
              numExpectedElements,
              partitionElements
                  .entrySet()
                  .stream()
                  .sorted(Comparator.comparingInt(Map.Entry::getKey))
                  .map(Map.Entry::getValue)
                  .collect(Collectors.toList())));
    }

    Assertions.assertEquals(numElements - subtractTotal, result.size());

    final OperatorSubtaskState mergedState =
        AbstractStreamOperatorTestHarness.repackageState(
            snapshots.toArray(new OperatorSubtaskState[0]));

    // Run the second iteration - restored from snapshot.
    for (int subtaskIndex = 0; subtaskIndex < numRestoredSubtasks; subtaskIndex++) {
      runSubtask(
          repository,
          Collections.singletonList(attribute),
          mergedState,
          result::add,
          numRestoredSubtasks,
          subtaskIndex,
          -1,
          partitionElements
              .entrySet()
              .stream()
              .sorted(Comparator.comparingInt(Map.Entry::getKey))
              .map(Map.Entry::getValue)
              .collect(Collectors.toList()));
    }

    final List<String> expectedKeys =
        emittedElements.stream().map(StreamElement::getKey).sorted().collect(Collectors.toList());
    final List<String> receivedKeys =
        result.stream().map(StreamElement::getKey).sorted().collect(Collectors.toList());
    Assertions.assertEquals(expectedKeys.size(), receivedKeys.size());
    Assertions.assertEquals(expectedKeys, receivedKeys);
  }

  private OperatorSubtaskState runSubtask(
      Repository repository,
      List<AttributeDescriptor<?>> attributeDescriptors,
      @Nullable OperatorSubtaskState state,
      Consumer<StreamElement> outputConsumer,
      int numSubtasks,
      int subtaskIndex,
      int expectedElements,
      List<List<StreamElement>> partitions)
      throws Exception {
    final CountDownLatch finished = new CountDownLatch(1);
    final CountDownLatch elementsReceived =
        new CountDownLatch(expectedElements > 0 ? expectedElements : Integer.MAX_VALUE);
    final CountDownLatch snapshotAcquiredCheckpointLock = new CountDownLatch(1);
    final BatchLogSourceFunction<StreamElement> sourceFunction =
        new BatchLogSourceFunction<StreamElement>(
            repository.asFactory(),
            attributeDescriptors,
            FlinkDataOperator.newBatchLogOptions().build(),
            ResultExtractor.identity()) {

          @Override
          BatchLogReader createLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
            final DirectDataOperator direct =
                repository.getOrCreateOperator(DirectDataOperator.class);
            final ListBatchReader reader =
                ListBatchReader.ofPartitioned(direct.getContext(), partitions);
            return OffsetTrackingBatchLogReader.of(reader);
          }

          @Override
          BatchLogObserver wrapSourceObserver(BatchLogObserver sourceObserver) {
            return new BatchLogReaders.ForwardingBatchLogObserver(sourceObserver) {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                // In first iteration, if we've consumed all elements, wait for snapshot to acquire
                // snapshot lock before processing the next element. This is to ensure determinism
                // for testing purpose.
                if (elementsReceived.getCount() == 0) {
                  try {
                    snapshotAcquiredCheckpointLock.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }
                return super.onNext(element, context);
              }
            };
          }

          @Override
          void finishAndMarkAsIdle(SourceContext<?> sourceContext) {
            finished.countDown();
            super.finishAndMarkAsIdle(sourceContext);
          }
        };

    final AbstractStreamOperatorTestHarness<StreamElement> testHarness =
        createTestHarness(sourceFunction, numSubtasks, subtaskIndex);
    if (state == null) {
      testHarness.initializeEmptyState();
    } else {
      testHarness.initializeState(state);
    }
    testHarness.open();
    final TestSourceContext<StreamElement> context =
        new TestSourceContext<StreamElement>() {

          @Override
          public void collect(StreamElement element) {
            if (elementsReceived.getCount() > 0) {
              outputConsumer.accept(element);
              elementsReceived.countDown();
            }
          }

          @Override
          public void collectWithTimestamp(StreamElement element, long timestamp) {
            collect(element);
          }
        };

    final CheckedThread runThread =
        new CheckedThread("run") {

          @Override
          public void go() throws Exception {
            sourceFunction.run(context);
          }
        };
    runThread.start();
    sourceFunction.awaitRunning();

    if (expectedElements > 0) {
      elementsReceived.await();
    } else {
      finished.await();
    }

    final OperatorSubtaskState snapshot;
    synchronized (context.getCheckpointLock()) {
      snapshotAcquiredCheckpointLock.countDown();
      snapshot = testHarness.snapshot(0, 0L);
    }

    sourceFunction.cancel();
    testHarness.close();

    // Make sure run thread finishes normally.
    runThread.sync();
    return snapshot;
  }
}
