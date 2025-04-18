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
package cz.o2.proxima.direct.core.storage;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.scheme.SerializationException;
import cz.o2.proxima.core.storage.AccessType;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.KeyAttributePartitioner;
import cz.o2.proxima.core.storage.commitlog.Partitioner;
import cz.o2.proxima.core.storage.commitlog.Partitioners;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.LogObserverUtils;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.ObserveHandleUtils;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader.Listing;
import cz.o2.proxima.direct.core.storage.InMemStorage.ConsumedOffset;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/** Test suite for {@link InMemStorage}. */
public class InMemStorageTest implements Serializable {

  final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  final EntityDescriptor entity = repo.getEntity("dummy");
  final AttributeDescriptor<?> data = entity.getAttribute("data");

  final AttributeDescriptor<?> wildcard = entity.getAttribute("wildcard.*");

  @Test(timeout = 10000)
  public void testObservePartitions() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    ObserveHandle handle =
        reader.observePartitions(
            reader.getPartitions(),
            new CommitLogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                assertEquals(1, context.partitions().size());
                latch.set(new CountDownLatch(1));
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                assertEquals(0, context.getPartition().getId());
                assertEquals("key", element.getKey());
                context.confirm();
                latch.get().countDown();
                return false;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });

    assertTrue(ObserveHandleUtils.isAtHead(handle, reader));

    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    latch.get().await();
  }

  @Test(timeout = 10000)
  public void testObservePartitionsEmpty() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    ObserveHandle handle =
        reader.observePartitions(
            reader.getPartitions(),
            Position.OLDEST,
            true,
            new CommitLogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                assertEquals(1, context.partitions().size());
                latch.set(new CountDownLatch(1));
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public void onCompleted() {
                latch.get().countDown();
              }
            });
    assertTrue(ObserveHandleUtils.isAtHead(handle, reader));
    latch.get().await();
  }

  @Test(timeout = 10000)
  public void testObservePartitionsWithSamePath() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test1")));
    DataAccessor accessor2 =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test2")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    CountDownLatch latch = new CountDownLatch(1);
    StreamElement element =
        StreamElement.upsert(
            entity,
            data,
            UUID.randomUUID().toString(),
            "key",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.online().write(element, (succ, exc) -> {});
    accessor2
        .getWriter(direct.getContext())
        .orElseThrow(() -> new IllegalStateException("Missing writer2"))
        .online()
        .write(element, (succ, exc) -> {});
    AtomicInteger count = new AtomicInteger();
    reader.observePartitions(
        reader.getPartitions(),
        Position.OLDEST,
        true,
        new CommitLogObserver() {

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            assertEquals("key", element.getKey());
            context.confirm();
            count.incrementAndGet();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
    assertEquals(1, count.get());
  }

  @Test(timeout = 10000)
  public void testObserveBatch() throws InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    BatchLogReader reader = Optionals.get(accessor.getBatchLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    CountDownLatch latch = new CountDownLatch(1);
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(data),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            assertEquals("key", element.getKey());
            latch.countDown();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testObserveBatchEmpty() throws InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    BatchLogReader reader = Optionals.get(accessor.getBatchLogReader(direct.getContext()));
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(data),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
  }

  @Test(timeout = 10000)
  public void testObserveBatchWithSamePath() throws InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test1")));
    DataAccessor accessor2 =
        storage.createAccessor(direct, createFamilyDescriptor(URI.create("inmem://test2")));
    BatchLogReader reader = Optionals.get(accessor.getBatchLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    CountDownLatch latch = new CountDownLatch(1);
    StreamElement element =
        StreamElement.upsert(
            entity,
            data,
            UUID.randomUUID().toString(),
            "key",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.online().write(element, (succ, exc) -> {});
    accessor2
        .getWriter(direct.getContext())
        .orElseThrow(() -> new IllegalStateException("Missing writer2"))
        .online()
        .write(element, (succ, exc) -> {});
    AtomicInteger count = new AtomicInteger();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(data),
        new BatchLogObserver() {

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            assertEquals("key", element.getKey());
            count.incrementAndGet();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        });
    latch.await();
    assertEquals(1, count.get());
  }

  @Test(timeout = 10000)
  public void testObserveCancel() {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    List<Byte> received = new ArrayList<>();
    AtomicBoolean cancelled = new AtomicBoolean();
    ObserveHandle handle =
        reader.observePartitions(
            reader.getPartitions(),
            new CommitLogObserver() {

              @Override
              public void onRepartition(CommitLogObserver.OnRepartitionContext context) {
                assertEquals(1, context.partitions().size());
              }

              @Override
              public boolean onNext(
                  StreamElement element, CommitLogObserver.OnNextContext context) {

                assertEquals(0, context.getPartition().getId());
                assertEquals("key", element.getKey());
                context.confirm();
                received.add(element.getValue()[0]);
                return false;
              }

              @Override
              public void onCancelled() {
                cancelled.set(true);
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });

    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1}),
            (succ, exc) -> {});
    List<Offset> offsets = handle.getCurrentOffsets();
    assertEquals(1, offsets.size());
    assertEquals(Collections.singletonList((byte) 1), received);
    handle.close();
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {2}),
            (succ, exc) -> {});
    assertEquals(Collections.singletonList((byte) 1), received);
    assertTrue(cancelled.get());
  }

  @Test(timeout = 10000)
  public void testObserveOffsets() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    List<Byte> received = new ArrayList<>();
    CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public void onRepartition(CommitLogObserver.OnRepartitionContext context) {
            assertEquals(1, context.partitions().size());
          }

          @Override
          public boolean onNext(StreamElement element, CommitLogObserver.OnNextContext context) {
            assertEquals(0, context.getPartition().getId());
            received.add(element.getValue()[0]);
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };

    ObserveHandle handle = reader.observePartitions(reader.getPartitions(), observer);

    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1}),
            (succ, exc) -> {});

    List<Offset> offsets = handle.getCurrentOffsets();
    assertEquals(1, offsets.size());
    assertTrue(offsets.get(0).getWatermark() > 0);
    assertEquals(Collections.singletonList((byte) 1), received);
    handle.close();
    handle = reader.observeBulkOffsets(offsets, observer);
    handle.waitUntilReady();
    offsets = handle.getCurrentOffsets();
    assertEquals(1, offsets.size());
    assertTrue(
        "Expected positive watermark, got " + offsets.get(0).getWatermark(),
        offsets.get(0).getWatermark() > 0);
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                UUID.randomUUID().toString(),
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {2}),
            (succ, exc) -> {});

    assertEquals(Arrays.asList((byte) 1, (byte) 1), received);
    assertEquals(
        0, ((ConsumedOffset) handle.getCurrentOffsets().get(0)).getConsumedKeyAttr().size());
    handle.close();
  }

  @Test(timeout = 10000)
  public void testObserveOffsetsEmpty() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    CountDownLatch latch = new CountDownLatch(1);
    List<StreamElement> read = new ArrayList<>();
    CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public void onRepartition(CommitLogObserver.OnRepartitionContext context) {
            assertEquals(1, context.partitions().size());
          }

          @Override
          public boolean onNext(StreamElement element, CommitLogObserver.OnNextContext context) {
            read.add(element);
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };

    Map<Partition, Offset> offsets = reader.fetchOffsets(Position.OLDEST, reader.getPartitions());
    try (ObserveHandle handle = reader.observeBulkOffsets(offsets.values(), true, observer)) {
      latch.await();
    }
  }

  @Test(timeout = 10000)
  public void testObserveBulkPartitionsEmpty() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    CountDownLatch latch = new CountDownLatch(1);
    CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public void onRepartition(CommitLogObserver.OnRepartitionContext context) {
            assertEquals(1, context.partitions().size());
          }

          @Override
          public boolean onNext(StreamElement element, CommitLogObserver.OnNextContext context) {
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };

    try (ObserveHandle handle =
        reader.observeBulkPartitions(reader.getPartitions(), Position.OLDEST, true, observer)) {
      latch.await();
    }
  }

  @Test(timeout = 10000)
  public void testFetchOffsetsSinglePartition() throws InterruptedException {
    testFetchOffsets(1);
  }

  @Test(timeout = 10000)
  public void testFetchOffsetsMultiplePartitions() throws InterruptedException {
    testFetchOffsets(3);
  }

  private void testFetchOffsets(int numPartitions) throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    long now = System.currentTimeMillis();
    List<StreamElement> updates = new ArrayList<>();
    for (int i = 0; i < 2 * numPartitions; i++) {
      updates.add(
          StreamElement.upsert(
              entity,
              data,
              UUID.randomUUID().toString(),
              "key" + (i + 1),
              data.getName(),
              now + i,
              new byte[] {1, 2, 3}));
    }
    updates.forEach(el -> writer.online().write(el, (succ, exc) -> {}));
    Map<Partition, Offset> startingOffsets =
        reader.fetchOffsets(Position.OLDEST, reader.getPartitions());
    assertEquals(numPartitions, startingOffsets.size());
    List<StreamElement> elements = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch startLatch = new CountDownLatch(1);
    CommitLogObserver observer =
        LogObserverUtils.toList(
            elements,
            ign -> latch.countDown(),
            el -> {
              ExceptionUtils.ignoringInterrupted(startLatch::await);
              return true;
            });
    ObserveHandle handle = reader.observeBulkOffsets(startingOffsets.values(), true, observer);
    assertFalse(ObserveHandleUtils.isAtHead(handle, reader));
    startLatch.countDown();
    latch.await();
    assertEquals(2 * numPartitions, elements.size());
    assertTrue(ObserveHandleUtils.isAtHead(handle, reader));
    assertEquals(
        IntStream.range(0, 2 * numPartitions)
            .mapToObj(i -> "key" + (i + 1))
            .collect(Collectors.toList()),
        elements.stream().map(StreamElement::getKey).collect(Collectors.toList()));
    elements.clear();
    Map<Partition, Offset> endOffsets =
        reader.fetchOffsets(Position.NEWEST, reader.getPartitions());
    assertEquals(numPartitions, endOffsets.size());
    CountDownLatch latch2 = new CountDownLatch(1);
    observer = LogObserverUtils.toList(elements, ign -> latch2.countDown());
    reader.observeBulkOffsets(endOffsets.values(), true, observer);
    latch2.await();
    assertEquals(numPartitions, elements.size());
    assertEquals(
        IntStream.range(numPartitions, 2 * numPartitions)
            .mapToObj(i -> "key" + (i + 1))
            .collect(Collectors.toList()),
        elements.stream().map(StreamElement::getKey).collect(Collectors.toList()));
  }

  @Test
  public void testObserveWithEndOfTime() throws InterruptedException {
    URI uri = URI.create("inmem:///inmemstoragetest");
    InMemStorage storage = new InMemStorage();
    InMemStorage.setWatermarkEstimatorFactory(
        uri,
        (stamp, name, offset) ->
            new WatermarkEstimator() {

              {
                Preconditions.checkArgument(offset != null);
              }

              @Override
              public long getWatermark() {
                return Watermarks.MAX_WATERMARK - InMemStorage.getBoundedOutOfOrderness();
              }

              @Override
              public void setMinWatermark(long minWatermark) {}
            });
    DataAccessor accessor = storage.createAccessor(direct, createFamilyDescriptor(uri));
    CommitLogReader reader =
        accessor
            .getCommitLogReader(direct.getContext())
            .orElseThrow(() -> new IllegalStateException("Missing commit log reader"));
    CountDownLatch completed = new CountDownLatch(1);
    reader.observe(
        "observer",
        new CommitLogObserver() {

          @Override
          public void onCompleted() {
            completed.countDown();
          }

          @Override
          public boolean onError(Throwable error) {
            return false;
          }

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            return false;
          }
        });
    assertTrue(completed.await(3, TimeUnit.SECONDS));
  }

  @Test(timeout = 1000L)
  public void testObserveError() throws InterruptedException {
    final URI uri = URI.create("inmem:///inmemstoragetest");
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor = storage.createAccessor(direct, createFamilyDescriptor(uri));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    final AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    final CountDownLatch failingObserverErrorReceived = new CountDownLatch(1);
    final AtomicInteger failingObserverMessages = new AtomicInteger(0);
    reader.observe(
        "failing-observer",
        new CommitLogObserver() {

          @Override
          public void onCompleted() {
            throw new UnsupportedOperationException("This should never happen.");
          }

          @Override
          public boolean onError(Throwable error) {
            failingObserverErrorReceived.countDown();
            return false;
          }

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            failingObserverMessages.incrementAndGet();
            throw new RuntimeException("Test exception.");
          }
        });

    final int numElements = 100;
    final CountDownLatch successObserverAllReceived = new CountDownLatch(numElements);
    reader.observe(
        "success-observer",
        new CommitLogObserver() {

          @Override
          public void onCompleted() {
            throw new UnsupportedOperationException("This should never happen.");
          }

          @Override
          public boolean onError(Throwable error) {
            return false;
          }

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            successObserverAllReceived.countDown();
            return true;
          }
        });

    for (int i = 0; i < numElements; i++) {
      writer
          .online()
          .write(
              StreamElement.upsert(
                  entity,
                  data,
                  UUID.randomUUID().toString(),
                  "key_" + i,
                  data.getName(),
                  System.currentTimeMillis(),
                  new byte[] {2}),
              (success, error) -> {});
    }
    failingObserverErrorReceived.await();
    successObserverAllReceived.await();
    assertEquals(1, failingObserverMessages.get());
  }

  @Test
  public void testObserveMultiplePartitions() throws InterruptedException {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    final AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    final int numElements = 1_000;
    final ConcurrentMap<Partition, Long> partitionHistogram = new ConcurrentHashMap<>();
    final CountDownLatch elementsReceived = new CountDownLatch(numElements);
    // Start observer.
    final ObserveHandle observeHandle1 =
        reader.observePartitions(
            reader.getPartitions().subList(0, numPartitions - 1),
            createObserver(numPartitions - 1, partitionHistogram, elementsReceived));
    final ObserveHandle observeHandle2 =
        reader.observePartitions(
            reader.getPartitions().subList(numPartitions - 1, numPartitions),
            createObserver(1, partitionHistogram, elementsReceived));
    // Write data.
    for (int i = 0; i < numElements; i++) {
      writer
          .online()
          .write(
              StreamElement.upsert(
                  entity,
                  data,
                  UUID.randomUUID().toString(),
                  "key_" + i,
                  data.getName(),
                  System.currentTimeMillis(),
                  new byte[] {1, 2, 3}),
              CommitCallback.noop());
    }
    // Wait for all elements to be received.
    elementsReceived.await();

    assertEquals(3, partitionHistogram.size());
    assertEquals(2, observeHandle1.getCurrentOffsets().size());
  }

  private static CommitLogObserver createObserver(
      int expectedPartitions,
      ConcurrentMap<Partition, Long> partitionHistogram,
      CountDownLatch elementsReceived) {
    return new CommitLogObserver() {

      @Override
      public void onRepartition(OnRepartitionContext context) {
        assertEquals(expectedPartitions, context.partitions().size());
      }

      @Override
      public boolean onNext(StreamElement element, OnNextContext context) {
        partitionHistogram.merge(context.getPartition(), 1L, Long::sum);
        context.confirm();
        elementsReceived.countDown();
        return elementsReceived.getCount() > 0;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }
    };
  }

  @Test(timeout = 10000)
  public void testObserveSinglePartitionOutOfMultiplePartitions() throws InterruptedException {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    final AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    final int numElements = 999;
    final ConcurrentMap<Partition, Long> partitionHistogram = new ConcurrentHashMap<>();
    // Elements are uniformly distributed between partitions.
    final CountDownLatch elementsReceived = new CountDownLatch(numElements / numPartitions);
    // Start observer.
    final List<Partition> consumedPartitions = reader.getPartitions().subList(0, 1);
    final ObserveHandle observeHandle =
        reader.observePartitions(
            reader.getPartitions().subList(0, 1),
            new CommitLogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                assertEquals(1, context.partitions().size());
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                partitionHistogram.merge(context.getPartition(), 1L, Long::sum);
                context.confirm();
                elementsReceived.countDown();
                return elementsReceived.getCount() > 0;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });
    // Write data.
    final Partitioner partitioner = new KeyAttributePartitioner();
    final Map<Partition, Long> expectedPartitionHistogram = new HashMap<>();
    for (int i = 0; i < numElements; i++) {
      final StreamElement element =
          StreamElement.upsert(
              entity,
              data,
              UUID.randomUUID().toString(),
              "key_" + i,
              data.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2, 3});
      expectedPartitionHistogram.merge(
          Partition.of(Partitioners.getTruncatedPartitionId(partitioner, element, numPartitions)),
          1L,
          Long::sum);
      writer.online().write(element, CommitCallback.noop());
    }
    assertEquals(3, expectedPartitionHistogram.size());

    // Wait for all elements to be received.
    elementsReceived.await();

    assertEquals(1, partitionHistogram.size());
    assertEquals(1, observeHandle.getCurrentOffsets().size());
    assertEquals(
        expectedPartitionHistogram.get(Iterables.getOnlyElement(consumedPartitions)),
        partitionHistogram.get(Iterables.getOnlyElement(consumedPartitions)));
  }

  @Test
  public void testRandomAccessReaderWithMultiplePartitions() {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    assertFalse(accessor.getRandomAccessReader(direct.getContext()).isPresent());
  }

  @Test
  public void testBatchLogReaderWithMultiplePartitions() {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    assertFalse(accessor.getBatchLogReader(direct.getContext()).isPresent());
  }

  @Test
  public void testCachedViewWithMultiplePartitions() {
    final int numPartitions = 3;
    final InMemStorage storage = new InMemStorage();
    final DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///test"), numPartitions));
    Optional<CachedView> maybeView = accessor.getCachedView(direct.getContext());
    assertTrue(maybeView.isPresent());
    CachedView view = maybeView.get();
    assertEquals(3, view.getPartitions().size());
    view.assign(view.getPartitions());
    StreamElement element =
        StreamElement.upsert(
            entity,
            data,
            UUID.randomUUID().toString(),
            "key",
            data.getName(),
            System.currentTimeMillis(),
            new byte[0]);
    view.write(element, (succ, exc) -> assertTrue(succ));
    Optional<? extends KeyValue<?>> written = view.get("key", data);
    assertTrue(written.isPresent());
    assertEquals("key", written.get().getKey());
  }

  @Test(timeout = 10000)
  public void testReadWriteSequentialIds() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(direct.getContext()));
    AttributeWriterBase writer = Optionals.get(accessor.getWriter(direct.getContext()));
    List<StreamElement> result = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    CommitLogObserver observer =
        LogObserverUtils.toList(
            result,
            Assert::assertTrue,
            elem -> {
              latch.countDown();
              return true;
            });
    reader.observe("test", observer);
    writer
        .online()
        .write(
            StreamElement.upsert(
                entity,
                data,
                1L,
                "key",
                data.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            (succ, exc) -> {});
    latch.await();
    assertEquals(1, result.size());
    assertTrue(result.get(0).hasSequentialId());
    assertEquals(1L, result.get(0).getSequentialId());
  }

  @Test
  public void testConsumedOffsetExternalizerToJson() throws JsonProcessingException {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();

    String json =
        externalizer.toJson(
            new ConsumedOffset(Partition.of(1), new HashSet<>(Arrays.asList("a", "b")), 1000L));

    HashMap<String, Object> jsonObject =
        new ObjectMapper().readValue(json, new TypeReference<>() {});

    assertEquals(1, jsonObject.get("partition"));
    assertEquals(Arrays.asList("a", "b"), jsonObject.get("offset"));
    assertEquals(1000, jsonObject.get("watermark"));
  }

  @Test
  public void testConsumedOffsetExternalizerFromJson() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    ConsumedOffset consumedOffset =
        new ConsumedOffset(Partition.of(1), new HashSet<>(Arrays.asList("a", "b")), 1000L);

    assertEquals(consumedOffset, externalizer.fromJson(externalizer.toJson(consumedOffset)));
  }

  @Test
  public void testOffsetExternalizerFromBytesWhenInvalidJson() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    assertThrows(SerializationException.class, () -> externalizer.fromJson(""));
  }

  @Test
  public void testConsumedOffsetExternalizerFromBytes() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    ConsumedOffset consumedOffset =
        new ConsumedOffset(Partition.of(1), new HashSet<>(Arrays.asList("a", "b")), 1000L);

    assertEquals(consumedOffset, externalizer.fromBytes(externalizer.toBytes(consumedOffset)));
  }

  @Test
  public void testOffsetExternalizerFromBytesWhenInvalidBytes() {
    InMemStorage.ConsumedOffsetExternalizer externalizer =
        new InMemStorage.ConsumedOffsetExternalizer();
    assertThrows(SerializationException.class, () -> externalizer.fromBytes(new byte[] {0x0}));
  }

  @Test(timeout = 10000)
  public void testScanWildcard() throws InterruptedException {
    InMemStorage storage = new InMemStorage();
    DataAccessor accessor =
        storage.createAccessor(
            direct, createFamilyDescriptor(URI.create("inmem:///inmemstoragetest")));
    RandomAccessReader reader = Optionals.get(accessor.getRandomAccessReader(direct.getContext()));
    OnlineAttributeWriter writer = Optionals.get(accessor.getWriter(direct.getContext())).online();
    String key = "key";
    Stream.of("prefix", "non-prefix")
        .forEach(
            name ->
                writer.write(
                    StreamElement.upsert(
                        entity,
                        wildcard,
                        UUID.randomUUID().toString(),
                        key,
                        wildcard.toAttributePrefix() + name,
                        System.currentTimeMillis(),
                        new byte[] {1}),
                    (succ, exc) -> {}));
    List<KeyValue<?>> kvs = new ArrayList<>();
    reader.scanWildcard(
        key,
        wildcard,
        reader.fetchOffset(Listing.ATTRIBUTE, wildcard.toAttributePrefix() + "prefi"),
        1,
        kvs::add);
    assertEquals(1, kvs.size());
    assertEquals(wildcard.toAttributePrefix() + "prefix", kvs.get(0).getAttribute());
  }

  private AttributeFamilyDescriptor createFamilyDescriptor(URI storageUri) {
    return createFamilyDescriptor(storageUri, 1);
  }

  private AttributeFamilyDescriptor createFamilyDescriptor(URI storageUri, int numPartitions) {
    final Map<String, Object> config = new HashMap<>();
    if (numPartitions > 1) {
      config.put(InMemStorage.NUM_PARTITIONS, numPartitions);
    }
    return AttributeFamilyDescriptor.newBuilder()
        .setName("test")
        .setEntity(entity)
        .setType(StorageType.PRIMARY)
        .setAccess(AccessType.from("commit-log"))
        .setStorageUri(storageUri)
        .setCfg(config)
        .build();
  }
}
