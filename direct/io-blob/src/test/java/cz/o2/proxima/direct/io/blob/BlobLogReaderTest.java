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
package cz.o2.proxima.direct.io.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogReader.Factory;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.io.blob.TestBlobStorageAccessor.BlobReader;
import cz.o2.proxima.direct.io.blob.TestBlobStorageAccessor.BlobWriter;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link BlobLogReader}. */
public class BlobLogReaderTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  private final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final Context context = repo.getOrCreateOperator(DirectDataOperator.class).getContext();

  private TestBlobStorageAccessor accessor;

  @Before
  public void setUp() {
    accessor =
        new TestBlobStorageAccessor(
            TestUtils.createTestFamily(gateway, URI.create("blob-test://bucket/path")));
  }

  @Test
  public void testListPartitions() throws InterruptedException {
    List<Pair<Long, Long>> stamps =
        Lists.newArrayList(
            Pair.of(1234566000000L, 1234566000000L + 3_600_000L),
            Pair.of(1234566000000L + 3_600_000L, (1234566000000L + 2 * 3_600_000L)));
    writePartitions(
        stamps.stream().map(p -> (p.getSecond() + p.getFirst()) / 2).collect(Collectors.toList()));
    BlobReader reader = accessor.new BlobReader(context);
    List<Partition> partitions = reader.getPartitions();
    assertEquals("Expected single partitions, got " + partitions, 1, partitions.size());
    assertEquals((long) stamps.get(0).getFirst(), partitions.get(0).getMinTimestamp());
    assertEquals((long) stamps.get(1).getSecond(), partitions.get(0).getMaxTimestamp());
  }

  @Test
  public void testListPartitionsWithMaxTimeSpan() throws InterruptedException {
    accessor.setCfg(BlobStorageAccessor.PARTITION_MAX_TIME_SPAN_MS, 1000);
    List<Pair<Long, Long>> stamps =
        Lists.newArrayList(
            Pair.of(1234566000000L, 1234566000000L + 3_600_000L),
            Pair.of(1234566000000L + 3_600_000L, (1234566000000L + 2 * 3_600_000L)));
    writePartitions(
        stamps.stream().map(p -> (p.getSecond() + p.getFirst()) / 2).collect(Collectors.toList()));
    BlobReader reader = accessor.new BlobReader(context);
    List<Partition> partitions = reader.getPartitions();
    partitions.sort(Comparator.comparing(Partition::getMaxTimestamp));
    assertEquals("Expected two partitions, got " + partitions, 2, partitions.size());
    assertEquals((long) stamps.get(0).getFirst(), partitions.get(0).getMinTimestamp());
    assertEquals((long) stamps.get(1).getSecond(), partitions.get(1).getMaxTimestamp());
  }

  @Test
  public void testObservePartitions() throws InterruptedException {
    List<Pair<Long, Long>> stamps =
        Lists.newArrayList(
            Pair.of(1234566000000L, 1234566000000L + 3_600_000L),
            Pair.of(1234566000000L + 3_600_000L, (1234566000000L + 2 * 3_600_000L)));
    writePartitions(
        stamps.stream().map(p -> (p.getSecond() + p.getFirst()) / 2).collect(Collectors.toList()));
    BlobReader reader = accessor.new BlobReader(context);
    List<StreamElement> observed = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(status),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            observed.add(element);
            return true;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(2, observed.size());
  }

  @Test
  public void testObservePartitionsOnNextReturnFalse() throws InterruptedException {
    List<Pair<Long, Long>> stamps =
        Lists.newArrayList(
            Pair.of(1234566000000L, 1234566000000L + 3_600_000L),
            Pair.of(1234566000000L + 3_600_000L, (1234566000000L + 2 * 3_600_000L)));
    writePartitions(
        stamps.stream().map(p -> (p.getSecond() + p.getFirst()) / 2).collect(Collectors.toList()));
    BlobReader reader = accessor.new BlobReader(context);
    List<StreamElement> observed = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(status),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            observed.add(element);
            return false;
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        });
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(1, observed.size());
  }

  @Test(timeout = 10_000)
  public void testObservePartitionsCancelled() throws InterruptedException {
    List<Pair<Long, Long>> stamps =
        Lists.newArrayList(
            Pair.of(1234566000000L, 1234566000000L + 3_600_000L),
            Pair.of(1234566000000L + 3_600_000L, (1234566000000L + 2 * 3_600_000L)));
    writePartitions(
        stamps.stream().map(p -> (p.getSecond() + p.getFirst()) / 2).collect(Collectors.toList()));
    BlobReader reader = accessor.new BlobReader(context);
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observe(
            reader.getPartitions(),
            Collections.singletonList(status),
            new BatchLogObserver() {
              @Override
              public boolean onNext(StreamElement element) {
                return true;
              }

              @Override
              public void onCancelled() {
                latch.countDown();
              }

              @Override
              public void onCompleted() {
                fail("onCompleted should not have been called");
              }
            });
    handle.close();
    latch.await();
  }

  @Test
  public void testObservePartitionsReaderException() throws InterruptedException {
    writePartitions(Arrays.asList(0L, 1L, 2L));
    final BlobReader reader = accessor.new BlobReader(context);
    final CountDownLatch errorReceived = new CountDownLatch(1);
    reader.observe(
        reader.getPartitions(),
        Lists.newArrayList(status),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element) {
            throw new UnsupportedOperationException("Failure.");
          }

          @Override
          public boolean onError(Throwable error) {
            errorReceived.countDown();
            return false;
          }

          @Override
          public void onCompleted() {
            // Noop.
          }
        });
    assertTrue(errorReceived.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testObservePartitionsReaderExceptionWithRetry() throws InterruptedException {
    writePartitions(Arrays.asList(0L, 1L, 2L));
    final BlobReader reader = accessor.new BlobReader(context);
    final CountDownLatch errorReceived = new CountDownLatch(10);
    reader.observe(
        reader.getPartitions(),
        Lists.newArrayList(status),
        new BatchLogObserver() {

          @Override
          public boolean onNext(StreamElement element) {
            throw new UnsupportedOperationException("Failure.");
          }

          @Override
          public boolean onError(Throwable error) {
            errorReceived.countDown();
            // Retry until zero.
            return errorReceived.getCount() > 0;
          }

          @Override
          public void onCompleted() {
            // Noop.
          }
        });
    assertTrue(errorReceived.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    BlobReader reader = accessor.new BlobReader(context);
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(
        reader.getAccessor().getUri(), ((BlobReader) factory.apply(repo)).getAccessor().getUri());
  }

  private void writePartitions(List<Long> stamps) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    BlobWriter writer = accessor.new BlobWriter(context);
    stamps.stream()
        .map(
            stamp ->
                StreamElement.upsert(
                    gateway,
                    status,
                    UUID.randomUUID().toString(),
                    "key",
                    status.getName(),
                    stamp,
                    new byte[] {1}))
        .forEach(
            update ->
                writer.write(
                    update,
                    Long.MIN_VALUE,
                    (succ, exc) -> {
                      latch.countDown();
                    }));
    writer.updateWatermark(Long.MAX_VALUE);
    assertTrue(latch.await(5, TimeUnit.SECONDS));
  }
}
