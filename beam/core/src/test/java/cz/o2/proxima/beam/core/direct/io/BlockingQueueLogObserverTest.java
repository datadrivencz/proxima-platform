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
package cz.o2.proxima.beam.core.direct.io;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.ReplicationRunner;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Test {@link BlockingQueueLogObserver}. */
public class BlockingQueueLogObserverTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Test
  public void testWatermarkUpdate() throws InterruptedException {
    testWithStartingWatermark(Long.MIN_VALUE);
    testWithStartingWatermark(0);
  }

  @Test
  public void testMaxWatermarkWhenOnCompleted() {
    final BlockingQueueLogObserver.CommitLogObserver observer =
        BlockingQueueLogObserver.createCommitLogObserver("name", Long.MIN_VALUE);
    assertEquals(Long.MIN_VALUE, observer.getWatermark());
    observer.onCompleted();
    assertFalse(observer.peekElement());
    assertEquals(Long.MAX_VALUE, observer.getWatermark());
  }

  @Test
  public void testPeekBlocking() {
    final BlockingQueueLogObserver.CommitLogObserver observer =
        BlockingQueueLogObserver.createCommitLogObserver("name", Long.MIN_VALUE);
    long now = System.currentTimeMillis();
    assertFalse(observer.peekElement());
    observer.onNext(newElement(now), newContext(now));
    assertTrue(observer.peekElement());
  }

  @Test(timeout = 10000)
  public void testCapacityFull() throws InterruptedException {
    final BlockingQueueLogObserver.CommitLogObserver observer =
        BlockingQueueLogObserver.createCommitLogObserver("name", Long.MIN_VALUE);
    long now = System.currentTimeMillis();
    int numElements = 1000;
    CountDownLatch latch = new CountDownLatch(100);
    Executor executor = Executors.newSingleThreadExecutor();
    executor.execute(
        () -> {
          for (int i = 0; i < numElements; i++) {
            latch.countDown();
            observer.onNext(newElement(now + i), newContext(now));
          }
        });
    latch.await();
    List<StreamElement> result = new ArrayList<>();
    while (result.size() != numElements) {
      result.add(observer.takeBlocking());
    }
    assertEquals(numElements, result.size());
  }

  @Test(timeout = 10000)
  public void testCapacityFullWithCancel() throws InterruptedException {
    final int capacity = 100;
    final CountDownLatch elements = new CountDownLatch(capacity + 1);
    final BlockingQueueLogObserver.BatchLogObserver observer =
        new BlockingQueueLogObserver.BatchLogObserver(
            "name", Long.MAX_VALUE, Long.MIN_VALUE, capacity) {
          @Override
          public boolean onNext(
              StreamElement element,
              cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext context) {

            elements.countDown();
            return super.onNext(element, context);
          }
        };
    long now = System.currentTimeMillis();
    int numElements = 1000;
    try (DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class)) {
      ReplicationRunner.runAttributeReplicas(direct);
      OnlineAttributeWriter writer = Optionals.get(direct.getWriter(status));
      for (int i = 0; i < numElements; i++) {
        writer.write(newElement("key" + i, now + i), CommitCallback.noop());
      }
      BatchLogReader reader = Optionals.get(direct.getBatchLogReader(status));
      ObserveHandle handle =
          reader.observe(reader.getPartitions(), Collections.singletonList(status), observer);
      // wait till queue is full
      elements.await();
      handle.close();
      // must not time out
    }
  }

  @Test(timeout = 10000)
  public void testCapacityEmpty() throws InterruptedException, ExecutionException {
    final BlockingQueueLogObserver.CommitLogObserver observer =
        BlockingQueueLogObserver.createCommitLogObserver("name", Long.MIN_VALUE);
    long now = System.currentTimeMillis();
    int numElements = 1000;
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Void> future =
        executor.submit(
            () -> {
              List<StreamElement> result = new ArrayList<>();
              while (result.size() != numElements) {
                ExceptionUtils.unchecked(
                    () -> {
                      StreamElement element = observer.takeBlocking(10, TimeUnit.MILLISECONDS);
                      if (element != null) {
                        result.add(element);
                      }
                      latch.countDown();
                    });
              }
              assertEquals(numElements, result.size());
              return null;
            });
    latch.await();
    for (int i = 0; i < numElements; i++) {
      observer.onNext(newElement(now + i), newContext(now));
    }
    assertNull(future.get());
  }

  void testWithStartingWatermark(long startingWatermark) throws InterruptedException {
    final BlockingQueueLogObserver.CommitLogObserver observer =
        BlockingQueueLogObserver.createCommitLogObserver("name", startingWatermark);
    long now = System.currentTimeMillis();
    observer.onNext(newElement(now), newContext(now));
    assertEquals(startingWatermark, observer.getWatermark());
    observer.takeBlocking();
    assertEquals(now, observer.getWatermark());
  }

  private static OnNextContext newContext(long watermark) {
    return new OnNextContext() {
      @Override
      public OffsetCommitter committer() {
        return (succ, exc) -> {};
      }

      @Override
      public Partition getPartition() {
        return Partition.of(0);
      }

      @Override
      public long getWatermark() {
        return watermark;
      }

      @Override
      public Offset getOffset() {
        return mock(Offset.class);
      }
    };
  }

  private StreamElement newElement(long stamp) {
    return newElement("key", stamp);
  }

  private StreamElement newElement(String key, long stamp) {
    return StreamElement.upsert(
        gateway,
        status,
        UUID.randomUUID().toString(),
        key,
        status.getName(),
        stamp,
        new byte[] {1});
  }
}
