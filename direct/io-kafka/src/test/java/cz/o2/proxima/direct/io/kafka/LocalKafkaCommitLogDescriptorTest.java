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
package cz.o2.proxima.direct.io.kafka;

import static cz.o2.proxima.core.util.TestUtils.createTestFamily;
import static org.junit.Assert.*;

import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeDescriptorBase;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.core.storage.commitlog.Partitioner;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkEstimatorFactory;
import cz.o2.proxima.core.time.WatermarkIdlePolicy;
import cz.o2.proxima.core.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext;
import cz.o2.proxima.direct.core.commitlog.CommitLogObservers;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.direct.io.kafka.LocalKafkaCommitLogDescriptor.Accessor;
import cz.o2.proxima.direct.io.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaLogReader;
import cz.o2.proxima.direct.io.kafka.LocalKafkaCommitLogDescriptor.LocalKafkaWriter;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.internal.com.google.common.collect.Iterators;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@code LocalKafkaCommitLogDescriptorTest}. */
@Slf4j
public class LocalKafkaCommitLogDescriptorTest implements Serializable {

  private final transient Factory<ExecutorService> serviceFactory =
      () ->
          Executors.newCachedThreadPool(
              r -> {
                Thread t = new Thread(r);
                t.setUncaughtExceptionHandler((thr, exc) -> exc.printStackTrace(System.err));
                return t;
              });
  private final transient Repository repo =
      ConfigRepository.Builder.ofTest(ConfigFactory.empty()).build();
  private final DirectDataOperator direct =
      repo.getOrCreateOperator(
          DirectDataOperator.class, op -> op.withExecutorFactory(serviceFactory));

  private final AttributeDescriptorBase<byte[]> attr;
  private final AttributeDescriptorBase<byte[]> attrWildcard;
  private final AttributeDescriptorBase<String> strAttr;
  private final EntityDescriptor entity;
  private final String topic;
  private final URI storageUri;

  private LocalKafkaCommitLogDescriptor kafka;

  public LocalKafkaCommitLogDescriptorTest() throws Exception {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("attr")
            .setSchemeUri(new URI("bytes:///"))
            .build();

    this.attrWildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("wildcard.*")
            .setSchemeUri(new URI("bytes:///"))
            .build();

    this.strAttr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("strAttr")
            .setSchemeUri(new URI("string:///"))
            .build();

    this.entity =
        EntityDescriptor.newBuilder()
            .setName("entity")
            .addAttribute(attr)
            .addAttribute(attrWildcard)
            .addAttribute(strAttr)
            .build();

    this.topic = "topic";
    this.storageUri = new URI("kafka-test://dummy/" + topic);
  }

  @Before
  public void setUp() {
    kafka = new LocalKafkaCommitLogDescriptor();
  }

  @Test(timeout = 10000)
  public void testSinglePartitionWriteAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    AttributeFamilyDescriptor testFamily = createTestFamily(entity, storageUri, partitionsCfg(1));
    Accessor accessor = kafka.createAccessor(direct, testFamily);
    assertTrue(accessor.isAcceptable(testFamily));
    LocalKafkaWriter<?, ?> writer = accessor.newWriter();
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
    assertEquals(0, partition.partition());
    assertEquals("topic", partition.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      assertEquals("key#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
      tested++;
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunAfterWrite()
      throws InterruptedException {

    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> consumer;
    final CountDownLatch latch = new CountDownLatch(2);

    consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(2, polled.count());
    assertEquals(2, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());
    TopicPartition second = iterator.next();
    assertEquals(1, second.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
    }
    assertEquals(2, tested);
  }

  @Test
  public void testEmptyPoll() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    assertTrue(consumer.poll(Duration.ofMillis(100)).isEmpty());
  }

  @Test
  public void testWriteNull() {
    LocalKafkaCommitLogDescriptor.Accessor accessor;
    accessor = kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    OnlineAttributeWriter writer = Optionals.get(accessor.getWriter(context())).online();
    long now = 1234567890000L;
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), "key", attr.getName(), now, new byte[] {1}),
        (succ, exc) -> {});
    writer.write(
        StreamElement.delete(
            entity, attr, UUID.randomUUID().toString(), "key", attr.getName(), now + 1000),
        (succ, exc) -> {});
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(100));
    assertEquals(2, polled.count());
    int matched = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      if (r.timestamp() == now) {
        assertEquals(1, ((byte[]) r.value()).length);
        matched++;
      } else if (r.timestamp() == now + 1000) {
        assertNull(r.value());
        matched++;
      }
    }
    assertEquals(2, matched);
  }

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndConsumeBySingleConsumerRunBeforeWrite() {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(2, polled.count());
    assertEquals(2, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());
    TopicPartition second = iterator.next();
    assertEquals(1, second.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
    }
    assertEquals(2, tested);
  }

  @Test(timeout = 10000)
  public void testTwoPartitionsTwoWritesAndTwoReads() {

    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> consumer;

    consumer = accessor.createConsumerFactory().create();
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
        });

    polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
  }

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testTwoIndependentConsumers() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object>[] consumers =
        new KafkaConsumer[] {
          accessor.createConsumerFactory().create("dummy1"),
          accessor.createConsumerFactory().create("dummy2"),
        };
    final CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    for (KafkaConsumer<Object, Object> consumer : consumers) {
      ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
      assertEquals(1, polled.count());
      assertEquals(1, polled.partitions().size());
      TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
      assertEquals(0, partition.partition());
      assertEquals("topic", partition.topic());
      int tested = 0;
      for (ConsumerRecord<Object, Object> r : polled) {
        assertEquals("key#attr", r.key());
        assertEquals("topic", r.topic());
        assertArrayEquals(emptyValue(), (byte[]) r.value());
        tested++;
      }
      assertEquals(1, tested);
    }
  }

  @Test(timeout = 10000)
  public void testManualPartitionAssignment() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> consumer =
        accessor
            .createConsumerFactory()
            .create(Position.NEWEST, Collections.singletonList(getPartition(0)));
    final CountDownLatch latch = new CountDownLatch(2);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    Iterator<TopicPartition> iterator = polled.partitions().iterator();

    TopicPartition first = iterator.next();
    assertEquals(0, first.partition());

    assertEquals("topic", first.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      tested++;
      assertEquals("key" + tested + "#attr", r.key());
      assertEquals("topic", r.topic());
      assertArrayEquals(emptyValue(), (byte[]) r.value());
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 10000)
  public void testPollAfterWrite() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(2);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    KafkaConsumer<Object, Object> consumer =
        accessor
            .createConsumerFactory()
            .create(Position.NEWEST, Collections.singletonList(getPartition(0)));

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(100));
    assertTrue(polled.isEmpty());
  }

  @Test(timeout = 10000)
  public void testPollWithSeek() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(2);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    KafkaConsumer<Object, Object> consumer =
        accessor
            .createConsumerFactory()
            .create(Position.NEWEST, Collections.singletonList(getPartition(0)));
    consumer.seek(new TopicPartition("topic", 0), 1);

    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(100));
    assertEquals(1, polled.count());
  }

  @Test
  public void testTwoPartitionsTwoConsumersRebalance() {
    final String name = "consumer";
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(2)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final KafkaConsumer<Object, Object> c1 = accessor.createConsumerFactory().create(name);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    ConsumerRecords<Object, Object> poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(2, c1.assignment().size());
    assertEquals(1, poll.count());

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(
        ImmutableMap.of(
            new TopicPartition("topic", 0), new OffsetAndMetadata(1),
            new TopicPartition("topic", 1), new OffsetAndMetadata(1)));

    // create another consumer
    KafkaConsumer<Object, Object> c2 = accessor.createConsumerFactory().create(name);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c2.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());
    poll = c1.poll(Duration.ofMillis(1000));
    assertTrue(poll.isEmpty());

    // rebalanced
    assertEquals(1, c1.assignment().size());
    assertEquals(1, c2.assignment().size());
  }

  @Test
  public void testSinglePartitionTwoConsumersRebalance() {
    String name = "consumer";
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> c1 = accessor.createConsumerFactory().create(name);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    ConsumerRecords<Object, Object> poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(1, c1.assignment().size());
    assertEquals(1, poll.count());

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c1.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());

    // commit already processed offsets
    c1.commitSync(ImmutableMap.of(new TopicPartition("topic", 0), new OffsetAndMetadata(2)));

    // create another consumer
    KafkaConsumer<Object, Object> c2 = accessor.createConsumerFactory().create(name);

    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key2",
            attr.getName(),
            System.currentTimeMillis(),
            emptyValue()),
        (succ, exc) -> {});

    poll = c2.poll(Duration.ofMillis(1000));
    assertEquals(1, poll.count());
    poll = c1.poll(Duration.ofMillis(1000));
    assertTrue(poll.isEmpty());

    // not rebalanced (there are no free partitions)
    assertEquals(0, c1.assignment().size());
    assertEquals(1, c2.assignment().size());
  }

  @Test(timeout = 10000)
  public void testObserveSuccess() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                context.confirm();
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            });

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals(3, handle.getCommittedOffsets().size());
    long sum =
        handle.getCommittedOffsets().stream()
            .mapToLong(
                o -> {
                  TopicOffset tpo = (TopicOffset) o;
                  assertTrue(tpo.getOffset() <= 1);
                  return tpo.getOffset();
                })
            .sum();

    // single partition has committed one element
    assertEquals(1, sum);
  }

  @Test(timeout = 10000)
  public void testObserveCancelled() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final CountDownLatch latch = new CountDownLatch(1);
    final ObserveHandle handle =
        reader.observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public void onCancelled() {
                latch.countDown();
              }
            });

    handle.close();
    latch.await();
  }

  @Test(timeout = 10000)
  public void testObserveMovesWatermark() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    long now = System.currentTimeMillis();
    final UnaryFunction<Integer, StreamElement> update =
        pos ->
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key" + pos,
                attr.getName(),
                now + pos,
                new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(100);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();

    for (int i = 0; i < 100; i++) {
      writer.write(update.apply(i), (succ, e) -> {});
    }

    latch.await();

    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testEmptyPollMovesWatermark() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000")))));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    long now = System.currentTimeMillis();
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now + 2000,
            new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(2);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();

    // then we write single element
    writer.write(update, (succ, e) -> {});

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    // finally, last update to save watermark
    writer.write(update, (succ, e) -> {});

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testEmptyPollWithNoDataMovesWatermark() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000")))));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final long now = System.currentTimeMillis();
    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(30);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public void onIdle(OnIdleContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
              }
            })
        .waitUntilReady();

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 10000)
  public void testSlowPollMovesWatermarkSlowly() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000")))));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final long now = System.currentTimeMillis();
    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(30);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

              @Override
              public void onIdle(OnIdleContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
              }
            })
        .waitUntilReady();

    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);

    latch.await();

    // watermark should be moved
    assertTrue(watermark.get() > 0);
    assertTrue(watermark.get() < now * 10);
  }

  @Test(timeout = 100_000)
  public void testPollFromMoreConsumersThanPartitionsMovesWatermark() throws InterruptedException {

    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                and(
                    partitionsCfg(3),
                    cfg(
                        Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"),
                        Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, "1")))));
    int numObservers = 4;

    testPollFromNConsumersMovesWatermarkWithNoWrite(accessor, numObservers);
    writeData(accessor);
    testPollFromNConsumersMovesWatermark(accessor, numObservers);
  }

  @Test(timeout = 100_000)
  public void testPollFromManyMoreConsumersThanPartitionsMovesWatermark()
      throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                and(
                    partitionsCfg(3),
                    cfg(
                        Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000"),
                        Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, "1")))));

    int numObservers = 40;
    testPollFromNConsumersMovesWatermarkWithNoWrite(accessor, numObservers);
    writeData(accessor);
    testPollFromNConsumersMovesWatermark(accessor, numObservers);
    accessor.clear();
  }

  void writeData(Accessor accessor) {
    LocalKafkaWriter writer = accessor.newWriter();
    long now = 1500000000000L;
    writer.write(
        StreamElement.upsert(
            entity, attr, "key", UUID.randomUUID().toString(), attr.getName(), now, new byte[] {1}),
        (succ, exc) -> {});
  }

  void testPollFromNConsumersMovesWatermark(Accessor accessor, int numObservers)
      throws InterruptedException {
    testPollFromNConsumersMovesWatermark(accessor, numObservers, true);
  }

  void testPollFromNConsumersMovesWatermarkWithNoWrite(Accessor accessor, int numObservers)
      throws InterruptedException {
    testPollFromNConsumersMovesWatermark(accessor, numObservers, false);
  }

  void testPollFromNConsumersMovesWatermark(
      Accessor accessor, int numObservers, boolean expectMoved) throws InterruptedException {

    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(numObservers);
    Map<CommitLogObserver, Long> observerWatermarks = new ConcurrentHashMap<>();
    Map<Integer, Integer> idles = new ConcurrentHashMap<>();
    for (int i = 0; i < numObservers; i++) {
      int id = i;
      reader
          .observe(
              "test-" + expectMoved,
              Position.OLDEST,
              new CommitLogObserver() {

                boolean confirmedLatch = false;

                @Override
                public boolean onNext(StreamElement element, OnNextContext context) {
                  log.debug("Received element {} on watermark {}", element, context.getWatermark());
                  return true;
                }

                @Override
                public boolean onError(Throwable error) {
                  throw new RuntimeException(error);
                }

                @Override
                public void onIdle(OnIdleContext context) {
                  idles.compute(id, (k, v) -> MoreObjects.firstNonNull(v, 0) + 1);
                  if (context.getWatermark() > 0) {
                    observerWatermarks.put(this, context.getWatermark());
                    if ((!expectMoved || context.getWatermark() > 0) && !confirmedLatch) {
                      confirmedLatch = true;
                      latch.countDown();
                    }
                  }
                }
              })
          .waitUntilReady();
    }

    assertTrue(
        String.format(
            "Timeout, observerWatermarks = %s, numObservers = %d",
            observerWatermarks, numObservers),
        latch.await(30, TimeUnit.SECONDS));

    assertEquals(numObservers, observerWatermarks.size());
    long watermark = observerWatermarks.values().stream().min(Long::compare).orElse(Long.MIN_VALUE);

    // watermark should be moved
    assertTrue(!expectMoved || watermark > 0);
    assertTrue(
        "Watermark should not be too far, got "
            + watermark
            + " calculated from "
            + observerWatermarks,
        watermark < now * 10);
  }

  @Test(timeout = 10000)
  public void testObserveBulkCommitsCorrectly() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                cfg(
                    Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, 1L),
                    Pair.of(LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, 3))));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      StreamElement update =
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key-" + i,
              attr.getName(),
              now + 2000,
              new byte[] {1, 2});
      // then we write single element
      writer.write(update, (succ, e) -> {});
    }
    CountDownLatch latch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observeBulk(
            "test",
            Position.OLDEST,
            true,
            new CommitLogObserver() {

              int processed = 0;

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                if (++processed == 100) {
                  context.confirm();
                }
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

    long offsetSum =
        handle.getCommittedOffsets().stream().mapToLong(o -> ((TopicOffset) o).getOffset()).sum();

    assertEquals(100, offsetSum);

    KafkaConsumer<Object, Object> consumer =
        ((LocalKafkaCommitLogDescriptor.LocalKafkaLogReader) reader).getConsumer();
    String topic = accessor.getTopic();

    assertEquals(
        100,
        consumer
            .committed(
                handle.getCommittedOffsets().stream()
                    .map(o -> new TopicPartition(topic, o.getPartition().getId()))
                    .collect(Collectors.toSet()))
            .values()
            .stream()
            .mapToLong(OffsetAndMetadata::offset)
            .sum());
  }

  @Test(timeout = 100000)
  public void testOnlineObserveWithRebalanceResetsOffsetCommitter() throws InterruptedException {
    int numWrites = 5;
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                cfg(
                    Pair.of(LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, 3),
                    // poll single record to commit it in atomic way
                    Pair.of(LocalKafkaCommitLogDescriptor.MAX_POLL_RECORDS, 1))));
    final CountDownLatch latch = new CountDownLatch(numWrites);
    AtomicInteger consumed = new AtomicInteger();
    List<OnNextContext> unconfirmed = Collections.synchronizedList(new ArrayList<>());

    CommitLogObserver observer =
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            switch (consumed.getAndIncrement()) {
              case 0:
                // we must confirm the first message to create a committed position
                context.confirm();
                break;
              case 2:
                throw new RuntimeException("Failing first consumer!");
              default:
                unconfirmed.add(context);
                break;
            }
            if (consumed.get() == numWrites) {
              unconfirmed.forEach(OnNextContext::confirm);
            }
            latch.countDown();
            return true;
          }

          @Override
          public void onCompleted() {}

          @Override
          public boolean onError(Throwable error) {
            return true;
          }
        };
    testOnlineObserveWithRebalanceResetsOffsetCommitterWithObserver(observer, accessor, numWrites);
    latch.await();
    assertEquals(
        "Invalid committed offsets: " + accessor.committedOffsets,
        3,
        accessor.committedOffsets.values().stream().mapToInt(AtomicInteger::get).sum());
  }

  private void testOnlineObserveWithRebalanceResetsOffsetCommitterWithObserver(
      CommitLogObserver observer, Accessor accessor, int numWrites) {

    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    // observe from two observers
    reader.observe("test", Position.NEWEST, observer);
    reader.observe("test", Position.NEWEST, observer);

    CountDownLatch latch = new CountDownLatch(numWrites);
    for (int i = 0; i < numWrites; i++) {
      writer.write(
          update,
          (succ, e) -> {
            assertTrue(succ);
            latch.countDown();
          });
    }
    ExceptionUtils.ignoringInterrupted(latch::await);
  }

  @Test(timeout = 10000)
  public void testObserveWithException() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                restarts.incrementAndGet();
                throw new RuntimeException("FAIL!");
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                exc.set(error);
                latch.countDown();
                throw new RuntimeException(error);
              }
            });

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals("FAIL!", exc.get().getMessage());
    assertEquals(1, restarts.get());
    assertEquals(3, handle.getCommittedOffsets().size());
    List<Long> startedOffsets =
        handle.getCurrentOffsets().stream()
            .map(o -> ((TopicOffset) o).getOffset())
            .filter(o -> o >= 0)
            .collect(Collectors.toList());
    assertEquals(Collections.singletonList(0L), startedOffsets);
  }

  @Test(timeout = 10000)
  public void testBulkObserveWithException() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observeBulk(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                restarts.incrementAndGet();
                throw new RuntimeException("FAIL!");
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                exc.set(error);
                latch.countDown();
                throw new RuntimeException(error);
              }
            });

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals("FAIL!", exc.get().getMessage());
    assertEquals(1, restarts.get());
    assertEquals(3, handle.getCommittedOffsets().size());
    List<Long> startedOffsets =
        handle.getCurrentOffsets().stream()
            .map(o -> ((TopicOffset) o).getOffset())
            .filter(o -> o >= 0)
            .collect(Collectors.toList());
    assertEquals(Collections.singletonList(0L), startedOffsets);
  }

  @Test(timeout = 10000)
  public void testBulkObserveWithExceptionAndRetry() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    AtomicInteger restarts = new AtomicInteger();
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final int numRetries = 3;
    final CountDownLatch latch = new CountDownLatch(numRetries);
    final CommitLogObserver observer =
        CommitLogObservers.withNumRetriedExceptions(
            "test",
            numRetries,
            new CommitLogObserver() {

              @Override
              public boolean onError(Throwable error) {
                latch.countDown();
                return true;
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext confirm) {
                restarts.incrementAndGet();
                throw new RuntimeException("FAIL!");
              }
            });
    reader.observe("test", observer);
    AtomicBoolean finished = new AtomicBoolean();
    Executors.newCachedThreadPool()
        .execute(
            () -> {
              while (!finished.get()) {
                try {
                  TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ex) {
                  break;
                }
                writer.write(update, (succ, e) -> assertTrue(succ));
              }
            });
    latch.await();
    assertEquals(3, restarts.get());
    finished.set(true);
  }

  @Test(timeout = 10000)
  public void testBulkObserveSuccess() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final AtomicInteger restarts = new AtomicInteger();
    final AtomicReference<Throwable> exc = new AtomicReference<>();
    final AtomicReference<StreamElement> input = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observeBulk(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                restarts.incrementAndGet();
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                input.set(element);
                context.confirm();
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                exc.set(error);
                throw new RuntimeException(error);
              }
            });

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertNull(exc.get());
    assertTrue(restarts.get() > 0);
    assertArrayEquals(update.getValue(), input.get().getValue());
    assertEquals(3, handle.getCommittedOffsets().size());
    assertEquals(
        handle.getCommittedOffsets().toString(),
        1L,
        (long)
            (Long)
                handle.getCommittedOffsets().stream()
                    .mapToLong(o -> ((TopicOffset) o).getOffset())
                    .sum());
  }

  @Test(timeout = 10000)
  public void testBulkObservePartitionsFromOldestSuccess() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    AtomicInteger consumed = new AtomicInteger();
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    for (int i = 0; i < 1000; i++) {
      writer.write(update, (succ, e) -> assertTrue(succ));
    }
    CountDownLatch latch = new CountDownLatch(1);
    reader.observeBulkPartitions(
        reader.getPartitions(),
        Position.OLDEST,
        true,
        new CommitLogObserver() {

          @Override
          public void onRepartition(OnRepartitionContext context) {}

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            consumed.incrementAndGet();
            context.confirm();
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
    assertEquals(1000, consumed.get());
  }

  @Test(timeout = 10000)
  public void testBulkObservePartitionsSuccess() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    AtomicReference<StreamElement> input = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(2);
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observeBulkPartitions(
            reader.getPartitions(),
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                restarts.incrementAndGet();
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                input.set(element);
                context.confirm();
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                exc.set(error);
                throw new RuntimeException(error);
              }
            });

    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertNull(exc.get());
    assertEquals(1, restarts.get());
    assertArrayEquals(update.getValue(), input.get().getValue());
    assertEquals(3, handle.getCommittedOffsets().size());
    assertEquals(
        1L,
        (long)
            (Long)
                handle.getCommittedOffsets().stream()
                    .mapToLong(o -> ((TopicOffset) o).getOffset())
                    .sum());
  }

  @Test(timeout = 10000)
  public void testBulkObservePartitionsResetOffsetsSuccess() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    AtomicInteger restarts = new AtomicInteger();
    AtomicReference<Throwable> exc = new AtomicReference<>();
    AtomicReference<StreamElement> input = new AtomicReference<>();
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(2));
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final ObserveHandle handle =
        reader.observePartitions(
            reader.getPartitions(),
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public void onRepartition(OnRepartitionContext context) {
                restarts.incrementAndGet();
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                input.set(element);
                context.confirm();
                latch.get().countDown();
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                exc.set(error);
                throw new RuntimeException(error);
              }
            });

    handle.waitUntilReady();
    writer.write(
        update,
        (succ, e) -> {
          assertTrue(succ);
          latch.get().countDown();
        });

    latch.get().await();
    latch.set(new CountDownLatch(1));
    handle.resetOffsets(
        reader.getPartitions().stream()
            .map(p -> (PartitionWithTopic) p)
            .map(
                p ->
                    new TopicOffset(
                        new PartitionWithTopic(p.getTopic(), p.getId()),
                        0,
                        Watermarks.MIN_WATERMARK))
            .collect(Collectors.toList()));
    latch.get().await();
    assertEquals(
        1L,
        (long)
            (Long)
                handle.getCommittedOffsets().stream()
                    .mapToLong(o -> ((TopicOffset) o).getOffset())
                    .sum());
  }

  @Test
  public void testObserveOnNonExistingTopic() {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaLogReader reader = accessor.newReader(context());
    try {
      // need this to initialize the consumer
      assertNotNull(reader.getPartitions());
      reader.validateTopic(reader.getConsumer(), "non-existing-topic");
      fail("Should throw exception");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "Received null or empty partitions for topic [non-existing-topic]. "
              + "Please check that the topic exists and has at least one partition.",
          ex.getMessage());
      return;
    }
    fail("Should throw IllegalArgumentException");
  }

  @Test(timeout = 10000)
  public void testBulkObserveOffsets() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final List<KafkaStreamElement> input = new ArrayList<>();
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(3));
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final Map<Integer, Offset> currentOffsets = new HashMap<>();

    final CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            input.add((KafkaStreamElement) element);
            context.confirm();
            latch.get().countDown();
            // terminate after reading first record
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };
    try (final ObserveHandle handle =
        reader.observeBulkPartitions(reader.getPartitions(), Position.NEWEST, observer)) {

      // write two elements
      for (int i = 0; i < 2; i++) {
        writer.write(
            update,
            (succ, e) -> {
              assertTrue(succ);
              latch.get().countDown();
            });
      }
      latch.get().await();
      latch.set(new CountDownLatch(1));

      handle.getCommittedOffsets().forEach(o -> currentOffsets.put(o.getPartition().getId(), o));
    }

    // each partitions has a record here
    assertEquals(3, currentOffsets.size());
    assertEquals(
        currentOffsets.toString(),
        1L,
        currentOffsets.values().stream().mapToLong(o -> ((TopicOffset) o).getOffset()).sum());

    // restart from old offset
    final ObserveHandle handle2 =
        reader.observeBulkOffsets(Lists.newArrayList(currentOffsets.values()), observer);
    latch.get().await();
    assertEquals(2, input.size());
    assertEquals(0, input.get(0).getOffset());
    assertEquals(1, input.get(1).getOffset());
    // committed offset 1 and 2
    assertEquals(
        2L,
        handle2.getCommittedOffsets().stream().mapToLong(o -> ((TopicOffset) o).getOffset()).sum());
  }

  @Test(timeout = 10000)
  public void testBulkObserveOffsets2() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final List<KafkaStreamElement> input = new ArrayList<>();
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(3));
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});

    final CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            input.add((KafkaStreamElement) element);
            latch.get().countDown();
            // terminate after reading first record
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };
    final List<Offset> offsets;
    try (final ObserveHandle handle =
        reader.observeBulkPartitions(reader.getPartitions(), Position.NEWEST, observer)) {

      // write two elements
      for (int i = 0; i < 2; i++) {
        writer.write(
            update,
            (succ, e) -> {
              assertTrue(succ);
              latch.get().countDown();
            });
      }
      latch.get().await();
      latch.set(new CountDownLatch(1));
      offsets = handle.getCurrentOffsets();
    }

    // restart from old offset
    reader.observeBulkOffsets(Lists.newArrayList(offsets), observer);
    latch.get().await();
    assertEquals(2, input.size());
    assertEquals(0, input.get(0).getOffset());
    assertEquals(0, input.get(1).getOffset());
  }

  @Test(timeout = 60000)
  public void testCurrentOffsetsReflectSeek() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CountDownLatch latch = new CountDownLatch(10);
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2});
    for (int i = 0; i < 10; i++) {
      writer.write(update, (succ, exc) -> latch.countDown());
    }
    latch.await();

    ObserveHandle handle =
        reader.observe(
            "name",
            Position.OLDEST,
            new CommitLogObserver() {

              @Override
              public boolean onError(Throwable error) {
                return false;
              }

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                return false;
              }
            });

    handle.waitUntilReady();
    handle.close();
    assertEquals(3, handle.getCurrentOffsets().size());
    assertEquals(
        0,
        handle.getCurrentOffsets().stream()
            .mapToLong(o -> ((TopicOffset) o).getOffset())
            .filter(o -> o >= 0)
            .sum());
  }

  @Test(timeout = 10000)
  public void testCachedView() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CachedView view = Optionals.get(accessor.getCachedView(context()));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
    long now = System.currentTimeMillis();
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now,
            new byte[] {1, 2});

    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });
    latch.get().await();
    latch.set(new CountDownLatch(1));
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertArrayEquals(new byte[] {1, 2}, Optionals.get(view.get("key", attr)).getValue());
    update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now + 1,
            new byte[] {1, 2, 3});
    assertEquals(Watermarks.MIN_WATERMARK, getMinWatermark(view.getRunningHandle().get()));
    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });
    assertEquals(Watermarks.MIN_WATERMARK, getMinWatermark(view.getRunningHandle().get()));
    latch.get().await();
    TimeUnit.SECONDS.sleep(1);
    assertArrayEquals(new byte[] {1, 2, 3}, Optionals.get(view.get("key", attr)).getValue());
    assertTrue(
        "Expected watermark to be at least " + (now + 500),
        now + 500 < getMinWatermark(view.getRunningHandle().get()));
  }

  @Test(timeout = 10000)
  public void testCurrentOffsetsWatermark() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    final LocalKafkaWriter<?, ?> writer = accessor.newWriter();
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
    final long now = System.currentTimeMillis();
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now,
            new byte[] {1, 2});

    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });

    latch.get().await();
    latch.set(new CountDownLatch(1));

    CommitLogObserver observer = (ingest, context) -> true;
    try (ObserveHandle handle = reader.observe("dummy", Position.OLDEST, observer)) {
      long minWatermark = getMinWatermark(handle);
      assertTrue(
          "Expecting watermark to be below " + now + ", got " + minWatermark, now >= minWatermark);
      update =
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key",
              attr.getName(),
              now + 100,
              new byte[] {1, 2, 3});
      writer.write(
          update,
          (succ, exc) -> {
            assertTrue(succ);
            latch.get().countDown();
          });
      latch.get().await();
      TimeUnit.MILLISECONDS.sleep(50);
      assertTrue(now + 100 >= getMinWatermark(handle));
      TimeUnit.MILLISECONDS.sleep(600);
      assertTrue(
          "Expected watermark to be at least " + (now + 500), now + 500 < getMinWatermark(handle));
    }
  }

  private long getMinWatermark(ObserveHandle handle) {
    return handle.getCurrentOffsets().stream()
        .map(Offset::getWatermark)
        .min(Comparator.naturalOrder())
        .orElse(Watermarks.MIN_WATERMARK);
  }

  private Partition getPartition(int partition) {
    return getPartition(topic, partition);
  }

  private Partition getPartition(String topic, int partition) {
    return new PartitionWithTopic(topic, partition);
  }

  @Test(timeout = 10000)
  public void testCachedViewReload() throws InterruptedException {
    final Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    final LocalKafkaWriter writer = accessor.newWriter();
    final CachedView view = Optionals.get(accessor.getCachedView(context()));
    final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(2));
    final List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key2",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {2, 3}));
    updates.forEach(
        update ->
            writer.write(
                update,
                (succ, exc) -> {
                  assertTrue(succ);
                  latch.get().countDown();
                }));
    latch.get().await();
    latch.set(new CountDownLatch(1));
    view.assign(IntStream.range(1, 2).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertFalse(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
    StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    writer.write(
        update,
        (succ, exc) -> {
          assertTrue(succ);
          latch.get().countDown();
        });
    latch.get().await();
    TimeUnit.SECONDS.sleep(1);
    view.assign(IntStream.range(1, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 10000)
  public void testCachedViewWrite() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key2",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(2);
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertTrue(view.get("key2", attr).isPresent());
    assertTrue(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndDelete() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    long now = System.currentTimeMillis();
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.delete(
                entity, attr, UUID.randomUUID().toString(), "key1", attr.getName(), now));
    CountDownLatch latch = new CountDownLatch(2);
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    assertFalse(view.get("key1", attr).isPresent());
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertFalse(view.get("key1", attr).isPresent());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndDeleteWildcard() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now - 500,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now + 500,
                new byte[] {2, 3}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.3",
                now - 500,
                new byte[] {3, 4}))
        .forEach(
            update ->
                view.write(
                    update,
                    (succ, exc) -> {
                      assertTrue("Exception: " + exc, succ);
                      latch.countDown();
                    }));
    latch.await();
    assertTrue(view.get("key1", "wildcard.1", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard, now + 500).isPresent());
    assertArrayEquals(
        new byte[] {2, 3},
        view.get("key1", "wildcard.1", attrWildcard, now + 500).get().getValue());
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertTrue(view.get("key1", "wildcard.1", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.2", attrWildcard, now + 500).isPresent());
    assertFalse(view.get("key1", "wildcard.3", attrWildcard, now + 500).isPresent());
    assertArrayEquals(
        new byte[] {2, 3},
        view.get("key1", "wildcard.1", attrWildcard, now + 500).get().getValue());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndList() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now - 500),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.3",
                now - 499,
                new byte[] {3, 4}))
        .forEach(
            update ->
                view.write(
                    update,
                    (succ, exc) -> {
                      assertTrue("Exception: ", succ);
                      latch.countDown();
                    }));
    latch.await();
    List<KeyValue<byte[]>> res = new ArrayList<>();
    view.scanWildcard("key1", attrWildcard, res::add);
    assertEquals(2, res.size());
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    res.clear();
    view.scanWildcard("key1", attrWildcard, res::add);
    assertEquals(2, res.size());
  }

  @Test(timeout = 10000)
  public void testCachedViewWriteAndListAll() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(5);
    Stream.of(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now - 2000,
                new byte[] {0}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now - 1000,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now - 500),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now,
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.3",
                now - 499,
                new byte[] {3, 4}))
        .forEach(
            update ->
                view.write(
                    update,
                    (succ, exc) -> {
                      assertTrue("Exception: " + exc, succ);
                      latch.countDown();
                    }));
    latch.await();
    List<KeyValue<?>> res = new ArrayList<>();
    view.scanWildcardAll("key1", res::add);
    assertEquals(3, res.size());
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    res.clear();
    view.scanWildcardAll("key1", res::add);
    assertEquals(3, res.size());
  }

  @Test(timeout = 10000)
  public void testCachedViewWritePreUpdate() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(entity, storageUri, partitionsCfg(3, FirstBytePartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2}),
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key2",
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(
        IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(2, calls.get());
  }

  @Test(timeout = 10000)
  public void testCachedViewWritePreUpdateAndDeleteWildcard() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, createTestFamily(entity, storageUri, partitionsCfg(3, KeyPartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    long now = System.currentTimeMillis();
    List<StreamElement> updates =
        Arrays.asList(
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.1",
                now,
                new byte[] {1, 2}),
            StreamElement.deleteWildcard(
                entity, attrWildcard, UUID.randomUUID().toString(), "key1", now + 1000L),
            StreamElement.upsert(
                entity,
                attrWildcard,
                UUID.randomUUID().toString(),
                "key1",
                "wildcard.2",
                now + 500L,
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Ex1ception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    AtomicInteger calls = new AtomicInteger();
    view.assign(
        IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()),
        (e, c) -> calls.incrementAndGet());
    assertEquals(3, calls.get());
  }

  @Test(timeout = 10000)
  public void testRewriteAndPrefetch() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(
            direct, createTestFamily(entity, storageUri, partitionsCfg(3, KeyPartitioner.class)));
    CachedView view = Optionals.get(accessor.getCachedView(context()));
    long now = System.currentTimeMillis();
    List<StreamElement> updates =
        Arrays.asList(
            // store first value
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now,
                new byte[] {1, 2}),
            // update the value at the same stamp
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key1",
                attr.getName(),
                now,
                new byte[] {2, 3}));
    CountDownLatch latch = new CountDownLatch(updates.size());
    updates.forEach(
        update ->
            view.write(
                update,
                (succ, exc) -> {
                  assertTrue("Exception: " + exc, succ);
                  latch.countDown();
                }));
    latch.await();
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertArrayEquals(new byte[] {2, 3}, view.get("key1", attr).get().getValue());
    view.write(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key1",
            attr.getName(),
            now,
            new byte[] {3, 4}),
        (succ, exc) -> assertTrue(succ));
    assertArrayEquals(new byte[] {3, 4}, view.get("key1", attr).get().getValue());
    view.close();
    assertFalse(view.get("key1", attr).isPresent());
    view.assign(IntStream.range(0, 3).mapToObj(this::getPartition).collect(Collectors.toList()));
    assertArrayEquals(new byte[] {3, 4}, view.get("key1", attr).get().getValue());
  }

  @Test(timeout = 5000)
  public void testMaxBytesPerSec() throws InterruptedException {
    long maxLatency = testSequentialConsumption(3);
    long expectedNanos = TimeUnit.MILLISECONDS.toNanos(500);
    assertTrue(
        String.format("maxLatency should be greater than %d, got %d", expectedNanos, maxLatency),
        maxLatency > expectedNanos);
  }

  @Test(timeout = 5000)
  public void testNoMaxBytesPerSec() throws InterruptedException {
    long maxLatency = testSequentialConsumption(Long.MAX_VALUE);
    assertTrue(maxLatency < 500_000_000L);
  }

  @Test(timeout = 10000)
  public void testCustomElementSerializer() throws InterruptedException {
    kafka =
        new LocalKafkaCommitLogDescriptor(
            accessor ->
                new Accessor(
                    accessor,
                    Collections.singletonMap(
                        KafkaAccessor.SERIALIZER_CLASS, SingleAttrSerializer.class.getName())));
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    LocalKafkaWriter writer = accessor.newWriter();
    KafkaConsumer<Object, Object> consumer = accessor.createConsumerFactory().create();
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        StreamElement.upsert(
            entity,
            strAttr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            System.currentTimeMillis(),
            "this is test".getBytes()),
        (succ, exc) -> {
          assertTrue(succ);
          assertNull(exc);
          latch.countDown();
        });
    latch.await();
    ConsumerRecords<Object, Object> polled = consumer.poll(Duration.ofMillis(1000));
    assertEquals(1, polled.count());
    assertEquals(1, polled.partitions().size());
    TopicPartition partition = Iterators.getOnlyElement(polled.partitions().iterator());
    assertEquals(0, partition.partition());
    assertEquals("topic", partition.topic());
    int tested = 0;
    for (ConsumerRecord<Object, Object> r : polled) {
      assertEquals("key", r.key());
      assertEquals("topic", r.topic());
      assertEquals("this is test", r.value());
      tested++;
    }
    assertEquals(1, tested);
  }

  @Test(timeout = 10000)
  public void testCustomWatermarkEstimator() throws InterruptedException {
    Map<String, Object> cfg = partitionsCfg(3);
    cfg.put("watermark.estimator-factory", FixedWatermarkEstimatorFactory.class.getName());

    Accessor accessor = kafka.createAccessor(direct, createTestFamily(entity, storageUri, cfg));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));

    long now = System.currentTimeMillis();
    final UnaryFunction<Integer, StreamElement> update =
        pos ->
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key" + pos,
                attr.getName(),
                now + pos,
                new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(100);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();

    for (int i = 0; i < 100; i++) {
      writer.write(update.apply(i), (succ, e) -> {});
    }

    latch.await();

    assertEquals(FixedWatermarkEstimatorFactory.FIXED_WATERMARK, watermark.get());
  }

  @Test(timeout = 10000)
  public void testCustomIdlePolicy() throws InterruptedException {
    Map<String, Object> cfg =
        and(partitionsCfg(3), cfg(Pair.of(KafkaAccessor.EMPTY_POLL_TIME, "1000")));
    cfg.put("watermark.idle-policy-factory", FixedWatermarkIdlePolicyFactory.class.getName());

    Accessor accessor = kafka.createAccessor(direct, createTestFamily(entity, storageUri, cfg));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));

    long now = System.currentTimeMillis();
    final StreamElement update =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            attr.getName(),
            now + 2000,
            new byte[] {1, 2});

    AtomicLong watermark = new AtomicLong();
    CountDownLatch latch = new CountDownLatch(2);
    reader
        .observe(
            "test",
            Position.NEWEST,
            new CommitLogObserver() {

              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                watermark.set(context.getWatermark());
                latch.countDown();
                return true;
              }

              @Override
              public void onCompleted() {
                fail("This should not be called");
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();

    // then we write single element
    writer.write(update, (succ, e) -> {});
    // for two seconds we have empty data
    TimeUnit.SECONDS.sleep(2);
    // finally, last update to save watermark
    writer.write(update, (succ, e) -> {});
    latch.await();

    assertEquals(FixedWatermarkIdlePolicyFactory.FIXED_IDLE_WATERMARK, watermark.get());
  }

  @Test
  public void testOffsetExternalizer() {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    assertTrue(reader.hasExternalizableOffsets());
    assertEquals(TopicOffsetExternalizer.class, reader.getOffsetExternalizer().getClass());
  }

  @Test(timeout = 10_000)
  public void testFetchOffsets() throws InterruptedException {
    Accessor accessor =
        kafka.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final CountDownLatch latch = new CountDownLatch(2);
    final StreamElement[] updates =
        new StreamElement[] {
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key",
              attr.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2}),
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key2",
              attr.getName(),
              System.currentTimeMillis(),
              new byte[] {1, 2, 3})
        };

    Arrays.stream(updates).forEach(update -> writer.write(update, (succ, exc) -> {}));

    CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            context.confirm();
            latch.countDown();
            return true;
          }

          @Override
          public void onCompleted() {
            fail("This should not be called");
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };

    ObserveHandle handle =
        reader.observeBulkOffsets(
            reader.fetchOffsets(Position.OLDEST, reader.getPartitions()).values(), observer);

    latch.await();
    assertEquals(
        handle.getCommittedOffsets().stream()
            .map(TopicOffset.class::cast)
            .sorted(Comparator.comparing(tp -> tp.getPartition().getId()))
            .collect(Collectors.toList()),
        reader.fetchOffsets(Position.NEWEST, reader.getPartitions()).values().stream()
            .map(TopicOffset.class::cast)
            .sorted(Comparator.comparing(tp -> tp.getPartition().getId()))
            .collect(Collectors.toList()));
  }

  @Test
  public void testKafkaConsumerFactoryAutoOffsetReset() {
    Properties props = new Properties();
    KafkaConsumerFactory.updateAutoOffsetReset(Position.NEWEST, props, true);
    assertEquals("latest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    props = new Properties();
    KafkaConsumerFactory.updateAutoOffsetReset(Position.OLDEST, props, true);
    assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    // explicitly set, cannot change
    KafkaConsumerFactory.updateAutoOffsetReset(Position.NEWEST, props, true);
    assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    props = new Properties();
    KafkaConsumerFactory.updateAutoOffsetReset(Position.CURRENT, props, false);
    assertEquals("latest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    props = new Properties();
    KafkaConsumerFactory.updateAutoOffsetReset(Position.CURRENT, props, true);
    assertEquals("none", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
  }

  @Test(timeout = 10000)
  public void testBatchObserveWithLogRoll() throws InterruptedException {
    String topic = Utils.topic(storageUri);
    Map<TopicPartition, Long> endOffsets =
        IntStream.range(0, 3)
            .mapToObj(i -> new TopicPartition(topic, i))
            .collect(Collectors.toMap(Function.identity(), e -> 2L));
    Map<TopicPartition, Long> beginningOffsets =
        IntStream.range(0, 3)
            .mapToObj(i -> new TopicPartition(topic, i))
            .collect(Collectors.toMap(Function.identity(), e -> 0L));
    final LocalKafkaCommitLogDescriptor descriptor =
        new LocalKafkaCommitLogDescriptor() {
          @Override
          public Accessor createAccessor(
              DirectDataOperator direct, AttributeFamilyDescriptor family) {
            AtomicInteger invokedCount = new AtomicInteger();
            return new Accessor(family.getEntity(), family.getStorageUri(), family.getCfg(), id) {
              @Override
              <K, V> KafkaConsumer<K, V> mockKafkaConsumer(
                  String name,
                  ConsumerGroup group,
                  ElementSerializer<K, V> serializer,
                  @Nullable Collection<Partition> assignedPartitions,
                  @Nullable ConsumerRebalanceListener listener) {

                return new MockKafkaConsumer<>(
                    name, group, serializer, assignedPartitions, listener) {
                  @Override
                  public Map<TopicPartition, Long> beginningOffsets(
                      Collection<TopicPartition> partitions) {
                    if (invokedCount.incrementAndGet() > 2) {
                      return endOffsets;
                    }
                    return beginningOffsets;
                  }

                  @Override
                  public Map<TopicPartition, Long> endOffsets(
                      Collection<TopicPartition> partitions, Duration timeout) {
                    return endOffsets;
                  }
                };
              }
            };
          }
        };
    final Accessor accessor =
        descriptor.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final CountDownLatch latch = new CountDownLatch(1);

    final CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            context.confirm();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        };

    try (final ObserveHandle handle =
        reader.observeBulk("dummy", Position.OLDEST, true, observer)) {
      latch.await();
    }
  }

  @Test(timeout = 10000)
  public void testObserveOffsetsWithLogRoll() throws InterruptedException {
    String topic = Utils.topic(storageUri);
    Map<TopicPartition, Long> endOffsets =
        IntStream.range(0, 3)
            .mapToObj(i -> new TopicPartition(topic, i))
            .collect(Collectors.toMap(Function.identity(), e -> 2L));
    Map<TopicPartition, Long> beginningOffsets =
        IntStream.range(0, 3)
            .mapToObj(i -> new TopicPartition(topic, i))
            .collect(Collectors.toMap(Function.identity(), e -> 0L));
    final LocalKafkaCommitLogDescriptor descriptor =
        new LocalKafkaCommitLogDescriptor() {
          @Override
          public Accessor createAccessor(
              DirectDataOperator direct, AttributeFamilyDescriptor family) {
            AtomicInteger invokedCount = new AtomicInteger();
            return new Accessor(family.getEntity(), family.getStorageUri(), family.getCfg(), id) {
              @Override
              <K, V> KafkaConsumer<K, V> mockKafkaConsumer(
                  String name,
                  ConsumerGroup group,
                  ElementSerializer<K, V> serializer,
                  @Nullable Collection<Partition> assignedPartitions,
                  @Nullable ConsumerRebalanceListener listener) {

                return new MockKafkaConsumer<>(
                    name, group, serializer, assignedPartitions, listener) {
                  @Override
                  public Map<TopicPartition, Long> beginningOffsets(
                      Collection<TopicPartition> partitions) {
                    if (invokedCount.incrementAndGet() > 2) {
                      return endOffsets;
                    }
                    return beginningOffsets;
                  }

                  @Override
                  public Map<TopicPartition, Long> endOffsets(
                      Collection<TopicPartition> partitions, Duration timeout) {
                    return endOffsets;
                  }
                };
              }
            };
          }
        };
    final Accessor accessor =
        descriptor.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(3)));
    final CommitLogReader reader = Optionals.get(accessor.getCommitLogReader(context()));
    final CountDownLatch latch = new CountDownLatch(1);

    final CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            context.confirm();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        };

    try (final ObserveHandle handle =
        reader.observeBulkOffsets(
            reader.fetchOffsets(Position.OLDEST, reader.getPartitions()).values(),
            true,
            observer)) {
      latch.await();
    }
  }

  @Test(timeout = 10000)
  public void testFailedPollDoesNotDeadlock() throws InterruptedException {
    final LocalKafkaCommitLogDescriptor descriptor =
        new LocalKafkaCommitLogDescriptor() {
          @Override
          public Accessor createAccessor(
              DirectDataOperator direct, AttributeFamilyDescriptor family) {
            return new Accessor(family.getEntity(), family.getStorageUri(), family.getCfg(), id) {
              @Override
              <K, V> KafkaConsumer<K, V> mockKafkaConsumer(
                  String name,
                  ConsumerGroup group,
                  ElementSerializer<K, V> serializer,
                  @Nullable Collection<Partition> assignedPartitions,
                  @Nullable ConsumerRebalanceListener listener) {

                return new MockKafkaConsumer<>(
                    name, group, serializer, assignedPartitions, listener) {
                  @Override
                  public ConsumerRecords<K, V> poll(Duration sleep) {
                    throw new RuntimeException("Failed poll");
                  }
                };
              }
            };
          }
        };
    Accessor accessor =
        descriptor.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    LocalKafkaLogReader reader = accessor.newReader(direct.getContext());
    ObserveHandle handle =
        reader.observe(
            "dummy",
            new CommitLogObserver() {
              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                return false;
              }

              @Override
              public boolean onError(Throwable error) {
                return false;
              }
            });
    handle.waitUntilReady();
    handle.close();
  }

  @Test(timeout = 10000)
  public void testHandleRebalanceInProgressException() throws InterruptedException {
    final AtomicInteger invokedCount = new AtomicInteger();
    final int numElements = 2000;
    final LocalKafkaCommitLogDescriptor descriptor =
        new LocalKafkaCommitLogDescriptor() {
          @Override
          public Accessor createAccessor(
              DirectDataOperator direct, AttributeFamilyDescriptor family) {
            return new Accessor(family.getEntity(), family.getStorageUri(), family.getCfg(), id) {
              @Override
              <K, V> KafkaConsumer<K, V> mockKafkaConsumer(
                  String name,
                  ConsumerGroup group,
                  ElementSerializer<K, V> serializer,
                  @Nullable Collection<Partition> assignedPartitions,
                  @Nullable ConsumerRebalanceListener listener) {

                final Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
                return new MockKafkaConsumer<>(
                    name, group, serializer, assignedPartitions, listener) {
                  @Override
                  public void commitSync(Map<TopicPartition, OffsetAndMetadata> toCommit) {
                    if (invokedCount.getAndIncrement() == 1) {
                      throw new RebalanceInProgressException();
                    }
                    committed.putAll(toCommit);
                  }

                  @Override
                  public Map<TopicPartition, OffsetAndMetadata> committed(
                      Set<TopicPartition> parts) {
                    return parts.stream()
                        .map(tp -> Pair.of(tp, committed.get(tp)))
                        .filter(p -> p.getSecond() != null)
                        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
                  }
                };
              }
            };
          }
        };
    Accessor accessor =
        descriptor.createAccessor(direct, createTestFamily(entity, storageUri, partitionsCfg(1)));
    LocalKafkaLogReader reader = accessor.newReader(direct.getContext());
    Map<String, StreamElement> observedAfterRepartition = new HashMap<>();
    LocalKafkaWriter<?, ?> writer = accessor.newWriter();
    CountDownLatch latch = new CountDownLatch(1);
    try (ObserveHandle handle =
        reader.observe(
            "dummy",
            new CommitLogObserver() {
              @Override
              public boolean onNext(StreamElement element, OnNextContext context) {
                observedAfterRepartition.put(element.getKey(), element);
                context.confirm();
                if (element.getKey().equals("last-key")) {
                  latch.countDown();
                  return false;
                }
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                return false;
              }
            })) {

      for (int i = 0; i < numElements; i++) {
        writer.write(
            StreamElement.upsert(
                entity,
                attr,
                UUID.randomUUID().toString(),
                "key" + i,
                attr.getName(),
                System.currentTimeMillis(),
                new byte[] {}),
            (succ, exc) -> {});
      }
      writer.write(
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "last-key",
              attr.getName(),
              System.currentTimeMillis(),
              new byte[] {}),
          (succ, exc) -> {});
      latch.await();
    }
    assertEquals(numElements + 1, observedAfterRepartition.size());
    assertTrue(invokedCount.get() > 1);
  }

  private long testSequentialConsumption(long maxBytesPerSec) throws InterruptedException {

    final Accessor accessor =
        kafka.createAccessor(
            direct,
            createTestFamily(
                entity,
                storageUri,
                cfg(
                    Pair.of(KafkaAccessor.ASSIGNMENT_TIMEOUT_MS, 1L),
                    Pair.of(KafkaAccessor.MAX_BYTES_PER_SEC, maxBytesPerSec),
                    Pair.of(LocalKafkaCommitLogDescriptor.MAX_POLL_RECORDS, 1))));
    final LocalKafkaWriter writer = accessor.newWriter();
    CommitLogReader reader =
        accessor
            .getCommitLogReader(context())
            .orElseThrow(() -> new IllegalStateException("Missing log reader"));
    final AtomicLong lastOnNext = new AtomicLong(Long.MIN_VALUE);
    final AtomicLong maxLatency = new AtomicLong(0);
    final int numElements = 2;
    final CountDownLatch latch = new CountDownLatch(numElements);

    CommitLogObserver observer =
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            long now = System.nanoTime();
            long last = lastOnNext.getAndSet(now);
            if (last > 0) {
              long latency = now - last;
              maxLatency.getAndUpdate(old -> Math.max(old, latency));
            }
            latch.countDown();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }
        };
    reader.observe("dummy", Position.OLDEST, observer);
    for (int i = 0; i < numElements; i++) {
      writer.write(
          StreamElement.upsert(
              entity,
              attr,
              UUID.randomUUID().toString(),
              "key1",
              attr.getName(),
              System.currentTimeMillis(),
              emptyValue()),
          (succ, exc) -> {
            assertTrue(succ);
            assertNull(exc);
          });
    }
    latch.await();
    return maxLatency.get();
  }

  private Context context() {
    return direct.getContext();
  }

  private static Map<String, Object> partitionsCfg(int partitions) {
    return partitionsCfg(partitions, null);
  }

  private static Map<String, Object> partitionsCfg(
      int partitions, @Nullable Class<? extends Partitioner> partitioner) {

    return cfg(
        Pair.of(LocalKafkaCommitLogDescriptor.CFG_NUM_PARTITIONS, String.valueOf(partitions)),
        partitioner != null
            ? Pair.of(KafkaAccessor.PARTITIONER_CLASS, partitioner.getName())
            : null);
  }

  @SafeVarargs
  private static Map<String, Object> cfg(Pair<String, Object>... pairs) {

    return Arrays.stream(pairs)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private static Map<String, Object> and(Map<String, Object> left, Map<String, Object> right) {

    return Stream.concat(left.entrySet().stream(), right.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static byte[] emptyValue() {
    return new byte[] {};
  }

  public static final class FirstBytePartitioner implements Partitioner {
    @Override
    public int getPartitionId(StreamElement element) {
      if (!element.isDelete()) {
        return element.getValue()[0];
      }
      return 0;
    }
  }

  public static final class SingleAttrSerializer implements ElementSerializer<String, String> {

    private AttributeDescriptor<?> attrDesc;

    @Override
    public void setup(EntityDescriptor entityDescriptor) {
      attrDesc =
          entityDescriptor
              .findAttribute("strAttr")
              .orElseThrow(() -> new IllegalStateException("Missing attribute 'strAttr'"));
    }

    @Override
    public StreamElement read(ConsumerRecord<String, String> record, EntityDescriptor entityDesc) {
      return StreamElement.upsert(
          entityDesc,
          attrDesc,
          attrDesc.getName(),
          UUID.randomUUID().toString(),
          record.key(),
          record.timestamp(),
          record.value().getBytes());
    }

    @Override
    public ProducerRecord<String, String> write(
        String topic, int partition, StreamElement element) {
      return new ProducerRecord<>(
          topic,
          partition,
          element.getStamp(),
          element.getKey(),
          (String) Optionals.get(element.getParsed()));
    }

    @Override
    public Serde<String> keySerde() {
      return Serdes.String();
    }

    @Override
    public Serde<String> valueSerde() {
      return Serdes.String();
    }
  }

  public static final class FixedWatermarkEstimatorFactory implements WatermarkEstimatorFactory {
    public static final long FIXED_WATERMARK = 333L;

    @Override
    public void setup(Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {}

    @Override
    public WatermarkEstimator create() {
      return new WatermarkEstimator() {
        @Override
        public long getWatermark() {
          return FIXED_WATERMARK;
        }

        @Override
        public void setMinWatermark(long minWatermark) {}
      };
    }
  }

  public static final class FixedWatermarkIdlePolicyFactory implements WatermarkIdlePolicyFactory {
    public static final long FIXED_IDLE_WATERMARK = 555L;

    @Override
    public void setup(Map<String, Object> cfg) {}

    @Override
    public WatermarkIdlePolicy create() {
      return () -> FIXED_IDLE_WATERMARK;
    }
  }
}
