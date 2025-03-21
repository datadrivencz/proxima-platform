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

import static java.util.stream.Collectors.toMap;

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.PartitionedWatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.commitlog.OffsetExternalizer;
import cz.o2.proxima.direct.core.time.MinimalPartitionWatermarkEstimator;
import cz.o2.proxima.direct.io.kafka.ElementConsumers.BulkConsumer;
import cz.o2.proxima.direct.io.kafka.ElementConsumers.OnlineConsumer;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;

/** A {@link CommitLogReader} implementation for Kafka. */
@Slf4j
public class KafkaLogReader extends AbstractStorage implements CommitLogReader {

  @Getter final KafkaAccessor accessor;
  @Getter private final Context context;
  private final long consumerPollInterval;
  private final long maxBytesPerSec;
  private final Map<String, Object> cfg;
  private final ElementSerializer<Object, Object> serializer;

  KafkaLogReader(KafkaAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
    this.context = context;
    this.consumerPollInterval = accessor.getConsumerPollInterval();
    this.maxBytesPerSec = accessor.getMaxBytesPerSec();
    this.cfg = accessor.getCfg();
    this.serializer = accessor.getSerializer();

    log.debug("Created {} for accessor {}", getClass().getSimpleName(), accessor);
  }

  @Override
  public boolean restoresSequentialIds() {
    return serializer.storesSequentialId();
  }

  /**
   * Subscribe observer by name to the commit log. Each observer maintains its own position in the
   * commit log, so that the observers with different names do not interfere If multiple observers
   * share the same name, then the ingests are load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   *
   * @param name identifier of the consumer
   */
  @Override
  public ObserveHandle observe(String name, Position position, CommitLogObserver observer) {

    return observeKafka(name, null, position, false, observer);
  }

  @Override
  public ObserveHandle observePartitions(
      String name,
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    return observeKafka(null, partitions, position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulk(
      String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

    return observeKafkaBulk(name, null, position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    // name is ignored, because when observing partition the offsets
    // are not committed to kafka
    return observeKafkaBulk(
        null, createDefaultOffsets(partitions), position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer) {
    return observeKafkaBulk(null, offsets, Position.CURRENT, stopAtCurrent, observer);
  }

  @Override
  public List<Partition> getPartitions() {
    if (accessor.isTopicRegex()) {
      throw new UnsupportedOperationException(
          String.format("Partitions of URI %s are unstable and should not be used.", getUri()));
    }
    try (KafkaConsumer<Object, Object> consumer = createConsumer()) {
      return consumer.partitionsFor(accessor.getTopic()).stream()
          .map(pi -> new PartitionWithTopic(pi.topic(), pi.partition()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public Map<Partition, Offset> fetchOffsets(Position position, List<Partition> partitions) {
    Preconditions.checkArgument(
        position == Position.NEWEST || position == Position.OLDEST,
        "Position %s does not have well defined offsets.",
        position);
    List<TopicPartition> topicPartitions =
        partitions.stream()
            .map(PartitionWithTopic.class::cast)
            .map(p -> new TopicPartition(p.getTopic(), p.getPartition()))
            .collect(Collectors.toList());
    final Map<TopicPartition, Long> res;
    try (KafkaConsumer<?, ?> consumer = createConsumer()) {
      if (position == Position.OLDEST) {
        res = consumer.beginningOffsets(topicPartitions);
      } else {
        res = consumer.endOffsets(topicPartitions);
      }
    }
    return res.entrySet().stream()
        .collect(
            Collectors.toMap(
                e -> new PartitionWithTopic(e.getKey().topic(), e.getKey().partition()),
                e ->
                    new TopicOffset(
                        new PartitionWithTopic(e.getKey().topic(), e.getKey().partition()),
                        e.getValue(),
                        Watermarks.MIN_WATERMARK)));
  }

  @VisibleForTesting
  ObserveHandle observeKafka(
      @Nullable String name,
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    Preconditions.checkArgument(
        name != null || partitions != null, "Either name or offsets have to be non null");

    Preconditions.checkArgument(
        !accessor.isTopicRegex() || partitions == null,
        "Regex URI %s cannot observe specific partitions, because these cannot be made stable.",
        getUri());

    try {
      return processConsumer(
          name,
          createDefaultOffsets(partitions),
          position,
          stopAtCurrent,
          name != null,
          observer,
          context.getExecutorService());
    } catch (InterruptedException ex) {
      log.warn("Interrupted waiting for kafka observer to start", ex);
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
  }

  private ObserveHandle observeKafkaBulk(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    Preconditions.checkArgument(
        name != null || offsets != null, "Either name or offsets have to be non null");

    Preconditions.checkArgument(position != null, "Position cannot be null");

    Preconditions.checkArgument(
        !accessor.isTopicRegex() || offsets == null,
        "Regex URI %s cannot observe specific offsets, because these cannot be made stable.",
        getUri());

    try {
      return processConsumerBulk(
          name,
          offsets,
          position,
          stopAtCurrent,
          name != null,
          observer,
          context.getExecutorService());
    } catch (InterruptedException ex) {
      log.warn("Interrupted waiting for kafka observer to start", ex);
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
  }

  /**
   * Process given consumer in online fashion.
   *
   * @param name name of the consumer
   * @param offsets assigned offsets
   * @param position where to read from
   * @param stopAtCurrent termination flag
   * @param commitToKafka should we commit to kafka
   * @param observer the observer
   * @param executor executor to use for async processing
   * @return observe handle
   */
  @VisibleForTesting
  ObserveHandle processConsumer(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      boolean commitToKafka,
      CommitLogObserver observer,
      ExecutorService executor)
      throws InterruptedException {

    // offsets that should be committed to kafka
    Map<TopicPartition, OffsetAndMetadata> kafkaCommitMap;
    kafkaCommitMap = Collections.synchronizedMap(new HashMap<>());

    final OffsetCommitter<TopicPartition> offsetCommitter = createOffsetCommitter();

    final BiConsumer<TopicPartition, ConsumerRecord<Object, Object>> preWrite =
        (tp, r) -> {
          final long offset = r.offset();
          offsetCommitter.register(
              tp,
              offset,
              1,
              () -> {
                OffsetAndMetadata mtd = new OffsetAndMetadata(offset + 1);
                if (commitToKafka) {
                  kafkaCommitMap.compute(tp, (k, v) -> v == null || v.offset() <= offset ? mtd : v);
                }
              });
        };

    OnlineConsumer<Object, Object> onlineConsumer =
        new OnlineConsumer<>(
            observer,
            offsetCommitter,
            () -> {
              synchronized (kafkaCommitMap) {
                Map<TopicPartition, OffsetAndMetadata> clone = new HashMap<>(kafkaCommitMap);
                kafkaCommitMap.clear();
                return clone;
              }
            });

    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    submitConsumerWithObserver(
        name, offsets, position, stopAtCurrent, preWrite, onlineConsumer, executor, handle);
    return dynamicHandle(handle);
  }

  /**
   * Process given consumer in bulk fashion.
   *
   * @param name name of the consumer
   * @param offsets assigned offsets
   * @param position where to read from
   * @param stopAtCurrent termination flag
   * @param commitToKafka should we commit to kafka
   * @param observer the observer
   * @param executor executor to use for async processing
   * @return observe handle
   */
  @VisibleForTesting
  ObserveHandle processConsumerBulk(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      boolean commitToKafka,
      CommitLogObserver observer,
      ExecutorService executor)
      throws InterruptedException {

    // offsets that should be committed to kafka
    Map<TopicPartition, OffsetAndMetadata> kafkaCommitMap;
    kafkaCommitMap = Collections.synchronizedMap(new HashMap<>());

    final BulkConsumer<Object, Object> bulkConsumer =
        new BulkConsumer<>(
            observer,
            (tp, o) -> {
              if (commitToKafka) {
                OffsetAndMetadata off = new OffsetAndMetadata(o);
                kafkaCommitMap.put(tp, off);
              }
            },
            () -> {
              synchronized (kafkaCommitMap) {
                Map<TopicPartition, OffsetAndMetadata> clone = new HashMap<>(kafkaCommitMap);
                kafkaCommitMap.clear();
                return clone;
              }
            },
            kafkaCommitMap::clear);

    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    submitConsumerWithObserver(
        name, offsets, position, stopAtCurrent, (tp, r) -> {}, bulkConsumer, executor, handle);
    return dynamicHandle(handle);
  }

  private void submitConsumerWithObserver(
      final @Nullable String name,
      final @Nullable Collection<Offset> offsets,
      final Position position,
      boolean stopAtCurrent,
      final BiConsumer<TopicPartition, ConsumerRecord<Object, Object>> preWrite,
      final ElementConsumer<Object, Object> consumer,
      final ExecutorService executor,
      final AtomicReference<ObserveHandle> handle)
      throws InterruptedException {

    final CountDownLatch readyLatch = new CountDownLatch(1);
    final CountDownLatch completedLatch = new CountDownLatch(1);
    final AtomicBoolean completed = new AtomicBoolean();
    final AtomicBoolean shutdown = new AtomicBoolean();
    List<TopicOffset> seekOffsets = Collections.synchronizedList(new ArrayList<>());

    Preconditions.checkArgument(
        !accessor.isTopicRegex() || !stopAtCurrent, "Cannot use stopAtCurrent with regex URI");

    executor.submit(
        () -> {
          final AtomicReference<KafkaConsumer<Object, Object>> consumerRef =
              new AtomicReference<>();
          final AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator =
              new AtomicReference<>(null);
          final Map<TopicPartition, Integer> topicPartitionToId = new HashMap<>();
          final Duration pollDuration = Duration.ofMillis(consumerPollInterval);
          final KafkaThroughputLimiter throughputLimiter =
              new KafkaThroughputLimiter(maxBytesPerSec);
          final Map<TopicPartition, Long> polledOffsets = new HashMap<>();
          final Map<TopicPartition, Integer> emptyPolls = new HashMap<>();
          final Map<TopicPartition, Long> assignmentEndOffsets = new HashMap<>();

          handle.set(
              createObserveHandle(shutdown, seekOffsets, consumer, readyLatch, completedLatch));
          consumer.onStart();
          ConsumerRebalanceListener listener =
              listener(
                  name,
                  consumerRef,
                  consumer,
                  topicPartitionToId,
                  emptyPolls,
                  polledOffsets,
                  assignmentEndOffsets,
                  watermarkEstimator);

          try (KafkaConsumer<Object, Object> kafka =
              createConsumer(name, offsets, name != null ? listener : null, position)) {

            consumerRef.set(kafka);

            // we need to poll first to initialize kafka assignments and rebalance listener
            ConsumerRecords<Object, Object> poll;
            Map<TopicPartition, Long> endOffsets;

            do {
              poll = kafka.poll(pollDuration);
              endOffsets = stopAtCurrent ? findNonEmptyEndOffsets(kafka) : null;
              if (log.isDebugEnabled()) {
                log.debug(
                    "End offsets of current assignment {}: {}", kafka.assignment(), endOffsets);
              }
            } while (poll.isEmpty()
                && accessor.isTopicRegex()
                && kafka.assignment().isEmpty()
                && !shutdown.get()
                && !Thread.currentThread().isInterrupted());

            notifyAssignedPartitions(kafka, listener);

            readyLatch.countDown();

            AtomicReference<Throwable> error = new AtomicReference<>();
            long pollTimeMs = 0L;

            do {
              logConsumerWatermark(name, offsets, watermarkEstimator, poll.count());
              poll =
                  seekToNewOffsetsIfNeeded(seekOffsets, consumer, watermarkEstimator, kafka, poll);
              long bytesPolled = 0L;
              // increment empty polls on each assigned partition
              kafka
                  .assignment()
                  .forEach(tp -> emptyPolls.compute(tp, (k, v) -> v == null ? 0 : v + 1));
              for (ConsumerRecord<Object, Object> r : poll) {
                bytesPolled += r.serializedKeySize() + r.serializedValueSize();
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                polledOffsets.put(tp, r.offset());
                emptyPolls.put(tp, 0);
                preWrite.accept(tp, r);
                StreamElement ingest = serializer.read(r, getEntityDescriptor());
                if (ingest != null) {
                  watermarkEstimator
                      .get()
                      .update(Objects.requireNonNull(topicPartitionToId.get(tp)), ingest);
                }
                if (log.isDebugEnabled()) {
                  log.debug(
                      "Processing element {} with {}",
                      ingest,
                      ingest == null ? null : ingest.getParsed());
                }
                boolean cont =
                    consumer.consumeWithConfirm(
                        ingest, tp, r.offset(), watermarkEstimator.get(), error::set);
                if (!cont) {
                  log.info("Terminating consumption by request");
                  completed.set(true);
                  shutdown.set(true);
                  break;
                }
                if (stopAtCurrent) {
                  Long end = endOffsets.get(tp);
                  if (end != null && end - 1 <= r.offset()) {
                    log.debug("Reached end of partition {} at offset {}", tp, r.offset());
                    endOffsets.remove(tp);
                  }
                }
              }
              if (!flushCommits(kafka, consumer)) {
                handleRebalanceInOffsetCommit(kafka, listener);
              }
              rethrowErrorIfPresent(name, error);
              log.debug("Current endOffsets {}, polledOffsets {}", endOffsets, polledOffsets);
              terminateIfConsumed(stopAtCurrent, kafka, endOffsets, polledOffsets, completed);

              progressWatermarkOnEmptyPartitions(
                  consumer,
                  emptyPolls,
                  assignmentEndOffsets,
                  polledOffsets,
                  topicPartitionToId,
                  watermarkEstimator);
              throughputLimiter.sleepToLimitThroughput(bytesPolled, pollTimeMs);
              long startTime = System.currentTimeMillis();
              poll = kafka.poll(pollDuration);
              pollTimeMs = System.currentTimeMillis() - startTime;
            } while (!shutdown.get()
                && !completed.get()
                && !Thread.currentThread().isInterrupted());
            if (log.isDebugEnabled()) {
              log.debug(
                  "Terminating poll loop for assignment {}: shutdown: {}, completed: {}, interrupted: {}",
                  kafka.assignment(),
                  shutdown.get(),
                  completed.get(),
                  Thread.currentThread().isInterrupted());
            }
            if (!Thread.currentThread().isInterrupted() && !shutdown.get()) {
              consumer.onCompleted();
            } else {
              consumer.onCancelled();
            }
            completedLatch.countDown();
          } catch (InterruptedException ex) {
            log.info("Interrupted while polling kafka. Terminating consumption.", ex);
            Thread.currentThread().interrupt();
            consumer.onCancelled();
            completedLatch.countDown();
          } catch (Throwable err) {
            completedLatch.countDown();
            log.error("Error processing consumer {}", name, err);
            if (consumer.onError(err) && !shutdown.get()) {
              try {
                submitConsumerWithObserver(
                    name, offsets, position, stopAtCurrent, preWrite, consumer, executor, handle);
              } catch (InterruptedException ex) {
                log.warn("Interrupted while restarting observer");
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
              }
            }
          } finally {
            readyLatch.countDown();
          }
        });
    readyLatch.await();
  }

  private void progressWatermarkOnEmptyPartitions(
      ElementConsumer<Object, Object> consumer,
      Map<TopicPartition, Integer> emptyPolls,
      Map<TopicPartition, Long> endOffsets,
      Map<TopicPartition, Long> polledOffsets,
      Map<TopicPartition, Integer> topicPartitionToId,
      AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator) {

    int partitions = emptyPolls.size();
    List<TopicPartition> idlingPartitions =
        emptyPolls.entrySet().stream()
            .filter(e -> e.getValue() >= partitions)
            .filter(
                e ->
                    MoreObjects.firstNonNull(polledOffsets.get(e.getKey()), -1L) + 1
                        >= MoreObjects.firstNonNull(endOffsets.get(e.getKey()), 0L))
            .map(Entry::getKey)
            .collect(Collectors.toList());

    idlingPartitions.forEach(tp -> watermarkEstimator.get().idle(topicPartitionToId.get(tp)));

    // all partitions are idle
    if (idlingPartitions.size() == partitions) {
      Optional.ofNullable(watermarkEstimator.get()).ifPresent(consumer::onIdle);
    }
  }

  private static void notifyAssignedPartitions(
      KafkaConsumer<?, ?> kafka, ConsumerRebalanceListener listener) {

    Set<TopicPartition> assignment = kafka.assignment();
    log.debug("Assignment before notification is {}", assignment);
    if (!assignment.isEmpty()) {
      listener.onPartitionsRevoked(assignment);
      listener.onPartitionsAssigned(assignment);
    }
  }

  private void handleRebalanceInOffsetCommit(
      KafkaConsumer<Object, Object> kafka, ConsumerRebalanceListener listener) {

    Set<TopicPartition> assigned = kafka.assignment();
    listener.onPartitionsRevoked(assigned);
    listener.onPartitionsAssigned(assigned);
    Map<TopicPartition, OffsetAndMetadata> committed = kafka.committed(assigned);
    committed.forEach(kafka::seek);
  }

  private ConsumerRecords<Object, Object> seekToNewOffsetsIfNeeded(
      final List<TopicOffset> seekOffsets,
      final ElementConsumer<Object, Object> consumer,
      final AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator,
      final KafkaConsumer<Object, Object> kafka,
      final ConsumerRecords<Object, Object> poll) {

    synchronized (seekOffsets) {
      if (!seekOffsets.isEmpty()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<Offset> toSeek = (List) seekOffsets;
        Utils.seekToOffsets(toSeek, kafka);
        consumer.onAssign(
            kafka,
            kafka.assignment().stream()
                .map(
                    tp ->
                        new TopicOffset(
                            new PartitionWithTopic(tp.topic(), tp.partition()),
                            kafka.position(tp),
                            watermarkEstimator.get().getWatermark()))
                .collect(Collectors.toList()));
        log.info("Seeked consumer to offsets {} as requested", seekOffsets);
        seekOffsets.clear();
        return ConsumerRecords.empty();
      }
    }
    return poll;
  }

  private void logConsumerWatermark(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator,
      int polledCount) {

    if (log.isDebugEnabled()) {
      log.debug(
          "Current watermark of consumer name {} with offsets {} on {} poll'd records is {}",
          name,
          offsets,
          polledCount,
          Optional.ofNullable(watermarkEstimator.get())
              .map(PartitionedWatermarkEstimator::getWatermark)
              .orElse(Watermarks.MIN_WATERMARK));
    }
  }

  private void rethrowErrorIfPresent(
      @Nullable String consumerName, AtomicReference<Throwable> error) {
    Throwable errorThrown = error.getAndSet(null);
    if (errorThrown != null) {
      log.warn("Error during processing {}", consumerName, errorThrown);
      throw new RuntimeException(errorThrown);
    }
  }

  private void terminateIfConsumed(
      boolean stopAtCurrent,
      KafkaConsumer<?, ?> consumer,
      Map<TopicPartition, Long> endOffsets,
      Map<TopicPartition, Long> polledOffsets,
      AtomicBoolean completed) {

    if (stopAtCurrent) {
      if (polledOffsets.entrySet().stream()
          .allMatch(
              e -> MoreObjects.firstNonNull(endOffsets.get(e.getKey()), -1L) <= e.getValue())) {
        log.info(
            "Assignment {} reached end of current data. Terminating consumption.",
            consumer.assignment());
        completed.set(true);
      }
    }
  }

  /**
   * @return {@code false} if {@link RebalanceInProgressException} was caught and offsets could not
   *     be committed, {@code true} otherwise.
   */
  private boolean flushCommits(
      final KafkaConsumer<Object, Object> kafka, ElementConsumer<?, ?> consumer) {

    try {
      Map<TopicPartition, OffsetAndMetadata> commitMap = consumer.prepareOffsetsForCommit();
      if (!commitMap.isEmpty()) {
        kafka.commitSync(commitMap);
      }
      return true;
    } catch (RebalanceInProgressException ex) {
      log.info(
          "Caught {}. Resetting the consumer to the last committed position",
          ex.getClass().getSimpleName());
      return false;
    }
  }

  private ObserveHandle createObserveHandle(
      AtomicBoolean shutdown,
      List<TopicOffset> seekOffsets,
      ElementConsumer<?, ?> consumer,
      CountDownLatch readyLatch,
      CountDownLatch completedLatch) {

    return new ObserveHandle() {

      @Override
      public void close() {
        shutdown.set(true);
        ExceptionUtils.ignoringInterrupted(completedLatch::await);
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<Offset> getCommittedOffsets() {
        return (List) consumer.getCommittedOffsets();
      }

      @SuppressWarnings("unchecked")
      @Override
      public void resetOffsets(List<Offset> offsets) {
        seekOffsets.addAll((Collection) offsets);
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<Offset> getCurrentOffsets() {
        return (List) consumer.getCurrentOffsets();
      }

      @Override
      public void waitUntilReady() throws InterruptedException {
        readyLatch.await();
      }
    };
  }

  private Map<TopicPartition, Long> findNonEmptyEndOffsets(final KafkaConsumer<?, ?> kafka) {
    Set<TopicPartition> assignment = kafka.assignment();
    Map<TopicPartition, Long> beginning = kafka.beginningOffsets(assignment);
    return kafka.endOffsets(assignment).entrySet().stream()
        .filter(entry -> beginning.get(entry.getKey()) < entry.getValue())
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private KafkaConsumer<Object, Object> createConsumer() {
    return createConsumer(UUID.randomUUID().toString(), null, null, Position.NEWEST);
  }

  /** Create kafka consumer for the data. */
  @VisibleForTesting
  KafkaConsumer<Object, Object> createConsumer(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      @Nullable ConsumerRebalanceListener listener,
      Position position) {

    Preconditions.checkArgument(
        name != null || listener == null,
        "Please use either named group (with listener) or offsets without listener");
    KafkaConsumerFactory<Object, Object> factory = accessor.createConsumerFactory();
    final KafkaConsumer<Object, Object> consumer;

    if ("".equals(name)) {
      throw new IllegalArgumentException("Consumer group cannot be empty string");
    }
    if (name != null) {
      consumer = factory.create(name, position, listener);
    } else if (offsets != null) {
      List<Partition> partitions =
          offsets.stream().map(Offset::getPartition).collect(Collectors.toList());
      consumer = factory.create(position, partitions);
    } else {
      throw new IllegalArgumentException("Need either name or offsets to observe");
    }
    if (!accessor.isTopicRegex()) {
      validateTopic(consumer, accessor.getTopic());
    }
    if (position == Position.OLDEST) {
      // seek all partitions to oldest data
      if (offsets == null) {
        boolean emptyPoll = true;
        if (consumer.assignment().isEmpty()) {
          // If we don't find assignment within timeout, poll results in IllegalStateException.
          // https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior
          emptyPoll =
              consumer.poll(Duration.ofMillis(accessor.getAssignmentTimeoutMillis())).isEmpty();
        }
        final Set<TopicPartition> assignment = consumer.assignment();
        final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
        if (committed.values().stream().allMatch(Objects::isNull)) {
          log.info("Seeking consumer name {} to beginning of partitions {}", name, assignment);
          consumer.seekToBeginning(assignment);
        } else {
          if (!emptyPoll) {
            log.info("Seeking consumer name {} to committed offsets {}", name, committed);
            committed.forEach(consumer::seek);
          }
        }
      } else {
        List<TopicPartition> tps =
            offsets.stream()
                .map(TopicOffset.class::cast)
                .map(p -> new TopicPartition(p.getPartition().getTopic(), p.getPartition().getId()))
                .collect(Collectors.toList());
        log.info("Seeking given partitions {} to the beginning", tps);
        consumer.seekToBeginning(tps);
      }
    } else if (position == Position.CURRENT) {
      Preconditions.checkArgument(
          offsets != null, "Please use %s only with specified offsets", position);
      log.info("Seeking to given offsets {}", offsets);
      Utils.seekToOffsets(offsets, consumer);
    } else {
      log.info("Starting to process kafka partitions from newest data");
    }
    return consumer;
  }

  @VisibleForTesting
  void validateTopic(KafkaConsumer<?, ?> consumer, String topicToValidate) {
    List<PartitionInfo> partitions = consumer.partitionsFor(topicToValidate);
    Preconditions.checkArgument(
        partitions != null && !partitions.isEmpty(),
        "Received null or empty partitions for topic [%s]. "
            + "Please check that the topic exists and has at least one partition.",
        topicToValidate);
  }

  @Override
  public boolean hasExternalizableOffsets() {
    return true;
  }

  @Override
  public OffsetExternalizer getOffsetExternalizer() {
    return new TopicOffsetExternalizer();
  }

  @Override
  public Factory asFactory() {
    final KafkaAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new KafkaLogReader(accessor, context);
  }

  private static Collection<Offset> createDefaultOffsets(Collection<Partition> partitions) {
    if (partitions != null) {
      return partitions.stream()
          .map(p -> new TopicOffset((PartitionWithTopic) p, -1, Long.MIN_VALUE))
          .collect(Collectors.toList());
    }
    return null;
  }

  private static ObserveHandle dynamicHandle(AtomicReference<ObserveHandle> proxy) {
    return new ObserveHandle() {
      @Override
      public void close() {
        proxy.get().close();
      }

      @Override
      public List<Offset> getCommittedOffsets() {
        return proxy.get().getCommittedOffsets();
      }

      @Override
      public void resetOffsets(List<Offset> offsets) {
        proxy.get().resetOffsets(offsets);
      }

      @Override
      public List<Offset> getCurrentOffsets() {
        return proxy.get().getCurrentOffsets();
      }

      @Override
      public void waitUntilReady() throws InterruptedException {
        proxy.get().waitUntilReady();
      }
    };
  }

  private OffsetCommitter<TopicPartition> createOffsetCommitter() {
    return new OffsetCommitter<>(
        accessor.getLogStaleCommitIntervalMs(), accessor.getAutoCommitIntervalMs());
  }

  // create rebalance listener from consumer
  private ConsumerRebalanceListener listener(
      String name,
      AtomicReference<KafkaConsumer<Object, Object>> kafka,
      ElementConsumer<Object, Object> consumer,
      Map<TopicPartition, Integer> topicPartitionToId,
      Map<TopicPartition, Integer> emptyPolls,
      Map<TopicPartition, Long> polledOffsets,
      Map<TopicPartition, Long> endOffsets,
      AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator) {

    return new ConsumerRebalanceListener() {

      private final Set<TopicPartition> currentlyAssigned = new HashSet<>();

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        currentlyAssigned.removeAll(parts);
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        currentlyAssigned.addAll(parts);
        log.info("Consumer {} has assigned partitions {}", name, currentlyAssigned);
        topicPartitionToId.clear();
        AtomicInteger id = new AtomicInteger();
        emptyPolls.clear();
        polledOffsets.clear();
        endOffsets.clear();
        Optional.ofNullable(kafka.get()).ifPresent(k -> endOffsets.putAll(k.endOffsets(parts)));

        currentlyAssigned.forEach(p -> topicPartitionToId.put(p, id.getAndIncrement()));

        if (currentlyAssigned.isEmpty()) {
          watermarkEstimator.set(createWatermarkEstimatorForEmptyParts());
        } else {
          watermarkEstimator.set(
              new MinimalPartitionWatermarkEstimator(
                  currentlyAssigned.stream()
                      .collect(
                          toMap(topicPartitionToId::get, item -> createWatermarkEstimator()))));
        }

        Optional.ofNullable(kafka.get())
            .ifPresent(
                c -> {
                  List<TopicOffset> newOffsets =
                      name != null
                          ? getCommittedTopicOffsets(currentlyAssigned, c)
                          : getCurrentTopicOffsets(currentlyAssigned, c);
                  newOffsets.stream()
                      .filter(o -> o.getOffset() >= 0)
                      .forEach(
                          o ->
                              polledOffsets.put(
                                  new TopicPartition(
                                      o.getPartition().getTopic(), o.getPartition().getPartition()),
                                  o.getOffset() - 1));
                  consumer.onAssign(c, newOffsets);
                });
      }

      List<TopicOffset> getCurrentTopicOffsets(
          Collection<TopicPartition> parts, KafkaConsumer<Object, Object> c) {
        return parts.stream()
            .map(
                tp ->
                    new TopicOffset(
                        new PartitionWithTopic(tp.topic(), tp.partition()),
                        c.position(tp),
                        watermarkEstimator.get().getWatermark()))
            .collect(Collectors.toList());
      }

      List<TopicOffset> getCommittedTopicOffsets(
          Collection<TopicPartition> parts, KafkaConsumer<Object, Object> c) {

        Map<TopicPartition, OffsetAndMetadata> committed =
            new HashMap<>(c.committed(new HashSet<>(parts)));
        for (TopicPartition tp : parts) {
          committed.putIfAbsent(tp, null);
        }
        return committed.entrySet().stream()
            .map(
                entry -> {
                  final long offset = entry.getValue() == null ? 0L : entry.getValue().offset();
                  return new TopicOffset(
                      new PartitionWithTopic(entry.getKey().topic(), entry.getKey().partition()),
                      offset,
                      watermarkEstimator.get().getWatermark());
                })
            .collect(Collectors.toList());
      }

      private WatermarkEstimator createWatermarkEstimator() {
        return accessor.getWatermarkConfiguration().getWatermarkEstimatorFactory().create();
      }
    };
  }

  private static PartitionedWatermarkEstimator createWatermarkEstimatorForEmptyParts() {
    return () -> Watermarks.MAX_WATERMARK;
  }
}
