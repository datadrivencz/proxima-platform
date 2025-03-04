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

import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asRepartitionContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.functional.UnaryPredicate;
import cz.o2.proxima.core.scheme.SerializationException;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.ObserverUtils;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.commitlog.OffsetExternalizer;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * A bounded {@link CommitLogReader} containing predefined data.
 *
 * <p>This is very simplistic implementation which just pushes all data to the provided observer.
 */
public class ListCommitLog implements CommitLogReader {

  private static final Partition PARTITION = () -> 0;

  private static final Map<String, List<StreamElement>> UUID_TO_DATA = new ConcurrentHashMap<>();

  private static final Map<String, Map<String, Consumer>> CONSUMERS = new ConcurrentHashMap<>();

  static class ListOffset implements Offset {

    private static final long serialVersionUID = 1L;

    @Getter private final String consumerName;
    @Getter final int offset;
    @Getter final long watermark;

    ListOffset(String consumerName, int offset, long watermark) {
      this.consumerName = Objects.requireNonNull(consumerName);
      this.offset = offset;
      this.watermark = watermark;
    }

    @Override
    public Partition getPartition() {
      return PARTITION;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("offset", offset)
          .add("watermark", watermark)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ListOffset) {
        ListOffset other = (ListOffset) obj;
        return other.offset == this.offset;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int) (offset % Integer.MAX_VALUE);
    }
  }

  static class ListOffsetExternalizer implements OffsetExternalizer {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Override
    public String toJson(Offset offset) {
      try {
        final ListOffset listOffset = (ListOffset) offset;
        final HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("consumer_name", listOffset.consumerName);
        jsonMap.put("offset", listOffset.offset);
        jsonMap.put("watermark", listOffset.watermark);

        return JSON_MAPPER.writeValueAsString(jsonMap);
      } catch (JsonProcessingException e) {
        throw new SerializationException("Offset can't be externalized to Json", e);
      }
    }

    @Override
    public ListOffset fromJson(String json) {
      try {
        final HashMap<String, Object> jsonMap =
            JSON_MAPPER.readValue(json, new TypeReference<HashMap<String, Object>>() {});

        return new ListOffset(
            (String) jsonMap.get("consumer_name"),
            (int) jsonMap.get("offset"),
            ((Number) jsonMap.get("watermark")).longValue());

      } catch (JsonProcessingException e) {
        throw new SerializationException("Offset can't be create from externalized Json", e);
      }
    }
  }

  /**
   * Create the new {@link ListCommitLog}, with externalizable offsets (that are offsets that can be
   * persisted in external system - e.g. a checkpoint - and be sure they represent the same element
   * even upon recovery). Commit-logs with "externalizable offsets" (e.g. Apache Kafka) need not
   * rely on ack() and nack() of elements, as offsets can be taken and recovered independently of
   * the actual acknowledgements. Consumers are free to ack messages nevertheless.
   *
   * @param data the data to be present in the commit log
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog of(List<StreamElement> data, Context context) {
    return of(data, null, context);
  }

  /**
   * Create the new {@link ListCommitLog}, with externalizable offsets (that are offsets that can be
   * persisted in external system - e.g. a checkpoint - and be sure they represent the same element
   * even upon recovery). Commit-logs with "externalizable offsets" (e.g. Apache Kafka) need not
   * rely on ack() and nack() of elements, as offsets can be taken and recovered independently of
   * the actual acknowledgements. Consumers are free to ack messages nevertheless.
   *
   * @param data the data to be present in the commit log
   * @param watermarkEstimator {@link WatermarkEstimator} that will be used to generate watermarks
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog of(
      List<StreamElement> data, @Nullable WatermarkEstimator watermarkEstimator, Context context) {

    return new ListCommitLog(data, watermarkEstimator, context);
  }

  /**
   * Create the new {@link ListCommitLog}, which mimics non-externalizable offsets (that are offsets
   * that cannot be persisted in external system - e.g. a checkpoint - and be sure they represent
   * the same element even upon recovery). Commit-logs with "non-externalizable offsets" (e.g.
   * Google PubSub) rely heavily on ack() and nack() of elements to ensure at-least-once semantics
   * (typically not exactly-once-semantics, because when consumer consumes element and does neither
   * ack() nor nack() it until timeout, the element is resend to another (or the same) consumer).
   *
   * @param data the data to be present in the commit log
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog ofNonExternalizable(List<StreamElement> data, Context context) {
    return ofNonExternalizable(data, null, context);
  }

  /**
   * Create the new {@link ListCommitLog}, which mimics non-externalizable offsets (that are offsets
   * that cannot be persisted in external system - e.g. a checkpoint - and be sure they represent
   * the same element even upon recovery). Commit-logs with "non-externalizable offsets" (e.g.
   * Google PubSub) rely heavily on ack() and nack() of elements to ensure at-least-once semantics
   * (typically not exactly-once-semantics, because when consumer consumes element and does neither
   * ack() nor nack() it until timeout, the element is resend to another (or the same) consumer).
   *
   * @param data the data to be present in the commit log
   * @param watermarkEstimator {@link WatermarkEstimator} that will be used to generate watermarks
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog ofNonExternalizable(
      List<StreamElement> data, @Nullable WatermarkEstimator watermarkEstimator, Context context) {

    return new ListCommitLog(data, false, watermarkEstimator, context);
  }

  @VisibleForTesting
  static final class ListObserveHandle implements ObserveHandle {

    private final String listUuid;
    @Getter private final String consumerName;

    @Getter private volatile boolean closed = false;

    private final Map<String, Consumer> consumers;
    private volatile int resetTo = -1;

    ListObserveHandle(String listUuid, String consumerName) {
      this.listUuid = listUuid;
      this.consumerName = Objects.requireNonNull(consumerName);
      consumers = CONSUMERS.get(listUuid);
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public List<Offset> getCommittedOffsets() {
      Consumer consumer = Objects.requireNonNull(consumers.get(consumerName));
      return consumer.getCommittedOffsets();
    }

    @Override
    public void resetOffsets(List<Offset> offsets) {
      this.resetTo = ((ListOffset) Iterables.getOnlyElement(offsets)).getOffset();
    }

    @Override
    public List<Offset> getCurrentOffsets() {
      Consumer consumer = Objects.requireNonNull(consumers.get(consumerName));
      return consumer.getCurrentOffsets();
    }

    @Override
    public void waitUntilReady() {}

    @VisibleForTesting
    Consumer getConsumer() {
      return consumers.get(consumerName);
    }

    public int takeResetOffset() {
      int res = this.resetTo;
      resetTo = -1;
      if (res >= 0) {
        Consumer consumer = getConsumer();
        Set<Integer> acked = consumer.getAckedOffsets();
        synchronized (acked) {
          List<Integer> removed =
              acked.stream().filter(off -> off >= res).collect(Collectors.toList());
          acked.removeAll(removed);
        }
      }
      return res;
    }
  }

  @VisibleForTesting
  class Consumer {

    /**
     * UUID of the {@link cz.o2.proxima.direct.core.storage.ListCommitLog} that this consumer reads
     * from.
     */
    @Getter private final String logUuid;

    private final String consumerName;

    @Getter
    private final Set<Integer> inflightOffsets = Collections.synchronizedSet(new HashSet<>());

    @Getter private final Set<Integer> ackedOffsets = Collections.synchronizedSet(new HashSet<>());

    private final Map<Integer, OffsetCommitter> offsetToContext = new ConcurrentHashMap<>();

    @Nullable private final WatermarkEstimator watermarkEstimator;

    /** Last offset pushed to consumer. */
    private int currentOffset = 0;

    private Consumer(
        String logUuid, String consumerName, @Nullable WatermarkEstimator watermarkEstimator) {
      this.logUuid = logUuid;
      this.consumerName = consumerName;
      this.watermarkEstimator = watermarkEstimator;
    }

    public long getWatermark() {
      return watermarkEstimator == null ? getWatermarkDefault() : watermarkEstimator.getWatermark();
    }

    private long getWatermarkDefault() {
      List<StreamElement> data = UUID_TO_DATA.get(logUuid);
      long watermark = Long.MAX_VALUE;
      for (int i = externalizableOffsets ? currentOffset : 0; i < data.size(); i++) {
        if (data.get(i).getStamp() < watermark && (externalizableOffsets || !isAcked(i))) {
          watermark = data.get(i).getStamp();
        }
      }
      return watermark;
    }

    List<Offset> getCommittedOffsets() {
      if (externalizableOffsets) {
        return Collections.emptyList();
      }
      return Collections.singletonList(new ListOffset(consumerName, -1, getWatermark()));
    }

    List<Offset> getCurrentOffsets() {
      return Collections.singletonList(
          externalizableOffsets
              ? new ListOffset(consumerName, currentOffset, getWatermark())
              : new ListOffset(consumerName, -1, getWatermark()));
    }

    public void moveCurrentOffset(int offset) {
      inflightOffsets.add(offset);
      currentOffset = Math.max(offset, currentOffset);
    }

    public synchronized void ack(int offset) {
      ackedOffsets.add(offset);
      if (watermarkEstimator != null) {
        watermarkEstimator.update(data().get(offset));
      }
      nack(offset);
    }

    public synchronized void nack(int offset) {
      inflightOffsets.remove(offset);
    }

    OnNextContext asOnNextContext(CommitLogObserver.OffsetCommitter committer, int offset) {
      return createOnNextContext(committer, offset, null);
    }

    OnNextContext asOnNextContextBulk(
        CommitLogObserver.OffsetCommitter committer, int offset, Set<Integer> consumerFedOffsets) {

      return createOnNextContext(committer, offset, consumerFedOffsets);
    }

    private synchronized OnNextContext createOnNextContext(
        CommitLogObserver.OffsetCommitter committer,
        int offset,
        @Nullable Set<Integer> consumerFedOffsets) {

      boolean bulk = consumerFedOffsets != null;
      ListOffset listOffset = new ListOffset(consumerName, offset, getWatermark());
      moveCurrentOffset(offset);
      final CommitLogObserver.OffsetCommitter contextCommitter;
      CommitLogObserver.OffsetCommitter singleCommitter =
          (succ, exc) -> {
            committer.commit(succ, exc);
            if (succ) {
              ack(offset);
            } else {
              nack(offset);
            }
          };
      if (bulk) {
        consumerFedOffsets.add(offset);
        final Set<Integer> toCommit = new HashSet<>(consumerFedOffsets);
        contextCommitter =
            (succ, exc) -> {
              synchronized (Consumer.this) {
                toCommit.stream()
                    .filter(consumerFedOffsets::contains)
                    .map(offsetToContext::remove)
                    .filter(Objects::nonNull)
                    .forEach(p -> p.commit(succ, exc));
                toCommit.forEach(consumerFedOffsets::remove);
              }
            };
      } else {
        contextCommitter = singleCommitter;
      }
      OnNextContext context = ObserverUtils.asOnNextContext(contextCommitter, listOffset);
      offsetToContext.put(offset, singleCommitter);
      return context;
    }

    boolean isAcked(int offset) {
      return ackedOffsets.contains(offset);
    }
  }

  private final String uuid;
  private final boolean externalizableOffsets;
  @Nullable private final WatermarkEstimator watermarkEstimator;
  private final Context context;
  private transient ExecutorService executor;

  private ListCommitLog(
      List<StreamElement> data, @Nullable WatermarkEstimator watermarkEstimator, Context context) {

    this(data, true, watermarkEstimator, context);
  }

  private ListCommitLog(
      List<StreamElement> data,
      boolean externalizableOffsets,
      @Nullable WatermarkEstimator watermarkEstimator,
      Context context) {

    this.uuid = UUID.randomUUID().toString();
    UUID_TO_DATA.put(uuid, Collections.unmodifiableList(new ArrayList<>(data)));
    this.externalizableOffsets = externalizableOffsets;
    this.watermarkEstimator = watermarkEstimator;
    this.context = context;

    Preconditions.checkState(CONSUMERS.put(uuid, new ConcurrentHashMap<>()) == null);
  }

  private ListCommitLog(
      String uuid,
      boolean externalizableOffsets,
      @Nullable WatermarkEstimator watermarkEstimator,
      Context context) {

    this.uuid = uuid;
    this.externalizableOffsets = externalizableOffsets;
    this.watermarkEstimator = watermarkEstimator;
    this.context = context;
  }

  @Override
  public URI getUri() {
    try {
      return new URI("list://" + this);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public List<Partition> getPartitions() {
    return Collections.singletonList(PARTITION);
  }

  @Override
  public ObserveHandle observe(
      @Nullable String name, Position position, CommitLogObserver observer) {
    String consumerName = name == null ? UUID.randomUUID().toString() : name;
    Consumer consumer =
        CONSUMERS
            .get(uuid)
            .computeIfAbsent(
                consumerName, k -> new Consumer(uuid, consumerName, watermarkEstimator));
    ListObserveHandle handle = new ListObserveHandle(uuid, consumerName);
    pushTo(
        (element, offset) -> {
          if (handle.isClosed()) {
            return false;
          }
          final CommitLogObserver.OffsetCommitter committer =
              (succ, exc) -> {
                if (exc != null) {
                  observer.onError(exc);
                }
              };
          final boolean acceptable;
          OnNextContext context = null;
          synchronized (consumer) {
            acceptable =
                (externalizableOffsets
                    || !consumer.getAckedOffsets().contains(offset)
                        && !consumer.getInflightOffsets().contains(offset));
            if (acceptable) {
              context = consumer.asOnNextContext(committer, offset);
            }
          }
          if (acceptable) {
            return observer.onNext(element, context);
          }
          return true;
        },
        externalizableOffsets ? () -> true : allMatchOffset(consumer::isAcked),
        handle::takeResetOffset,
        () -> {},
        observer::onCompleted,
        observer::onCancelled);
    return handle;
  }

  @Override
  public Map<Partition, Offset> fetchOffsets(Position position, List<Partition> partitions) {
    Preconditions.checkArgument(position == Position.NEWEST || position == Position.OLDEST);
    if (position == Position.OLDEST) {
      return partitions.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  p -> new ListOffset(UUID.randomUUID().toString(), 0, Watermarks.MIN_WATERMARK)));
    }
    return partitions.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                p ->
                    new ListOffset(
                        UUID.randomUUID().toString(),
                        UUID_TO_DATA.get(uuid).size() - 1,
                        Watermarks.MIN_WATERMARK)));
  }

  private List<StreamElement> data() {
    return Objects.requireNonNull(UUID_TO_DATA.get(uuid));
  }

  @Override
  public ObserveHandle observePartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    return observe(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulk(
      @Nullable String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

    String consumerName = name == null ? UUID.randomUUID().toString() : name;
    return pushToObserverBulk(consumerName, 0, observer);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    return observeBulk(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer) {

    Set<String> consumers =
        offsets.stream().map(o -> ((ListOffset) o).getConsumerName()).collect(Collectors.toSet());
    final String name = Iterables.getOnlyElement(consumers);
    if (externalizableOffsets) {
      ListOffset offset = (ListOffset) Iterables.getOnlyElement(offsets);
      return pushToObserverBulk(name, offset.getOffset(), observer);
    }
    return pushToObserverBulk(name, o -> true, observer);
  }

  @Override
  public Factory<?> asFactory() {
    final String uuid = this.uuid;
    final Context context = this.context;
    final boolean externalizableOffsets = this.externalizableOffsets;
    final WatermarkEstimator watermarkEstimator = this.watermarkEstimator;
    return repo -> new ListCommitLog(uuid, externalizableOffsets, watermarkEstimator, context);
  }

  @Override
  public boolean hasExternalizableOffsets() {
    return externalizableOffsets;
  }

  @Override
  public OffsetExternalizer getOffsetExternalizer() {
    return new ListOffsetExternalizer();
  }

  private ObserveHandle pushToObserverBulk(
      @Nonnull String name, int skip, CommitLogObserver observer) {

    AtomicInteger skipCounter = new AtomicInteger(skip);
    return pushToObserverBulk(name, offset -> skipCounter.decrementAndGet() < 0, observer);
  }

  private ObserveHandle pushToObserverBulk(
      @Nonnull String name,
      UnaryPredicate<Integer> allowOffsetPredicate,
      CommitLogObserver observer) {

    observer.onRepartition(asRepartitionContext(Collections.singletonList(PARTITION)));
    Consumer consumer =
        CONSUMERS
            .get(uuid)
            .computeIfAbsent(name, k -> new Consumer(uuid, name, watermarkEstimator));
    ListObserveHandle handle = new ListObserveHandle(uuid, name);
    Set<Integer> consumerFedOffsets = Collections.synchronizedSet(new HashSet<>());
    pushTo(
        (element, offset) -> {
          if (handle.isClosed()) {
            return false;
          }
          final CommitLogObserver.OffsetCommitter committer =
              (succ, exc) -> {
                if (exc != null) {
                  observer.onError(exc);
                }
              };
          final boolean acceptable;
          OnNextContext context = null;
          synchronized (consumer) {
            acceptable =
                (externalizableOffsets
                        || !consumer.getAckedOffsets().contains(offset)
                            && !consumer.getInflightOffsets().contains(offset))
                    && allowOffsetPredicate.apply(offset);
            if (acceptable) {
              context = consumer.asOnNextContextBulk(committer, offset, consumerFedOffsets);
            }
          }
          if (acceptable) {
            return observer.onNext(element, context);
          }
          return true;
        },
        externalizableOffsets ? () -> true : allMatchOffset(consumer::isAcked),
        handle::takeResetOffset,
        () -> observer.onRepartition(asRepartitionContext(Collections.singletonList(PARTITION))),
        observer::onCompleted,
        observer::onCancelled);
    return handle;
  }

  private cz.o2.proxima.core.functional.Factory<Boolean> allMatchOffset(
      UnaryPredicate<Integer> offsetPredicate) {
    return () -> IntStream.range(0, data().size()).allMatch(offsetPredicate::apply);
  }

  private void pushTo(
      BiFunction<StreamElement, Integer, Boolean> consumer,
      cz.o2.proxima.core.functional.Factory<Boolean> finishedCheck,
      cz.o2.proxima.core.functional.Factory<Integer> offsetReset,
      Runnable onReset,
      Runnable onFinished,
      Runnable onCancelled) {

    executor()
        .execute(
            () -> {
              List<StreamElement> data = data();
              for (int index = 0; index < data.size(); index++) {
                int reset = offsetReset.apply();
                if (reset >= 0) {
                  index = reset;
                  onReset.run();
                }
                StreamElement el = data.get(index);
                if (!consumer.apply(el, index)) {
                  onCancelled.run();
                  return;
                }
              }
              while (!Thread.currentThread().isInterrupted() && !finishedCheck.apply()) {
                ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(100));
              }
              if (Thread.currentThread().isInterrupted()) {
                onCancelled.run();
              } else {
                onFinished.run();
              }
            });
  }

  private ExecutorService executor() {
    if (executor == null) {
      executor = context.getExecutorService();
    }
    return executor;
  }
}
