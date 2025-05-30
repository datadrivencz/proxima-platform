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

import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asOnIdleContext;
import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asOnNextContext;
import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asRepartitionContext;

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.time.WatermarkSupplier;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Placeholder class for {@link ElementConsumer ElementConsumers}. */
@Slf4j
class ElementConsumers {

  private abstract static class ConsumerBase<K, V> implements ElementConsumer<K, V> {

    final Map<TopicPartition, Long> committed = Collections.synchronizedMap(new HashMap<>());
    final Map<TopicPartition, Long> processing = Collections.synchronizedMap(new HashMap<>());

    long watermark;

    @Override
    public void onCompleted() {
      observer().onCompleted();
    }

    @Override
    public void onCancelled() {
      observer().onCancelled();
    }

    @Override
    public boolean onError(Throwable err) {
      return observer().onError(err);
    }

    @Override
    public void onAssign(KafkaConsumer<K, V> consumer, Collection<TopicOffset> offsets) {
      committed.clear();
      committed.putAll(
          offsets.stream()
              .collect(
                  Collectors.toMap(
                      o ->
                          new TopicPartition(o.getPartition().getTopic(), o.getPartition().getId()),
                      TopicOffset::getOffset)));
      processing.clear();
      offsets.forEach(
          tp ->
              processing.put(
                  new TopicPartition(tp.getPartition().getTopic(), tp.getPartition().getId()),
                  tp.getOffset() - 1));
      watermark = Watermarks.MIN_WATERMARK;
    }

    @Override
    public List<TopicOffset> getCurrentOffsets() {
      return TopicOffset.fromMap(processing, watermark);
    }

    @Override
    public List<TopicOffset> getCommittedOffsets() {
      return TopicOffset.fromMap(committed, watermark);
    }

    abstract CommitLogObserver observer();
  }

  static final class OnlineConsumer<K, V> extends ConsumerBase<K, V> {

    private final CommitLogObserver observer;
    private final OffsetCommitter<TopicPartition> committer;
    private final Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit;

    OnlineConsumer(
        CommitLogObserver observer,
        OffsetCommitter<TopicPartition> committer,
        Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit) {

      this.observer = observer;
      this.committer = committer;
      this.prepareCommit = prepareCommit;
    }

    @Override
    public boolean consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp,
        long offset,
        WatermarkSupplier watermarkSupplier,
        Consumer<Throwable> errorHandler) {

      processing.put(tp, offset);
      watermark = watermarkSupplier.getWatermark();
      if (element != null && watermark < Watermarks.MAX_WATERMARK) {
        return observer.onNext(
            element,
            asOnNextContext(
                (succ, exc) -> {
                  if (succ) {
                    confirmOffset(tp, offset);
                  } else {
                    errorHandler.accept(
                        Objects.requireNonNullElseGet(
                            exc,
                            () ->
                                new IllegalStateException(
                                    "Either confirm processing or return exception.")));
                  }
                },
                new TopicOffset(
                    new PartitionWithTopic(tp.topic(), tp.partition()), offset, watermark)));
      }
      confirmOffset(tp, offset);
      return watermark < Watermarks.MAX_WATERMARK;
    }

    private void confirmOffset(TopicPartition tp, long offset) {
      committed.compute(tp, (k, v) -> v == null || v <= offset ? offset + 1 : v);
      committer.confirm(tp, offset);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareOffsetsForCommit() {
      return prepareCommit.apply();
    }

    @Override
    CommitLogObserver observer() {
      return observer;
    }

    @Override
    public void onAssign(KafkaConsumer<K, V> consumer, Collection<TopicOffset> offsets) {
      super.onAssign(consumer, offsets);
      committer.clear();
      observer.onRepartition(
          asRepartitionContext(
              offsets.stream().map(TopicOffset::getPartition).collect(Collectors.toList())));
    }

    @Override
    public void onStart() {
      committer.clear();
    }

    @Override
    public void onIdle(WatermarkSupplier watermarkSupplier) {
      this.watermark = watermarkSupplier.getWatermark();
      observer.onIdle(asOnIdleContext(() -> watermark));
    }
  }

  static final class BulkConsumer<K, V> extends ConsumerBase<K, V> {

    private final CommitLogObserver observer;
    private final BiConsumer<TopicPartition, Long> commit;
    private final Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit;
    private final Runnable onStart;

    BulkConsumer(
        CommitLogObserver observer,
        BiConsumer<TopicPartition, Long> commit,
        Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit,
        Runnable onStart) {

      this.observer = observer;
      this.commit = commit;
      this.prepareCommit = prepareCommit;
      this.onStart = onStart;
    }

    @Override
    public boolean consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp,
        long offset,
        WatermarkSupplier watermarkSupplier,
        Consumer<Throwable> errorHandler) {

      processing.put(tp, offset);
      watermark = watermarkSupplier.getWatermark();
      if (element != null) {
        return observer.onNext(element, context(tp, offset, watermarkSupplier, errorHandler));
      }
      return true;
    }

    private OnNextContext context(
        TopicPartition tp,
        long offset,
        WatermarkSupplier watermarkSupplier,
        Consumer<Throwable> errorHandler) {

      Map<TopicPartition, Long> toCommit = new HashMap<>(this.processing);
      return asOnNextContext(
          (succ, err) -> {
            if (succ) {
              toCommit.forEach(
                  (part, off) ->
                      committed.compute(
                          part, (k, v) -> Math.max(MoreObjects.firstNonNull(v, 0L), off + 1)));
              committed.forEach(commit::accept);
            } else if (err != null) {
              errorHandler.accept(err);
            }
          },
          new TopicOffset(
              new PartitionWithTopic(tp.topic(), tp.partition()),
              offset,
              watermarkSupplier.getWatermark()));
    }

    @Override
    CommitLogObserver observer() {
      return observer;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareOffsetsForCommit() {
      return prepareCommit.apply();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onAssign(KafkaConsumer<K, V> consumer, Collection<TopicOffset> offsets) {
      super.onAssign(consumer, offsets);
      observer.onRepartition(
          asRepartitionContext(
              offsets.stream().map(TopicOffset::getPartition).collect(Collectors.toList())));
    }

    @Override
    public void onStart() {
      onStart.run();
    }

    @Override
    public void onIdle(WatermarkSupplier watermarkSupplier) {
      this.watermark = watermarkSupplier.getWatermark();
      observer.onIdle(asOnIdleContext(() -> watermark));
    }
  }

  private ElementConsumers() {
    // nop
  }
}
