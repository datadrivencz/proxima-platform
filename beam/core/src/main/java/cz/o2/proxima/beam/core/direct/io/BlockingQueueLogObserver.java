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

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.time.WatermarkSupplier;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.LogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link cz.o2.proxima.direct.core.commitlog.CommitLogObserver} that caches data in {@link
 * BlockingQueue}.
 */
@Slf4j
abstract class BlockingQueueLogObserver<
        OffsetT extends Serializable, ContextT extends LogObserver.OnNextContext<OffsetT>>
    implements LogObserver<OffsetT, ContextT> {

  private static final int DEFAULT_CAPACITY = 100;

  static CommitLogObserver createCommitLogObserver(String name, long startingWatermark) {
    return createCommitLogObserver(name, Long.MAX_VALUE, startingWatermark);
  }

  static CommitLogObserver createCommitLogObserver(
      String name, long limit, long startingWatermark) {
    return createCommitLogObserver(name, limit, startingWatermark, DEFAULT_CAPACITY);
  }

  static CommitLogObserver createCommitLogObserver(
      String name, long limit, long startingWatermark, int queueCapacity) {
    return new CommitLogObserver(name, limit, startingWatermark, queueCapacity);
  }

  static BatchLogObserver createBatchLogObserver(String name, long startingWatermark) {
    return createBatchLogObserver(name, Long.MAX_VALUE, startingWatermark);
  }

  static BatchLogObserver createBatchLogObserver(String name, long limit, long startingWatermark) {
    return createBatchLogObserver(name, limit, startingWatermark, DEFAULT_CAPACITY);
  }

  static BatchLogObserver createBatchLogObserver(
      String name, long limit, long startingWatermark, int queueCapacity) {
    return new BatchLogObserver(name, limit, startingWatermark, queueCapacity);
  }

  @Internal
  interface UnifiedContext extends OffsetCommitter, WatermarkSupplier {

    boolean isBounded();

    @Nullable
    Offset getOffset();
  }

  static class BatchLogObserver
      extends BlockingQueueLogObserver<
          cz.o2.proxima.direct.core.batch.Offset,
          cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext>
      implements cz.o2.proxima.direct.core.batch.BatchLogObserver {

    public BatchLogObserver(String name, long limit, long startingWatermark, int capacity) {
      super(name, limit, startingWatermark, capacity);
    }

    @Override
    public boolean onNext(
        StreamElement element,
        cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext context) {

      if (log.isDebugEnabled()) {
        log.debug(
            "{}: Received next element {} on partition {}",
            getName(),
            element,
            context.getPartition());
      }
      return enqueue(element, new BatchLogObserverUnifiedContext(context));
    }

    @Override
    public void onInterrupted() {
      stop();
    }
  }

  static class CommitLogObserver
      extends BlockingQueueLogObserver<
          Offset, cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext>
      implements cz.o2.proxima.direct.core.commitlog.CommitLogObserver {

    public CommitLogObserver(String name, long limit, long startingWatermark, int capacity) {
      super(name, limit, startingWatermark, capacity);
    }

    @Override
    public boolean onNext(
        StreamElement element,
        cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext context) {
      if (log.isDebugEnabled()) {
        log.debug(
            "{}: Received next element {} at watermark {} offset {}",
            getName(),
            element,
            context.getWatermark(),
            context.getOffset());
      }
      return enqueue(element, new LogObserverUnifiedContext(context));
    }

    @Override
    public void onIdle(OnIdleContext context) {
      onIdle(context.getWatermark());
    }
  }

  @VisibleForTesting
  static class LogObserverUnifiedContext implements UnifiedContext {

    private static final long serialVersionUID = 1L;

    private final cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext context;

    LogObserverUnifiedContext(
        cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext context) {
      this.context = context;
    }

    @Override
    public void commit(boolean success, Throwable error) {
      context.commit(success, error);
    }

    @Override
    public void nack() {
      context.nack();
    }

    @Override
    public long getWatermark() {
      return context.getWatermark();
    }

    @Override
    public boolean isBounded() {
      return false;
    }

    @Override
    public Offset getOffset() {
      return Objects.requireNonNull(context.getOffset());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("offset", getOffset())
          .add("watermark", getWatermark())
          .toString();
    }
  }

  @ToString
  private static class BatchLogObserverUnifiedContext implements UnifiedContext {

    private static final long serialVersionUID = 1L;

    private final cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext context;

    private BatchLogObserverUnifiedContext(
        cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext context) {
      this.context = context;
    }

    @Override
    public void commit(boolean success, Throwable error) {
      if (error != null) {
        throw new RuntimeException(error);
      }
    }

    @Override
    public void nack() {
      // nop
    }

    @Override
    public long getWatermark() {
      return context.getWatermark();
    }

    @Override
    public boolean isBounded() {
      return true;
    }

    @Nullable
    @Override
    public Offset getOffset() {
      return null;
    }
  }

  @Getter private final String name;
  private final AtomicReference<Throwable> error = new AtomicReference<>();
  private final AtomicLong watermark;

  @Getter(AccessLevel.PROTECTED)
  private final BlockingQueue<Pair<StreamElement, UnifiedContext>> queue;

  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicBoolean nackAllIncoming = new AtomicBoolean(false);
  private final AtomicReference<UnifiedContext> lastWrittenContext = new AtomicReference<>();
  private final AtomicReference<UnifiedContext> lastReadContext = new AtomicReference<>();
  @Nullable private Pair<StreamElement, UnifiedContext> peekElement = null;
  private long limit;
  private boolean cancelled = false;
  private transient CountDownLatch cancelledLatch = new CountDownLatch(1);

  private BlockingQueueLogObserver(String name, long limit, long startingWatermark, int capacity) {
    Preconditions.checkArgument(limit >= 0, "Please provide non-negative limit");

    this.name = Objects.requireNonNull(name);
    this.watermark = new AtomicLong(startingWatermark);
    this.limit = limit;
    queue = new ArrayBlockingQueue<>(capacity);
    log.debug("Created {}", this);
  }

  @Override
  public boolean onError(Throwable error) {
    this.error.set(error);
    // unblock any waiting thread
    putToQueue(null, null);
    return false;
  }

  public UnifiedContext getLastReadContext() {
    return lastReadContext.get();
  }

  public UnifiedContext getLastWrittenContext() {
    return lastWrittenContext.get();
  }

  boolean enqueue(StreamElement element, UnifiedContext context) {
    try {
      Preconditions.checkArgument(element != null && context != null);
      lastWrittenContext.set(context);
      if (limit-- > 0) {
        return putToQueue(element, context);
      }
      log.debug(
          "{}: Terminating consumption due to limit {} while enqueuing {}", name, limit, element);
    } finally {
      if (nackAllIncoming.get() && context != null) {
        context.nack();
      }
    }
    return false;
  }

  @Override
  public void onCancelled() {
    cancelled = true;
    cancelledLatch.countDown();
    log.debug("{}: Cancelled consumption by request.", name);
  }

  @Override
  public void onCompleted() {
    log.debug("{}: Finished reading from observer", name);
    cancelledLatch.countDown();
    putToQueue(null, null);
  }

  private boolean putToQueue(@Nullable StreamElement element, @Nullable UnifiedContext context) {
    Pair<StreamElement, UnifiedContext> p = Pair.of(element, context);
    while (!stopped.get() && !cancelled) {
      try {
        if (queue.offer(p, 500, TimeUnit.MILLISECONDS)) {
          return true;
        }
      } catch (InterruptedException ex) {
        log.debug("Caught interrupted exception", ex);
        stop(true, false);
        Thread.currentThread().interrupt();
      }
    }
    log.debug("{}: Finishing consumption due to source being stopped", name);
    return false;
  }

  void onIdle(long watermark) {
    if (queue.isEmpty()) {
      updateAndLogWatermark(watermark);
    }
  }

  /**
   * Take next element without blocking.
   *
   * @return element that was taken without blocking or {@code null} otherwise
   */
  @Nullable
  StreamElement take() {
    if (!stopped.get()) {
      try {
        if (peekElement(0, TimeUnit.MILLISECONDS)) {
          return consumePeek();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  @Nullable
  public UnifiedContext getPeekContext() {
    if (peekElement != null) {
      return peekElement.getSecond();
    }
    return null;
  }

  /**
   * Take next element waiting for input if necessary.
   *
   * @return element that was taken or {@code null} on end of input
   */
  @Nullable
  StreamElement takeBlocking(long timeout, TimeUnit unit) throws InterruptedException {
    if (!stopped.get() && peekElement(timeout, unit)) {
      return consumePeek();
    }
    return null;
  }

  /**
   * Take next element waiting for input if necessary.
   *
   * @return element that was taken or {@code null} on end of input
   */
  @Nullable
  StreamElement takeBlocking() throws InterruptedException {
    while (!stopped.get()) {
      if (peekElement(50, TimeUnit.MILLISECONDS)) {
        return consumePeek();
      }
    }
    return null;
  }

  /**
   * Peek element or return {@code null} if queue is empty.
   *
   * @return {@code true} if queue is not empty after the call
   */
  public boolean peekElement() {
    try {
      return peekElement(0, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  /**
   * Peek element or return {@code null} if queue is empty within given timeout
   *
   * @param timeout the timeout
   * @param unit time unit of timeout
   * @throws InterruptedException when interrupted
   */
  public boolean peekElement(long timeout, TimeUnit unit) throws InterruptedException {
    if (peekElement == null) {
      peekElement = queue.poll(timeout, unit);
    }
    if (peekElement != null && peekElement.getFirst() == null) {
      // we have end of input mark
      consumePeek();
      return false;
    }
    return peekElement != null;
  }

  @Nullable
  private StreamElement consumePeek() {
    final @Nullable Pair<StreamElement, UnifiedContext> taken = peekElement;
    peekElement = null;
    if (taken != null && taken.getFirst() != null) {
      lastReadContext.set(taken.getSecond());
      if (lastReadContext.get() != null) {
        updateAndLogWatermark(lastReadContext.get().getWatermark());
      }
      log.debug(
          "{}: Consuming taken element {} with offset {}",
          name,
          taken.getFirst(),
          lastReadContext.get() != null ? lastReadContext.get().getOffset() : null);
      return taken.getFirst();
    } else if (taken != null) {
      // we have read the finalizing marker
      updateAndLogWatermark(Watermarks.MAX_WATERMARK);
      stopped.set(true);
    }
    return null;
  }

  @Nullable
  Throwable getError() {
    return error.get();
  }

  long getWatermark() {
    return watermark.get();
  }

  void stop() {
    stop(true);
  }

  void stop(boolean nack) {
    stop(nack, true);
  }

  void stop(boolean nack, boolean wait) {
    nackAllIncoming.set(nack);
    stopped.set(true);
    if (nack) {
      nack();
    } else {
      queue.clear();
    }
    if (getWatermark() < Watermarks.MAX_WATERMARK && wait) {
      ExceptionUtils.ignoringInterrupted(() -> cancelledLatch.await(1, TimeUnit.SECONDS));
    }
  }

  private void nack() {
    List<Pair<StreamElement, UnifiedContext>> drop = new ArrayList<>();
    if (peekElement != null) {
      drop.add(peekElement);
      peekElement = null;
    }
    queue.drainTo(drop);
    drop.stream().map(Pair::getSecond).filter(Objects::nonNull).forEach(UnifiedContext::nack);
  }

  void clearIncomingQueue() {
    queue.clear();
  }

  private void updateAndLogWatermark(long newWatermark) {
    if (!cancelled) {
      if (watermark.get() < newWatermark) {
        if (log.isDebugEnabled()) {
          log.debug(
              "{}: Watermark updated from {} to {}",
              name,
              Instant.ofEpochMilli(watermark.get()),
              Instant.ofEpochMilli(newWatermark));
        }
        watermark.set(newWatermark);
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("limit", limit).toString();
  }

  Object readResolve() {
    this.cancelledLatch = new CountDownLatch(1);
    return this;
  }
}
