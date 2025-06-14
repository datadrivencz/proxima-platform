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
package cz.o2.proxima.direct.core.view;

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.direct.core.randomaccess.RawOffset;
import cz.o2.proxima.direct.core.view.TimeBoundedVersionedCache.Payload;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** A transformation view from {@link CommitLogReader} to {@link CachedView}. */
@Slf4j
public class LocalCachedPartitionedView implements CachedView {

  @Value
  @VisibleForTesting
  static class IntOffset implements RandomOffset {
    int offset;
  }

  private final CommitLogReader reader;
  private final EntityDescriptor entity;

  /** Writer to persist data to. */
  private final OnlineAttributeWriter writer;

  /** Cache for data in memory. */
  private final TimeBoundedVersionedCache cache;

  /** Handle of the observation thread (if any running). */
  private final AtomicReference<ObserveHandle> handle = new AtomicReference<>();

  private BiConsumer<StreamElement, Pair<Long, Object>> updateCallback = (e, old) -> {};

  public LocalCachedPartitionedView(
      EntityDescriptor entity, CommitLogReader reader, OnlineAttributeWriter writer) {

    this(entity, reader, writer, 60_000L);
  }

  public LocalCachedPartitionedView(
      EntityDescriptor entity,
      CommitLogReader reader,
      OnlineAttributeWriter writer,
      long keepCachedDuration) {

    this.cache = new TimeBoundedVersionedCache(entity, keepCachedDuration);
    this.reader = Objects.requireNonNull(reader);
    this.entity = Objects.requireNonNull(entity);
    this.writer = Objects.requireNonNull(writer);
  }

  protected void onCache(StreamElement element, boolean overwrite) {
    final Optional<Object> parsed = element.isDelete() ? Optional.empty() : element.getParsed();
    if (element.isDelete() || parsed.isPresent()) {
      final String attrName;
      if (element.isDeleteWildcard()) {
        attrName = element.getAttributeDescriptor().toAttributePrefix();
      } else {
        attrName = element.getAttribute();
      }
      final boolean updated;
      final Pair<Long, Payload> oldVal;
      synchronized (cache) {
        oldVal = cache.get(element.getKey(), attrName, Long.MAX_VALUE);
        updated = cache.put(element, overwrite);
      }
      if (updated) {
        updateCallback.accept(
            element,
            oldVal != null && !oldVal.getSecond().getData().isDelete()
                ? Pair.of(
                    oldVal.getFirst(), Optionals.get(oldVal.getSecond().getData().getParsed()))
                : null);
      }
    }
  }

  @Override
  public void assign(
      Collection<Partition> partitions,
      BiConsumer<StreamElement, Pair<Long, Object>> updateCallback,
      @Nullable Duration ttl) {

    close();
    this.updateCallback = Objects.requireNonNull(updateCallback);
    BlockingQueue<Optional<Throwable>> errorDuringPrefetch = new ArrayBlockingQueue<>(1);
    AtomicLong prefetchedCount = new AtomicLong();
    final long prefetchStartTime = getCurrentTimeMillis();

    final long ttlMs = ttl == null ? Long.MAX_VALUE : ttl.toMillis();
    CommitLogObserver prefetchObserver =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            log.debug("Prefetched element {} with ttlMs {}", element, ttlMs);
            final long prefetched = prefetchedCount.incrementAndGet();
            if (ttl == null || getCurrentTimeMillis() - element.getStamp() < ttlMs) {
              if (prefetched % 10000 == 0) {
                log.info(
                    "Prefetched so far {} elements in {} millis",
                    prefetched,
                    getCurrentTimeMillis() - prefetchStartTime);
              }
              onCache(element, false);
            }
            context.confirm();
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            log.error("Failed to prefetch data", error);
            ExceptionUtils.unchecked(() -> errorDuringPrefetch.put(Optional.of(error)));
            return false;
          }

          @Override
          public void onCompleted() {
            ExceptionUtils.unchecked(() -> errorDuringPrefetch.put(Optional.empty()));
          }
        };

    CommitLogObserver observer =
        new CommitLogObserver() {

          private long lastCleanup = 0;

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            onCache(element, false);
            context.confirm();
            if (ttl != null) {
              lastCleanup = maybeDoCleanup(lastCleanup, ttlMs);
            }
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            log.error("Error in caching data. Restarting consumption.", error);
            assign(partitions, updateCallback, ttl);
            return false;
          }

          @Override
          public void onIdle(OnIdleContext context) {
            if (ttl != null) {
              lastCleanup = maybeDoCleanup(lastCleanup, ttlMs);
            }
          }
        };

    synchronized (this) {
      try {
        // prefetch the data
        log.info(
            "Starting prefetching old topic data for partitions {} with preUpdate {}",
            partitions.stream()
                .map(p -> String.format("%s[%d]", getUri(), p.getId()))
                .collect(Collectors.toList()),
            updateCallback);
        ObserveHandle h =
            reader.observeBulkPartitions(partitions, Position.OLDEST, true, prefetchObserver);
        errorDuringPrefetch.take().ifPresent(ExceptionUtils::rethrowAsIllegalStateException);
        log.info(
            "Finished prefetching after {} records in {} millis. Starting consumption of updates.",
            prefetchedCount.get(),
            getCurrentTimeMillis() - prefetchStartTime);
        List<Offset> offsets = h.getCommittedOffsets();
        // continue the processing
        handle.set(reader.observeBulkOffsets(offsets, observer));
        handle.get().waitUntilReady();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }
  }

  @VisibleForTesting
  long getCurrentTimeMillis() {
    return System.currentTimeMillis();
  }

  /** Possibly do a cleanup and return timestamp of the last run of the cleanup. */
  private long maybeDoCleanup(long lastCleanup, long ttlMs) {
    long now = getCurrentTimeMillis();
    long cleanTime = now - ttlMs;
    if (cleanTime < lastCleanup) {
      return lastCleanup;
    }
    cache.clearStaleRecords(cleanTime);
    return getCurrentTimeMillis();
  }

  @Override
  public Collection<Partition> getAssigned() {
    if (handle.get() != null) {
      return handle.get().getCommittedOffsets().stream()
          .map(Offset::getPartition)
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  @Override
  public RandomOffset fetchOffset(Listing type, String key) {
    switch (type) {
      case ATTRIBUTE:
        return new RawOffset(key);
      case ENTITY:
        return new IntOffset(cache.findPosition(key));
      default:
        throw new IllegalArgumentException("Unknown listing type " + type);
    }
  }

  @Override
  public <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

    long deleteStamp = Long.MIN_VALUE;
    synchronized (cache) {
      if (desc.isWildcard()) {
        // check for wildcard delete
        Pair<Long, Payload> wildcard = cache.get(key, desc.toAttributePrefix(), stamp);
        if (wildcard != null && wildcard.getSecond().getData().isDelete()) {
          // this is delete
          // move the required stamp after the delete
          deleteStamp = wildcard.getFirst();
        }
      }
      final long filterStamp = deleteStamp;
      return Optional.ofNullable(cache.get(key, attribute, stamp))
          .filter(e -> e.getFirst() >= filterStamp)
          .flatMap(e -> Optional.ofNullable(toKv(e)));
    }
  }

  @Override
  public void scanWildcardAll(
      String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {

    String off = offset == null ? "" : ((RawOffset) offset).getOffset();
    scanWildcardPrefix(key, "", off, stamp, limit, consumer);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    String off = offset == null ? wildcard.toAttributePrefix() : ((RawOffset) offset).getOffset();
    scanWildcardPrefix(key, wildcard.toAttributePrefix(), off, stamp, limit, (Consumer) consumer);
  }

  @SuppressWarnings("unchecked")
  private void scanWildcardPrefix(
      String key,
      String prefix,
      String offset,
      long stamp,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    Preconditions.checkArgument(
        offset.startsWith(prefix),
        "Offset must start with prefix, got offset %s prefix %s",
        offset,
        prefix);
    AtomicInteger missing = new AtomicInteger(limit);
    cache.scan(
        key,
        prefix,
        offset,
        stamp,
        attr -> {
          AttributeDescriptor<?> desc = entity.getAttribute(attr);
          if (desc.isWildcard()) {
            return desc.toAttributePrefix();
          }
          return null;
        },
        (attr, e) -> {
          KeyValue<Object> kv = toKv(e);
          if (kv != null) {
            if (missing.getAndDecrement() != 0) {
              consumer.accept(kv);
            } else {
              return false;
            }
          }
          return true;
        });
  }

  @Override
  public void listEntities(
      RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {
    final IntOffset off = offset == null ? new IntOffset(0) : (IntOffset) offset;
    final AtomicInteger newOff = new AtomicInteger(off.getOffset());
    cache.keys(
        off.getOffset(),
        limit,
        key -> consumer.accept(Pair.of(new IntOffset(newOff.incrementAndGet()), key)));
  }

  @Override
  public void close() {
    Optional.ofNullable(handle.getAndSet(null)).ifPresent(ObserveHandle::close);
    cache.clear();
  }

  @SuppressWarnings("unchecked")
  private @Nullable <T> KeyValue<T> toKv(@Nullable Pair<Long, Payload> p) {
    if (p == null || p.getSecond() == null || p.getSecond().getData().isDelete()) {
      return null;
    }
    StreamElement data = p.getSecond().getData();
    if (data.getSequentialId() > 0) {
      return KeyValue.of(
          data.getEntityDescriptor(),
          (AttributeDescriptor<T>) data.getAttributeDescriptor(),
          data.getSequentialId(),
          data.getKey(),
          data.getAttribute(),
          new RawOffset(data.getAttribute()),
          (T) data.getParsed().orElse(null),
          data.getValue(),
          data.getStamp());
    }
    return KeyValue.of(
        data.getEntityDescriptor(),
        (AttributeDescriptor<T>) data.getAttributeDescriptor(),
        data.getKey(),
        data.getAttribute(),
        new RawOffset(data.getAttribute()),
        (T) data.getParsed().orElse(null),
        data.getValue(),
        data.getStamp());
  }

  @Override
  public EntityDescriptor getEntityDescriptor() {
    return entity;
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    try {
      cache(data);
      writer.write(data, statusCallback);
    } catch (Exception ex) {
      statusCallback.commit(false, ex);
    }
  }

  @Override
  public URI getUri() {
    return reader.getUri();
  }

  @Override
  public void cache(StreamElement element) {
    onCache(element, true);
  }

  @Override
  public CommitLogReader getUnderlyingReader() {
    return reader;
  }

  @Override
  public OnlineAttributeWriter getUnderlyingWriter() {
    return writer;
  }

  @Override
  public Optional<ObserveHandle> getRunningHandle() {
    return Optional.ofNullable(handle.get());
  }

  @Override
  public Factory asFactory() {
    final CommitLogReader.Factory<?> readerFactory = reader.asFactory();
    final OnlineAttributeWriter.Factory<?> writerFactory = writer.asFactory();
    final EntityDescriptor entity = this.entity;
    return repo ->
        new LocalCachedPartitionedView(
            entity, readerFactory.apply(repo), writerFactory.apply(repo));
  }
}
