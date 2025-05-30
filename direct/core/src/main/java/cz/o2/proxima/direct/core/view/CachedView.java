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

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A view of commit-log that caches (and potentially checkpoints) data from partitions and makes in
 * random accessible.
 *
 * <p>Client can provide callbacks to be called on data updates. Client might also update the view,
 * all updates must be persisted in a way that will guarantee that subsequent reloads will contain
 * the new data and that read-write operations have happens-before relation semantics.
 */
@Stable
public interface CachedView extends RandomAccessReader, OnlineAttributeWriter {

  /** {@link Serializable} factory for {@link CachedView}. */
  @FunctionalInterface
  interface Factory
      extends OnlineAttributeWriter.Factory<CachedView>, RandomAccessReader.Factory<CachedView> {}

  /**
   * Assign and make given partitions accessible by random reads. If the view contains any
   * partitions not listed in the collection, these partitions are dropped.
   *
   * @param partitions the partitions to cache locally
   */
  default void assign(Collection<Partition> partitions) {
    assign(partitions, (e, c) -> {});
  }

  /**
   * Assign and make given partitions accessible by random reads. If the view contains any
   * partitions not listed in the collection, these partitions are dropped.
   *
   * @param partitions the partitions to cache locally
   * @param ttl maximaul Duration that an update should be kept in the cache NOTE: this is not a
   *     hard limit, it is only a signal, that after this time the element can be removed to free up
   *     memory
   */
  default void assign(Collection<Partition> partitions, Duration ttl) {
    assign(partitions, (e, c) -> {}, ttl);
  }

  /**
   * Assign and make given partitions accessible by random reads. If the view contains any
   * partitions not listed in the collection, these partitions are dropped.
   *
   * @param partitions the partitions to cache locally
   * @param updateCallback function that is called when cache gets updated the function takes the
   *     new ingest element and pair of the most recent object that was associated with the key and
   *     it's currently associated stamp
   */
  default void assign(
      Collection<Partition> partitions,
      BiConsumer<StreamElement, Pair<Long, Object>> updateCallback) {

    assign(partitions, updateCallback, null);
  }

  /**
   * Assign and make given partitions accessible by random reads. If the view contains any
   * partitions not listed in the collection, these partitions are dropped.
   *
   * @param partitions the partitions to cache locally
   * @param updateCallback function that is called when cache gets updated the function takes the
   *     new ingest element and pair of the most recent object that was associated with the key and
   *     it's currently associated stamp
   * @param ttl maximaul Duration that an update should be kept in the cache NOTE: this is not a
   *     hard limit, it is only a signal, that after this time the element can be removed to free up
   *     memory
   */
  void assign(
      Collection<Partition> partitions,
      BiConsumer<StreamElement, Pair<Long, Object>> updateCallback,
      @Nullable Duration ttl);

  /**
   * Retrieve currently assigned partitions.
   *
   * @return currently assigned partitions
   */
  Collection<Partition> getAssigned();

  /**
   * Cache given {@link StreamElement} into local cache without writing it to the underlying writer.
   * This is used in conjunction with attribute family proxy.
   *
   * @param element the data to cache
   */
  void cache(StreamElement element);

  /**
   * Retrieve all partitions of the underlying commit log.
   *
   * @return all partitions of underlying commit log
   */
  default Collection<Partition> getPartitions() {
    return getUnderlyingReader().getPartitions();
  }

  /** Retrieve underlying {@link CommitLogReader}. */
  CommitLogReader getUnderlyingReader();

  /**
   * Retrieve underlying {@link OnlineAttributeWriter}. Note that using this write might not update
   * the cache. In most cases, use {@link #write(StreamElement, CommitCallback)} directly.
   */
  OnlineAttributeWriter getUnderlyingWriter();

  /** Retrieve a running handle (if present). */
  Optional<ObserveHandle> getRunningHandle();

  /**
   * Convert instance of this view to {@link Factory} suitable for serialization.
   *
   * @return the {@link Factory} representing this view
   */
  @Override
  Factory asFactory();

  @Override
  void close();
}
