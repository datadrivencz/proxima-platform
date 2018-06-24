/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.io;

import cz.o2.proxima.beam.StreamElementCoder;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.Pair;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/**
 * A Beam source for {@link CommitLogReader}.
 */
@Experimental
@Slf4j
public class CommitLogSource extends UnboundedSource<
    StreamElement, CommitLogSource.WatermarkCommitCheckpoint> {


  /**
   * Create source from {@link CommitLogReader}.
   * @param repo repository
   * @param reader the {@link CommitLogReader}.
   * @param position where to start reading
   * @param allowedLatenessMs allowed lateness in the source
   * @return source suitable for reading in Pipeline
   */
  public static CommitLogSource of(
      Repository repo, CommitLogReader reader, Position position,
      long allowedLatenessMs) {

    return of(repo, reader, position, null, allowedLatenessMs);
  }

  /**
   * Create source from {@link CommitLogReader}.
   * @param repo repository
   * @param reader the {@link CommitLogReader}.
   * @param position where to start reading
   * @param name (optional) name of the consumer
   * @param allowedLatenessMs allowed lateness in the source
   * @return source suitable for reading in Pipeline
   */
  public static CommitLogSource of(
      Repository repo, CommitLogReader reader, Position position,
      String name, long allowedLatenessMs) {

    return new CommitLogSource(
        repo, reader, position, name, allowedLatenessMs, null);
  }

  public static class WatermarkCommitCheckpoint implements UnboundedSource.CheckpointMark {

    static WatermarkCommitCheckpoint of(long watermark) {
      return new WatermarkCommitCheckpoint(watermark);
    }

    @Getter
    private final long watermark;

    WatermarkCommitCheckpoint(long watermark) {
      this.watermark = watermark;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // nop
    }
  }

  private final Repository repo;
  @Nullable
  private final String name;
  private final CommitLogReader reader;
  private final Position position;
  private final long allowedLatenessMs;
  private final BlockingQueue<Pair<BulkLogObserver.OffsetCommitter, StreamElement>> batch;
  @Nullable
  private final Partition part;

  private CommitLogSource(
      Repository repo, CommitLogReader reader, Position position,
      String name, long allowedLatenessMs, @Nullable Partition part) {

    this.repo = repo;
    this.reader = reader;
    this.position = position;
    this.name = name;
    this.allowedLatenessMs = allowedLatenessMs;
    this.batch = new ArrayBlockingQueue<>(1000);
    this.part = part;
  }

  private CommitLogSource(CommitLogSource other) {
    this(
        other.repo, other.reader, other.position,
        other.name, other.allowedLatenessMs, null);
  }

  private CommitLogSource(CommitLogSource other, Partition part) {
    this(
        other.repo, other.reader, other.position,
        other.name, other.allowedLatenessMs, part);
  }

  @Override
  public List<? extends UnboundedSource<StreamElement, WatermarkCommitCheckpoint>> split(
      int desiredCount, PipelineOptions po) throws Exception {

    System.err.println(" *** split ");
    if (name != null) {
      return IntStream.range(0, desiredCount)
          .mapToObj(i -> new CommitLogSource(this))
          .collect(Collectors.toList());
    }
    System.err.println(" *** to partitions " + reader.getPartitions());
    return reader.getPartitions()
        .stream()
        .map(p -> new CommitLogSource(this, p))
        .collect(Collectors.toList());
  }

  @Override
  public UnboundedReader<StreamElement> createReader(
      PipelineOptions po, WatermarkCommitCheckpoint cmt) throws IOException {

    final ObserveHandle handle;
    final AtomicBoolean initialized = new AtomicBoolean();
    final AtomicLong watermark = new AtomicLong(
        cmt == null ? Long.MIN_VALUE : cmt.getWatermark());

    log.info("Creating reader from watermark {}", watermark);
    BulkLogObserver observer = new BulkLogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OffsetCommitter committer) {
        try {
          if (!batch.offer(Pair.of(committer, ingest), 200, TimeUnit.MILLISECONDS)) {
            log.warn("Nacking incoming element {} due to write timeout.", ingest);
            committer.nack();
          }
          return true;
        } catch (InterruptedException ex) {
          log.warn("Interrupted while inserting element into queue.", ex);
          Thread.currentThread().interrupt();
          return false;
        }
      }

      @Override
      public void onRestart(List<Offset> offsets) {
        initialized.set(true);
        log.info(
            "Successfully initialized bulk observer from watermark {}",
            watermark.get());
      }

      @Override
      public boolean onError(Throwable error) {
        log.error("Error during observing commit log", error);
        throw new RuntimeException(error);
      }
    };

    if (part == null) {
      handle = reader.observeBulk(name, position, observer);
    } else {
      handle = reader.observeBulkPartitions(
          Arrays.asList(part), position, observer);
    }
    final AtomicInteger emptyPolls = new AtomicInteger();

    return new UnboundedReader<StreamElement>() {

      StreamElement current;
      BulkLogObserver.OffsetCommitter storedCommitter = null;

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        Pair<BulkLogObserver.OffsetCommitter, StreamElement> poll;
        try {
          poll = batch.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
          log.warn("Interrupted while polling queue", ex);
          Thread.currentThread().interrupt();
          return false;
        }
        if (poll != null) {
          storedCommitter = poll.getFirst();
          current = poll.getSecond();
          emptyPolls.set(0);
          return true;
        } else if (initialized.get() && emptyPolls.updateAndGet(old -> ++old >= 20 ? 0 : old) == 0) {
          watermark.set(System.currentTimeMillis() - allowedLatenessMs);
        }
        current = null;
        return false;
      }

      @Override
      public Instant getWatermark() {
        return new Instant(watermark.get());
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        if (storedCommitter != null) {
          storedCommitter.confirm();
          storedCommitter = null;
        }
        return WatermarkCommitCheckpoint.of(watermark.get());
      }

      @Override
      public UnboundedSource<StreamElement, ?> getCurrentSource() {
        return CommitLogSource.this;
      }

      @Override
      public StreamElement getCurrent() throws NoSuchElementException {
        System.err.println(" *** getCurrent " + current);
        return current;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current == null) {
          return new Instant(Long.MIN_VALUE);
        }
        return new Instant(current.getStamp());
      }

      @Override
      public void close() throws IOException {
        log.info("Closing handle {}", handle);
        handle.cancel();
      }

    };
  }

  @Override
  public Coder<WatermarkCommitCheckpoint> getCheckpointMarkCoder() {
    return new KryoCoder<>();
  }

  @Override
  public Coder<StreamElement> getOutputCoder() {
    return StreamElementCoder.of(repo);
  }

}
