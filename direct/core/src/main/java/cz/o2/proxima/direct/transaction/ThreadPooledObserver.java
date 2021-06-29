/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.transaction;

import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class ThreadPooledObserver implements CommitLogObserver {

  private final ExecutorService executor;
  private final CommitLogObserver delegate;
  private final List<BlockingQueue<Pair<StreamElement, OnNextContext>>> workQueues =
      new ArrayList<>();
  private final List<Future<?>> futures = new ArrayList<>();

  public ThreadPooledObserver(
      ExecutorService executorService, CommitLogObserver requestObserver, int parallelism) {

    this.executor = executorService;
    this.delegate = requestObserver;
    for (int i = 0; i < parallelism; i++) {
      BlockingQueue<Pair<StreamElement, OnNextContext>> queue = new ArrayBlockingQueue<>(50);
      workQueues.add(queue);
      futures.add(executor.submit(() -> processQueue(queue)));
    }
  }

  private void processQueue(BlockingQueue<Pair<StreamElement, OnNextContext>> queue) {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        ExceptionUtils.ignoringInterrupted(
            () -> {
              Pair<StreamElement, OnNextContext> polled = queue.take();
              delegate.onNext(polled.getFirst(), polled.getSecond());
            });
      }
    } catch (Throwable err) {
      onError(err);
    }
  }

  @Override
  public void onCompleted() {
    waitTillQueueEmpty();
    exitThreads();
    if (!Thread.currentThread().isInterrupted()) {
      delegate.onCompleted();
    }
  }

  @Override
  public void onCancelled() {
    waitTillQueueEmpty();
    exitThreads();
    if (!Thread.currentThread().isInterrupted()) {
      delegate.onCancelled();
    }
  }

  @Override
  public boolean onError(Throwable error) {
    exitThreads();
    return delegate.onError(error);
  }

  private void exitThreads() {
    futures.forEach(f -> f.cancel(true));
  }

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    OnNextContext synchronizedContext = synchronize(context);
    return !ExceptionUtils.ignoringInterrupted(
        () ->
            workQueues
                .get((ingest.getKey().hashCode() & Integer.MAX_VALUE) % workQueues.size())
                .put(Pair.of(ingest, synchronizedContext)));
  }

  private OnNextContext synchronize(OnNextContext context) {
    OffsetCommitter synchronizedCommitter =
        new OffsetCommitter() {
          @Override
          public void commit(boolean success, @Nullable Throwable error) {
            synchronized (this) {
              context.commit(success, error);
            }
          }
        };
    return new OnNextContext() {
      @Override
      public OffsetCommitter committer() {
        return synchronizedCommitter;
      }

      @Override
      public Partition getPartition() {
        return context.getPartition();
      }

      @Override
      public Offset getOffset() {
        return context.getOffset();
      }

      @Override
      public long getWatermark() {
        return context.getWatermark();
      }
    };
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {
    workQueues.forEach(BlockingQueue::clear);
    delegate.onRepartition(context);
  }

  @Override
  public void onIdle(OnIdleContext context) {
    if (workQueueEmpty()) {
      delegate.onIdle(context);
    }
  }

  private boolean workQueueEmpty() {
    return workQueues.stream().allMatch(BlockingQueue::isEmpty);
  }

  private void waitTillQueueEmpty() {
    while (!workQueueEmpty() && !Thread.currentThread().isInterrupted()) {
      ExceptionUtils.ignoringInterrupted(() -> TimeUnit.MILLISECONDS.sleep(100));
    }
  }
}
