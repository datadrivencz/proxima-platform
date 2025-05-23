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
package cz.o2.proxima.direct.server;

import static cz.o2.proxima.direct.server.IngestServer.*;

import com.google.protobuf.TextFormat;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.server.metrics.Metrics;
import cz.o2.proxima.direct.server.rpc.proto.service.IngestServiceGrpc;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.Ingest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.IngestBulk;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.Status;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.StatusBulk;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.TransactionCommitRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.TransactionCommitResponse;
import cz.o2.proxima.direct.server.transaction.TransactionContext;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Strings;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** The ingestion service. */
@Slf4j
public class IngestService extends IngestServiceGrpc.IngestServiceImplBase {

  private final Repository repo;
  private final DirectDataOperator direct;
  private final TransactionContext transactionContext;
  private final ScheduledExecutorService scheduler;

  public IngestService(
      Repository repo,
      DirectDataOperator direct,
      TransactionContext transactionContext,
      ScheduledExecutorService scheduler) {

    this.repo = repo;
    this.direct = direct;
    this.transactionContext = transactionContext;
    this.scheduler = scheduler;
  }

  private class IngestObserver implements StreamObserver<Ingest> {

    final StreamObserver<Status> responseObserver;
    final Object inflightRequestsLock = new Object();
    final AtomicInteger inflightRequests = new AtomicInteger(0);
    final Object responseObserverLock = new Object();

    IngestObserver(StreamObserver<Status> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(Ingest request) {
      Metrics.INGEST_SINGLE.increment();
      inflightRequests.incrementAndGet();
      if (!Strings.isNullOrEmpty(request.getTransactionId())) {
        responseObserver.onNext(
            status(
                request.getUuid(),
                403,
                "Please use ingestBulk when committing outputs of a transaction."));
      } else {
        processSingleIngest(
            request,
            status -> {
              synchronized (responseObserverLock) {
                responseObserver.onNext(status);
              }
              if (inflightRequests.decrementAndGet() == 0) {
                synchronized (inflightRequestsLock) {
                  inflightRequestsLock.notifyAll();
                }
              }
            });
      }
    }

    @Override
    public void onError(Throwable thrwbl) {
      log.error("Error on channel", thrwbl);
      synchronized (responseObserverLock) {
        responseObserver.onError(thrwbl);
      }
    }

    @Override
    public void onCompleted() {
      inflightRequests.accumulateAndGet(
          0,
          (a, b) -> {
            int res = a + b;
            if (res > 0) {
              synchronized (inflightRequestsLock) {
                try {
                  while (inflightRequests.get() > 0) {
                    inflightRequestsLock.wait();
                  }
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                }
              }
            }
            synchronized (responseObserverLock) {
              responseObserver.onCompleted();
            }
            return res;
          });
    }
  }

  private class IngestBulkObserver implements StreamObserver<IngestBulk> {

    final StreamObserver<StatusBulk> responseObserver;
    final Queue<Status> statusQueue = new ConcurrentLinkedQueue<>();
    final AtomicBoolean completed = new AtomicBoolean(false);
    final Object inflightRequestsLock = new Object();
    final AtomicInteger inflightRequests = new AtomicInteger();
    final AtomicLong lastFlushMs = new AtomicLong(System.currentTimeMillis());
    final StatusBulk.Builder builder = StatusBulk.newBuilder();
    static final long MAX_SLEEP_MILLIS = 100;
    static final int MAX_QUEUED_STATUSES = 500;

    // schedule the flush periodically
    final ScheduledFuture<?> flushFuture =
        scheduler.scheduleAtFixedRate(
            this::flush, MAX_SLEEP_MILLIS, MAX_SLEEP_MILLIS, TimeUnit.MILLISECONDS);

    IngestBulkObserver(StreamObserver<StatusBulk> responseObserver) {
      this.responseObserver = responseObserver;
    }

    private void flush() {
      try {
        synchronized (builder) {
          while (statusQueue.size() > MAX_QUEUED_STATUSES) {
            peekQueueToBuilderAndFlush();
          }
          long now = System.currentTimeMillis();
          if (now - lastFlushMs.get() >= MAX_SLEEP_MILLIS) {
            while (!statusQueue.isEmpty()) {
              peekQueueToBuilderAndFlush();
            }
          }
          if (builder.getStatusCount() > 0) {
            responseObserver.onNext(builder.build());
            builder.clear();
          }
          if (completed.get() && inflightRequests.get() == 0 && statusQueue.isEmpty()) {
            responseObserver.onCompleted();
          }
        }
      } catch (Throwable ex) {
        log.error("Failed to send bulk status", ex);
      }
    }

    private void peekQueueToBuilderAndFlush() {
      synchronized (builder) {
        builder.addStatus(statusQueue.poll());
        if (builder.getStatusCount() >= 1000) {
          flushBuilder();
        }
      }
    }

    /** Flush response(s) to the observer. */
    private void flushBuilder() {
      synchronized (builder) {
        lastFlushMs.set(System.currentTimeMillis());
        StatusBulk bulk = builder.build();
        if (bulk.getStatusCount() > 0) {
          responseObserver.onNext(bulk);
        }
        builder.clear();
      }
    }

    @Override
    public void onNext(IngestBulk bulk) {
      Metrics.INGEST_BULK.increment();
      Metrics.BULK_SIZE.increment(bulk.getIngestCount());
      inflightRequests.addAndGet(bulk.getIngestCount());
      Map<String, List<Ingest>> groupedByTransaction =
          bulk.getIngestList().stream().collect(Collectors.groupingBy(Ingest::getTransactionId));

      List<Ingest> nonTransactionalWrites =
          MoreObjects.firstNonNull(groupedByTransaction.remove(""), Collections.emptyList());

      handleNonTransactionalWrites(nonTransactionalWrites);
      groupedByTransaction.forEach(this::handleTransactionalWrites);
    }

    private void handleTransactionalWrites(String transactionId, List<Ingest> writes) {
      List<Pair<Ingest, StreamElement>> outputs =
          writes.stream()
              .map(r -> Pair.of(r, validateAndConvertToStreamElement(r, createRpcConsumer(r))))
              .collect(Collectors.toList());
      if (outputs.stream().anyMatch(p -> p.getSecond() == null)) {
        // when we have a single fail in the transaction we need to discard the complete outputs
        outputs.stream()
            .filter(p -> p.getSecond() != null)
            .forEach(
                p ->
                    createRpcConsumer(p.getFirst())
                        .accept(
                            status(
                                p.getFirst().getUuid(),
                                412,
                                "Invalid update was part of transaction " + transactionId)));

        transactionContext.get(transactionId).rollback();
      } else {
        List<StreamElement> toWrite =
            outputs.stream().map(Pair::getSecond).collect(Collectors.toList());
        transactionContext.get(transactionId).addOutputs(toWrite);
        writes.forEach(r -> createRpcConsumer(r).accept(ok(r.getUuid())));
      }
    }

    private void handleNonTransactionalWrites(List<Ingest> writes) {
      writes.forEach(r -> processSingleIngest(r, createRpcConsumer(r)));
    }

    private Consumer<Status> createRpcConsumer(Ingest request) {
      boolean needsCommit = !request.getTransactionId().isEmpty();
      return status -> {
        if (needsCommit) {
          log.info(
              "Uncommitted input ingest {}: {}, {}, waiting for transaction {}",
              TextFormat.shortDebugString(request),
              status.getStatus(),
              status.getStatus() == 200 ? "OK" : status.getStatusMessage(),
              request.getTransactionId());
        } else {
          log.info(
              "Committed input ingest {}: {}, {}",
              TextFormat.shortDebugString(request),
              status.getStatus(),
              status.getStatus() == 200 ? "OK" : status.getStatusMessage());
        }
        statusQueue.add(status);
        if (statusQueue.size() >= MAX_QUEUED_STATUSES) {
          // enqueue flush
          lastFlushMs.set(0L);
          scheduler.execute(this::flush);
        }
        if (inflightRequests.decrementAndGet() == 0) {
          // there are no more in-flight requests
          synchronized (inflightRequestsLock) {
            inflightRequestsLock.notifyAll();
          }
        }
      };
    }

    @Override
    public void onError(Throwable error) {
      log.error("Error from client", error);
      // close the connection
      responseObserver.onError(error);
      flushFuture.cancel(true);
    }

    @Override
    public void onCompleted() {
      completed.set(true);
      flushFuture.cancel(true);
      // flush all responses to the observer
      synchronized (inflightRequestsLock) {
        while (inflightRequests.get() > 0) {
          try {
            inflightRequestsLock.wait(100);
          } catch (InterruptedException ex) {
            log.warn("Interrupted while waiting to send responses to client", ex);
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      while (!statusQueue.isEmpty()) {
        peekQueueToBuilderAndFlush();
      }
      flushBuilder();
      responseObserver.onCompleted();
    }
  }

  private void processSingleIngest(Ingest request, Consumer<Status> consumer) {
    if (log.isDebugEnabled()) {
      log.debug("Processing input ingest {}", TextFormat.shortDebugString(request));
    }
    if (request.getTransactionId().isEmpty()) {
      Metrics.INGESTS.increment();
      try {
        if (!writeRequest(request, consumer)) {
          Metrics.INVALID_REQUEST.increment();
        }
      } catch (Exception err) {
        log.error("Error processing user request {}", TextFormat.shortDebugString(request), err);
        consumer.accept(status(request.getUuid(), 500, err.getMessage()));
      }
    } else {
      @Nullable StreamElement streamElement = validateAndConvertToStreamElement(request, consumer);
      if (streamElement != null) {
        transactionContext
            .get(request.getTransactionId())
            .addOutputs(Collections.singletonList(streamElement));
        consumer.accept(ok(request.getUuid()));
      }
    }
  }

  /**
   * Ingest the given request and return {@code true} if successfully ingested and {@code false} if
   * the request is invalid.
   */
  private boolean writeRequest(Ingest request, Consumer<Status> consumer) {
    StreamElement element = validateAndConvertToStreamElement(request, consumer);
    if (element == null) {
      return false;
    }
    return ingestRequest(direct, element, request.getUuid(), consumer);
  }

  @Nullable
  private StreamElement validateAndConvertToStreamElement(
      Ingest request, Consumer<Status> consumer) {

    if (Strings.isNullOrEmpty(request.getKey())
        || Strings.isNullOrEmpty(request.getEntity())
        || Strings.isNullOrEmpty(request.getAttribute())) {

      consumer.accept(status(request.getUuid(), 400, "Missing required fields in input message"));
      return null;
    }
    Optional<EntityDescriptor> entity = repo.findEntity(request.getEntity());

    if (!entity.isPresent()) {
      consumer.accept(notFound(request.getUuid(), "Entity " + request.getEntity() + " not found"));
      return null;
    }
    Optional<AttributeDescriptor<Object>> attr = entity.get().findAttribute(request.getAttribute());
    if (!attr.isPresent()) {
      consumer.accept(
          notFound(
              request.getUuid(),
              "Attribute "
                  + request.getAttribute()
                  + " of entity "
                  + entity.get().getName()
                  + " not found"));
      return null;
    }

    boolean valid =
        request.getDelete() /* delete is always valid */
            || attr.get().getValueSerializer().isValid(request.getValue().toByteArray());

    if (!valid) {
      log.info("Request {} is not valid", TextFormat.shortDebugString(request));
      consumer.accept(
          status(
              request.getUuid(),
              412,
              "Invalid scheme for " + entity.get().getName() + "." + attr.get().getName()));
      return null;
    }

    return toStreamElement(request, entity.get(), attr.get());
  }

  @Override
  public void ingest(Ingest request, StreamObserver<Status> responseObserver) {
    Metrics.INGEST_SINGLE.increment();
    processSingleIngest(
        request,
        status -> {
          responseObserver.onNext(status);
          responseObserver.onCompleted();
        });
  }

  @Override
  public StreamObserver<Ingest> ingestSingle(StreamObserver<Status> responseObserver) {
    return new IngestObserver(responseObserver);
  }

  @Override
  public StreamObserver<IngestBulk> ingestBulk(StreamObserver<StatusBulk> responseObserver) {
    // the responseObserver doesn't have to be synchronized in this
    // case, because the communication with the observer is done
    // in single flush thread
    return new IngestBulkObserver(responseObserver);
  }

  @Override
  public void commit(
      TransactionCommitRequest request,
      StreamObserver<TransactionCommitResponse> responseObserver) {

    log.debug("Committing transaction {}", request.getTransactionId());
    try {
      long start = System.currentTimeMillis();
      transactionContext
          .get(request.getTransactionId())
          .commit(
              (succ, exc) -> {
                Metrics.INGEST_LATENCY.increment(System.currentTimeMillis() - start);
                if (exc != null) {
                  log.warn(
                      "Error during committing transaction {}", request.getTransactionId(), exc);
                } else {
                  log.info("Transaction {} committed", request.getTransactionId());
                }
                responseObserver.onNext(
                    TransactionCommitResponse.newBuilder()
                        .setStatus(
                            succ
                                ? TransactionCommitResponse.Status.COMMITTED
                                : TransactionCommitResponse.Status.FAILED)
                        .build());
                responseObserver.onCompleted();
              });
    } catch (TransactionRejectedException e) {
      log.info("Transaction {} rejected.", request.getTransactionId());
      responseObserver.onNext(
          TransactionCommitResponse.newBuilder()
              .setStatus(TransactionCommitResponse.Status.REJECTED)
              .build());
      responseObserver.onCompleted();
    } catch (Exception err) {
      log.error("Error during committing transaction {}", request.getTransactionId(), err);
      responseObserver.onNext(
          TransactionCommitResponse.newBuilder()
              .setStatus(TransactionCommitResponse.Status.FAILED)
              .build());
      responseObserver.onCompleted();
    }
  }

  private static StreamElement toStreamElement(
      Ingest request, EntityDescriptor entity, AttributeDescriptor<?> attr) {

    long stamp = request.getStamp() == 0 ? System.currentTimeMillis() : request.getStamp();

    if (request.getDelete()) {
      return attr.isWildcard() && attr.getName().equals(request.getAttribute())
          ? StreamElement.deleteWildcard(entity, attr, request.getUuid(), request.getKey(), stamp)
          : StreamElement.delete(
              entity, attr, request.getUuid(), request.getKey(), request.getAttribute(), stamp);
    }
    String uuid = request.getUuid().isEmpty() ? UUID.randomUUID().toString() : request.getUuid();
    return StreamElement.upsert(
        entity,
        attr,
        uuid,
        request.getKey(),
        request.getAttribute(),
        stamp,
        request.getValue().toByteArray());
  }
}
