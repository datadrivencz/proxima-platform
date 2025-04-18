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
package cz.o2.proxima.direct.core.transaction;

import static org.junit.Assert.*;

import cz.o2.proxima.core.annotations.DeclaredThreadSafe;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.TransformationRunner;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.transaction.TransactionResourceManager.CachedTransaction;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test transactions are working according to the specification. */
public class TransactionResourceManagerTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-transactions.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<byte[]> status = Regular.of(gateway, gateway.getAttribute("status"));
  private final EntityDescriptor user = repo.getEntity("user");
  private final Wildcard<byte[]> allGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
  private final EntityDescriptor transaction = repo.getEntity("_transaction");
  private final Wildcard<Request> requestDesc =
      Wildcard.of(transaction, transaction.getAttribute("request.*"));

  @Before
  public void setUp() {
    TransformationRunner.runTransformations(repo, direct);
  }

  @After
  public void tearDown() {
    direct.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDirectWriterFetchFails() {
    direct.getWriter(requestDesc);
  }

  @Test(timeout = 10000)
  public void testTransactionRequestResponse() throws ExecutionException, InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      String transactionId = UUID.randomUUID().toString();

      // create a simple ping-pong communication
      runObservations(
          manager,
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              assertEquals(1, request.getInputAttributes().size());
              long stamp = System.currentTimeMillis();
              manager.writeResponseAndUpdateState(
                  key,
                  State.open(1L, stamp, new HashSet<>(request.getInputAttributes())),
                  requestId,
                  Response.forRequest(request).open(1L, stamp),
                  context::commit);
            } else {
              context.confirm();
            }
            return true;
          });

      Future<Response> response =
          manager.begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      assertEquals(Response.Flags.OPEN, response.get().getFlags());

      State state = manager.getCurrentState(transactionId);
      assertNotNull(state);
      assertEquals(State.Flags.OPEN, state.getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionRequestCommit() throws InterruptedException, ExecutionException {
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      String transactionId = UUID.randomUUID().toString();
      // create a simple ping-pong communication
      runObservations(
          manager,
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  (succ, exc) -> {
                    latch.countDown();
                    context.commit(succ, exc);
                  };
              long stamp = System.currentTimeMillis();
              if (request.getFlags() == Request.Flags.COMMIT) {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, Collections.emptyList())
                        .committed(new HashSet<>(request.getOutputs())),
                    requestId,
                    Response.forRequest(request).committed(),
                    commit);
              } else {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).open(1L, stamp),
                    commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      Future<Response> response =
          manager.begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)));

      response.get();
      response =
          manager.commit(
              transactionId,
              Collections.singletonList(status.upsert(1L, "gw1", 1L, new byte[] {})));

      assertEquals(Response.Flags.COMMITTED, response.get().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionRequestRollback() throws InterruptedException, ExecutionException {
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      String transactionId = UUID.randomUUID().toString();

      // create a simple ping-pong communication
      runObservations(
          manager,
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  (succ, exc) -> {
                    latch.countDown();
                    context.commit(succ, exc);
                  };
              long stamp = System.currentTimeMillis();
              if (request.getFlags() == Request.Flags.ROLLBACK) {
                manager.writeResponseAndUpdateState(
                    key, State.empty(), requestId, Response.forRequest(request).aborted(), commit);
              } else if (request.getFlags() == Request.Flags.OPEN) {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).open(1L, stamp),
                    commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      BlockingQueue<Response> responseQueue = new ArrayBlockingQueue<>(5);

      manager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)))
          .thenApply(responseQueue::add);

      // discard
      responseQueue.take();
      manager.rollback(transactionId).thenAccept(responseQueue::add);

      assertEquals(Response.Flags.ABORTED, responseQueue.take().getFlags());
      State currentState = manager.getCurrentState(transactionId);
      assertEquals(State.Flags.UNKNOWN, currentState.getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionRequestUpdate() throws InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> receivedResponses = new ArrayBlockingQueue<>(5);

      // create a simple ping-pong communication
      runObservations(
          manager,
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  (succ, exc) -> {
                    latch.countDown();
                    context.commit(succ, exc);
                  };
              long stamp = System.currentTimeMillis();
              if (request.getFlags() == Request.Flags.UPDATE) {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, Collections.emptyList())
                        .update(new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).updated(),
                    commit);
              } else {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).open(1L, stamp),
                    commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      manager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)))
          .whenComplete((response, err) -> receivedResponses.add(response));

      receivedResponses.take();
      manager
          .updateTransaction(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw2", status, 1L)))
          .whenComplete((response, err) -> receivedResponses.add(response));

      Response response = receivedResponses.take();
      assertEquals(Response.Flags.UPDATED, response.getFlags());
      State currentState = manager.getCurrentState(transactionId);
      assertEquals("gw2", Iterables.getOnlyElement(currentState.getInputAttributes()).getKey());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionRequestUpdateWithEmptyList() throws InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> receivedResponses = new ArrayBlockingQueue<>(5);

      // create a simple ping-pong communication
      runObservations(
          manager,
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  (succ, exc) -> {
                    latch.countDown();
                    context.commit(succ, exc);
                  };
              long stamp = System.currentTimeMillis();
              if (request.getFlags() == Request.Flags.UPDATE) {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, Collections.emptyList())
                        .update(new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).aborted(),
                    commit);
              } else {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).open(1L, stamp),
                    commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      manager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)))
          .whenComplete((response, err) -> receivedResponses.add(response));

      receivedResponses.take();
      manager
          .updateTransaction(transactionId, Collections.emptyList())
          .whenComplete((response, err) -> receivedResponses.add(response));

      Response response = receivedResponses.take();
      assertEquals(Response.Flags.UPDATED, response.getFlags());
      State currentState = manager.getCurrentState(transactionId);
      assertEquals(State.Flags.OPEN, currentState.getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionRequestOpenAfterAbort() throws InterruptedException {
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Response> receivedResponses = new ArrayBlockingQueue<>(5);

      // create a simple ping-pong communication
      runObservations(
          manager,
          "requests",
          (ingest, context) -> {
            if (ingest.getAttributeDescriptor().equals(requestDesc)) {
              String key = ingest.getKey();
              String requestId = requestDesc.extractSuffix(ingest.getAttribute());
              Request request = Optionals.get(requestDesc.valueOf(ingest));
              CountDownLatch latch = new CountDownLatch(1);
              CommitCallback commit =
                  (succ, exc) -> {
                    latch.countDown();
                    context.commit(succ, exc);
                  };
              long stamp = System.currentTimeMillis();
              if (request.getFlags() == Request.Flags.UPDATE) {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, Collections.emptyList()).aborted(),
                    requestId,
                    Response.forRequest(request).aborted(),
                    commit);
              } else {
                manager.writeResponseAndUpdateState(
                    key,
                    State.open(1L, stamp, new HashSet<>(request.getInputAttributes())),
                    requestId,
                    Response.forRequest(request).open(1L, stamp),
                    commit);
              }
              ExceptionUtils.ignoringInterrupted(latch::await);
            } else {
              context.confirm();
            }
            return true;
          });

      manager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)))
          .whenComplete((response, err) -> receivedResponses.add(response));

      receivedResponses.take();
      manager
          .updateTransaction(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw2", status, 1L)))
          .whenComplete((response, err) -> receivedResponses.add(response));

      Response response = receivedResponses.take();
      assertEquals(Response.Flags.ABORTED, response.getFlags());

      manager
          .begin(
              transactionId,
              Collections.singletonList(
                  KeyAttributes.ofAttributeDescriptor(gateway, "gw1", status, 1L)))
          .whenComplete((r, err) -> receivedResponses.add(r));

      response = receivedResponses.take();
      assertEquals(Response.Flags.OPEN, response.getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateCachedTransactionWhenMissing() {
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "g", status, 1L);
    long stamp = System.currentTimeMillis();
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      CachedTransaction transaction =
          manager.createCachedTransaction(
              "transaction", State.open(1L, stamp, Collections.singletonList(ka)));
      assertEquals("transaction", transaction.getTransactionId());
    }
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      CachedTransaction transaction =
          manager.createCachedTransaction(
              "transaction",
              State.open(
                  2L,
                  stamp + 1,
                  Collections.singletonList(
                      KeyAttributes.ofStreamElement(status.upsert(1L, "g", 0L, new byte[] {})))));
      assertEquals("transaction", transaction.getTransactionId());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionWriteToCorrectFamily() throws InterruptedException {
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(user, "u", allGateways, 1L, "gw");
    long stamp = System.currentTimeMillis();
    try (TransactionResourceManager manager = TransactionResourceManager.create(direct)) {
      CountDownLatch repartitionLatch = new CountDownLatch(1);
      runObservations(
          manager,
          "name",
          new CommitLogObserver() {
            @Override
            public boolean onNext(StreamElement element, OnNextContext context) {
              return true;
            }

            @Override
            public void onRepartition(OnRepartitionContext context) {
              repartitionLatch.countDown();
            }
          });
      CachedTransaction transaction =
          manager.createCachedTransaction(
              "transaction",
              State.open(
                  2L,
                  stamp + 1,
                  Collections.singletonList(
                      KeyAttributes.ofStreamElement(
                          allGateways.upsert(1L, "u", "gw", 0L, new byte[] {})))));
      transaction.open(Collections.singletonList(ka));
      repartitionLatch.await();
      assertEquals(
          Optionals.get(direct.getFamilyByName("all-transaction-commit-log-request").getWriter()),
          transaction.getRequestWriter().getSecond());
      assertEquals(
          Optionals.get(direct.getFamilyByName("transactions-commit").getWriter()),
          transaction.getCommitWriter());
      assertEquals(
          direct.getFamilyByName("all-transaction-commit-log-state").getCachedView().get(),
          transaction.getStateView());
    }
  }

  @Test
  public void testParsingTransactionConfig() {
    Repository repo =
        Repository.of(
            ConfigFactory.parseString("transactions.timeout = 1000")
                .withFallback(ConfigFactory.load("test-transactions.conf")));
    ServerTransactionManager manager =
        repo.getOrCreateOperator(DirectDataOperator.class).getServerTransactionManager();
    assertEquals(1000L, ((TransactionResourceManager) manager).getTransactionTimeoutMs());
  }

  @Test
  public void testSynchronizationTesting() {
    assertTrue(TransactionResourceManager.isNotThreadSafe((ingest, context) -> false));
    assertFalse(TransactionResourceManager.isNotThreadSafe(new ThreadSafeCommitLogObserver()));
    assertFalse(
        TransactionResourceManager.getDeclaredParallelism((ingest, context) -> false).isPresent());
    assertEquals(
        5,
        (int)
            Optionals.get(
                TransactionResourceManager.getDeclaredParallelism(
                    new ThreadSafeCommitLogObserver())));
  }

  public static void runObservations(
      ServerTransactionManager manager, String name, CommitLogObserver observer) {

    manager.runObservations(name, (a, b) -> {}, observer);
  }

  @DeclaredThreadSafe(allowedParallelism = 5)
  private static class ThreadSafeCommitLogObserver implements CommitLogObserver {
    @Override
    public boolean onNext(StreamElement element, OnNextContext context) {
      return false;
    }
  }
}
