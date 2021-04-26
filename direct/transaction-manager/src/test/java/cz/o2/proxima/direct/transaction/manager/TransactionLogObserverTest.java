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
package cz.o2.proxima.direct.transaction.manager;

import static org.junit.Assert.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.ClientTransactionManager;
import cz.o2.proxima.direct.transaction.TransactionManager;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link TransactionLogObserver}. */
public class TransactionLogObserverTest {

  private final Config conf =
      ConfigFactory.defaultApplication()
          .withFallback(ConfigFactory.load("test-transactions.conf"))
          .resolve();
  private final Repository repo = Repository.ofTest(conf);
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final EntityDescriptor user = repo.getEntity("user");
  private final AttributeDescriptor<byte[]> gatewayStatus = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> userGateways = user.getAttribute("gateway.*");
  private final EntityDescriptor transaction = repo.getEntity("_transaction");
  private final Wildcard<Request> request =
      Wildcard.wildcard(transaction, transaction.getAttribute("request.*"));
  private final Wildcard<Response> response =
      Wildcard.wildcard(transaction, transaction.getAttribute("response.*"));
  private final Regular<State> state =
      Regular.regular(transaction, transaction.getAttribute("state"));
  private long now;
  private TransactionLogObserver observer;

  @Before
  public void setUp() {
    now = System.currentTimeMillis();
    observer = new TransactionLogObserverFactory.Default().create(direct);
    observer.run(getClass().getSimpleName());
  }

  @After
  public void tearDown() {
    direct.close();
  }

  @Test(timeout = 10000)
  public void testCreateTransaction() throws InterruptedException {
    try (ClientTransactionManager clientManager = TransactionManager.client(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      Pair<String, Response> response = responseQueue.take();
      assertEquals("open", response.getFirst());
      assertEquals(Response.Flags.OPEN, response.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionCommit() throws InterruptedException {
    try (ClientTransactionManager clientManager = TransactionManager.client(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      responseQueue.take();
      clientManager.commit(
          transactionId,
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user", userGateways, 2L, "1")));
      Pair<String, Response> response = responseQueue.take();
      assertEquals("commit", response.getFirst());
      assertEquals(Response.Flags.COMMITTED, response.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testCreateTransactionDuplicate() throws InterruptedException {
    try (ClientTransactionManager clientManager = TransactionManager.client(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      // discard this
      responseQueue.take();
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      Pair<String, Response> response = responseQueue.take();
      assertEquals("open", response.getFirst());
      assertEquals(Response.Flags.DUPLICATE, response.getSecond().getFlags());
    }
  }

  @Test(timeout = 10000)
  public void testTransactionUpdate() throws InterruptedException {
    try (ClientTransactionManager clientManager = TransactionManager.client(direct)) {
      String transactionId = UUID.randomUUID().toString();
      BlockingQueue<Pair<String, Response>> responseQueue = new ArrayBlockingQueue<>(1);
      clientManager.begin(
          transactionId,
          ExceptionUtils.uncheckedBiConsumer((k, v) -> responseQueue.put(Pair.of(k, v))),
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user", userGateways, 1L, "1")));
      // discard this
      responseQueue.take();
      clientManager.updateTransaction(
          transactionId,
          Collections.singletonList(
              KeyAttribute.ofAttributeDescriptor(user, "user2", userGateways, 2L, "1")));
      Pair<String, Response> response = responseQueue.take();
      assertEquals("update", response.getFirst());
      assertEquals(Response.Flags.UPDATED, response.getSecond().getFlags());
    }
  }

  @Test
  public void testFailedTransactionCommit() {}
}