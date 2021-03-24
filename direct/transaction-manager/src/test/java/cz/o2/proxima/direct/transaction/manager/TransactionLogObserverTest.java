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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.transaction.TransactionUtils;
import cz.o2.proxima.direct.transaction.manager.TransactionLogObserverFactory.Context;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link TransactionLogObserver}. */
public class TransactionLogObserverTest {

  private final Config conf = ConfigFactory.load("test-transactions.conf").resolve();
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
  private LogObserver observer;

  @Before
  public void setUp() {
    Context context = Context.of(repo, direct);
    now = System.currentTimeMillis();
    observer = new TransactionLogObserverFactory.Default().create(context);
  }

  @After
  public void tearDown() {
    direct.close();
  }

  @Test
  public void testCreateTransaction() {
    OnlineAttributeWriter requestWriter = TransactionUtils.getRequestWriter(direct, gatewayStatus);
    String transactionId = UUID.randomUUID().toString();
    writeSync(requestWriter, request.upsert(transactionId, "1", now, Request.of()));
  }

  private void writeSync(OnlineAttributeWriter writer, StreamElement element) {
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        element,
        (succ, exc) -> {
          latch.countDown();
        });
    ExceptionUtils.ignoringInterrupted(latch::await);
  }
}
