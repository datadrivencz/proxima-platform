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

import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.transaction.TransactionManager;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link LogObserver} performing the overall transaction logic via keeping state of transactions
 * and responding to requests.
 */
@Slf4j
class TransactionLogObserver implements LogObserver {

  private final DirectDataOperator direct;
  private final ServerTransactionManager manager;

  TransactionLogObserver(DirectDataOperator direct) {
    this.direct = direct;
    this.manager = TransactionManager.server(direct);
  }

  @Override
  public void onCompleted() {}

  @Override
  public void onCancelled() {}

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    log.debug("Received element {} for transaction processing", ingest);
    Wildcard<Request> requestDesc = manager.getRequestDesc();
    if (ingest.getAttributeDescriptor().equals(requestDesc)) {
      handleRequest(
          ingest.getKey(),
          requestDesc.extractSuffix(ingest.getAttribute()),
          ingest.getStamp(),
          requestDesc.valueOf(ingest),
          context);
    } else if (ingest.getAttributeDescriptor().equals(manager.getStateDesc())) {
      handleState(manager.getStateDesc().valueOf(ingest), context);
    } else {
      // unknown attribute, probably own response, can be safely ignored
      log.debug("Unknown attribute {}. Ignored.", ingest.getAttributeDescriptor());
      context.confirm();
    }
    return true;
  }

  private void handleState(Optional<State> maybeState, OnNextContext context) {
    context.confirm();
  }

  private void handleRequest(
      String transactionId,
      String requestId,
      long stamp,
      Optional<Request> maybeRequest,
      OnNextContext context) {

    if (maybeRequest.isPresent()) {
      processTransactionRequest(transactionId, requestId, stamp, maybeRequest.get(), context);
    } else {
      log.error("Unable to parse request at offset {}", context.getOffset());
      context.confirm();
    }
  }

  private void processTransactionRequest(
      String transactionId, String requestId, long stamp, Request request, OnNextContext context) {

    log.debug("Processing request {} for transaction {}", requestId, transactionId);
    State currentState = manager.getCurrentState(transactionId);
    if (currentState.getFlags() == State.Flags.UNKNOWN) {
      State newState = transitionState(currentState, request);
      CompletableFuture<?> stateFuture = new CompletableFuture<>();
      CompletableFuture<?> responseFuture = new CompletableFuture<>();
      CommitCallback commitCallback = Utils.callbackForFutures(stateFuture, responseFuture);
      manager.setCurrentState(transactionId, newState, commitCallback);
      manager.writeResponse(transactionId, requestId, Response.open(), commitCallback);
      CompletableFuture.allOf(stateFuture, responseFuture)
          .whenComplete(
              (val, exc) -> {
                if (exc != null) {
                  context.confirm();
                } else {
                  context.fail(exc);
                }
              });
    } else {
      // FIXME
      context.confirm();
    }
  }

  private State transitionState(State currentState, Request request) {
    Set<KeyAttribute> attrSet = new HashSet<>(request.getInputAttributes());
    if (!currentState.getInputAttributes().isEmpty()) {
      attrSet.addAll(currentState.getInputAttributes());
    }
    System.err.println(" *** statetransition: " + attrSet);
    return State.open(attrSet);
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {}

  @Override
  public void onIdle(OnIdleContext context) {}

  public void run(String name) {
    manager.runObservations(name, this);
  }
}
