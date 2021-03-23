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
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;

/**
 * A {@link LogObserver} performing the overall transaction logic via keeping state of transactions
 * and responding to requests.
 */
class TransactionLogObserver implements LogObserver {

  private final Repository repo;
  private final DirectDataOperator direct;
  private final EntityDescriptor transaction;
  private final Wildcard<Request> request;
  private final Wildcard<Response> response;
  private final Regular<State> state;

  TransactionLogObserver(
      Repository repo,
      DirectDataOperator direct,
      EntityDescriptor transaction,
      Wildcard<Request> request,
      Wildcard<Response> response,
      Regular<State> state) {

    this.repo = repo;
    this.direct = direct;
    this.transaction = transaction;
    this.request = request;
    this.response = response;
    this.state = state;
  }

  @Override
  public void onCompleted() {}

  @Override
  public void onCancelled() {}

  @Override
  public boolean onError(Throwable error) {
    return false;
  }

  @Override
  public boolean onException(Exception exception) {
    return false;
  }

  @Override
  public boolean onFatalError(Error error) {
    return false;
  }

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    return false;
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {}

  @Override
  public void onIdle(OnIdleContext context) {}
}
