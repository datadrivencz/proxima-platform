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

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;

@FunctionalInterface
@Internal
public interface TransactionLogObserverFactory {

  interface Context {
    static Context of(Repository repo, DirectDataOperator direct) {

      EntityDescriptor transaction = repo.getEntity("_transaction");
      Wildcard<Request> request =
          Wildcard.wildcard(transaction, transaction.getAttribute("request.*"));
      Wildcard<Response> response =
          Wildcard.wildcard(transaction, transaction.getAttribute("response.*"));
      Regular<State> state = Regular.regular(transaction, transaction.getAttribute("state"));

      return new Context() {

        @Override
        public Repository getRepo() {
          return repo;
        }

        @Override
        public DirectDataOperator getDirect() {
          return direct;
        }

        @Override
        public EntityDescriptor getTransformationEntity() {
          return transaction;
        }

        @Override
        public Wildcard<Request> getRequestDescriptor() {
          return request;
        }

        @Override
        public Wildcard<Response> getResponseDescriptor() {
          return response;
        }

        @Override
        public Regular<State> getStateDescriptor() {
          return state;
        }
      };
    }

    Repository getRepo();

    DirectDataOperator getDirect();

    EntityDescriptor getTransformationEntity();

    Wildcard<Request> getRequestDescriptor();

    Wildcard<Response> getResponseDescriptor();

    Regular<State> getStateDescriptor();
  }

  class Default implements TransactionLogObserverFactory {
    @Override
    public LogObserver create(Context context) {
      return new TransactionLogObserver(
          context.getRepo(),
          context.getDirect(),
          context.getTransformationEntity(),
          context.getRequestDescriptor(),
          context.getResponseDescriptor(),
          context.getStateDescriptor());
    }
  }

  /**
   * A factory for {@link LogObserver} responsible for transaction management.
   *
   * @param context the context for the observer
   * @return the {@link LogObserver}
   */
  LogObserver create(Context context);
}
