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

import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;

public interface ServerTransactionManager extends AutoCloseable, TransactionManager {

  /**
   * Observe all transactional families with given observer.
   *
   * @param name name of the observer (will be appended with name of the family)
   * @param requestObserver the observer (need not be synchronized)
   */
  void runObservations(String name, LogObserver requestObserver);

  /**
   * Retrieve current state of the transaction.
   *
   * @param transactionId ID of the transaction
   * @return the {@link State} associated with the transaction on server
   */
  State getCurrentState(String transactionId);

  /**
   * Write new {@link State} for the transaction.
   *
   * @param transactionId ID of the transaction
   * @param state the new state
   * @param callback callback for committing the write
   */
  void setCurrentState(String transactionId, State state, CommitCallback callback);

  /**
   * Write response for a request to the caller.
   *
   * @param transactionId ID of transaction
   * @param responseId ID of response
   * @param response the response
   * @param callback callback for commit after write
   */
  void writeResponse(
      String transactionId, String responseId, Response response, CommitCallback callback);

  @Override
  void close();
}
