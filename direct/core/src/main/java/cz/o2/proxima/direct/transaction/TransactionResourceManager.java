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

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObservers;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.TransactionMode;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manager of open transactional resources - e.g. writers, commit-log readers, etc.
 *
 * <p>The implementation is thread-safe in the sense it is okay to access this class from multiple
 * threads, <b>with different transactions</b>. The same transaction <b>must</b> be processed from
 * single thread only, otherwise the behavior is undefined.
 */
@Internal
@ThreadSafe
@Slf4j
public class TransactionResourceManager implements AutoCloseable {

  public static TransactionResourceManager of(DirectDataOperator direct) {
    return new TransactionResourceManager(direct);
  }

  private static class CachedWriters {
    @Nullable OnlineAttributeWriter requestWriter;
    @Nullable OnlineAttributeWriter responseWriter;
    @Nullable OnlineAttributeWriter stateWriter;

    public void close() {
      Optional.ofNullable(requestWriter).ifPresent(OnlineAttributeWriter::close);
      Optional.ofNullable(responseWriter).ifPresent(OnlineAttributeWriter::close);
      Optional.ofNullable(stateWriter).ifPresent(OnlineAttributeWriter::close);
    }

    public OnlineAttributeWriter getOrCreateRequestWriter(DirectAttributeFamilyDescriptor family) {
      if (requestWriter == null) {
        requestWriter = Optionals.get(family.getWriter()).online();
      }
      return requestWriter;
    }
  }

  private class CachedTransaction {

    final String transactionId;
    final Set<AttributeDescriptor<?>> affectedAttributes = new HashSet<>();
    final Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor> attributeToFamily =
        new HashMap<>();
    final Thread owningThread = Thread.currentThread();
    final AtomicReference<State> state = new AtomicReference<>(State.empty());
    final @Nullable BiConsumer<String, Response> responseConsumer;
    @Nullable OnlineAttributeWriter requestWriter;
    @Nullable OnlineAttributeWriter responseWriter;

    CachedTransaction(
        String transactionId,
        List<AttributeDescriptor<?>> attributes,
        @Nullable BiConsumer<String, Response> responseConsumer) {

      this.transactionId = transactionId;
      affectedAttributes.addAll(attributes);
      attributeToFamily.putAll(findFamilyForTransactionalAttribute(attributes));
      this.responseConsumer = responseConsumer;
    }

    void open() {
      Preconditions.checkState(responseConsumer != null);
      addTransactionResponseConsumer(
          transactionId, attributeToFamily.get(requestDesc), responseConsumer);
    }

    void close() {
      Optional.ofNullable(requestWriter).ifPresent(OnlineAttributeWriter::close);
      Optional.ofNullable(responseWriter).ifPresent(OnlineAttributeWriter::close);
      requestWriter = responseWriter = null;
    }

    OnlineAttributeWriter getRequestWriter() {
      checkThread();
      if (requestWriter == null) {
        DirectAttributeFamilyDescriptor family = attributeToFamily.get(requestDesc);
        requestWriter = getCachedAccessors(family).getOrCreateRequestWriter(family);
      }
      return requestWriter;
    }

    private void checkThread() {
      Preconditions.checkState(owningThread == Thread.currentThread());
    }

    public State getState() {
      return state.get();
    }
  }

  private final DirectDataOperator direct;
  @Getter private final EntityDescriptor transaction;
  @Getter private final Wildcard<Request> requestDesc;
  @Getter private final Wildcard<Response> responseDesc;
  @Getter private final Regular<State> stateDesc;
  private final Map<String, CachedTransaction> openTransactionMap = new ConcurrentHashMap<>();
  private final Map<AttributeFamilyDescriptor, CachedWriters> cachedAccessors =
      new ConcurrentHashMap<>();
  private final Map<DirectAttributeFamilyDescriptor, ObserveHandle> observedFamilies =
      new ConcurrentHashMap<>();
  private final Map<String, BiConsumer<String, Response>> transactionResponseConsumers =
      new ConcurrentHashMap<>();

  private TransactionResourceManager(DirectDataOperator direct) {
    this.direct = direct;
    this.transaction = direct.getRepository().getEntity("_transaction");
    this.requestDesc = Wildcard.wildcard(transaction, transaction.getAttribute("request.*"));
    this.responseDesc = Wildcard.wildcard(transaction, transaction.getAttribute("response.*"));
    this.stateDesc = Regular.regular(transaction, transaction.getAttribute("state"));
  }

  @Override
  public synchronized void close() {
    openTransactionMap.forEach((k, v) -> v.close());
    cachedAccessors.forEach((k, v) -> v.close());
    openTransactionMap.clear();
    cachedAccessors.clear();
  }

  /**
   * Observe all transactional families with given observer.
   *
   * @param name name of the observer (will be appended with name of the family)
   * @param observer the observer (need not be synchronized)
   */
  public void observeRequests(String name, LogObserver observer) {
    LogObserver synchronizedObserver = LogObservers.synchronizedObserver(observer);
    direct
        .getRepository()
        .getAllFamilies(true)
        .filter(af -> af.getAttributes().contains(requestDesc))
        .map(
            af ->
                Pair.of(
                    af.getName(),
                    Optionals.get(direct.getFamilyByName(af.getName()).getCommitLogReader())))
        .forEach(r -> r.getSecond().observe(name + "-" + r.getFirst(), synchronizedObserver));
  }

  public OnlineAttributeWriter getRequestWriter(String transactionId) {
    CachedTransaction cachedTransaction = openTransactionMap.get(transactionId);
    Preconditions.checkState(
        cachedTransaction != null, "Transaction %s is not open", transactionId);
    return cachedTransaction.getRequestWriter();
  }

  private void addTransactionResponseConsumer(
      String transactionId,
      DirectAttributeFamilyDescriptor responseFamily,
      BiConsumer<String, Response> responseConsumer) {

    transactionResponseConsumers.put(transactionId, responseConsumer);
    observedFamilies.computeIfAbsent(
        responseFamily,
        k ->
            Optionals.get(k.getCommitLogReader())
                .observe(
                    "transactionResponseObserver-" + k.getDesc().getName(),
                    newTransactionResponseObserver()));
  }

  private LogObserver newTransactionResponseObserver() {
    return new LogObserver() {

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        log.debug("Received transaction event {}", ingest);
        if (ingest.getAttributeDescriptor().equals(responseDesc)) {
          String transactionId = ingest.getKey();
          BiConsumer<String, Response> consumer = transactionResponseConsumers.get(transactionId);
          if (consumer != null) {
            Optional<Response> response = responseDesc.valueOf(ingest);
            if (response.isPresent()) {
              String suffix = responseDesc.extractSuffix(ingest.getAttribute());
              consumer.accept(suffix, response.get());
            } else {
              log.error("Failed to parse response from {}", ingest);
            }
          } else {
            log.warn("Missing consumer for transaction {}", transactionId);
          }
        }
        context.confirm();
        return true;
      }
    };
  }

  private synchronized CachedWriters getCachedAccessors(DirectAttributeFamilyDescriptor family) {
    return cachedAccessors.computeIfAbsent(family.getDesc(), k -> new CachedWriters());
  }

  /**
   * Initialize (possibly) new transaction. If the transaction already existed prior to this call,
   * its old state is returned. Otherwise {@link State#empty()} is returned.
   *
   * @param transactionId ID of transaction
   * @param responseConsumer consumer of responses related to the transaction
   * @param attributes attributes affected by this transaction (both input and output)
   * @return current state of the transaction
   */
  public synchronized State begin(
      String transactionId,
      BiConsumer<String, Response> responseConsumer,
      List<AttributeDescriptor<?>> attributes) {

    log.debug("Opening transaction {} with attributes {}", transactionId, attributes);
    CachedTransaction cachedTransaction =
        openTransactionMap.computeIfAbsent(
            transactionId, k -> new CachedTransaction(transactionId, attributes, responseConsumer));
    cachedTransaction.open();
    return cachedTransaction.getState();
  }

  public synchronized void updateTransaction(
      String transactionId, List<AttributeDescriptor<?>> attributes) {

    openTransactionMap.computeIfAbsent(
        transactionId, k -> new CachedTransaction(transactionId, attributes, null));
  }

  private Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor>
      findFamilyForTransactionalAttribute(List<AttributeDescriptor<?>> attributes) {

    Preconditions.checkArgument(
        !attributes.isEmpty(), "Cannot return families for empty attribute list");

    TransactionMode mode = attributes.get(0).getTransactionMode();
    Preconditions.checkArgument(
        attributes.stream().allMatch(a -> a.getTransactionMode() == mode),
        "All passed attributes must have the same transaction mode. Got attributes %s.",
        attributes);

    List<DirectAttributeFamilyDescriptor> candidates =
        attributes
            .stream()
            .flatMap(a -> a.getTransactionalManagerFamilies().stream())
            .distinct()
            .map(direct::findFamilyByName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    long numAttributes = candidates.stream().mapToLong(f -> f.getAttributes().size()).sum();

    Preconditions.checkState(
        numAttributes == 3,
        "Should have received only families for unique transactional attributes, got %s",
        candidates);

    return candidates
        .stream()
        .flatMap(f -> f.getAttributes().stream().map(a -> Pair.of(a, f)))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }
}
