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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.core.annotations.DeclaredThreadSafe;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.DataOperator;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.Repository.Validate;
import cz.o2.proxima.core.repository.TransactionMode;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.core.storage.ThroughputLimiter;
import cz.o2.proxima.core.storage.internal.DataAccessorLoader;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.core.util.internal.ClassLoaders.ChildLayerFirstClassLoader;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.BatchLogReaders;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.CommitLogReaders;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.transaction.ClientTransactionManager;
import cz.o2.proxima.direct.core.transaction.ServerTransactionManager;
import cz.o2.proxima.direct.core.transaction.TransactionResourceManager;
import cz.o2.proxima.direct.core.transaction.TransactionalCachedView;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.typesafe.config.Config;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** {@link DataOperator} implementation for direct accesses. */
@Slf4j
public class DirectDataOperator implements DataOperator, ContextProvider {

  private static final String THROUGHPUT_LIMITER_PREFIX = "direct.throughput-limiter.";
  private static final String KW_CLASS = "class";
  private static final AtomicInteger threadId = new AtomicInteger();
  private static final String ID = UUID.randomUUID().toString();

  private static Factory<ExecutorService> createExecutorFactory() {
    return () ->
        Executors.newCachedThreadPool(
            r -> {
              final Thread t = new Thread(r);
              t.setName(
                  String.format("DirectDataOperatorThread-%s-%d", ID, threadId.incrementAndGet()));
              t.setUncaughtExceptionHandler(
                  (thr, exc) -> {
                    log.error(
                        "Error running task in thread {}, bailing out...", thr.getName(), exc);
                    Runtime.getRuntime().exit(1);
                  });
              if (Repository.getJpmsClassloader() != null) {
                t.setContextClassLoader(Repository.getJpmsClassloader());
              }
              return t;
            });
  }

  /** Repository. */
  private final Repository repo;

  /** AttributeFamilyDescriptor with associated DirectAttributeFamilyDescriptor. */
  private final Map<AttributeFamilyDescriptor, DirectAttributeFamilyDescriptor> familyMap =
      Collections.synchronizedMap(new HashMap<>());

  /** Cache of writers for all attributes. */
  private final Map<AttributeDescriptor<?>, OnlineAttributeWriter> writers =
      Collections.synchronizedMap(new HashMap<>());

  private Factory<ExecutorService> executorFactory = createExecutorFactory();

  private final Context context;

  private final Config config;

  private final DataAccessorLoader<DirectDataOperator, DataAccessor, DataAccessorFactory> loader;

  private volatile TransactionResourceManager transactionManager;

  DirectDataOperator(Repository repo) {
    this.repo = repo;
    this.context = new Context(familyMap::get, executorFactory);
    this.config = ((ConfigRepository) repo).getConfig();

    final ModuleLayer contextLayer;
    if (Thread.currentThread().getContextClassLoader() instanceof ChildLayerFirstClassLoader) {
      contextLayer =
          ((ChildLayerFirstClassLoader) Thread.currentThread().getContextClassLoader()).getLayer();
    } else {
      contextLayer = getClass().getModule().getLayer();
    }
    if (contextLayer != null) {
      this.loader = DataAccessorLoader.of(repo, DataAccessorFactory.class, contextLayer);
    } else {
      this.loader = DataAccessorLoader.of(repo, DataAccessorFactory.class);
    }
    reload();
  }

  /**
   * Explicitly specify {@link ExecutorService} to use when constructing threads.
   *
   * @param factory the factory for {@link ExecutorService}
   * @return this
   */
  public DirectDataOperator withExecutorFactory(Factory<ExecutorService> factory) {
    this.executorFactory = factory;
    return this;
  }

  @Override
  public final void reload() {
    close();
    familyMap.clear();
    dependencyOrdered(repo.getAllFamilies(true)).forEach(this::addResolvedFamily);
  }

  /** Create list of families ordered by dependencies between them (non-proxy first). */
  private List<AttributeFamilyDescriptor> dependencyOrdered(
      Stream<AttributeFamilyDescriptor> families) {

    Set<AttributeFamilyDescriptor> available = new HashSet<>();
    List<AttributeFamilyDescriptor> resolved = new ArrayList<>();
    Set<AttributeFamilyDescriptor> toResolve = families.collect(Collectors.toSet());
    while (!toResolve.isEmpty()) {
      // prevent infinite cycles
      List<AttributeFamilyDescriptor> remove = new ArrayList<>();
      List<AttributeFamilyDescriptor> add = new ArrayList<>();
      toResolve.stream()
          .filter(af -> !available.contains(af))
          .forEachOrdered(
              af -> {
                if (!af.isProxy()) {
                  available.add(af);
                  resolved.add(af);
                  remove.add(af);
                } else {
                  AttributeFamilyProxyDescriptor proxy = af.toProxy();
                  if (available.contains(proxy.getTargetFamilyRead())
                      && available.contains(proxy.getTargetFamilyWrite())) {

                    available.add(af);
                    resolved.add(af);
                    remove.add(af);
                  } else if (!available.contains(proxy.getTargetFamilyRead())) {
                    add.add(proxy.getTargetFamilyRead());
                  } else {
                    add.add(proxy.getTargetFamilyWrite());
                  }
                }
              });
      if (add.isEmpty() && remove.isEmpty()) {
        throw new IllegalStateException(
            "Cannot make progress in resolving families "
                + toResolve.stream()
                    .map(AttributeFamilyDescriptor::getName)
                    .collect(Collectors.toList())
                + ", currently resolved "
                + available.stream()
                    .map(AttributeFamilyDescriptor::getName)
                    .collect(Collectors.toList()));
      }
      toResolve.addAll(add);
      remove.forEach(toResolve::remove);
    }
    return resolved;
  }

  private void addResolvedFamily(AttributeFamilyDescriptor family) {
    try {
      if (!familyMap.containsKey(family)) {
        log.debug("Adding new family {} to familyMap", family);
        if (family.isProxy()) {
          AttributeFamilyProxyDescriptor proxy = family.toProxy();
          addResolvedFamily(proxy.getTargetFamilyRead());
          addResolvedFamily(proxy.getTargetFamilyWrite());
          if (isValidFamily(proxy.getTargetFamilyRead())
              && isValidFamily(proxy.getTargetFamilyWrite())) {
            familyMap.put(family, DirectAttributeFamilyProxyDescriptor.of(repo, context, proxy));
          } else {
            familyMap.put(family, new InvalidDirectAttributeFamilyDescriptor(repo, family));
          }
        } else {
          @Nullable DataAccessor accessor = findAccessorFor(family);
          if (accessor != null) {
            familyMap.put(
                family, new DirectAttributeFamilyDescriptor(repo, family, context, accessor));
          } else {
            familyMap.put(family, new InvalidDirectAttributeFamilyDescriptor(repo, family));
          }
        }
      }
    } catch (Exception ex) {
      log.error("Failed to add family {}", family, ex);
      throw ex;
    }
  }

  private boolean isValidFamily(AttributeFamilyDescriptor family) {
    return !(familyMap.get(family) instanceof InvalidDirectAttributeFamilyDescriptor);
  }

  private @Nullable DataAccessor findAccessorFor(AttributeFamilyDescriptor desc) {
    return getAccessorFactory(desc.getStorageUri())
        .map(f -> f.createAccessor(this, desc))
        .filter(f -> !repo.isShouldValidate(Validate.ACCESSES) || f.isAcceptable(desc))
        .orElse(null);
  }

  /**
   * Retrieve {@link Context} that is used in all distributed operations.
   *
   * @return the serializable context
   */
  @Override
  public Context getContext() {
    return context;
  }

  /**
   * Convert given core family to direct representation.
   *
   * @param family the family to convert
   * @return the converted family
   */
  public DirectAttributeFamilyDescriptor resolveRequired(AttributeFamilyDescriptor family) {
    return context.resolveRequired(family);
  }

  /**
   * Optionally convert given family to direct representation.
   *
   * @param family the family to convert
   * @return the optionally converted family
   */
  public Optional<DirectAttributeFamilyDescriptor> resolve(AttributeFamilyDescriptor family) {
    return context.resolve(family);
  }

  /**
   * Retrieve factory that first matches given uri.
   *
   * @param uri the URI to search factory for
   * @return optional {@link DataAccessorFactory} for specified URI
   */
  public Optional<DataAccessorFactory> getAccessorFactory(URI uri) {
    return loader.findForUri(uri).map(DelegateDataAccessorFactory::new);
  }

  /**
   * Retrieve writer for given {@link AttributeDescriptor}.
   *
   * @param attr the attribute to find writer for
   * @return optional writer
   */
  public Optional<OnlineAttributeWriter> getWriter(AttributeDescriptor<?> attr) {
    synchronized (writers) {
      OnlineAttributeWriter writer = writers.get(attr);
      if (writer == null) {
        if (attr.getEntity().equals(ConfigConstants.TRANSACTION_ENTITY)
            && attr.getName().equals("commit")) {

          repo.getAllFamilies(true)
              .filter(af -> af.getEntity().getName().equals(attr.getEntity()))
              .filter(af -> af.getAttributes().contains(attr))
              .map(af -> getFamilyByName(af.getName()))
              .forEach(this::cacheOrRetrieveWriterFor);
        } else {
          repo.getFamiliesForAttribute(attr).stream()
              .filter(af -> af.getType() == StorageType.PRIMARY)
              .filter(af -> !af.getAccess().isReadonly())
              .findAny()
              .flatMap(context::resolve)
              .ifPresent(this::cacheOrRetrieveWriterFor);
        }

        OnlineAttributeWriter result = writers.get(attr);
        if (result == null) {
          log.warn(
              "Failed to find writer for attribute {} from candidates {} (all families {})",
              attr,
              repo.getFamiliesForAttribute(attr).stream()
                  .filter(af -> af.getType() == StorageType.PRIMARY)
                  .filter(af -> !af.getAccess().isReadonly())
                  .collect(Collectors.toList()),
              repo.getFamiliesForAttribute(attr));
        }
        return Optional.ofNullable(result);
      }
      return Optional.of(writer);
    }
  }

  /**
   * Retrieve a {@link TransactionalOnlineAttributeWriter} with GLOBAL transactions capable to
   * fulfil any transactional requests.
   *
   * <p>Note that global transactions are required to be processed by a single instance of {@link
   * cz.o2.proxima.direct.core.transaction.TransactionManager} and therefore are not scalable.
   *
   * <p>Use with caution.
   *
   * @return the global {@link TransactionalOnlineAttributeWriter} that is able to handle all
   *     transactions
   */
  public TransactionalOnlineAttributeWriter getGlobalTransactionWriter() {
    return TransactionalOnlineAttributeWriter.global(this);
  }

  private void cacheOrRetrieveWriterFor(DirectAttributeFamilyDescriptor af) {
    Optional<AttributeWriterBase> maybeWriter = af.getWriter();
    if (maybeWriter.isPresent()) {
      @Nullable OnlineAttributeWriter maybeTransactionalWriter = null;
      OnlineAttributeWriter familyWriter = maybeWriter.get().online();
      if (af.getAttributes().stream()
          .anyMatch(a -> a.getTransactionMode() != TransactionMode.NONE)) {

        maybeTransactionalWriter = wrapTransactionalWriterAround(familyWriter);
      }
      // store writer of this family to all attributes
      for (AttributeDescriptor<?> a : af.getAttributes()) {
        if (a.getTransactionMode() == TransactionMode.NONE) {
          if (familyWriter.getClass().getAnnotation(DeclaredThreadSafe.class) == null) {
            writers.put(a, OnlineAttributeWriters.synchronizedWriter(familyWriter));
          } else {
            writers.put(a, familyWriter);
          }
        } else {
          writers.put(a, maybeTransactionalWriter);
        }
      }
    }
  }

  private TransactionalOnlineAttributeWriter wrapTransactionalWriterAround(
      OnlineAttributeWriter familyWriter) {

    return TransactionalOnlineAttributeWriter.of(this, familyWriter);
  }

  /**
   * Retrieve {@link CommitLogReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find commit log reader for
   * @return optional commit log reader
   */
  public Optional<CommitLogReader> getCommitLogReader(Collection<AttributeDescriptor<?>> attrs) {
    return getFamilyForAttributes(attrs, DirectAttributeFamilyDescriptor::hasCommitLogReader)
        .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader);
  }

  /**
   * Retrieve {@link CommitLogReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find commit log reader for
   * @return optional commit log reader
   */
  @SafeVarargs
  public final Optional<CommitLogReader> getCommitLogReader(AttributeDescriptor<?>... attrs) {
    return getCommitLogReader(Arrays.asList(attrs));
  }

  /**
   * Retrieve {@link BatchLogReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find commit log reader for
   * @return optional batch log reader
   */
  public Optional<BatchLogReader> getBatchLogReader(Collection<AttributeDescriptor<?>> attrs) {
    return getFamilyForAttributes(attrs, DirectAttributeFamilyDescriptor::hasBatchReader)
        .flatMap(DirectAttributeFamilyDescriptor::getBatchReader);
  }

  /**
   * Retrieve {@link CommitLogReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find commit log reader for
   * @return optional commit log reader
   */
  @SafeVarargs
  public final Optional<BatchLogReader> getBatchLogReader(AttributeDescriptor<?>... attrs) {
    return getBatchLogReader(Arrays.asList(attrs));
  }

  /**
   * Retrieve {@link CachedView} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find cached view for
   * @return optional cached view
   */
  public Optional<CachedView> getCachedView(Collection<AttributeDescriptor<?>> attrs) {
    boolean hasTransactions =
        attrs.stream().anyMatch(a -> a.getTransactionMode() != TransactionMode.NONE);
    Optional<CachedView> view =
        getFamilyForAttributes(attrs, DirectAttributeFamilyDescriptor::hasCachedView)
            .flatMap(DirectAttributeFamilyDescriptor::getCachedView);
    if (hasTransactions) {
      return view.map(v -> new TransactionalCachedView(this, v));
    }
    return view;
  }

  /**
   * Retrieve {@link CachedView} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find cached view for
   * @return optional cached view
   */
  @SafeVarargs
  public final Optional<CachedView> getCachedView(AttributeDescriptor<?>... attrs) {
    return getCachedView(Arrays.asList(attrs));
  }

  /**
   * Retrieve {@link RandomAccessReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find radom access reader for
   * @return optional random access reader
   */
  public Optional<RandomAccessReader> getRandomAccess(Collection<AttributeDescriptor<?>> attrs) {
    return getFamilyForAttributes(attrs, DirectAttributeFamilyDescriptor::hasRandomAccessReader)
        .flatMap(DirectAttributeFamilyDescriptor::getRandomAccessReader);
  }

  /**
   * Retrieve {@link RandomAccessReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find random access reader for
   * @return optional random access reader
   */
  @SafeVarargs
  public final Optional<RandomAccessReader> getRandomAccess(AttributeDescriptor<?>... attrs) {
    return getRandomAccess(Arrays.asList(attrs));
  }

  private Optional<DirectAttributeFamilyDescriptor> getFamilyForAttributes(
      Collection<AttributeDescriptor<?>> attrs,
      UnaryFunction<DirectAttributeFamilyDescriptor, Boolean> mask) {

    return attrs.stream()
        .map(
            a ->
                getFamiliesForAttribute(a).stream().filter(mask::apply).collect(Collectors.toSet()))
        .reduce(Sets::intersection)
        .flatMap(s -> s.stream().findAny());
  }

  /** Close the operator and release all allocated resources. */
  @Override
  public void close() {
    final List<OnlineAttributeWriter> toClose;
    synchronized (writers) {
      toClose = writers.values().stream().distinct().collect(Collectors.toList());
      writers.clear();
    }
    toClose.forEach(OnlineAttributeWriter::close);
    Optional.ofNullable(transactionManager).ifPresent(TransactionResourceManager::close);
  }

  /**
   * Resolve all direct attribute representations of given attribute.
   *
   * @param desc descriptor of attribute
   * @return the set of all direct attribute representations
   */
  public Set<DirectAttributeFamilyDescriptor> getFamiliesForAttribute(AttributeDescriptor<?> desc) {
    return repo.getFamiliesForAttribute(desc).stream()
        .map(this::resolveRequired)
        .collect(Collectors.toSet());
  }

  /**
   * Retrieve all families with their direct representation.
   *
   * @return stream of all {@link DirectAttributeFamilyDescriptor}s.
   */
  public Stream<DirectAttributeFamilyDescriptor> getAllFamilies() {
    return repo.getAllFamilies().map(this::resolveRequired);
  }

  /**
   * Retrieve family by given name.
   *
   * @param name name of the family to search for
   * @return {@link DirectAttributeFamilyDescriptor} for given family
   * @throws IllegalArgumentException when family not found
   */
  public DirectAttributeFamilyDescriptor getFamilyByName(String name) {
    return findFamilyByName(name)
        .orElseThrow(() -> new IllegalArgumentException("Family " + name + " not found"));
  }

  /**
   * Retrieve family by given name.
   *
   * @param name name of the family to search for
   * @return {@link Optional} of {@link DirectAttributeFamilyDescriptor} for given family of {@link
   *     Optional#empty()}
   */
  public Optional<DirectAttributeFamilyDescriptor> findFamilyByName(String name) {
    return repo.findFamilyByName(name).flatMap(f -> Optional.ofNullable(familyMap.get(f)));
  }

  /**
   * @return cached implementation of {@link ClientTransactionManager}. Should not be called from
   *     client code.
   */
  @Internal
  public ClientTransactionManager getClientTransactionManager() {
    return getTransactionManager();
  }

  /**
   * @return cached implementation of {@link ServerTransactionManager}. Should not be called from
   *     client code.
   */
  @Internal
  public ServerTransactionManager getServerTransactionManager() {
    return getTransactionManager();
  }

  private TransactionResourceManager getTransactionManager() {
    if (transactionManager == null) {
      synchronized (this) {
        if (transactionManager == null) {
          Map<String, Object> cfg =
              config.hasPath(ConfigConstants.TRANSACTIONS)
                  ? config.getObject(ConfigConstants.TRANSACTIONS).unwrapped()
                  : Collections.emptyMap();
          transactionManager = new TransactionResourceManager(this, cfg);
        }
      }
    }
    return transactionManager;
  }

  @Override
  public Repository getRepository() {
    return repo;
  }

  @VisibleForTesting
  public static class DelegateDataAccessorFactory implements DataAccessorFactory {

    private static final long serialVersionUID = 1L;

    @Getter private final DataAccessorFactory delegate;

    public DelegateDataAccessorFactory(DataAccessorFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setup(Repository repo) {
      delegate.setup(repo);
    }

    @Override
    public Accept accepts(URI uri) {
      return delegate.accepts(uri);
    }

    @Override
    public DataAccessor createAccessor(
        DirectDataOperator operator, AttributeFamilyDescriptor familyDescriptor) {
      return new ForwardingDataAccessor(
          delegate.createAccessor(operator, familyDescriptor), familyDescriptor.getCfg());
    }

    private static class ForwardingDataAccessor implements DataAccessor {

      private static final long serialVersionUID = 1L;

      private final DataAccessor delegate;
      @Nullable private final ThroughputLimiter limiter;

      public ForwardingDataAccessor(DataAccessor delegate, Map<String, Object> cfg) {
        this.delegate = delegate;
        this.limiter = configureLimiter(cfg);
        log.info("Created new {} for {}", this, delegate.getUri());
      }

      @Nullable
      private ThroughputLimiter configureLimiter(Map<String, Object> cfg) {
        Map<String, Object> prefixed =
            cfg.entrySet().stream()
                .filter(e -> e.getKey().startsWith(THROUGHPUT_LIMITER_PREFIX))
                .map(
                    e ->
                        Pair.of(
                            e.getKey().substring(THROUGHPUT_LIMITER_PREFIX.length()), e.getValue()))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        return Optional.ofNullable(prefixed.get(KW_CLASS))
            .map(Object::toString)
            .map(
                cls ->
                    ExceptionUtils.uncheckedFactory(
                        () -> Classpath.newInstance(cls, ThroughputLimiter.class)))
            .map(
                l -> {
                  l.setup(prefixed);
                  return l;
                })
            .orElse(null);
      }

      @Override
      public URI getUri() {
        return delegate.getUri();
      }

      @Override
      public Optional<AttributeWriterBase> getWriter(Context context) {
        return delegate.getWriter(context);
      }

      @Override
      public Optional<CommitLogReader> getCommitLogReader(Context context) {
        return delegate
            .getCommitLogReader(context)
            .map(reader -> CommitLogReaders.withThroughputLimit(reader, limiter));
      }

      @Override
      public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
        return delegate.getRandomAccessReader(context);
      }

      @Override
      public Optional<BatchLogReader> getBatchLogReader(Context context) {
        return delegate
            .getBatchLogReader(context)
            .map(reader -> BatchLogReaders.withLimitedThroughput(reader, limiter));
      }

      @Override
      public Optional<CachedView> getCachedView(Context context) {
        return delegate.getCachedView(context);
      }

      @Override
      public boolean isAcceptable(AttributeFamilyDescriptor familyDescriptor) {
        return delegate.isAcceptable(familyDescriptor);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("delegate", delegate)
            .add("limiter", limiter)
            .toString();
      }
    }
  }
}
