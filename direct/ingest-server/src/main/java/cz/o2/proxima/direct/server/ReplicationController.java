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

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.TransformationDescriptor;
import cz.o2.proxima.core.repository.TransformationDescriptor.InputTransactionMode;
import cz.o2.proxima.core.repository.TransformationDescriptor.OutputTransactionMode;
import cz.o2.proxima.core.storage.StorageFilter;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transform.ElementWiseTransformation;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObservers;
import cz.o2.proxima.direct.core.commitlog.CommitLogObservers.TerminationStrategy;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.transform.DirectElementWiseTransform;
import cz.o2.proxima.direct.core.transform.TransformationObserver;
import cz.o2.proxima.direct.server.metrics.Metrics;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Server that controls replications of primary commit logs to replica attribute families. */
@Slf4j
public class ReplicationController {

  /**
   * Run the controller.
   *
   * @param args command line arguments
   * @throws Throwable on error
   */
  public static void main(String[] args) throws Throwable {
    final Repository repo;
    if (args.length == 0) {
      repo = Repository.of(ConfigFactory.load().resolve());
    } else {
      repo = Repository.of(ConfigFactory.parseFile(new File(args[0])).resolve());
    }
    try {
      ReplicationController.of(repo).runReplicationThreads().get();
    } catch (Throwable err) {
      log.error("Error running replication controller.", err);
      System.exit(1);
    }
  }

  /**
   * Constructs a new {@link ReplicationController}.
   *
   * @param repository Repository to use for replication.
   * @return New replication controller.
   */
  public static ReplicationController of(Repository repository) {
    return new ReplicationController(repository);
  }

  @VisibleForTesting
  @AllArgsConstructor
  abstract class ReplicationLogObserver implements CommitLogObserver {

    private final String consumerName;
    private final boolean bulk;
    private final CommitLogReader commitLog;
    private final Set<AttributeDescriptor<?>> allowedAttributes;
    private final StorageFilter filter;
    private final AttributeWriterBase writer;

    @Override
    public boolean onNext(StreamElement element, OnNextContext context) {
      final boolean allowed = allowedAttributes.contains(element.getAttributeDescriptor());
      log.debug(
          "Consumer {}: received new ingest element {} at watermark {}",
          consumerName,
          element,
          context.getWatermark());
      if (allowed && filter.apply(element)) {
        Metrics.ingestsForAttribute(element.getAttributeDescriptor()).increment();
        if (!element.isDelete()) {
          Metrics.sizeForAttribute(element.getAttributeDescriptor())
              .increment(element.getValue().length);
        }
        Failsafe.with(retryPolicy).run(() -> ingestElement(element, context));
      } else {
        Metrics.COMMIT_UPDATE_DISCARDED.increment();
        log.debug(
            "Consumer {}: discarding write of {} to {} because of {}, "
                + "with allowedAttributes {} and filter class {}",
            consumerName,
            element,
            writer.getUri(),
            allowed ? "applied filter" : "invalid attribute",
            allowedAttributes,
            filter.getClass());
        maybeCommitInvalidWrite(context);
      }
      return true;
    }

    void maybeCommitInvalidWrite(OnNextContext context) {}

    @Override
    public boolean onError(Throwable error) {
      return true;
    }

    public TerminationStrategy onFatalError(Throwable error) {
      onReplicationError(
          new IllegalStateException(
              String.format(
                  "Consumer %s: too many errors retrying the consumption of commit log %s.",
                  consumerName, commitLog.getUri()),
              error));
      return TerminationStrategy.RETHROW;
    }

    @Override
    public void onRepartition(OnRepartitionContext context) {
      log.info(
          "Consumer {}: restarting bulk processing of {} from {}, rolling back the writer",
          consumerName,
          writer.getUri(),
          context.partitions());
      writer.rollback();
    }

    @Override
    public void onIdle(OnIdleContext context) {
      reportConsumerWatermark(context.getWatermark(), -1);
    }

    void reportConsumerWatermark(long watermark, long elementStamp) {
      Metrics.reportConsumerWatermark(consumerName, bulk, watermark, elementStamp);
    }

    abstract void ingestElement(StreamElement ingest, OnNextContext context);
  }

  @Getter
  RetryPolicy<Void> retryPolicy =
      RetryPolicy.<Void>builder()
          .withMaxRetries(3)
          .withBackoff(Duration.ofSeconds(3), Duration.ofSeconds(20), 2.0)
          .build();

  private final Repository repository;
  private final DirectDataOperator dataOperator;
  private final ScheduledExecutorService scheduler =
      new ScheduledThreadPoolExecutor(
          1,
          runnable -> {
            Thread ret = new Thread(runnable);
            ret.setName("replication-scheduler");
            return ret;
          });
  private static final boolean ignoreErrors = false;

  private final List<CompletableFuture<Void>> replications = new CopyOnWriteArrayList<>();

  ReplicationController(Repository repository) {
    this.repository = repository;
    this.dataOperator = repository.getOrCreateOperator(DirectDataOperator.class);
  }

  public CompletableFuture<Void> runReplicationThreads() {

    final CompletableFuture<Void> completed = new CompletableFuture<>();
    replications.add(completed);

    // index the repository
    Map<DirectAttributeFamilyDescriptor, Set<DirectAttributeFamilyDescriptor>> familyToCommitLog;
    familyToCommitLog = indexFamilyToCommitLogs();

    log.info("Starting consumer threads for familyToCommitLog {}", familyToCommitLog);

    // execute threads to consume the commit log
    familyToCommitLog.forEach(
        (replicaFamily, primaryFamilies) -> {
          for (DirectAttributeFamilyDescriptor primaryFamily : primaryFamilies) {
            if (!replicaFamily.getDesc().getAccess().isReadonly()) {
              consumeLog(primaryFamily, replicaFamily);
            } else {
              log.debug("Not starting thread for read-only family {}", replicaFamily);
            }
          }
        });

    // execute transformer threads
    Map<String, TransformationDescriptor> transformations = repository.getTransformations();
    log.info("Starting transformations {}", transformations);
    transformations.forEach(this::runTransformer);

    scheduler.scheduleAtFixedRate(this::checkLiveness, 0, 1, TimeUnit.SECONDS);

    return completed;
  }

  @VisibleForTesting
  boolean checkLiveness() {
    Pair<Long, Long> minWatermarks = Metrics.minWatermarkOfConsumers();
    boolean isLive =
        minWatermarks.getFirst() > System.currentTimeMillis() - 10_000
            || minWatermarks.getSecond() > 2 * 3600_000;
    if (log.isDebugEnabled()) {
      log.debug("Min watermark of consumers calculated as {}", minWatermarks);
    }
    if (!isLive) {
      log.warn(
          "{} is not alive due to consumer watermark lags {}",
          ReplicationController.class.getSimpleName(),
          Metrics.consumerWatermarkLags());
    }
    Metrics.LIVENESS.increment(isLive ? 1 : 0);
    return isLive;
  }

  private void consumeLog(
      DirectAttributeFamilyDescriptor primaryFamily,
      DirectAttributeFamilyDescriptor replicaFamily) {

    final CommitLogReader commitLog =
        primaryFamily
            .getCommitLogReader()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Failed to find commit-log reader in family %s.", primaryFamily)));

    final AttributeWriterBase writer =
        replicaFamily
            .getWriter()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unable to get writer for family %s.",
                            replicaFamily.getDesc().getName())));

    final StorageFilter filter = replicaFamily.getDesc().getFilter();

    final Set<AttributeDescriptor<?>> allowedAttributes =
        new HashSet<>(replicaFamily.getAttributes());

    final String name = replicaFamily.getDesc().getReplicationConsumerNameFactory().apply();
    log.info(
        "Using consumer name {} to replicate family {}", name, replicaFamily.getDesc().getName());

    registerWriterTo(name, commitLog, allowedAttributes, filter, writer);

    log.info(
        "Started consumer {} consuming from log {} with URI {} into {} attributes {}",
        name,
        commitLog,
        commitLog.getUri(),
        writer.getUri(),
        allowedAttributes);
  }

  /**
   * Retrieve attribute family and it's associated commit log(s). The families returned are only
   * those which are not used as commit log themselves.
   */
  private Map<DirectAttributeFamilyDescriptor, Set<DirectAttributeFamilyDescriptor>>
      indexFamilyToCommitLogs() {

    // each attribute and its associated primary family
    final Map<AttributeDescriptor<?>, DirectAttributeFamilyDescriptor> primaryFamilies =
        dataOperator
            .getAllFamilies()
            .filter(family -> family.getDesc().getType() == StorageType.PRIMARY)
            // take pair of attribute to associated commit log
            .flatMap(
                primaryFamily ->
                    primaryFamily.getAttributes().stream()
                        .map(attribute -> Pair.of(attribute, primaryFamily)))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    return dataOperator
        .getAllFamilies()
        .filter(family -> family.getDesc().getType() == StorageType.REPLICA)
        // map to pair of attribute family and associated commit log(s) via attributes
        .map(
            replicaFamily -> {
              if (replicaFamily.getSource().isPresent()) {
                final String source = replicaFamily.getSource().get();
                return Pair.of(
                    replicaFamily,
                    Collections.singleton(
                        dataOperator
                            .getAllFamilies()
                            .filter(af2 -> af2.getDesc().getName().equals(source))
                            .findAny()
                            .orElseThrow(
                                () ->
                                    new IllegalArgumentException(
                                        String.format("Unknown family %s.", source)))));
              }
              return Pair.of(
                  replicaFamily,
                  replicaFamily.getAttributes().stream()
                      .map(
                          attr -> {
                            final DirectAttributeFamilyDescriptor primaryFamily =
                                primaryFamilies.get(attr);
                            final Optional<OnlineAttributeWriter> maybeWriter =
                                dataOperator.getWriter(attr);
                            if (primaryFamily == null && maybeWriter.isPresent()) {
                              throw new IllegalStateException(
                                  String.format("Missing source commit log family for %s.", attr));
                            }
                            return primaryFamily;
                          })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toSet()));
            })
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private void runTransformer(String name, TransformationDescriptor transform) {
    if (transform.getInputTransactionMode() == InputTransactionMode.TRANSACTIONAL) {
      log.info(
          "Skipping run of transformation {} which reads from transactional attributes {}. "
              + "Will be executed during transaction commit.",
          name,
          transform.getAttributes());
      return;
    }
    DirectAttributeFamilyDescriptor family =
        transform.getAttributes().stream()
            .map(
                attr ->
                    getAttributeDescriptorStreamFor(dataOperator, attr).collect(Collectors.toSet()))
            .reduce(Sets::intersection)
            .filter(s -> !s.isEmpty())
            .flatMap(s -> s.stream().filter(f -> f.getCommitLogReader().isPresent()).findAny())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Cannot obtain attribute family for " + transform.getAttributes()));

    runTransform(name, transform, family);
  }

  private void runTransform(
      String name, TransformationDescriptor transform, DirectAttributeFamilyDescriptor family) {

    final StorageFilter filter = transform.getFilter();
    final String consumer = transform.getConsumerNameFactory().apply();

    final CommitLogReader reader =
        family
            .getCommitLogReader()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Unable to get reader for family " + family.getDesc().getName() + "."));

    final TransformationObserver observer;
    if (transform.getTransformation().isContextual()) {
      DirectElementWiseTransform transformation =
          transform.getTransformation().as(DirectElementWiseTransform.class);
      observer =
          contextualObserver(
              dataOperator,
              name,
              transformation,
              transform.getOutputTransactionMode() == OutputTransactionMode.ENABLED,
              filter);
    } else {
      ElementWiseTransformation transformation =
          transform.getTransformation().asElementWiseTransform();
      observer =
          nonContextualObserver(
              dataOperator,
              name,
              transformation,
              transform.getOutputTransactionMode() == OutputTransactionMode.ENABLED,
              filter);
    }
    startTransformationObserverUsing(consumer, reader, observer);
    log.info(
        "Started transformer {} reading from {} using {}",
        consumer,
        reader.getUri(),
        transform.getTransformation().getClass());
  }

  private TransformationObserver nonContextualObserver(
      DirectDataOperator dataOperator,
      String name,
      ElementWiseTransformation transformation,
      boolean supportTransactions,
      StorageFilter filter) {

    return new TransformationObserver.NonContextual(
        dataOperator, name, transformation, supportTransactions, filter) {

      @Override
      protected void reportConsumerWatermark(String name, long watermark, long elementStamp) {
        Metrics.reportConsumerWatermark(name, false, watermark, elementStamp);
      }

      @Override
      protected void die(String msg) {
        Utils.die(msg);
      }
    };
  }

  private TransformationObserver contextualObserver(
      DirectDataOperator dataOperator,
      String name,
      DirectElementWiseTransform transformation,
      boolean supportTransactions,
      StorageFilter filter) {

    return new TransformationObserver.Contextual(
        dataOperator, name, transformation, supportTransactions, filter) {

      @Override
      protected void reportConsumerWatermark(String name, long watermark, long elementStamp) {
        Metrics.reportConsumerWatermark(name, false, watermark, elementStamp);
      }

      @Override
      protected void die(String msg) {
        Utils.die(msg);
      }
    };
  }

  private Stream<DirectAttributeFamilyDescriptor> getAttributeDescriptorStreamFor(
      DirectDataOperator direct, AttributeDescriptor<?> attr) {

    EntityDescriptor entity = direct.getRepository().getEntity(attr.getEntity());
    if (entity.isSystemEntity()) {
      return direct
          .getRepository()
          .getAllFamilies(true)
          .filter(af -> af.getEntity().equals(entity))
          .filter(af -> af.getAttributes().contains(attr))
          .filter(af -> af.getType() == StorageType.PRIMARY)
          .map(af -> direct.getFamilyByName(af.getName()));
    }
    return direct.getFamiliesForAttribute(attr).stream()
        .filter(af -> af.getDesc().getAccess().canReadCommitLog());
  }

  private void startTransformationObserverUsing(
      String consumerName, CommitLogReader reader, TransformationObserver observer) {

    reader.observe(
        consumerName,
        CommitLogObservers.withNumRetriedExceptions(
            consumerName, 3, observer::onFatalError, observer));
  }

  private void registerWriterTo(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      AttributeWriterBase writerBase) {
    log.info(
        "Registering {} writer to {} from commit log {}",
        writerBase.getType(),
        writerBase.getUri(),
        commitLog.getUri());
    switch (writerBase.getType()) {
      case ONLINE:
        {
          final CommitLogObserver observer =
              createOnlineObserver(
                  consumerName, commitLog, allowedAttributes, filter, writerBase.online());
          commitLog.observe(consumerName, observer);
          break;
        }
      case BULK:
        {
          final CommitLogObserver observer =
              createBulkObserver(
                  consumerName, commitLog, allowedAttributes, filter, writerBase.bulk());
          commitLog.observeBulk(consumerName, observer);
          break;
        }
      default:
        throw new IllegalStateException(
            String.format("Unknown writer type %s.", writerBase.getType()));
    }
  }

  /**
   * Get observer for that replicates data using {@link BulkAttributeWriter}.
   *
   * @param consumerName Name of the observer.
   * @param commitLog Commit log to observe.
   * @param allowedAttributes Attributes to replicate.
   * @param filter Filter for elements that we don't want to replicate.
   * @param writer Writer for replica.
   * @return Log observer.
   */
  @VisibleForTesting
  CommitLogObserver createBulkObserver(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      BulkAttributeWriter writer) {

    final ReplicationLogObserver observer =
        new ReplicationLogObserver(
            consumerName, true, commitLog, allowedAttributes, filter, writer) {

          @Override
          void ingestElement(StreamElement ingest, OnNextContext context) {
            final long watermark = context.getWatermark();
            reportConsumerWatermark(watermark, ingest.getStamp());
            log.debug(
                "Consumer {}: writing element {} into {} at watermark {}",
                consumerName,
                ingest,
                writer,
                watermark);
            writer.write(
                ingest,
                watermark,
                (success, error) ->
                    confirmWrite(
                        consumerName,
                        ingest,
                        writer,
                        success,
                        error,
                        context::confirm,
                        context::fail));
          }

          @Override
          public void onIdle(OnIdleContext context) {
            writer.updateWatermark(context.getWatermark());
          }
        };
    return CommitLogObservers.withNumRetriedExceptions(
        consumerName, 3, observer::onFatalError, observer);
  }

  /**
   * Get observer for that replicates data using {@link OnlineAttributeWriter}.
   *
   * @param consumerName Name of the observer.
   * @param commitLog Commit log to observe.
   * @param allowedAttributes Attributes to replicate.
   * @param filter Filter for elements that we don't want to replicate.
   * @param writer Writer for replica.
   * @return Log observer.
   */
  @VisibleForTesting
  CommitLogObserver createOnlineObserver(
      String consumerName,
      CommitLogReader commitLog,
      Set<AttributeDescriptor<?>> allowedAttributes,
      StorageFilter filter,
      OnlineAttributeWriter writer) {

    final ReplicationLogObserver observer =
        new ReplicationLogObserver(
            consumerName, false, commitLog, allowedAttributes, filter, writer) {

          @Override
          void ingestElement(StreamElement ingest, OnNextContext context) {
            reportConsumerWatermark(context.getWatermark(), ingest.getStamp());
            log.debug("Consumer {}: writing element {} into {}", consumerName, ingest, writer);
            writer.write(
                ingest,
                (success, exc) ->
                    confirmWrite(
                        consumerName,
                        ingest,
                        writer,
                        success,
                        exc,
                        context::confirm,
                        context::fail));
          }

          @Override
          void maybeCommitInvalidWrite(OnNextContext context) {
            context.confirm();
          }
        };
    return CommitLogObservers.withNumRetriedExceptions(
        consumerName, 3, observer::onFatalError, observer);
  }

  private void confirmWrite(
      String consumerName,
      StreamElement ingest,
      AttributeWriterBase writer,
      boolean success,
      Throwable exc,
      Runnable onSuccess,
      Consumer<Throwable> onError) {

    if (!success) {
      log.error(
          "Consumer {}: failed to write ingest {} to {}",
          consumerName,
          ingest,
          writer.getUri(),
          exc);
      Metrics.NON_COMMIT_WRITES_RETRIES.increment();
      if (ignoreErrors) {
        log.error(
            "Consumer {}: retries exhausted trying to ingest {} to {}. "
                + "Configured to ignore. Skipping.",
            consumerName,
            ingest,
            writer.getUri());
        onSuccess.run();
      } else {
        onError.accept(exc);
      }
    } else {
      if (ingest.isDelete()) {
        Metrics.NON_COMMIT_LOG_DELETES.increment();
      } else {
        Metrics.NON_COMMIT_LOG_UPDATES.increment();
      }
      onSuccess.run();
    }
  }

  private static void onReplicationError(Throwable t) {
    Utils.die(t.getMessage(), t);
  }
}
