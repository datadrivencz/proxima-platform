/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.flink.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

@Experimental(value = "API can be changed.")
@Slf4j
public class CommitLogSourceFunction<OutputT>
    extends AbstractLogSourceFunction<
        FlinkDataOperator.CommitLogOptions,
        CommitLogReader,
        CommitLogSourceFunction.LogObserver<OutputT>,
        Offset,
        CommitLogObserver.OnNextContext,
        OutputT> {

  private static final String CONSUMER_NAME_STATE_NAME = "consumer-name";

  @Nullable private transient ListState<String> consumerNameState;

  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private String consumerName;

  static class LogObserver<OutputT>
      extends AbstractSourceLogObserver<Offset, CommitLogObserver.OnNextContext, OutputT>
      implements CommitLogObserver {

    LogObserver(
        SourceContext<OutputT> sourceContext,
        ResultExtractor<OutputT> resultExtractor,
        Set<Partition> skipFirstElementFromEachPartition,
        List<AttributeDescriptor<?>> attributesToEmit) {
      super(sourceContext, resultExtractor, skipFirstElementFromEachPartition, attributesToEmit);
    }

    @Override
    public void onIdle(OnIdleContext context) {
      maybeUpdateWatermark(context.getWatermark());
    }

    @Override
    void markOffsetAsConsumed(CommitLogObserver.OnNextContext context) {
      // No-op.
    }
  }

  public CommitLogSourceFunction(
      String consumerName,
      RepositoryFactory repositoryFactory,
      List<AttributeDescriptor<?>> attributeDescriptors,
      FlinkDataOperator.CommitLogOptions options,
      ResultExtractor<OutputT> resultExtractor) {
    super(repositoryFactory, attributeDescriptors, options, resultExtractor);
    this.consumerName = Objects.requireNonNull(consumerName);
  }

  @Override
  CommitLogReader createLogReader(List<AttributeDescriptor<?>> attributeDescriptors) {
    return getRepositoryFactory()
        .apply()
        .getOrCreateOperator(DirectDataOperator.class)
        .getCommitLogReader(attributeDescriptors)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Unable to find commit log reader for [%s].", attributeDescriptors)));
  }

  @Override
  List<Partition> getPartitions(CommitLogReader reader) {
    return reader.getPartitions();
  }

  @Override
  Partition getOffsetPartition(Offset offset) {
    return offset.getPartition();
  }

  @Override
  Set<Partition> getSkipFirstElementFromPartitions(List<Offset> offsets) {
    return offsets.stream().map(Offset::getPartition).collect(Collectors.toSet());
  }

  @Override
  LogObserver<OutputT> createLogObserver(
      SourceContext<OutputT> sourceContext,
      ResultExtractor<OutputT> resultExtractor,
      Set<Partition> skipFirstElement,
      List<AttributeDescriptor<?>> attributesToEmit) {
    return new LogObserver<>(sourceContext, resultExtractor, skipFirstElement, attributesToEmit);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
    consumerNameState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>(CONSUMER_NAME_STATE_NAME, String.class));
    if (context.isRestored()) {
      Objects.requireNonNull(consumerNameState);
      consumerName = Iterables.getFirst(consumerNameState.get(), consumerName);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    super.snapshotState(functionSnapshotContext);
    Objects.requireNonNull(consumerNameState).clear();
    consumerNameState.add(Objects.requireNonNull(consumerName));
  }

  @Override
  UnifiedObserveHandle<Offset> observePartitions(
      CommitLogReader reader,
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributeDescriptors,
      LogObserver<OutputT> observer) {
    final FlinkDataOperator.CommitLogOptions options = getOptions();
    final ObserveHandle commitLogHandle =
        reader.observeBulkPartitions(
            consumerName, partitions, options.initialPosition(), options.stopAtCurrent(), observer);
    return new UnifiedObserveHandle<Offset>() {

      @Override
      public List<Offset> getConsumedOffsets() {
        return commitLogHandle.getCurrentOffsets();
      }

      @Override
      public void close() {
        commitLogHandle.close();
      }
    };
  }

  @Override
  UnifiedObserveHandle<Offset> observeRestoredOffsets(
      CommitLogReader reader,
      List<Offset> offsets,
      List<AttributeDescriptor<?>> attributeDescriptors,
      LogObserver<OutputT> observer) {
    @SuppressWarnings("resource")
    final ObserveHandle delegate =
        reader.observeBulkOffsets(offsets, getOptions().stopAtCurrent(), observer);
    return new UnifiedObserveHandle<Offset>() {

      @Override
      public List<Offset> getConsumedOffsets() {
        return delegate.getCurrentOffsets();
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }
}
