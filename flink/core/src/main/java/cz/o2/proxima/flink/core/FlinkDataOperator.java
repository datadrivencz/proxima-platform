/*
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
package cz.o2.proxima.flink.core;

import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.Watermarks;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Operate {@link Repository} using Apache Flink. */
@Experimental(value = "API can be changed.")
public class FlinkDataOperator implements DataOperator {

  private final Repository repository;

  @Getter private final StreamExecutionEnvironment executionEnvironment;

  @Getter(value = AccessLevel.PACKAGE)
  private final StreamTableEnvironment tableEnvironment;

  public FlinkDataOperator(Repository repository) {
    this.repository = repository;
    this.executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    this.tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
  }

  /**
   * Create a standardized name for a new source.
   *
   * @param type Type of the source (eg. CommitLog, BatchLog).
   * @param attributeDescriptors Attributes the source is reading.
   * @return Source name.
   */
  private static String createSourceName(
      String type, List<AttributeDescriptor<?>> attributeDescriptors) {
    return String.format(
        "%s: %s",
        type,
        attributeDescriptors
            .stream()
            .sorted()
            .map(a -> String.format("%s.%s", a.getEntity(), a.getName()))
            .collect(Collectors.joining(",")));
  }

  /**
   * Create empty {@link CommitLogOptions.CommitLogOptionsBuilder}.
   *
   * @return builder
   */
  public static CommitLogOptions.CommitLogOptionsBuilder newCommitLogOptions() {
    return CommitLogOptions.newBuilder();
  }

  /**
   * Create empty {@link BatchLogOptions.BatchLogOptionsBuilder}.
   *
   * @return builder
   */
  public static BatchLogOptions.BatchLogOptionsBuilder newBatchLogOptions() {
    return BatchLogOptions.newBuilder();
  }

  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public void reload() {
    // Nothing to reload.
  }

  @Override
  public Repository getRepository() {
    return repository;
  }

  /**
   * Create a new stream from historical batch data.
   *
   * @param attributeDescriptors Attributes we want to get data from.
   * @return A new DataStream.
   */
  public DataStream<StreamElement> createBatchLogStream(
      Collection<AttributeDescriptor<?>> attributeDescriptors) {
    return createBatchLogStream(attributeDescriptors, BatchLogOptions.newBuilder().build());
  }

  /**
   * Create a new stream from historical batch data.
   *
   * @param attributeDescriptors Attributes we want to get data from.
   * @param options Options for {@link BatchLogSourceFunction}.
   * @return A new DataStream.
   */
  public DataStream<StreamElement> createBatchLogStream(
      Collection<AttributeDescriptor<?>> attributeDescriptors, BatchLogOptions options) {
    final List<AttributeDescriptor<?>> attributeDescriptorsCopy =
        new ArrayList<>(attributeDescriptors);
    return executionEnvironment
        .addSource(
            new BatchLogSourceFunction<>(
                repository.asFactory(),
                attributeDescriptorsCopy,
                options,
                ResultExtractor.identity()),
            createSourceName("BatchLog", attributeDescriptorsCopy))
        .returns(StreamElement.class);
  }

  public DataStream<StreamElement> createCommitLogStream(
      AttributeDescriptor<?> attributeDescriptor) {
    return createCommitLogStream(Collections.singletonList(attributeDescriptor));
  }

  /**
   * Create a new stream from realtime streaming data.
   *
   * @param attributeDescriptors Attributes we want to get data from.
   * @return A new DataStream.
   */
  public DataStream<StreamElement> createCommitLogStream(
      Collection<AttributeDescriptor<?>> attributeDescriptors) {
    return createCommitLogStream(attributeDescriptors, CommitLogOptions.newBuilder().build());
  }

  /**
   * Create a new stream from realtime streaming data.
   *
   * @param attributeDescriptors Attributes we want to get data from.
   * @param options Options for {@link CommitLogSourceFunction}.
   * @return A new DataStream.
   */
  public DataStream<StreamElement> createCommitLogStream(
      Collection<AttributeDescriptor<?>> attributeDescriptors, CommitLogOptions options) {
    final List<AttributeDescriptor<?>> attributeDescriptorsCopy =
        new ArrayList<>(attributeDescriptors);
    final String consumerName;
    if (options.consumerName() == null) {
      consumerName = String.format("proxima-flink-consumer-%s", UUID.randomUUID());
    } else {
      consumerName = options.consumerName();
    }
    return executionEnvironment
        .addSource(
            new CommitLogSourceFunction<>(
                consumerName,
                repository.asFactory(),
                attributeDescriptorsCopy,
                options,
                ResultExtractor.identity()),
            createSourceName("CommitLog", attributeDescriptorsCopy))
        .returns(StreamElement.class);
  }

  /** Common options for all log based sources. */
  public interface LogOptions extends Serializable {

    /**
     * Shutdown sources, when they have no more work to do.
     *
     * @return True to shutdown when finished.
     */
    boolean shutdownFinishedSources();
  }

  /** {@link LogOptions} specific to {@link CommitLogSourceFunction}. */
  @Accessors(fluent = true)
  @Builder(builderMethodName = "newBuilder", setterPrefix = "with", toBuilder = true)
  @Value
  public static class CommitLogOptions implements LogOptions {

    private static final long serialVersionUID = 1L;

    /** Identifier of the consumer. Auto-generated in case of null. */
    @Builder.Default String consumerName = null;

    /** @see LogOptions#shutdownFinishedSources() */
    @Builder.Default boolean shutdownFinishedSources = false;

    /**
     * An initial position to start reading from, when sources is starting from a clean state (no
     * checkpoint to restore from).
     */
    @Builder.Default Position initialPosition = Position.OLDEST;

    /** Signal to stop reading, when event time aligns with processing time. */
    @Builder.Default boolean stopAtCurrent = false;
  }

  /** {@link LogOptions} specific to {@link BatchLogSourceFunction}. */
  @Accessors(fluent = true)
  @Builder(builderMethodName = "newBuilder", setterPrefix = "with", toBuilder = true)
  @Value
  public static class BatchLogOptions implements LogOptions {

    private static final long serialVersionUID = 1L;

    /** @see LogOptions#shutdownFinishedSources() */
    @Builder.Default boolean shutdownFinishedSources = false;

    @Builder.Default long startTimestamp = Watermarks.MIN_WATERMARK;

    @Builder.Default long endTimestamp = Watermarks.MAX_WATERMARK;
  }
}
