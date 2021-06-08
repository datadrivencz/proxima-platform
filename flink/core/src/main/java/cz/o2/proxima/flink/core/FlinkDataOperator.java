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
package cz.o2.proxima.flink.core;

import cz.o2.proxima.flink.core.table.LogCatalogTable;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Optionals;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.ChangelogMode;

/** Operate {@link Repository} using Apache Flink. */
public class FlinkDataOperator implements DataOperator {

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
            .map(AttributeDescriptor::getName)
            .collect(Collectors.joining(",")));
  }

  public static CommitLogOptions.CommitLogOptionsBuilder newCommitLogOptions() {
    return CommitLogOptions.newBuilder();
  }

  public static BatchLogOptions.BatchLogOptionsBuilder newBatchLogOptions() {
    return BatchLogOptions.newBuilder();
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

  private final Repository repository;
  @Getter private final StreamExecutionEnvironment executionEnvironment;
  @Getter private final StreamTableEnvironment tableEnvironment;

  public FlinkDataOperator(Repository repository) {
    this.repository = repository;
    this.executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    this.tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
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
    return executionEnvironment.addSource(
        new BatchLogSourceFunction<>(
            repository.asFactory(), attributeDescriptorsCopy, options, ResultExtractor.identity()),
        createSourceName("BatchLog", attributeDescriptorsCopy));
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
    return executionEnvironment.addSource(
        new CommitLogSourceFunction<>(
            repository.asFactory(), attributeDescriptorsCopy, options, ResultExtractor.identity()),
        createSourceName("CommitLog", attributeDescriptorsCopy));
  }

  /**
   * Create a new table of realtime streaming data in current {@link Catalog catalog's} default
   * database.
   *
   * @param tableName Table name.
   * @param attributeDescriptors Attributes we want to get data from.
   * @param changelogMode Changelog mode of the underlying source.
   */
  public void registerCommitLogTable(
      String tableName,
      Collection<AttributeDescriptor<?>> attributeDescriptors,
      ChangelogMode changelogMode) {
    registerCommitLogTable(
        tableName, attributeDescriptors, changelogMode, newCommitLogOptions().build());
  }

  /**
   * Create a new table of realtime streaming data in current {@link Catalog catalog's} default
   * database.
   *
   * @param tableName Table name.
   * @param attributeDescriptors Attributes we want to get data from.
   * @param changelogMode Changelog mode of the underlying source.
   * @param options Options for {@link CommitLogSourceFunction}.
   */
  public void registerCommitLogTable(
      String tableName,
      Collection<AttributeDescriptor<?>> attributeDescriptors,
      ChangelogMode changelogMode,
      CommitLogOptions options) {
    final List<AttributeDescriptor<?>> attributeDescriptorsCopy =
        new ArrayList<>(attributeDescriptors);
    final Catalog catalog =
        Optionals.get(tableEnvironment.getCatalog(tableEnvironment.getCurrentCatalog()));
    try {
      catalog.createTable(
          ObjectPath.fromString(String.format("%s.%s", catalog.getDefaultDatabase(), tableName)),
          LogCatalogTable.ofCommitLog(repository, attributeDescriptorsCopy, options),
          false);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Unable to register a new table [%s].", tableName), e);
    }
  }

  /**
   * Create a new table of historical batch data in current {@link Catalog catalog's} default
   * database.
   *
   * @param tableName Table name.
   * @param attributeDescriptors Attributes we want to get data from.
   * @param changelogMode Changelog mode of the underlying source.
   */
  public void registerBatchLogTable(
      String tableName,
      Collection<AttributeDescriptor<?>> attributeDescriptors,
      ChangelogMode changelogMode) {
    registerBatchLogTable(
        tableName, attributeDescriptors, changelogMode, newBatchLogOptions().build());
  }

  /**
   * Create a new table of historical batch data in current {@link Catalog catalog's} default
   * database.
   *
   * @param tableName Table name.
   * @param attributeDescriptors Attributes we want to get data from.
   * @param changelogMode Changelog mode of the underlying source.
   * @param options Options for {@link BatchLogSourceFunction}.
   */
  public void registerBatchLogTable(
      String tableName,
      Collection<AttributeDescriptor<?>> attributeDescriptors,
      ChangelogMode changelogMode,
      BatchLogOptions options) {
    final List<AttributeDescriptor<?>> attributeDescriptorsCopy =
        new ArrayList<>(attributeDescriptors);
    final Catalog catalog =
        Optionals.get(tableEnvironment.getCatalog(tableEnvironment.getCurrentCatalog()));
    try {
      catalog.createTable(
          ObjectPath.fromString(String.format("%s.%s", catalog.getDefaultDatabase(), tableName)),
          LogCatalogTable.ofBatchLog(repository, attributeDescriptorsCopy, options),
          false);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Unable to register a new table [%s].", tableName), e);
    }
  }

  /** @see StreamTableEnvironment#from(String) for more details. */
  public Table getTable(String tableName) {
    return tableEnvironment.from(tableName);
  }

  /** @see StreamExecutionEnvironment#execute(String) for more details. */
  public JobExecutionResult execute(String jobName) throws Exception {
    return executionEnvironment.execute(jobName);
  }

  /** @see StreamExecutionEnvironment#executeAsync(String) for more details. */
  public JobClient executeAsync(String jobName) throws Exception {
    return executionEnvironment.executeAsync(jobName);
  }
}
