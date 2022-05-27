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
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/** Simple Sink uses {@link OnlineAttributeWriter}. */
@Slf4j
public class OnlineAttributeWriterSink extends RichSinkFunction<StreamElement> {

  private static final long serialVersionUID = -1L;
  static final String METRIC_TOTAL_WRITTEN = "total_written";
  static final String METRIC_WRITTEN = "written_elements_count";

  private final RepositoryFactory repositoryFactory;
  @Nullable private transient Map<AttributeDescriptor<?>, OnlineAttributeWriter> writers;

  @Nullable private transient DirectDataOperator direct;

  @Nullable private transient Counter totalWrittenCounter;
  @Nullable private transient MetricGroup writtenMetricGroup;

  public OnlineAttributeWriterSink(RepositoryFactory repositoryFactory) {
    this.repositoryFactory = repositoryFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    totalWrittenCounter = getRuntimeContext().getMetricGroup().counter(METRIC_TOTAL_WRITTEN);
    writtenMetricGroup = getRuntimeContext().getMetricGroup();
  }

  @Override
  public void close() throws Exception {
    if (writers != null) {
      writers.values().forEach(AttributeWriterBase::close);
    }
    if (direct != null) {
      direct.close();
    }
  }

  @Override
  public void invoke(StreamElement value, Context context) throws Exception {
    getOrCreateWriter(value.getAttributeDescriptor())
        .write(
            value,
            ((success, error) -> {
              log.debug("Written element {}", value.getKey());
              if (!success) {
                throw new RuntimeException("Unable to write element.", error);
              }
              Objects.requireNonNull(writtenMetricGroup)
                  .addGroup("entity", value.getEntityDescriptor().getName())
                  .addGroup("attribute", value.getAttributeDescriptor().getName())
                  .counter(METRIC_WRITTEN)
                  .inc();
              Objects.requireNonNull(totalWrittenCounter).inc();
            }));
  }

  OnlineAttributeWriter getOrCreateWriter(AttributeDescriptor<?> attribute) {
    if (writers == null) {
      writers = new HashMap<>();
    }
    return writers.computeIfAbsent(
        attribute,
        a ->
            getOrCreateDirect()
                .getWriter(a)
                .orElseThrow(
                    () ->
                        new RuntimeException(
                            "Unable to find online writer for attribute: "
                                + a.getEntity()
                                + "."
                                + a.getName())));
  }

  @VisibleForTesting
  DirectDataOperator getOrCreateDirect() {
    if (direct == null) {
      direct = repositoryFactory.apply().getOrCreateOperator(DirectDataOperator.class);
    }
    return direct;
  }
}
