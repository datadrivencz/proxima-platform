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
package cz.o2.proxima.direct.io.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogObservers;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.core.batch.TerminationContext;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** A {@link BatchLogReader} implementation for cassandra. */
class CassandraLogReader implements BatchLogReader {

  private final CassandraDBAccessor accessor;
  private final int parallelism;
  private final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory;
  private final ExecutorService executor;

  CassandraLogReader(
      CassandraDBAccessor accessor,
      cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory) {
    this.accessor = accessor;
    this.parallelism = accessor.getBatchParallelism();
    this.executorFactory = executorFactory;
    this.executor = executorFactory.apply();
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    List<Partition> ret = new ArrayList<>();
    double step = (((double) Long.MAX_VALUE) * 2 + 1) / parallelism;
    double tokenStart = Long.MIN_VALUE;
    double tokenEnd = tokenStart + step;
    for (int i = 0; i < parallelism; i++) {
      // FIXME: we ignore the start stamp for now
      ret.add(
          new CassandraPartition(
              i, startStamp, endStamp, (long) tokenStart, (long) tokenEnd, i == parallelism - 1));
      tokenStart = tokenEnd;
      tokenEnd += step;
      if (i == parallelism - 2) {
        tokenEnd = Long.MAX_VALUE;
      }
    }
    return ret;
  }

  @Override
  public ObserveHandle observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    TerminationContext terminationContext = new TerminationContext(observer);
    observeInternal(partitions, attributes, observer, terminationContext);
    return terminationContext;
  }

  private void observeInternal(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer,
      TerminationContext terminationContext) {

    executor.submit(
        () -> {
          try {
            for (Partition p : partitions) {
              if (!processSinglePartition(
                  (CassandraPartition) p, attributes, terminationContext, observer)) {
                break;
              }
            }
            terminationContext.finished();
          } catch (Throwable err) {
            terminationContext.handleErrorCaught(
                err, () -> observeInternal(partitions, attributes, observer, terminationContext));
          }
        });
  }

  private boolean processSinglePartition(
      CassandraPartition partition,
      List<AttributeDescriptor<?>> attributes,
      TerminationContext terminationContext,
      BatchLogObserver observer) {

    ResultSet result;
    CqlSession session = accessor.ensureSession();
    result =
        accessor.execute(accessor.getCqlFactory().scanPartition(attributes, partition, session));
    for (Row row : result) {
      if (terminationContext.isCancelled()) {
        return false;
      }
      String key = row.getString(0);
      int field = 1;
      for (AttributeDescriptor<?> attribute : attributes) {
        String attributeName = attribute.getName();
        if (attribute.isWildcard()) {
          // FIXME: this is wrong
          // need mapping between attribute and accessor
          String suffix = accessor.asString(row.getObject(field++));
          attributeName = attribute.toAttributePrefix() + suffix;
        }
        ByteBuffer bytes = row.get(field++, ByteBuffer.class);
        if (bytes != null) {
          byte[] array = bytes.slice().array();
          StreamElement element =
              accessor
                  .getCqlFactory()
                  .toKeyValue(
                      accessor.getEntityDescriptor(),
                      attribute,
                      key,
                      attributeName,
                      System.currentTimeMillis(),
                      Offsets.empty(),
                      array);
          if (!observer.onNext(element, BatchLogObservers.defaultContext(partition))) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /** Retrieve associated URI of this {@link BatchLogReader}. */
  public URI getUri() {
    return accessor.getUri();
  }

  @Override
  public Factory<?> asFactory() {
    final CassandraDBAccessor accessor = this.accessor;
    final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory =
        this.executorFactory;
    return repo -> new CassandraLogReader(accessor, executorFactory);
  }
}
