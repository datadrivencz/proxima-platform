/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam;

import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.BulkAttributeWriter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Create replication and transformation {@link Pipeline} from given
 * {@link Repository}.
 */
@Slf4j
public class ReplicationPipeline {

  /**
   * Create replication pipeline from {@link Repository}.
   * @param repository the repository to create pipeline for
   * @return the replication pipeline to be run
   */
  public static Pipeline from(Repository repository) {
    return from(repository, PipelineOptionsFactory.create(), -1);
  }


  /**
   * Create replication pipeline from {@link Repository}.
   * @param repository the repository to create pipeline for
   * @param defaultParallelism the parallelism to use when constructing
   *                           stateful (grouping) replications
   * @return the replication pipeline to be run
   */
  public static Pipeline from(Repository repository, int defaultParallelism) {
    return from(repository, PipelineOptionsFactory.create(), defaultParallelism);
  }

  /**
   * Create replication pipeline from {@link Repository}.
   * @param repo the repository to create pipeline for
   * @param opts pipeline options to be used when the pipeline is constructed
   * @return the replication pipeline to be run
   */
  public static Pipeline from(
      Repository repo, PipelineOptions opts) {

    return from(repo, opts, -1);
  }


  /**
   * Create replication pipeline from {@link Repository}.
   * @param repo the repository to create pipeline for
   * @param opts pipeline options to be used when the pipeline is constructed
   * @param defaultParallelism the parallelism to use when constructing
   *                           stateful (grouping) replications
   * @return the replication pipeline to be run
   */
  public static Pipeline from(
      Repository repo, PipelineOptions opts, int defaultParallelism) {

    Pipeline pipeline = Pipeline.create(opts);
    ProximaIO io = ProximaIO.from(repo);
    repo.getAllFamilies()
        .filter(af -> af.getType() == StorageType.REPLICA)
        .forEach(af -> addReplicationFor(io, af, defaultParallelism, pipeline));

    return pipeline;
  }

  private static void addReplicationFor(
      ProximaIO io,
      AttributeFamilyDescriptor target,
      int defaultParallelism,
      Pipeline pipeline) {

    PCollection<StreamElement> input = pipeline
        .apply(io.read(Position.CURRENT, target.getAttributes()))
        .apply(MapElements.via(new SimpleFunction<StreamElement, StreamElement>(e -> {
          System.err.println(" ** READ " + e);
          return e;
        }) { }));

    AttributeWriterBase writer = target.getWriter()
        .orElseThrow(() -> new IllegalArgumentException(
            "Family " + target + " has no writer"));
    if (writer.getType() == AttributeWriterBase.Type.BULK) {
      BulkAttributeWriter bulk = writer.bulk();
      log.info(
          "Creating new {} writer for attributes {} writing to {}, with flush period {}",
          writer.getType(),
          target.getAttributes(),
          writer.getURI(),
          bulk.getFlushPeriod());
      input.apply(
          io.writeBulk(
              bulk.getFlushPeriod(),
              TimeUnit.MILLISECONDS,
              defaultParallelism,
              af -> bulk));
    } else {
      log.info(
          "Creating new {} writer for attributes {} writing to {}",
          writer.getType(),
          target.getAttributes(),
          writer.getURI());
      input.apply(io.write(af -> writer.online()));
    }
  }

}
