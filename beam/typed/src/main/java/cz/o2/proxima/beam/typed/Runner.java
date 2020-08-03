/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.typed;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.beam.typed.io.ProximaIO;
import cz.o2.proxima.beam.typed.transform.BufferUntilCheckpoint;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;

/** Runner of pipelines. */
@Slf4j
public class Runner {

  private static List<PipelineMaterializer<? extends POutput>> createMaterializersFromParams(
      PipelineOptions pipelineOptions, List<Instances.Parameter<?>> parameters) {

    final Instances.Parameter<?>[] parameterArray =
        parameters.toArray(new Instances.Parameter<?>[0]);
    return pipelineOptions
        .as(RunnerPipelineOptions.class)
        .getMaterializers()
        .stream()
        .map(clazz -> Instances.create(clazz, parameterArray))
        .collect(Collectors.toList());
  }

  /** Proxima repository for the underlying pipelines. */
  private final Repository repository;

  /** Parameters that can be used to construct materializers. */
  private final List<Instances.Parameter<?>> parameters;

  private PipelineOptions opts;

  /** Created materializers */
  private List<PipelineMaterializer<?>> materializers;

  public Runner(Repository repository, List<Instances.Parameter<?>> parameters) {
    this.repository = Objects.requireNonNull(repository);
    this.parameters = parameters;
  }

  public void runBlocking(PipelineOptions pipelineOptions) {
    runInternal(pipelineOptions).waitUntilFinish();
  }

  public List<PipelineMaterializer<?>> createMaterializers(PipelineOptions options) {
    if (opts == options) {
      return materializers;
    }
    this.opts = options;
    this.materializers = createMaterializersFromParams(options, parameters);
    return materializers;
  }

  @VisibleForTesting
  public PipelineResult runInternal(PipelineOptions pipelineOptions) {
    final Pipeline pipeline = Pipeline.create(pipelineOptions);
    final List<PipelineMaterializer<? extends POutput>> materializers =
        createMaterializers(pipelineOptions);

    final TypedElementCoderProvider coderProvider = TypedElementCoderProvider.of(repository);
    pipeline.getCoderRegistry().registerCoderProvider(coderProvider);

    final TypedElementSchemaProvider schemaProvider = new TypedElementSchemaProvider(repository);
    schemaProvider
        .getDescriptors()
        .forEach(desc -> pipeline.getSchemaRegistry().registerSchemaProvider(desc, schemaProvider));

    log.info("Running new pipeline with materializers {}", materializers);
    final List<PCollection<StreamElement>> materialized =
        materializers
            .stream()
            .flatMap(
                (materializer) -> {
                  final Context.MaterializerContext ctx =
                      Context.materializerContext(
                          pipeline,
                          repository,
                          coderProvider,
                          materializer instanceof MultiOutputPipelineMaterializer);

                  materializer.registerInputs(ctx);
                  materializer.registerOutputs(ctx);

                  final POutput output =
                      Objects.requireNonNull(
                          materializer.materialize(ctx), "Materializer has no output.");
                  final List<PCollection<StreamElement>> result = new ArrayList<>();
                  ctx.consumeOutputs(output, result::add);

                  if (materializer.isDeterministic()) {
                    return result.stream();
                  } else {
                    // Ensures exactly once output for non deterministic materializer
                    return result
                        .stream()
                        .map(
                            pCollection ->
                                pCollection.apply(
                                    "BufferUntilCheckpoint",
                                    ParDo.of(new BufferUntilCheckpoint<>())));
                  }
                })
            .map(
                item ->
                    item.apply(
                            Window.<StreamElement>into(new GlobalWindows())
                                .triggering(
                                    AfterWatermark.pastEndOfWindow()
                                        .withEarlyFirings(AfterPane.elementCountAtLeast(1)))
                                .discardingFiredPanes())
                        .setCoder(StreamElementCoder.of(repository)))
            .collect(Collectors.toList());

    // Persist results.
    PCollectionList.of(materialized)
        .apply(Flatten.pCollections())
        .apply(ProximaIO.write(repository.asFactory()));

    return pipeline.run();
  }
}
