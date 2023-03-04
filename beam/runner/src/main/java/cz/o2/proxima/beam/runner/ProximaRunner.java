/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.runner;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.SplittableParDoExpander;
import org.apache.beam.runners.core.construction.graph.TrivialNativeTransformExpander;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ProximaRunner implements PortablePipelineRunner {

  private final PipelineOptions options;

  public ProximaRunner(PipelineOptions options) {
    this.options = options;
  }

  @Override
  public @NonNull PortablePipelineResult run(RunnerApi.Pipeline pipeline, @NonNull JobInfo jobInfo)
      throws Exception {

    SdkHarnessOptions.getConfiguredLoggerFromOptions(options.as(SdkHarnessOptions.class));
    ProximaPipelineTranslator translator = new ProximaPipelineTranslator();

    // Expand any splittable ParDos within the graph to enable sizing and splitting of bundles.
    Pipeline pipelineWithSdfExpanded =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            pipeline,
            SplittableParDoExpander.createSizedReplacement());

    // Don't let the fuser fuse any subcomponents of native transforms.
    Pipeline trimmedPipeline =
        TrivialNativeTransformExpander.forKnownUrns(
            pipelineWithSdfExpanded, translator.knownUrns());

    // Fused pipeline proto.
    RunnerApi.Pipeline fusedPipeline =
        trimmedPipeline
                .getComponents()
                .getTransformsMap()
                .values()
                .stream()
                .anyMatch(proto -> ExecutableStage.URN.equals(proto.getSpec().getUrn()))
            ? trimmedPipeline
            : GreedyPipelineFuser.fuse(trimmedPipeline).toPipeline();

    ProximaBeamExecutor executor =
        translator.translate(translator.createTranslationContext(jobInfo, options), fusedPipeline);

    return executor.execute();
  }
}
