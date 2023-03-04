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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PipelineOptions;

@Slf4j
public class ProximaPipelineTranslator {

  @FunctionalInterface
  interface PTransformTranslator {
    void translate(String id, Pipeline pipeline, ProximaTranslationContext context);
  }

  private final Map<String, PTransformTranslator> urnToTransformTranslator;

  ProximaPipelineTranslator() {
    this.urnToTransformTranslator =
        ImmutableMap.<String, PTransformTranslator>builder()
            .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, this::translateFlatten)
            .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, this::translateGroupByKey)
            .put(PTransformTranslation.IMPULSE_TRANSFORM_URN, this::translateImpulse)
            .put(ExecutableStage.URN, this::translateExecutableStage)
            .put(PTransformTranslation.RESHUFFLE_URN, this::translateReshuffle)
            // For testing only
            .put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, this::translateTestStream)
            .build();
  }

  private void translateTestStream(
      String id, Pipeline pipeline, ProximaTranslationContext context) {

    log.debug("Translating TestStream {}", id);
  }

  private void translateReshuffle(String id, Pipeline pipeline, ProximaTranslationContext context) {

    log.debug("Translating Reshuffle {}", id);
  }

  private void translateExecutableStage(
      String id, Pipeline pipeline, ProximaTranslationContext context) {

    log.debug("Translating ExecutableStage {}", id);
  }

  private void translateImpulse(String id, Pipeline pipeline, ProximaTranslationContext context) {
    log.debug("Translating Impulse {}", id);
  }

  private void translateGroupByKey(
      String id, Pipeline pipeline, ProximaTranslationContext context) {

    log.debug("Translating GroupByKey {}", id);
  }

  private void translateFlatten(String id, Pipeline pipeline, ProximaTranslationContext context) {
    log.debug("Translating Flatten {}", id);
  }

  public Set<String> knownUrns() {
    return urnToTransformTranslator.keySet();
  }

  public ProximaTranslationContext createTranslationContext(
      JobInfo jobInfo, PipelineOptions options) {

    return new ProximaTranslationContext(jobInfo, options);
  }

  public ProximaBeamExecutor translate(ProximaTranslationContext context, Pipeline pipeline) {

    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, context);
    }

    return new ProximaBeamExecutor(pipeline, context);
  }

  private void urnNotFound(
      String id, RunnerApi.Pipeline pipeline, ProximaTranslationContext context) {

    throw new IllegalArgumentException(
        String.format(
            "Unknown type of URN %s for PTransform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(), id));
  }
}
