/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.util.state;

import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

public class ExternalStateExpander {

  /**
   * Expand the given @{link Pipeline} to support external state store and restore
   *
   * @param pipeline the Pipeline to expand
   * @param inputs transform to read inputs
   * @param sink transform to store outputs
   */
  public static void expand(
      Pipeline pipeline,
      PTransform<PBegin, PCollection<KV<String, StateValue>>> inputs,
      PTransform<PCollection<KV<String, StateValue>>, PDone> sink) {

    validatePipeline(pipeline);
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(StateValue.class, new StateValue.StateValueCoder());
    PCollection<KV<String, StateValue>> inputsMaterialized = pipeline.apply(inputs);
    pipeline.replaceAll(getOverrides(inputsMaterialized, sink));
  }

  private static void validatePipeline(Pipeline pipeline) {
    // check that all nodes have unique names
    Set<String> names = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor() {
          @Override
          public void enterPipeline(Pipeline p) {}

          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            Preconditions.checkState(names.add(node.getFullName()));
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void leaveCompositeTransform(TransformHierarchy.Node node) {}

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            Preconditions.checkState(names.add(node.getFullName()));
          }

          @Override
          public void visitValue(PValue value, TransformHierarchy.Node producer) {}

          @Override
          public void leavePipeline(Pipeline pipeline) {}
        });
  }

  private static List<PTransformOverride> getOverrides(
      PCollection<KV<String, StateValue>> inputsMaterialized,
      PTransform<PCollection<KV<String, StateValue>>, PDone> sink) {

    return Arrays.asList(statefulParDoOverride(inputsMaterialized));
  }

  @SuppressWarnings({"rawtypes"})
  private static PTransformOverride statefulParDoOverride(
      PCollection<KV<String, StateValue>> inputs) {
    return PTransformOverride.of(
        application -> application.getTransform() instanceof ParDo.MultiOutput,
        new PTransformOverrideFactory() {
          @Override
          public PTransformReplacement getReplacementTransform(AppliedPTransform transform) {
            return replaceParDo(transform, inputs);
          }

          @Override
          public Map<PCollection<?>, ReplacementOutput> mapOutputs(Map outputs, POutput newOutput) {
            return ReplacementOutputs.tagged(outputs, newOutput);
          }
        });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PTransformReplacement<PInput, POutput> replaceParDo(
      AppliedPTransform<PInput, POutput, ?> transform, PCollection<KV<String, StateValue>> inputs) {

    ParDo.MultiOutput<PInput, POutput> rawTransform =
        (ParDo.MultiOutput<PInput, POutput>) (PTransform) transform.getTransform();
    DoFn<PInput, POutput> doFn = rawTransform.getFn();
    PInput pMainInput = getMainInput(transform);
    if (!DoFnSignatures.isStateful(doFn)) {
      return PTransformReplacement.of(pMainInput, (PTransform) transform.getTransform());
    }
    String transformName = transform.getFullName();
    PCollection<KV<String, StateValue>> transformInputs =
        inputs.apply(Filter.by(kv -> kv.getKey().equals(transformName)));
    return PTransformReplacement.of(
        pMainInput, transformedParDo((PCollection) pMainInput, transformInputs, doFn));
  }

  private static <InputT, OutputT>
      PTransform<PCollection<? extends InputT>, PCollection<OutputT>> transformedParDo(
          PCollection<InputT> mainInput,
          PCollection<KV<String, StateValue>> transformInputs,
          DoFn<InputT, OutputT> doFn) {

    return ParDo.of(doFn);
  }

  private static PInput getMainInput(AppliedPTransform<PInput, POutput, ?> transform) {
    Map<TupleTag<?>, PCollection<?>> mainInputs = transform.getMainInputs();
    if (mainInputs.size() == 1) {
      return Iterables.getOnlyElement(mainInputs.values());
    }
    return asTuple(mainInputs);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PInput asTuple(Map<TupleTag<?>, PCollection<?>> mainInputs) {
    PCollectionTuple res = null;
    for (Map.Entry<TupleTag<?>, PCollection<?>> e : mainInputs.entrySet()) {
      if (res == null) {
        res = PCollectionTuple.of((TupleTag) e.getKey(), e.getValue());
      } else {
        res = res.and((TupleTag) e.getKey(), e.getValue());
      }
    }
    return res;
  }
}
