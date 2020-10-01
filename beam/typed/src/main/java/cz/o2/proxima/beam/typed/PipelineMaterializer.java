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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;

/** Materializer of the user-provided pipelines. */
public interface PipelineMaterializer<OutputT extends POutput> extends Serializable {

  /** Context for {@link PipelineMaterializer#registerInputs(RegisterInputsContext)}. */
  interface RegisterInputsContext {

    /**
     * Register input of the pipeline.
     *
     * @param attributeDescriptors Descriptors of attributes that this {@link PipelineMaterializer}
     *     reads data from.
     * @return This for chaining.
     */
    RegisterInputsContext registerInput(AttributeDescriptor<?>... attributeDescriptors);
  }

  /** Context for {@link PipelineMaterializer#registerOutputs(RegisterOutputsContext)}. */
  interface RegisterOutputsContext {

    /**
     * Register output. This method is solely for {@link SingleOutputPipelineMaterializer}. Multiple
     * invocations will result in {@link IllegalStateException}.
     *
     * @param attributeDescriptor Descriptor of attributed that this {@link PipelineMaterializer}
     *     writes data to.
     */
    void registerOutput(AttributeDescriptor<?> attributeDescriptor);

    /**
     * Register output. This method is solely for {@link MultiOutputPipelineMaterializer}.
     *
     * @param outputTag Resulting output tag with data for the provided attribute.
     * @param attributeDescriptor Descriptor of attributed that this {@link PipelineMaterializer}
     *     writes data to.
     * @param <T> Type of attribute value.
     * @return This for chaining.
     */
    <T> RegisterOutputsContext registerOutput(
        TupleTag<TypedElement<T>> outputTag, AttributeDescriptor<T> attributeDescriptor);
  }

  /** Context for {@link PipelineMaterializer#materialize(MaterializeContext)}. */
  interface MaterializeContext {

    /**
     * Get input {@link PCollection} of {@link TypedElement typed elements}. This is the preferred
     * way of retrieving input data. This call will fail for un-registered inputs.
     *
     * @param attributeDescriptor Descriptor of attribute we want an input PCollection for.
     * @param <T> Type of attribute value.
     * @return PCollection.
     */
    <T> PCollection<TypedElement<T>> getInput(AttributeDescriptor<T> attributeDescriptor);

    /**
     * Get input {@link PCollection} of {@link TypedElement typed elements}. This is the preferred
     * way of retrieving input data. This call will fail for un-registered inputs.
     *
     * @param inputMode Input mode for reading the data.
     * @param attributeDescriptor Descriptor of attribute we want an input PCollection for.
     * @param <T> Type of attribute value.
     * @return PCollection.
     */
    <T> PCollection<TypedElement<T>> getInput(
        InputMode inputMode, AttributeDescriptor<T> attributeDescriptor);

    /**
     * Get input {@link PCollection} of {@link TypedElement typed elements}. This is the preferred
     * way of retrieving input data. This call will fail for un-registered inputs.
     *
     * @param inputMode Input mode for reading the data.
     * @param attributeDescriptor Descriptor of attribute we want an input PCollection for.
     * @param <T> Type of attribute value.
     * @param params Input mode parameters.
     * @return PCollection.
     */
    <T> PCollection<TypedElement<T>> getInput(
        InputMode inputMode,
        Map<InputModeParams, Object> params,
        AttributeDescriptor<T> attributeDescriptor);

    /**
     * Get input {@link PCollection} of {@link StreamElement untyped elements}. This is useful when
     * more fine-grained access to input data is necessary. This call will fail for un-registered
     * inputs.
     *
     * @param attributeDescriptors Descriptors of attribute we want an input PCollection for.
     * @return PCollection.
     */
    PCollection<StreamElement> getRawInput(AttributeDescriptor<?>... attributeDescriptors);

    /**
     * Get input {@link PCollection} of {@link StreamElement untyped elements}. This is useful when
     * more fine-grained access to input data is necessary. This call will fail for un-registered
     * inputs.
     *
     * @param inputMode Input mode for reading the data.
     * @param attributeDescriptors Descriptors of attribute we want an input PCollection for.
     * @return PCollection.
     */
    PCollection<StreamElement> getRawInput(
        InputMode inputMode, AttributeDescriptor<?>... attributeDescriptors);

    /**
     * Get input {@link PCollection} of {@link StreamElement untyped elements}. This is useful when
     * more fine-grained access to input data is necessary. This call will fail for un-registered
     * inputs.
     *
     * @param inputMode Input mode for reading the data.
     * @param attributeDescriptors Descriptors of attribute we want an input PCollection for.
     *     * @param params Input mode parameters.
     * @return PCollection.
     */
    PCollection<StreamElement> getRawInput(
        InputMode inputMode,
        Map<InputModeParams, Object> params,
        AttributeDescriptor<?>... attributeDescriptors);

    /**
     * Get an underlying pipeline.
     *
     * @return Pipeline.
     */
    Pipeline getPipeline();
  }

  /**
   * Get pipeline materializer name.
   *
   * @return Name.
   */
  default String getName() {
    return getClass().getSimpleName();
  }

  /**
   * Register inputs of this materializer. These are the only inputs that user is allowed to be
   * retrieved from {@link Context}.
   *
   * @param ctx - register inputs into this context
   */
  void registerInputs(RegisterInputsContext ctx);

  /**
   * Register outputs of this materializer. User must specify all of the possible outputs the {@link
   * PipelineMaterializer} needs, so the runner can properly register appropriate {@link
   * org.apache.beam.sdk.coders.Coder coders}. Also we use this as a "pipeline description" for
   * visualization and debugging purpose.
   *
   * @param ctx - register outputs into this context
   */
  void registerOutputs(RegisterOutputsContext ctx);

  /**
   * Materialize {@link POutput} to be persisted.
   *
   * @param ctx Context for this materializer.
   * @return Materialized PValue.
   */
  OutputT materialize(MaterializeContext ctx);

  /**
   * If materializer has deterministic output. Deterministic materializer must ensure that for the
   * same input always creates the same output.
   *
   * @return true if materializer outputs deterministic values.
   */
  default boolean isDeterministic() {
    return true;
  };
}
