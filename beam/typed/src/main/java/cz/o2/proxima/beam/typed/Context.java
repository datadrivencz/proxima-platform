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

import static cz.o2.proxima.beam.typed.InputModeParams.BOOTSTRAP_TIMESTAMP_BARRIER;

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;

/** A context of pipeline. */
public class Context {

  public static class MaterializerContext
      implements PipelineMaterializer.RegisterInputsContext,
          PipelineMaterializer.RegisterOutputsContext,
          PipelineMaterializer.MaterializeContext {

    private static final TupleTag<TypedElement<?>> SINGLE_OUTPUT = new TupleTag<>();

    private final Set<AttributeDescriptor<?>> inputs = new HashSet<>();
    private final Map<TupleTag<? extends TypedElement<?>>, AttributeDescriptor<?>> outputs =
        new HashMap<>();

    private final Pipeline pipeline;
    private final Repository repository;
    private final TypedElementCoderProvider coderProvider;
    private final boolean multipleOutputs;

    MaterializerContext(
        Pipeline pipeline,
        Repository repository,
        TypedElementCoderProvider coderProvider,
        boolean multipleOutputs) {
      this.pipeline = pipeline;
      this.repository = repository;
      this.coderProvider = coderProvider;
      this.multipleOutputs = multipleOutputs;
    }

    @Override
    public PipelineMaterializer.RegisterInputsContext registerInput(
        AttributeDescriptor<?>... attributeDescriptors) {
      for (AttributeDescriptor<?> attributeDescriptor : attributeDescriptors) {
        inputs.add(attributeDescriptor);
        coderProvider.registerAttribute(attributeDescriptor);
      }
      return this;
    }

    @Override
    public void registerOutput(AttributeDescriptor<?> attributeDescriptor) {
      if (multipleOutputs) {
        throw new IllegalStateException(
            "This context is intended for multiple outputs. Please use RegisterOutputsContext#registerOutput(TupleTag, AttributeDescriptor) instead.");
      }
      outputs.put(SINGLE_OUTPUT, attributeDescriptor);
    }

    @Override
    public <T> PipelineMaterializer.RegisterOutputsContext registerOutput(
        TupleTag<TypedElement<T>> outputTag, AttributeDescriptor<T> attributeDescriptor) {
      if (!multipleOutputs) {
        throw new IllegalStateException(
            "This context is intended for single output . Please use RegisterOutputsContext#registerOutput(AttributeDescriptor) instead.");
      }
      outputs.put(outputTag, attributeDescriptor);
      return this;
    }

    @Override
    public <T> PCollection<TypedElement<T>> getInput(AttributeDescriptor<T> attributeDescriptor) {
      return getRawInput(attributeDescriptor).apply("Type", TypedElements.of(attributeDescriptor));
    }

    @Override
    public <T> PCollection<TypedElement<T>> getInput(
        InputMode inputMode, AttributeDescriptor<T> attributeDescriptor) {
      return getRawInput(inputMode, attributeDescriptor)
          .apply("Type", TypedElements.of(attributeDescriptor));
    }

    @Override
    public <T> PCollection<TypedElement<T>> getInput(
        InputMode inputMode,
        Map<InputModeParams, Object> params,
        AttributeDescriptor<T> attributeDescriptor) {
      return getRawInput(inputMode, params, attributeDescriptor)
          .apply("Type", TypedElements.of(attributeDescriptor));
    }

    @Override
    public PCollection<StreamElement> getRawInput(AttributeDescriptor<?>... attributeDescriptors) {
      final InputMode defaultInputMode =
          pipeline.getOptions().as(RunnerPipelineOptions.class).getInputMode();
      return getRawInput(defaultInputMode, attributeDescriptors);
    }

    @Override
    public PCollection<StreamElement> getRawInput(
        InputMode inputMode, AttributeDescriptor<?>... attributeDescriptors) {
      return getRawInput(inputMode, Collections.emptyMap(), attributeDescriptors);
    }

    @Override
    public PCollection<StreamElement> getRawInput(
        InputMode inputMode,
        Map<InputModeParams, Object> params,
        AttributeDescriptor<?>... attributeDescriptors) {
      // Todo warn about unused inputs.
      for (AttributeDescriptor<?> attributeDescriptor : attributeDescriptors) {
        if (!inputs.contains(attributeDescriptor)) {
          throw new IllegalStateException(
              String.format(
                  "Input [%s.%s] is not registered.",
                  attributeDescriptor.getEntity(), attributeDescriptor.getName()));
        }
      }
      final BeamDataOperator operator = repository.getOrCreateOperator(BeamDataOperator.class);
      switch (inputMode) {
        case STREAM_FROM_OLDEST:
          return operator.getStream(pipeline, Position.OLDEST, false, true, attributeDescriptors);
        case STREAM_FROM_OLDEST_BATCH:
          return operator.getStream(pipeline, Position.OLDEST, true, true, attributeDescriptors);
        case STREAM_FROM_CURRENT:
          return operator.getStream(pipeline, Position.CURRENT, false, true, attributeDescriptors);
        case BOOTSTRAP:
          final long timestamp =
              (long)
                  params.getOrDefault(
                      BOOTSTRAP_TIMESTAMP_BARRIER,
                      pipeline
                          .getOptions()
                          .as(RunnerPipelineOptions.class)
                          .getBootstrapBarrierTimestamp());
          return PCollectionList.of(
                  Arrays.asList(
                      operator
                          .getBatchUpdates(
                              pipeline, Long.MIN_VALUE, timestamp, true, attributeDescriptors)
                          .apply(Filter.by(new UntilTimestamp(timestamp))),
                      operator
                          .getStream(pipeline, Position.OLDEST, false, true, attributeDescriptors)
                          .apply(Filter.by(new FromTimestamp(timestamp)))))
              .apply(Flatten.pCollections());
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported input mode [%s].", inputMode));
      }
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    <T> void consumeOutputs(POutput output, Consumer<PCollection<StreamElement>> consumer) {
      if (multipleOutputs) {
        final PCollectionTuple castOutput = (PCollectionTuple) output;
        for (TupleTag<?> outputTag : castOutput.getAll().keySet()) {
          consumer.accept(generalizeOutput(castOutput, outputTag));
        }
      } else {
        @SuppressWarnings("unchecked")
        final PCollection<TypedElement<T>> castOutput = (PCollection<TypedElement<T>>) output;
        @SuppressWarnings("unchecked")
        final AttributeDescriptor<T> attributeDescriptor =
            (AttributeDescriptor<T>) Objects.requireNonNull(outputs.get(SINGLE_OUTPUT));
        consumer.accept(
            castOutput
                .setTypeDescriptor(TypedElement.typeDescriptor(attributeDescriptor))
                .apply(TypedElements.toStreamElement(repository)));
      }
    }

    private <T> PCollection<StreamElement> generalizeOutput(
        PCollectionTuple result, TupleTag<?> outputTag) {
      @SuppressWarnings("unchecked")
      final PCollection<TypedElement<T>> output =
          (PCollection<TypedElement<T>>) result.get(outputTag);
      @SuppressWarnings("unchecked")
      final AttributeDescriptor<T> attributeDescriptor =
          (AttributeDescriptor<T>) Objects.requireNonNull(outputs.get(outputTag));
      return output
          .setTypeDescriptor(TypedElement.typeDescriptor(attributeDescriptor))
          .apply(TypedElements.toStreamElement(repository));
    }
  }

  /** Filter predicates that accepts elements with timestamp lower than given timestamp. */
  private static class UntilTimestamp implements SerializableFunction<StreamElement, Boolean> {

    private final long timestamp;

    UntilTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public Boolean apply(StreamElement input) {
      if (input.getStamp() < timestamp) {
        return true;
      }
      Metrics.counter("filter-timestamp", "until").inc();
      return false;
    }
  }

  /**
   * Filter predicates that accepts elements with timestamp greater or equal the given timestamp.
   */
  private static class FromTimestamp implements SerializableFunction<StreamElement, Boolean> {

    private final long timestamp;

    FromTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public Boolean apply(StreamElement input) {
      if (input.getStamp() >= timestamp) {
        return true;
      }
      Metrics.counter("filter-timestamp", "from").inc();
      return false;
    }
  }

  public static MaterializerContext materializerContext(
      Pipeline pipeline,
      Repository repository,
      TypedElementCoderProvider coderProvider,
      boolean multipleOutputs) {
    return new MaterializerContext(pipeline, repository, coderProvider, multipleOutputs);
  }
}
