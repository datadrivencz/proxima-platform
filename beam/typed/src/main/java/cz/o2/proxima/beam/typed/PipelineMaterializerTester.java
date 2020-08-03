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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Convenient tool for testing of {@link PipelineMaterializer pipeline materializars}. */
public class PipelineMaterializerTester {

  /**
   * Create a new tester builder for the provided {@link PipelineMaterializer}.
   *
   * @param repository Repository used for inputs / outputs of this pipeline.
   * @param pipelineMaterializer Pipeline materializer we want to test.
   * @param <OutputT> Type of materializer output.
   * @return Input builder.
   */
  public static <OutputT extends POutput> InputBuilder<OutputT> of(
      Repository repository, PipelineMaterializer<OutputT> pipelineMaterializer) {
    return new Builder<>(repository, pipelineMaterializer);
  }

  public interface InputBuilder<OutputT extends POutput> {

    /**
     * Define a {@link TestStream} for a given input.
     *
     * @param attributeDescriptor Descriptor of input attribute.
     * @param testStream Test stream builder for a given attribute.
     * @param <InputT> Type of input elements.
     * @return More inputs builder.
     */
    <InputT> MoreInputsBuilder<OutputT> withInput(
        AttributeDescriptor<InputT> attributeDescriptor,
        Function<TestStream.Builder<TypedElement<InputT>>, TestStream<TypedElement<InputT>>>
            testStream);

    /**
     * Pipeline with no input.
     *
     * @return Output builder.
     * @deprecated We don't want to allow pipelines with no input, but we need to tweak event
     *     generator first.
     */
    @Deprecated
    OutputBuilder<OutputT> withNoInput();
  }

  public interface MoreInputsBuilder<OutputT extends POutput>
      extends InputBuilder<OutputT>, OutputBuilder<OutputT> {}

  public interface OutputBuilder<OutputT extends POutput> {

    /**
     * Define assertions for a single output. Assertions are defined using custom consumer and
     * {@link org.apache.beam.sdk.testing.PAssert}.
     *
     * <p>Example: <code>
     * withOutputConsumer(input -&gt; PAssert.that(input).containsInAnyOrder(...))
     * </code>
     *
     * @param outputConsumer Output consumer.
     * @return Output builder.
     */
    OutputBuilder<OutputT> withOutputAssert(Consumer<OutputT> outputConsumer);

    /**
     * Define assertions for a given output. Assertions are defined using custom consumer and {@link
     * org.apache.beam.sdk.testing.PAssert}.
     *
     * <p>Example: <code>
     * withOutputConsumer(attr, input -&gt; PAssert.that(input).containsInAnyOrder(...))
     * </code>
     *
     * @param attributeDescriptor Descriptor of output attribute we want to assert.
     * @param outputConsumer Output consumer.
     * @param <T> - Type of processed element
     * @return Output builder.
     */
    <T> OutputBuilder<OutputT> withOutputAssert(
        AttributeDescriptor<T> attributeDescriptor,
        Consumer<PCollection<TypedElement<T>>> outputConsumer);

    /** Run test suite. */
    default void run() {
      run(PipelineOptionsFactory.create());
    }

    /**
     * Run test suite.
     *
     * @param options Pipeline options.
     */
    void run(PipelineOptions options);
  }

  /**
   * Implementation of builder interfaces.
   *
   * @param <OutputT> Type of pipeline output.
   */
  private static class Builder<OutputT extends POutput>
      implements InputBuilder<OutputT>, MoreInputsBuilder<OutputT>, OutputBuilder<OutputT> {

    private final Repository repository;

    private final TypedElementCoderProvider coderProvider;

    private final TestRegisterOutputsContext registerOutputsContext;

    private final Map<
            AttributeDescriptor<?>,
            Function<TestStream.Builder<TypedElement<?>>, TestStream<TypedElement<?>>>>
        testInputs = new HashMap<>();

    private final Map<AttributeDescriptor<?>, Consumer<PCollection<?>>> testOutputs =
        new HashMap<>();

    private final PipelineMaterializer<OutputT> pipelineMaterializer;

    Builder(Repository repository, PipelineMaterializer<OutputT> pipelineMaterializer) {
      this.repository = repository;
      this.coderProvider = new TypedElementCoderProvider(repository);
      this.registerOutputsContext = new TestRegisterOutputsContext(coderProvider);
      this.pipelineMaterializer = pipelineMaterializer;
      pipelineMaterializer.registerInputs(
          new PipelineMaterializer.RegisterInputsContext() {

            @Override
            public PipelineMaterializer.RegisterInputsContext registerInput(
                AttributeDescriptor<?>... attributeDescriptors) {
              for (AttributeDescriptor<?> attributeDescriptor : attributeDescriptors) {
                coderProvider.registerAttribute(attributeDescriptor);
              }
              return this;
            }
          });
      pipelineMaterializer.registerOutputs(registerOutputsContext);
    }

    @Override
    public OutputBuilder<OutputT> withNoInput() {
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <InputT> MoreInputsBuilder<OutputT> withInput(
        AttributeDescriptor<InputT> attributeDescriptor,
        Function<TestStream.Builder<TypedElement<InputT>>, TestStream<TypedElement<InputT>>>
            testStream) {
      testInputs.put(attributeDescriptor, (Function) testStream);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputBuilder<OutputT> withOutputAssert(Consumer<OutputT> outputConsumer) {
      return withOutputAssert(registerOutputsContext.getOnlyOutput(), (Consumer) outputConsumer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> OutputBuilder<OutputT> withOutputAssert(
        AttributeDescriptor<T> attributeDescriptor,
        Consumer<PCollection<TypedElement<T>>> outputConsumer) {
      testOutputs.put(attributeDescriptor, (Consumer) outputConsumer);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(PipelineOptions options) {
      options.as(StreamingOptions.class).setStreaming(true);
      final Pipeline pipeline = Pipeline.create(options);
      pipeline.getCoderRegistry().registerCoderProvider(coderProvider);

      final OutputT output =
          pipelineMaterializer.materialize(
              new PipelineMaterializer.MaterializeContext() {

                @Override
                public <T> PCollection<TypedElement<T>> getInput(
                    InputMode inputMode, AttributeDescriptor<T> attributeDescriptor) {
                  return getInput(attributeDescriptor);
                }

                @Override
                public <T> PCollection<TypedElement<T>> getInput(
                    InputMode inputMode,
                    Map<InputModeParams, Object> params,
                    AttributeDescriptor<T> attributeDescriptor) {
                  return getInput(attributeDescriptor);
                }

                @Override
                @SuppressWarnings({"unchecked", "rawtypes"})
                public <T> PCollection<TypedElement<T>> getInput(
                    AttributeDescriptor<T> attributeDescriptor) {
                  if (!testInputs.containsKey(attributeDescriptor)) {
                    throw new IllegalStateException(
                        String.format(
                            "Mock input for [%s.%s] does not exist.",
                            attributeDescriptor.getEntity(), attributeDescriptor.getName()));
                  }
                  final TestStream.Builder<TypedElement<?>> typedElementBuilder =
                      (TestStream.Builder)
                          TestStream.create(TypedElementCoder.of(attributeDescriptor));
                  return (PCollection)
                      pipeline.apply(
                          testInputs.get(attributeDescriptor).apply(typedElementBuilder));
                }

                @Override
                public PCollection<StreamElement> getRawInput(
                    AttributeDescriptor<?>... attributeDescriptors) {
                  return PCollectionList.of(
                          Arrays.stream(attributeDescriptors)
                              .map(
                                  attributeDescriptor ->
                                      getInput(attributeDescriptor)
                                          .apply(TypedElements.toStreamElement(repository)))
                              .collect(Collectors.toList()))
                      .apply(Flatten.pCollections());
                }

                @Override
                public PCollection<StreamElement> getRawInput(
                    InputMode inputMode, AttributeDescriptor<?>... attributeDescriptors) {
                  return getRawInput(attributeDescriptors);
                }

                @Override
                public PCollection<StreamElement> getRawInput(
                    InputMode inputMode,
                    Map<InputModeParams, Object> params,
                    AttributeDescriptor<?>... attributeDescriptors) {
                  return getRawInput(attributeDescriptors);
                }

                @Override
                public Pipeline getPipeline() {
                  return pipeline;
                }
              });

      if (output instanceof PCollectionTuple) {
        final Map<AttributeDescriptor<?>, PCollection<?>> results = new HashMap<>();
        ((PCollectionTuple) output)
            .getAll()
            .forEach(
                (tag, coll) -> {
                  final AttributeDescriptor<?> attr =
                      registerOutputsContext.getOutput((TupleTag) tag);
                  coll.setTypeDescriptor((TypeDescriptor) TypedElement.typeDescriptor(attr));
                  results.put(attr, coll);
                });
        testOutputs.forEach(
            (attr, assertFn) -> {
              // check that we have correct output type descriptor
              PCollection<?> result = results.get(attr);
              Preconditions.checkArgument(
                  result.getTypeDescriptor().getRawType().isAssignableFrom(TypedElement.class),
                  "PCollection [%s] should be of type TypedElement , got [%s]",
                  result.getName(),
                  result.getTypeDescriptor());
              assertFn.accept(result);
            });
      } else {
        @SuppressWarnings("unchecked")
        final PCollection<TypedElement<Object>> cast = (PCollection<TypedElement<Object>>) output;
        cast.setTypeDescriptor(TypedElement.typeDescriptor(registerOutputsContext.getOnlyOutput()));
        testOutputs.forEach((attr, assertFn) -> assertFn.accept(cast));
      }

      pipeline.run().waitUntilFinish();
    }
  }

  private static class TestRegisterOutputsContext
      implements PipelineMaterializer.RegisterOutputsContext {

    private static TupleTag<?> SINGLE_OUTPUT = new TupleTag<>();

    private final TypedElementCoderProvider coderProvider;

    private final Map<TupleTag<?>, AttributeDescriptor<?>> outputs = new HashMap<>();

    TestRegisterOutputsContext(TypedElementCoderProvider coderProvider) {
      this.coderProvider = coderProvider;
    }

    @Override
    public void registerOutput(AttributeDescriptor<?> attributeDescriptor) {
      outputs.put(SINGLE_OUTPUT, attributeDescriptor);
      coderProvider.registerAttribute(attributeDescriptor);
    }

    @Override
    public <T> PipelineMaterializer.RegisterOutputsContext registerOutput(
        TupleTag<TypedElement<T>> outputTag, AttributeDescriptor<T> attributeDescriptor) {
      outputs.put(outputTag, attributeDescriptor);
      coderProvider.registerAttribute(attributeDescriptor);
      return this;
    }

    @SuppressWarnings("unchecked")
    <T> AttributeDescriptor<T> getOutput(TupleTag<TypedElement<T>> tag) {
      return (AttributeDescriptor<T>) outputs.get(tag);
    }

    @SuppressWarnings("unchecked")
    <T> AttributeDescriptor<T> getOnlyOutput() {
      return (AttributeDescriptor<T>) Iterables.getOnlyElement(outputs.values());
    }
  }
}
