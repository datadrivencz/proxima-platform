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

import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType.Builder;
import net.bytebuddy.dynamic.DynamicType.Builder.MethodDefinition;
import net.bytebuddy.dynamic.DynamicType.Unloaded;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Composable;
import net.bytebuddy.implementation.MethodCall;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.ByteBuddyUtils;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.TypeToken;

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
    pipeline.getCoderRegistry().registerCoderForClass(StateValue.class, StateValue.coder());
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
    DoFn<KV<?, ?>, ?> doFn = (DoFn) rawTransform.getFn();
    PInput pMainInput = getMainInput(transform);
    if (!DoFnSignatures.isStateful(doFn)) {
      return PTransformReplacement.of(pMainInput, (PTransform) transform.getTransform());
    }
    String transformName = transform.getFullName();
    PCollection<StateValue> transformInputs =
        inputs
            .apply(Filter.by(kv -> kv.getKey().equals(transformName)))
            .apply(MapElements.into(TypeDescriptor.of(StateValue.class)).via(KV::getValue));
    TupleTag<POutput> mainOutputTag = rawTransform.getMainOutputTag();
    return PTransformReplacement.of(
        pMainInput,
        transformedParDo(
            transformInputs,
            (DoFn) doFn,
            mainOutputTag,
            TupleTagList.of(
                transform.getOutputs().keySet().stream()
                    .filter(t -> !t.equals(mainOutputTag))
                    .collect(Collectors.toList()))));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <K, V, InputT extends KV<K, V>, OutputT>
      PTransform<PCollection<InputT>, PCollectionTuple> transformedParDo(
          PCollection<StateValue> transformInputs,
          DoFn<KV<K, V>, OutputT> doFn,
          TupleTag<OutputT> mainOutputTag,
          TupleTagList otherOutputs) {

    return new PTransform<>() {
      @Override
      public PCollectionTuple expand(PCollection<InputT> input) {
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
        Coder<K> keyCoder = coder.getKeyCoder();
        Coder<V> valueCoder = coder.getValueCoder();
        TypeDescriptor<StateOrInput<V>> valueDescriptor =
            new TypeDescriptor<>(new TypeToken<StateOrInput<V>>() {}) {};
        PCollection<KV<K, StateOrInput<V>>> state =
            transformInputs
                .apply(
                    MapElements.into(
                            TypeDescriptors.kvs(
                                keyCoder.getEncodedTypeDescriptor(), valueDescriptor))
                        .via(
                            e ->
                                ExceptionUtils.uncheckedFactory(
                                    () ->
                                        KV.of(
                                            CoderUtils.decodeFromByteArray(keyCoder, e.getKey()),
                                            StateOrInput.<V>state(e)))))
                .setCoder(KvCoder.of(keyCoder, StateOrInput.coder(valueCoder)));
        PCollection<KV<K, StateOrInput<V>>> inputs =
            input
                .apply(
                    MapElements.into(
                            TypeDescriptors.kvs(
                                keyCoder.getEncodedTypeDescriptor(), valueDescriptor))
                        .via(e -> KV.of(e.getKey(), StateOrInput.input(e.getValue()))))
                .setCoder(KvCoder.of(keyCoder, StateOrInput.coder(valueCoder)));
        PCollection<KV<K, StateOrInput<V>>> flattened =
            PCollectionList.of(state).and(inputs).apply(Flatten.pCollections());
        return flattened.apply(
            ParDo.of(transformedDoFn(doFn)).withOutputTags(mainOutputTag, otherOutputs));
      }
    };
  }

  @VisibleForTesting
  static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      DoFn<InputT, OutputT> transformedDoFn(DoFn<KV<K, V>, OutputT> doFn) {

    @SuppressWarnings("unchecked")
    Class<? extends DoFn<KV<K, V>, OutputT>> doFnClass =
        (Class<? extends DoFn<KV<K, V>, OutputT>>) doFn.getClass();

    ClassLoadingStrategy<ClassLoader> strategy = ByteBuddyUtils.getClassLoadingStrategy(doFnClass);
    final String className = doFnClass.getName() + "$Expanded";
    final ClassLoader classLoader = ExternalStateExpander.class.getClassLoader();
    try {
      @SuppressWarnings("unchecked")
      Class<? extends DoFn<InputT, OutputT>> aClass =
          (Class<? extends DoFn<InputT, OutputT>>) classLoader.loadClass(className);
      // class found, return instance
      return ExceptionUtils.uncheckedFactory(
          () -> aClass.getConstructor(doFnClass).newInstance(doFn));
    } catch (ClassNotFoundException e) {
      // class not found, create it
    }

    ByteBuddy buddy = new ByteBuddy();
    @SuppressWarnings("unchecked")
    ParameterizedType parameterizedSuperClass =
        getParameterizedDoFn((Class<DoFn<InputT, OutputT>>) doFn.getClass());
    Type inputType = parameterizedSuperClass.getActualTypeArguments()[0];
    Type outputType = parameterizedSuperClass.getActualTypeArguments()[1];

    Generic doFnGeneric =
        Generic.Builder.parameterizedType(DoFn.class, inputType, outputType).build();
    @SuppressWarnings("unchecked")
    Builder<DoFn<InputT, OutputT>> builder =
        (Builder<DoFn<InputT, OutputT>>)
            buddy
                .subclass(doFnGeneric)
                .name(className)
                .defineField("delegate", doFnClass, Visibility.PRIVATE);
    builder = addStateAndTimers(doFnClass, builder);
    builder =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(doFnClass)
            .intercept(
                addStateAndTimerValues(
                    doFn,
                    MethodCall.invoke(
                            ExceptionUtils.uncheckedFactory(() -> DoFn.class.getConstructor()))
                        .andThen(FieldAccessor.ofField("delegate").setsArgumentAt(0))));

    builder = addProcessingMethods(doFn, builder);
    Unloaded<DoFn<InputT, OutputT>> dynamicClass = builder.make();
    // FIXME
    ExceptionUtils.unchecked(() -> dynamicClass.saveIn(new File("/tmp/dynamic-debug")));
    return ExceptionUtils.uncheckedFactory(
        () ->
            dynamicClass
                .load(null, strategy)
                .getLoaded()
                .getDeclaredConstructor(doFnClass)
                .newInstance(doFn));
  }

  private static <InputT, OutputT> Implementation addStateAndTimerValues(
      DoFn<InputT, OutputT> doFn, Composable delegate) {

    List<Class<? extends Annotation>> acceptable = Arrays.asList(StateId.class, TimerId.class);
    @SuppressWarnings("unchecked")
    Class<? extends DoFn<InputT, OutputT>> doFnClass =
        (Class<? extends DoFn<InputT, OutputT>>) doFn.getClass();
    for (Field f : doFnClass.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers())
          && acceptable.stream().anyMatch(a -> f.getAnnotation(a) != null)) {
        f.setAccessible(true);
        Object value = ExceptionUtils.uncheckedFactory(() -> f.get(doFn));
        delegate = delegate.andThen(FieldAccessor.ofField(f.getName()).setsValue(value));
      }
    }
    return delegate;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <OutputT, InputT> ParameterizedType getParameterizedDoFn(
      Class<? extends DoFn<InputT, OutputT>> doFnClass) {

    Type type = doFnClass.getGenericSuperclass();
    if (type instanceof ParameterizedType) {
      return (ParameterizedType) type;
    }
    if (doFnClass.getSuperclass().isAssignableFrom(DoFn.class)) {
      return getParameterizedDoFn((Class) doFnClass.getGenericSuperclass());
    }
    throw new IllegalStateException("Cannot get parameterized type of " + doFnClass);
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addProcessingMethods(
          DoFn<KV<K, V>, OutputT> doFn, Builder<DoFn<InputT, OutputT>> builder) {

    builder = addProcessingMethod(doFn, DoFn.Setup.class, builder);
    builder = addProcessingMethod(doFn, DoFn.StartBundle.class, builder);
    builder = addProcessingMethod(doFn, DoFn.ProcessElement.class, builder);
    builder = addProcessingMethod(doFn, DoFn.FinishBundle.class, builder);
    builder = addProcessingMethod(doFn, DoFn.Teardown.class, builder);
    builder = addProcessingMethod(doFn, DoFn.OnWindowExpiration.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetInitialRestriction.class, builder);
    builder = addProcessingMethod(doFn, DoFn.SplitRestriction.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetRestrictionCoder.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetWatermarkEstimatorStateCoder.class, builder);
    builder = addProcessingMethod(doFn, DoFn.GetInitialWatermarkEstimatorState.class, builder);
    builder = addProcessingMethod(doFn, DoFn.NewWatermarkEstimator.class, builder);
    builder = addProcessingMethod(doFn, DoFn.NewTracker.class, builder);

    // FIXME: timer callbacks
    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT, T extends Annotation>
      Builder<DoFn<InputT, OutputT>> addProcessingMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          Class<T> annotation,
          Builder<DoFn<InputT, OutputT>> builder) {

    Method method =
        Iterables.getOnlyElement(
            Arrays.stream(doFn.getClass().getMethods())
                .filter(m -> m.getAnnotation(annotation) != null)
                .collect(Collectors.toList()),
            null);
    if (method != null) {
      MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
          builder
              .defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
              .withParameters(method.getGenericParameterTypes())
              .intercept(MethodCall.invoke(method).onField("delegate").withAllArguments());

      // retrieve parameter annotations and apply them
      Annotation[][] parameterAnnotations = method.getParameterAnnotations();
      for (int i = 0; i < parameterAnnotations.length; i++) {
        for (Annotation paramAnnotation : parameterAnnotations[i]) {
          methodDefinition = methodDefinition.annotateParameter(i, paramAnnotation);
        }
      }
      return methodDefinition.annotateMethod(
          AnnotationDescription.Builder.ofType(annotation).build());
    }
    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addStateAndTimers(
          Class<? extends DoFn<KV<K, V>, OutputT>> doFnClass,
          Builder<DoFn<InputT, OutputT>> builder) {

    builder = cloneFields(doFnClass, StateId.class, builder);
    return cloneFields(doFnClass, TimerId.class, builder);
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT, T extends Annotation>
      Builder<DoFn<InputT, OutputT>> cloneFields(
          Class<? extends DoFn<KV<K, V>, OutputT>> doFnClass,
          Class<T> annotationClass,
          Builder<DoFn<InputT, OutputT>> builder) {

    for (Field f : doFnClass.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers()) && f.getAnnotation(annotationClass) != null) {
        builder =
            builder
                .defineField(f.getName(), f.getGenericType(), f.getModifiers())
                .annotateField(f.getDeclaredAnnotations());
      }
    }
    return builder;
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
