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

import static cz.o2.proxima.beam.util.state.MethodCallUtils.getInputKvType;
import static cz.o2.proxima.beam.util.state.MethodCallUtils.getWrapperInputType;

import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType.Builder;
import net.bytebuddy.dynamic.DynamicType.Builder.MethodDefinition;
import net.bytebuddy.dynamic.DynamicType.Unloaded;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Composable;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
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
import org.checkerframework.checker.nullness.qual.NonNull;

public class ExternalStateExpander {

  static final String EXPANDER_STATE_SPEC = "expanderStateSpec";
  static final String EXPANDER_STATE_NAME = "_expanderBuf";

  /**
   * Expand the given @{link Pipeline} to support external state store and restore
   *
   * @param pipeline the Pipeline to expand
   * @param inputs transform to read inputs
   * @param stateSink transform to store outputs
   */
  public static void expand(
      Pipeline pipeline,
      PTransform<PBegin, PCollection<KV<String, StateValue>>> inputs,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

    validatePipeline(pipeline);
    pipeline.getCoderRegistry().registerCoderForClass(StateValue.class, StateValue.coder());
    PCollection<KV<String, StateValue>> inputsMaterialized = pipeline.apply(inputs);

    // replace all MultiParDos
    pipeline.replaceAll(
        Collections.singletonList(statefulParMultiDoOverride(inputsMaterialized, stateSink)));
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

  private static PTransformOverride statefulParMultiDoOverride(
      PCollection<KV<String, StateValue>> inputs,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

    return PTransformOverride.of(
        application -> application.getTransform() instanceof ParDo.MultiOutput,
        parMultiDoReplacementFactory(inputs, stateSink));
  }

  private static PTransformOverrideFactory parMultiDoReplacementFactory(
      PCollection<KV<String, StateValue>> inputs,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

    return new PTransformOverrideFactory() {
      @Override
      public PTransformReplacement getReplacementTransform(AppliedPTransform transform) {
        return replaceParMultiDo(transform, inputs, stateSink);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Map<PCollection<?>, ReplacementOutput> mapOutputs(Map outputs, POutput newOutput) {
        return ReplacementOutputs.tagged(outputs, newOutput);
      }
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PTransformReplacement<PInput, POutput> replaceParMultiDo(
      AppliedPTransform<PInput, POutput, ?> transform,
      PCollection<KV<String, StateValue>> inputs,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

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
    TupleTag<StateValue> stateValueTupleTag = new TupleTag<>() {};
    return PTransformReplacement.of(
        pMainInput,
        transformedParDo(
            Objects.requireNonNull(transformName),
            transformInputs,
            (DoFn) doFn,
            mainOutputTag,
            stateValueTupleTag,
            TupleTagList.of(
                transform.getOutputs().keySet().stream()
                    .filter(t -> !t.equals(mainOutputTag))
                    .collect(Collectors.toList())),
            stateSink));
  }

  @SuppressWarnings("unchecked")
  private static <K, V, InputT extends KV<K, V>, OutputT>
      PTransform<PCollection<InputT>, PCollectionTuple> transformedParDo(
          String transformName,
          PCollection<StateValue> transformInputs,
          DoFn<KV<K, V>, OutputT> doFn,
          TupleTag<OutputT> mainOutputTag,
          TupleTag<StateValue> stateValueTupleTag,
          TupleTagList otherOutputs,
          PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink) {

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
                                            StateOrInput.state(e)))))
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
        PCollectionTuple tuple =
            flattened.apply(
                ParDo.of(transformedDoFn(doFn, input.getCoder(), mainOutputTag))
                    .withOutputTags(mainOutputTag, otherOutputs.and(stateValueTupleTag)));
        PCollection<StateValue> stateValuePCollection = tuple.get(stateValueTupleTag);
        stateValuePCollection.apply(WithKeys.of(transformName)).apply(stateSink);
        PCollectionTuple res = PCollectionTuple.empty(input.getPipeline());
        for (Entry<TupleTag<Object>, PCollection<Object>> e :
            (Set<Entry<TupleTag<Object>, PCollection<Object>>>) (Set) tuple.getAll().entrySet()) {
          if (!e.getKey().equals(stateValueTupleTag)) {
            res = res.and(e.getKey(), e.getValue());
          }
        }
        return res;
      }
    };
  }

  @VisibleForTesting
  static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      DoFn<InputT, OutputT> transformedDoFn(
          DoFn<KV<K, V>, OutputT> doFn,
          Coder<? extends KV<K, V>> inputCoder,
          TupleTag<OutputT> mainTag) {

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
        getParameterizedDoFn((Class<DoFn<KV<K, V>, OutputT>>) doFn.getClass());
    ParameterizedType inputType =
        (ParameterizedType) parameterizedSuperClass.getActualTypeArguments()[0];
    Preconditions.checkArgument(
        inputType.getRawType().equals(KV.class),
        "Input type to stateful DoFn must be KV, go %s",
        inputType);

    Type outputType = parameterizedSuperClass.getActualTypeArguments()[1];
    Generic wrapperInput = getWrapperInputType(inputType);

    Generic doFnGeneric =
        Generic.Builder.parameterizedType(
                TypeDescription.ForLoadedType.of(DoFn.class),
                wrapperInput,
                TypeDescription.Generic.Builder.of(outputType).build())
            .build();
    @SuppressWarnings("unchecked")
    Builder<DoFn<InputT, OutputT>> builder =
        (Builder<DoFn<InputT, OutputT>>)
            buddy
                .subclass(doFnGeneric)
                .name(className)
                .defineField("delegate", doFnClass, Visibility.PRIVATE);
    builder = addStateAndTimers(doFnClass, inputType, inputCoder, builder);
    builder =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(doFnClass)
            .intercept(
                addStateAndTimerValues(
                    doFn,
                    inputCoder,
                    MethodCall.invoke(
                            ExceptionUtils.uncheckedFactory(() -> DoFn.class.getConstructor()))
                        .andThen(FieldAccessor.ofField("delegate").setsArgumentAt(0))));

    builder = addProcessingMethods(doFn, inputType, mainTag, outputType, builder);
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

  private static <K, V, InputT extends KV<K, V>, OutputT> Implementation addStateAndTimerValues(
      DoFn<InputT, OutputT> doFn, Coder<? extends KV<K, V>> inputCoder, Composable delegate) {

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
    delegate =
        delegate.andThen(
            FieldAccessor.ofField(EXPANDER_STATE_SPEC).setsValue(StateSpecs.bag(inputCoder)));
    return delegate;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <InputT, OutputT> ParameterizedType getParameterizedDoFn(
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
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          TupleTag<OutputT> mainTag,
          Type outputType,
          Builder<DoFn<InputT, OutputT>> builder) {

    builder = addProcessingMethod(doFn, DoFn.Setup.class, builder);
    builder = addProcessingMethod(doFn, DoFn.StartBundle.class, builder);
    builder = addProcessElementMethod(doFn, inputType, mainTag, outputType, builder);
    builder = addProcessingMethod(doFn, DoFn.FinishBundle.class, builder);
    builder = addProcessingMethod(doFn, DoFn.Teardown.class, builder);
    builder = addOnWindowExpirationMethod(doFn, inputType, mainTag, builder);
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

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addProcessElementMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          TupleTag<OutputT> mainTag,
          Type outputType,
          Builder<DoFn<InputT, OutputT>> builder) {

    Class<? extends Annotation> annotation = ProcessElement.class;
    Method method = findMethod(doFn, annotation);
    if (method != null) {
      ProcessElementParameterExpander expander =
          ProcessElementParameterExpander.of(doFn, method, inputType, mainTag, outputType);
      List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
      MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
          builder
              .defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
              .withParameters(
                  wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
              .intercept(
                  MethodDelegation.to(new ProcessElementInterceptor<>(doFn, expander, method)));

      for (int i = 0; i < wrapperArgs.size(); i++) {
        Pair<AnnotationDescription, TypeDefinition> arg = wrapperArgs.get(i);
        if (arg.getFirst() != null) {
          methodDefinition = methodDefinition.annotateParameter(i, arg.getFirst());
        }
      }
      return methodDefinition.annotateMethod(
          AnnotationDescription.Builder.ofType(annotation).build());
    }
    return builder;
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addOnWindowExpirationMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          ParameterizedType inputType,
          TupleTag<OutputT> mainTag,
          Builder<DoFn<InputT, OutputT>> builder) {

    Class<? extends Annotation> annotation = DoFn.OnWindowExpiration.class;
    Method onWindowExpirationMethod = findMethod(doFn, annotation);
    Method processElementMethod = findMethod(doFn, DoFn.ProcessElement.class);
    Type outputType = doFn.getOutputTypeDescriptor().getType();
    if (processElementMethod != null) {
      OnWindowParameterExpander expander =
          OnWindowParameterExpander.of(
              inputType, processElementMethod, onWindowExpirationMethod, mainTag, outputType);
      List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs = expander.getWrapperArgs();
      MethodDefinition<DoFn<InputT, OutputT>> methodDefinition =
          builder
              .defineMethod(
                  onWindowExpirationMethod.getName(),
                  onWindowExpirationMethod.getReturnType(),
                  Visibility.PUBLIC)
              .withParameters(
                  wrapperArgs.stream().map(Pair::getSecond).collect(Collectors.toList()))
              .intercept(
                  MethodDelegation.to(
                      new OnWindowExpirationInterceptor<>(
                          doFn, processElementMethod, onWindowExpirationMethod, expander)));

      // retrieve parameter annotations and apply them
      for (int i = 0; i < wrapperArgs.size(); i++) {
        AnnotationDescription ann = wrapperArgs.get(i).getFirst();
        if (ann != null) {
          methodDefinition = methodDefinition.annotateParameter(i, ann);
        }
      }
      return methodDefinition.annotateMethod(
          AnnotationDescription.Builder.ofType(annotation).build());
    }
    return builder;
  }

  private static <K, V, OutputT> Method findMethod(
      DoFn<KV<K, V>, OutputT> doFn, Class<? extends Annotation> annotation) {

    return Iterables.getOnlyElement(
        Arrays.stream(doFn.getClass().getMethods())
            .filter(m -> m.getAnnotation(annotation) != null)
            .collect(Collectors.toList()),
        null);
  }

  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT, T extends Annotation>
      Builder<DoFn<InputT, OutputT>> addProcessingMethod(
          DoFn<KV<K, V>, OutputT> doFn,
          Class<T> annotation,
          Builder<DoFn<InputT, OutputT>> builder) {

    Method method = findMethod(doFn, annotation);
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
          ParameterizedType inputType,
          Coder<? extends KV<K, V>> inputCoder,
          Builder<DoFn<InputT, OutputT>> builder) {

    builder = cloneFields(doFnClass, StateId.class, builder);
    builder = cloneFields(doFnClass, TimerId.class, builder);
    builder = addBufferingStatesAndTimer(inputType, builder);
    return builder;
  }

  /** Add state that buffers inputs until we process all state updates. */
  private static <K, V, InputT extends KV<K, StateOrInput<V>>, OutputT>
      Builder<DoFn<InputT, OutputT>> addBufferingStatesAndTimer(
          ParameterizedType inputType, Builder<DoFn<InputT, OutputT>> builder) {

    Generic kvType = getInputKvType(inputType);

    // type: StateSpec<BagState<KV<K, V>>>
    Generic stateSpecFieldType =
        Generic.Builder.parameterizedType(
                TypeDescription.ForLoadedType.of(StateSpec.class),
                Generic.Builder.parameterizedType(
                        TypeDescription.ForLoadedType.of(BagState.class), kvType)
                    .build())
            .build();

    builder =
        builder
            .defineField(
                EXPANDER_STATE_SPEC,
                stateSpecFieldType,
                Visibility.PUBLIC.getMask() + FieldManifestation.FINAL.getMask())
            .annotateField(
                AnnotationDescription.Builder.ofType(DoFn.StateId.class)
                    .define("value", EXPANDER_STATE_NAME)
                    .build());
    return builder;
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
  private static @NonNull PInput asTuple(Map<TupleTag<?>, PCollection<?>> mainInputs) {
    Preconditions.checkArgument(!mainInputs.isEmpty());
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

  private static class ProcessElementInterceptor<K, V> {

    private final DoFn<KV<K, V>, ?> doFn;
    private final ProcessElementParameterExpander expander;
    private final Method process;
    private final UnaryFunction<Object[], Boolean> processFn;

    ProcessElementInterceptor(
        DoFn<KV<K, V>, ?> doFn, ProcessElementParameterExpander expander, Method process) {

      this.doFn = doFn;
      this.expander = expander;
      this.process = process;
      this.processFn = expander.getProcessFn();
    }

    @RuntimeType
    public void intercept(
        @This DoFn<KV<V, StateOrInput<V>>, ?> proxy, @AllArguments Object[] allArgs) {

      if (processFn.apply(allArgs)) {
        Object[] methodArgs = expander.getProcessElementArgs(allArgs);
        ExceptionUtils.unchecked(() -> process.invoke(doFn, methodArgs));
      }
    }
  }

  private static class OnWindowExpirationInterceptor<K, V> {
    private final DoFn<KV<K, V>, ?> doFn;
    private final Method processElement;
    private final Method onWindowExpiration;
    private final OnWindowParameterExpander expander;

    public OnWindowExpirationInterceptor(
        DoFn<KV<K, V>, ?> doFn,
        Method processElementMethod,
        Method onWindowExpirationMethod,
        OnWindowParameterExpander expander) {

      this.doFn = doFn;
      this.processElement = processElementMethod;
      this.onWindowExpiration = onWindowExpirationMethod;
      this.expander = expander;
    }

    @RuntimeType
    public void intercept(
        @This DoFn<KV<V, StateOrInput<V>>, ?> proxy, @AllArguments Object[] allArgs) {

      @SuppressWarnings("unchecked")
      BagState<KV<K, V>> buf = (BagState<KV<K, V>>) allArgs[allArgs.length - 1];
      Iterable<KV<K, V>> buffered = buf.read();
      // feed all data to @ProcessElement
      for (KV<K, V> kv : buffered) {
        ExceptionUtils.unchecked(
            () -> processElement.invoke(doFn, expander.getProcessElementArgs(kv, allArgs)));
      }
      // invoke onWindowExpiration
      ExceptionUtils.unchecked(
          () -> onWindowExpiration.invoke(doFn, expander.getOnWindowExpirationArgs(allArgs)));
    }
  }
}