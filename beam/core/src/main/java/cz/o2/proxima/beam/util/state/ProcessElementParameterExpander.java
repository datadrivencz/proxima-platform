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

import static cz.o2.proxima.beam.util.state.MethodCallUtils.*;
import static cz.o2.proxima.beam.util.state.MethodCallUtils.projectArgs;

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.annotation.AnnotationDescription.ForLoadedAnnotation;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.description.type.TypeDescription.Generic.Builder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

public interface ProcessElementParameterExpander {

  static ProcessElementParameterExpander of(
      DoFn<?, ?> doFn,
      Method processElement,
      ParameterizedType inputType,
      TupleTag<?> mainTag,
      Type outputType) {

    final LinkedHashMap<TypeId, Pair<Annotation, Type>> processArgs = extractArgs(processElement);
    final LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs =
        createWrapperArgs(inputType, outputType, processArgs.values());
    final List<BiFunction<Object[], KV<?, ?>, Object>> processArgsGenerators =
        projectArgs(wrapperArgs, processArgs, mainTag, outputType);

    return new ProcessElementParameterExpander() {
      @Override
      public List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs() {
        return new ArrayList<>(wrapperArgs.values());
      }

      @Override
      public Object[] getProcessElementArgs(Object[] wrapperArgs) {
        return fromGenerators(processArgsGenerators, wrapperArgs);
      }

      @Override
      public UnaryFunction<Object[], Boolean> getProcessFn() {
        return createProcessFn(wrapperArgs, doFn, processElement);
      }
    };
  }

  /** Get arguments that must be declared by wrapper's call. */
  List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs();

  /**
   * Get parameters that should be passed to {@code @}ProcessElement from wrapper's
   * {@code @}ProcessElement
   */
  Object[] getProcessElementArgs(Object[] wrapperArgs);

  /** Get function to process elements and delegate to original DoFn. */
  UnaryFunction<Object[], Boolean> getProcessFn();

  private static UnaryFunction<Object[], Boolean> createProcessFn(
      LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgs,
      DoFn<?, ?> doFn,
      Method method) {

    int elementPos = findParameter(wrapperArgs.keySet(), TypeId::isElement);
    Preconditions.checkState(elementPos >= 0, "Missing @Element annotation on method %s", method);
    Map<String, BiConsumer<Object, StateValue>> stateUpdaterMap = getStateUpdaters(doFn);
    return args -> {
      @SuppressWarnings("unchecked")
      KV<?, StateOrInput<?>> elem = (KV<?, StateOrInput<?>>) args[elementPos];
      Timer flushTimer = (Timer) args[args.length - 3];
      // FIXME: set for particular timestamp
      flushTimer.set(BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.standardDays(90)));
      boolean isState = Objects.requireNonNull(elem.getValue(), "elem").isState();
      if (isState) {
        StateValue state = elem.getValue().getState();
        String stateName = state.getName();
        // find state accessor
        int statePos = findParameter(wrapperArgs.keySet(), a -> a.isState(stateName));
        Preconditions.checkArgument(
            statePos < method.getParameterCount(), "Missing state accessor for %s", stateName);
        Object stateAccessor = args[statePos];
        // find declaration of state to find coder
        BiConsumer<Object, StateValue> updater = stateUpdaterMap.get(stateName);
        Preconditions.checkArgument(
            updater != null, "Missing updater for state %s in %s", stateName, stateUpdaterMap);
        updater.accept(stateAccessor, state);
        return false;
      }
      // FIXME: read this from state
      boolean shouldBuffer = true;
      if (shouldBuffer) {
        // store to state
        @SuppressWarnings("unchecked")
        BagState<KV<?, ?>> buffer = (BagState<KV<?, ?>>) args[args.length - 2];
        buffer.add(KV.of(elem.getKey(), elem.getValue().getInput()));
        return false;
      }
      return true;
    };
  }

  private static int findParameter(Collection<TypeId> args, Predicate<TypeId> predicate) {
    int i = 0;
    for (TypeId t : args) {
      if (predicate.test(t)) {
        return i;
      }
      i++;
    }
    return -1;
  }

  static LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> createWrapperArgs(
      ParameterizedType inputType,
      Type outputType,
      Collection<Pair<Annotation, Type>> processArgs) {

    LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> res = new LinkedHashMap<>();
    TypeId mainOutputReceiverId =
        TypeId.of(Builder.parameterizedType(OutputReceiver.class, outputType).build());
    processArgs.stream()
        .map(p -> transformProcessArg(inputType, p))
        .filter(p -> !p.getFirst().equals(mainOutputReceiverId))
        .forEachOrdered(p -> res.put(p.getFirst(), p.getSecond()));

    // add @TimerId for flush timer
    AnnotationDescription timerAnnotation =
        AnnotationDescription.Builder.ofType(DoFn.TimerId.class)
            .define("value", ExternalStateExpander.EXPANDER_TIMER_NAME)
            .build();
    res.put(
        TypeId.of(timerAnnotation),
        Pair.of(timerAnnotation, TypeDescription.Generic.Builder.of(Timer.class).build()));
    // add @StateId for buffer
    AnnotationDescription stateAnnotation =
        AnnotationDescription.Builder.ofType(StateId.class)
            .define("value", ExternalStateExpander.EXPANDER_STATE_NAME)
            .build();
    res.put(
        TypeId.of(stateAnnotation),
        Pair.of(
            stateAnnotation,
            TypeDescription.Generic.Builder.parameterizedType(
                    TypeDescription.ForLoadedType.of(BagState.class), getInputKvType(inputType))
                .build()));

    // add MultiOutputReceiver
    TypeDescription receiver = ForLoadedType.of(MultiOutputReceiver.class);
    res.put(TypeId.of(receiver), Pair.of(null, receiver));
    return res;
  }

  static Pair<TypeId, Pair<AnnotationDescription, TypeDefinition>> transformProcessArg(
      ParameterizedType inputType, Pair<Annotation, Type> p) {

    TypeId typeId = p.getFirst() == null ? TypeId.of(p.getSecond()) : TypeId.of(p.getFirst());
    AnnotationDescription annotation =
        p.getFirst() != null ? ForLoadedAnnotation.of(p.getFirst()) : null;
    Generic parameterType = Builder.of(p.getSecond()).build();
    if (typeId.equals(
        TypeId.of(AnnotationDescription.Builder.ofType(DoFn.Element.class).build()))) {
      parameterType = getWrapperInputType(inputType);
    }
    return Pair.of(typeId, Pair.of(annotation, parameterType));
  }
}
