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
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.annotation.AnnotationDescription.ForLoadedAnnotation;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.description.type.TypeDescription.Generic.Builder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

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

  private static Map<String, BiConsumer<Object, StateValue>> getStateUpdaters(DoFn<?, ?> doFn) {
    Field[] fields = doFn.getClass().getDeclaredFields();
    return Arrays.stream(fields)
        .map(f -> Pair.of(f, f.getAnnotation(DoFn.StateId.class)))
        .filter(p -> p.getSecond() != null)
        .map(
            p -> {
              p.getFirst().setAccessible(true);
              return p;
            })
        .map(
            p ->
                Pair.of(
                    p.getSecond().value(),
                    createUpdater(
                        ((StateSpec<?>)
                            ExceptionUtils.uncheckedFactory(() -> p.getFirst().get(doFn))))))
        .filter(p -> p.getSecond() != null)
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  @SuppressWarnings("unchecked")
  private static @Nullable BiConsumer<Object, StateValue> createUpdater(StateSpec<?> stateSpec) {
    AtomicReference<BiConsumer<Object, StateValue>> consumer = new AtomicReference<>();
    stateSpec.bind(
        "dummy",
        new StateBinder() {
          @Override
          public <T> ValueState<T> bindValue(
              String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
            consumer.set(
                (accessor, value) ->
                    ((ValueState<T>) accessor)
                        .write(
                            ExceptionUtils.uncheckedFactory(
                                () -> CoderUtils.decodeFromByteArray(coder, value.getValue()))));
            return null;
          }

          @Override
          public <T> BagState<T> bindBag(
              String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
            consumer.set(
                (accessor, value) -> {
                  ((BagState<T>) accessor)
                      .add(
                          ExceptionUtils.uncheckedFactory(
                              () -> CoderUtils.decodeFromByteArray(elemCoder, value.getValue())));
                });
            return null;
          }

          @Override
          public <T> SetState<T> bindSet(
              String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
            consumer.set(
                (accessor, value) -> {
                  ((SetState<T>) accessor)
                      .add(
                          ExceptionUtils.uncheckedFactory(
                              () -> CoderUtils.decodeFromByteArray(elemCoder, value.getValue())));
                });
            return null;
          }

          @Override
          public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
              String id,
              StateSpec<MapState<KeyT, ValueT>> spec,
              Coder<KeyT> mapKeyCoder,
              Coder<ValueT> mapValueCoder) {
            KvCoder<KeyT, ValueT> coder = KvCoder.of(mapKeyCoder, mapValueCoder);
            consumer.set(
                (accessor, value) -> {
                  KV<KeyT, ValueT> decoded =
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.decodeFromByteArray(coder, value.getValue()));
                  ((MapState<KeyT, ValueT>) accessor).put(decoded.getKey(), decoded.getValue());
                });
            return null;
          }

          @Override
          public <T> OrderedListState<T> bindOrderedList(
              String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
            KvCoder<T, Instant> coder = KvCoder.of(elemCoder, InstantCoder.of());
            consumer.set(
                (accessor, value) -> {
                  KV<T, Instant> decoded =
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.decodeFromByteArray(coder, value.getValue()));
                  ((OrderedListState<T>) accessor)
                      .add(TimestampedValue.of(decoded.getKey(), decoded.getValue()));
                });
            return null;
          }

          @Override
          public <KeyT, ValueT> MultimapState<KeyT, ValueT> bindMultimap(
              String id,
              StateSpec<MultimapState<KeyT, ValueT>> spec,
              Coder<KeyT> keyCoder,
              Coder<ValueT> valueCoder) {
            KvCoder<KeyT, ValueT> coder = KvCoder.of(keyCoder, valueCoder);
            consumer.set(
                (accessor, value) -> {
                  KV<KeyT, ValueT> decoded =
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.decodeFromByteArray(coder, value.getValue()));
                  ((MapState<KeyT, ValueT>) accessor).put(decoded.getKey(), decoded.getValue());
                });
            return null;
          }

          @Override
          public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFn<InputT, AccumT, OutputT> combineFn) {
            consumer.set(
                (accessor, value) -> {
                  ((CombiningState<InputT, AccumT, OutputT>) accessor)
                      .addAccum(
                          ExceptionUtils.uncheckedFactory(
                              () -> CoderUtils.decodeFromByteArray(accumCoder, value.getValue())));
                });
            return null;
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
                  String id,
                  StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
                  Coder<AccumT> accumCoder,
                  CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            consumer.set(
                (accessor, value) -> {
                  ((CombiningState<InputT, AccumT, OutputT>) accessor)
                      .addAccum(
                          ExceptionUtils.uncheckedFactory(
                              () -> CoderUtils.decodeFromByteArray(accumCoder, value.getValue())));
                });
            return null;
          }

          @Override
          public WatermarkHoldState bindWatermark(
              String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
            return null;
          }
        });
    return consumer.get();
  }

  static List<Pair<Annotation, Type>> annotatedArgs(
      LinkedHashMap<Type, Pair<Annotation, Type>> processArgs) {

    return processArgs.entrySet().stream()
        // filter output receivers out, will be replaced
        .filter(
            e ->
                !e.getKey().equals(MultiOutputReceiver.class)
                    && !(e.getKey() instanceof ParameterizedType
                        && ((ParameterizedType) e.getKey())
                            .getRawType()
                            .equals(OutputReceiver.class)))
        .map(Entry::getKey)
        .map(processArgs::get)
        .collect(Collectors.toList());
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
