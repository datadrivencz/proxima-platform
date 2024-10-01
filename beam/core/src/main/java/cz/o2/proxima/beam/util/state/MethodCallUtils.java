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

import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
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
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

class MethodCallUtils {

  static Object[] fromGenerators(
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> generators,
      Object[] wrapperArgs) {

    return fromGenerators(null, generators, wrapperArgs);
  }

  static Object[] fromGenerators(
      @Nullable TimestampedValue<KV<?, ?>> elem,
      List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> generators,
      Object[] wrapperArgs) {

    Object[] res = new Object[generators.size()];
    for (int i = 0; i < generators.size(); i++) {
      res[i] = generators.get(i).apply(wrapperArgs, elem);
    }
    return res;
  }

  static List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> projectArgs(
      LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgList,
      LinkedHashMap<TypeId, Pair<Annotation, Type>> argsMap,
      TupleTag<?> mainTag,
      Type outputType) {

    List<BiFunction<Object[], TimestampedValue<KV<?, ?>>, Object>> res =
        new ArrayList<>(argsMap.size());
    List<TypeDefinition> wrapperParamsIds =
        wrapperArgList.values().stream()
            .map(p -> p.getFirst() != null ? p.getFirst().getAnnotationType() : p.getSecond())
            .collect(Collectors.toList());
    for (Map.Entry<TypeId, Pair<Annotation, Type>> e : argsMap.entrySet()) {
      int wrapperArg = findArgIndex(wrapperArgList.keySet(), e.getKey());
      if (wrapperArg < 0) {
        // the wrapper does not have the required argument
        if (e.getKey().isElement()) {
          // wrapper does not have @Element, we need to provide it from input element
          res.add((args, elem) -> Objects.requireNonNull(elem.getValue()));
        } else if (e.getKey().isTimestamp()) {
          // wrapper does not have timestamp
          res.add((args, elem) -> elem.getTimestamp());
        } else if (e.getKey().isOutput(outputType)) {
          int wrapperPos =
              wrapperParamsIds.indexOf(
                  TypeDescription.ForLoadedType.of(DoFn.MultiOutputReceiver.class));
          if (e.getKey().isMultiOutput()) {
            // inject timestamp
            res.add(
                (args, elem) ->
                    remapTimestampIfNeeded((DoFn.MultiOutputReceiver) args[wrapperPos], elem));
          } else {
            // remap MultiOutputReceiver to OutputReceiver
            Preconditions.checkState(wrapperPos >= 0);
            res.add(
                (args, elem) ->
                    singleOutput((DoFn.MultiOutputReceiver) args[wrapperPos], elem, mainTag));
          }
        } else {
          throw new IllegalStateException(
              "Missing argument "
                  + e.getKey()
                  + " in wrapper. Options are "
                  + wrapperArgList.keySet());
        }
      } else {
        if (e.getKey().isElement()) {
          // this applies to @ProcessElement only, the input element holds KV<?, StateOrInput>
          res.add(
              (args, elem) ->
                  extractValue((TimestampedValue<KV<?, StateOrInput<?>>>) args[wrapperArg]));
        } else if (e.getKey().isTimestamp()) {
          res.add((args, elem) -> elem == null ? args[wrapperArg] : elem.getTimestamp());
        } else if (e.getKey().isOutput(outputType)) {
          if (e.getKey().isMultiOutput()) {
            res.add(
                (args, elem) ->
                    elem == null
                        ? args[wrapperArg]
                        : remapTimestampIfNeeded(
                            (DoFn.MultiOutputReceiver) args[wrapperArg], elem));
          } else {
            res.add(
                (args, elem) -> {
                  if (elem == null) {
                    return args[wrapperArg];
                  }
                  OutputReceiver<?> parent = (OutputReceiver<?>) args[wrapperArg];
                  return new TimestampedOutputReceiver<>(parent, elem.getTimestamp());
                });
          }
        } else {
          res.add((args, elem) -> args[wrapperArg]);
        }
      }
    }
    return res;
  }

  private static DoFn.MultiOutputReceiver remapTimestampIfNeeded(
      MultiOutputReceiver parent, @Nullable TimestampedValue<KV<?, ?>> elem) {

    if (elem == null) {
      return parent;
    }
    return new MultiOutputReceiver() {
      @Override
      public <T> OutputReceiver<T> get(TupleTag<T> tag) {
        OutputReceiver<T> parentReceiver = parent.get(tag);
        return new TimestampedOutputReceiver<>(parentReceiver, elem.getTimestamp());
      }

      @Override
      public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
        OutputReceiver<Row> parentReceiver = parent.getRowReceiver(tag);
        return new TimestampedOutputReceiver<>(parentReceiver, elem.getTimestamp());
      }
    };
  }

  private static KV<?, ?> extractValue(TimestampedValue<KV<?, StateOrInput<?>>> arg) {
    Preconditions.checkArgument(!arg.getValue().getValue().isState());
    return KV.of(arg.getValue().getKey(), arg.getValue().getValue().getInput());
  }

  private static int findArgIndex(Collection<TypeId> collection, TypeId key) {
    int i = 0;
    for (TypeId t : collection) {
      if (key.equals(t)) {
        return i;
      }
      i++;
    }
    return -1;
  }

  static LinkedHashMap<TypeId, Pair<Annotation, Type>> extractArgs(Method method) {
    LinkedHashMap<TypeId, Pair<Annotation, Type>> res = new LinkedHashMap<>();
    if (method != null) {
      for (int i = 0; i < method.getParameterCount(); i++) {
        Type parameterType = method.getGenericParameterTypes()[i];
        verifyArg(parameterType);
        Annotation[] annotations = method.getParameterAnnotations()[i];
        TypeId paramId =
            annotations.length > 0
                ? TypeId.of(getSingleAnnotation(annotations))
                : TypeId.of(parameterType);
        res.put(paramId, Pair.of(annotations.length == 0 ? null : annotations[0], parameterType));
      }
    }
    return res;
  }

  private static void verifyArg(Type parameterType) {
    Preconditions.checkArgument(
        !(parameterType instanceof DoFn.ProcessContext),
        "ProcessContext is not supported. Please use the new-style @Element, @Timestamp, etc.");
  }

  static Annotation getSingleAnnotation(Annotation[] annotations) {
    Preconditions.checkArgument(annotations.length == 1, Arrays.toString(annotations));
    return annotations[0];
  }

  static Generic getInputKvType(ParameterizedType inputType) {
    Type keyType = inputType.getActualTypeArguments()[0];
    Type valueType = inputType.getActualTypeArguments()[1];

    // generic type: KV<K, V>
    return Generic.Builder.parameterizedType(KV.class, keyType, valueType).build();
  }

  private static <T> OutputReceiver<T> singleOutput(
      MultiOutputReceiver multiOutput,
      @Nullable TimestampedValue<KV<?, ?>> elem,
      TupleTag<T> mainTag) {

    return new OutputReceiver<T>() {
      @Override
      public void output(T output) {
        if (elem == null) {
          multiOutput.get(mainTag).output(output);
        } else {
          multiOutput.get(mainTag).outputWithTimestamp(output, elem.getTimestamp());
        }
      }

      @Override
      public void outputWithTimestamp(T output, Instant timestamp) {
        multiOutput.get(mainTag).outputWithTimestamp(output, timestamp);
      }
    };
  }

  static Generic getWrapperInputType(ParameterizedType inputType) {
    Type kType = inputType.getActualTypeArguments()[0];
    Type vType = inputType.getActualTypeArguments()[1];
    return Generic.Builder.parameterizedType(
            ForLoadedType.of(KV.class),
            Generic.Builder.of(kType).build(),
            Generic.Builder.parameterizedType(StateOrInput.class, vType).build())
        .build();
  }

  static Map<String, BiConsumer<Object, StateValue>> getStateUpdaters(DoFn<?, ?> doFn) {
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
    stateSpec.bind("dummy", createUpdaterBinder(consumer));
    return consumer.get();
  }

  static LinkedHashMap<String, BiFunction<Object, byte[], Iterable<StateValue>>> getStateReaders(
      DoFn<?, ?> doFn) {

    Field[] fields = doFn.getClass().getDeclaredFields();
    LinkedHashMap<String, BiFunction<Object, byte[], Iterable<StateValue>>> res =
        new LinkedHashMap<>();
    Arrays.stream(fields)
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
                    createReader(
                        ((StateSpec<?>)
                            ExceptionUtils.uncheckedFactory(() -> p.getFirst().get(doFn))))))
        .filter(p -> p.getSecond() != null)
        .forEachOrdered(p -> res.put(p.getFirst(), p.getSecond()));
    return res;
  }

  @SuppressWarnings("unchecked")
  private static @Nullable BiFunction<Object, byte[], Iterable<StateValue>> createReader(
      StateSpec<?> stateSpec) {
    AtomicReference<BiFunction<Object, byte[], Iterable<StateValue>>> res = new AtomicReference<>();
    stateSpec.bind("dummy", createStateReaderBinder(res));
    return res.get();
  }

  @VisibleForTesting
  static StateBinder createUpdaterBinder(AtomicReference<BiConsumer<Object, StateValue>> consumer) {
    return new StateBinder() {
      @Override
      public <T> @Nullable ValueState<T> bindValue(
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
      public <T> @Nullable BagState<T> bindBag(
          String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
        consumer.set(
            (accessor, value) ->
                ((BagState<T>) accessor)
                    .add(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(elemCoder, value.getValue()))));
        return null;
      }

      @Override
      public <T> @Nullable SetState<T> bindSet(
          String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
        consumer.set(
            (accessor, value) ->
                ((SetState<T>) accessor)
                    .add(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(elemCoder, value.getValue()))));
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MapState<KeyT, ValueT> bindMap(
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
      public <T> @Nullable OrderedListState<T> bindOrderedList(
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
      public <KeyT, ValueT> @Nullable MultimapState<KeyT, ValueT> bindMultimap(
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
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombining(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFn<InputT, AccumT, OutputT> combineFn) {
        consumer.set(
            (accessor, value) ->
                ((CombiningState<InputT, AccumT, OutputT>) accessor)
                    .addAccum(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(accumCoder, value.getValue()))));
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        consumer.set(
            (accessor, value) ->
                ((CombiningState<InputT, AccumT, OutputT>) accessor)
                    .addAccum(
                        ExceptionUtils.uncheckedFactory(
                            () -> CoderUtils.decodeFromByteArray(accumCoder, value.getValue()))));
        return null;
      }

      @Override
      public @Nullable WatermarkHoldState bindWatermark(
          String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
        return null;
      }
    };
  }

  @VisibleForTesting
  static StateBinder createStateReaderBinder(
      AtomicReference<BiFunction<Object, byte[], Iterable<StateValue>>> res) {

    return new StateBinder() {
      @Override
      public <T> @Nullable ValueState<T> bindValue(
          String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
        res.set(
            (accessor, key) -> {
              T value = ((ValueState<T>) accessor).read();
              if (value != null) {
                byte[] bytes =
                    ExceptionUtils.uncheckedFactory(
                        () -> CoderUtils.encodeToByteArray(coder, value));
                return Collections.singletonList(new StateValue(key, id, bytes));
              }
              return Collections.emptyList();
            });
        return null;
      }

      @Override
      public <T> @Nullable BagState<T> bindBag(
          String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((BagState<T>) accessor).read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () -> CoderUtils.encodeToByteArray(elemCoder, v)))));
        return null;
      }

      @Override
      public <T> @Nullable SetState<T> bindSet(
          String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((SetState<T>) accessor).read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () -> CoderUtils.encodeToByteArray(elemCoder, v)))));
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MapState<KeyT, ValueT> bindMap(
          String id,
          StateSpec<MapState<KeyT, ValueT>> spec,
          Coder<KeyT> mapKeyCoder,
          Coder<ValueT> mapValueCoder) {
        KvCoder<KeyT, ValueT> coder = KvCoder.of(mapKeyCoder, mapValueCoder);
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((MapState<KeyT, ValueT>) accessor).entries().read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () ->
                                    CoderUtils.encodeToByteArray(
                                        coder, KV.of(v.getKey(), v.getValue()))))));
        return null;
      }

      @Override
      public <T> @Nullable OrderedListState<T> bindOrderedList(
          String id, StateSpec<OrderedListState<T>> spec, Coder<T> elemCoder) {
        KvCoder<T, Instant> coder = KvCoder.of(elemCoder, InstantCoder.of());
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((OrderedListState<T>) accessor).read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () ->
                                    CoderUtils.encodeToByteArray(
                                        coder, KV.of(v.getValue(), v.getTimestamp()))))));
        return null;
      }

      @Override
      public <KeyT, ValueT> @Nullable MultimapState<KeyT, ValueT> bindMultimap(
          String id,
          StateSpec<MultimapState<KeyT, ValueT>> spec,
          Coder<KeyT> keyCoder,
          Coder<ValueT> valueCoder) {
        KvCoder<KeyT, ValueT> coder = KvCoder.of(keyCoder, valueCoder);
        res.set(
            (accessor, key) ->
                Iterables.transform(
                    ((MultimapState<KeyT, ValueT>) accessor).entries().read(),
                    v ->
                        new StateValue(
                            key,
                            id,
                            ExceptionUtils.uncheckedFactory(
                                () ->
                                    CoderUtils.encodeToByteArray(
                                        coder, KV.of(v.getKey(), v.getValue()))))));
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombining(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFn<InputT, AccumT, OutputT> combineFn) {
        res.set(
            (accessor, key) -> {
              AccumT accum = ((CombiningState<InputT, AccumT, OutputT>) accessor).getAccum();
              return Collections.singletonList(
                  new StateValue(
                      key,
                      id,
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.encodeToByteArray(accumCoder, accum))));
            });
        return null;
      }

      @Override
      public <InputT, AccumT, OutputT> @Nullable
          CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(
              String id,
              StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
              Coder<AccumT> accumCoder,
              CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
        res.set(
            (accessor, key) -> {
              AccumT accum = ((CombiningState<InputT, AccumT, OutputT>) accessor).getAccum();
              return Collections.singletonList(
                  new StateValue(
                      key,
                      id,
                      ExceptionUtils.uncheckedFactory(
                          () -> CoderUtils.encodeToByteArray(accumCoder, accum))));
            });
        return null;
      }

      @Override
      public @Nullable WatermarkHoldState bindWatermark(
          String id, StateSpec<WatermarkHoldState> spec, TimestampCombiner timestampCombiner) {
        return null;
      }
    };
  }

  private MethodCallUtils() {}

  private static class TimestampedOutputReceiver<T> implements OutputReceiver<T> {

    private final OutputReceiver<T> parentReceiver;
    private final Instant elementTimestamp;

    public TimestampedOutputReceiver(OutputReceiver<T> parentReceiver, Instant timestamp) {
      this.parentReceiver = parentReceiver;
      this.elementTimestamp = timestamp;
    }

    @Override
    public void output(T output) {
      outputWithTimestamp(output, elementTimestamp);
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {
      parentReceiver.outputWithTimestamp(output, timestamp);
    }
  }
}
