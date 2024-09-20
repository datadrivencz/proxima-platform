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

import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import net.bytebuddy.description.type.TypeDescription.Generic;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class MethodCallUtils {

  static Object[] fromGenerators(
      List<BiFunction<Object[], KV<?, ?>, Object>> generators, Object[] wrapperArgs) {

    return fromGenerators(null, generators, wrapperArgs);
  }

  static Object[] fromGenerators(
      @Nullable KV<?, ?> elem,
      List<BiFunction<Object[], KV<?, ?>, Object>> generators,
      Object[] wrapperArgs) {

    Object[] res = new Object[generators.size()];
    for (int i = 0; i < generators.size(); i++) {
      res[i] = generators.get(i).apply(wrapperArgs, elem);
    }
    return res;
  }

  static List<BiFunction<Object[], KV<?, ?>, Object>> projectArgs(
      LinkedHashMap<TypeId, Pair<AnnotationDescription, TypeDefinition>> wrapperArgList,
      LinkedHashMap<TypeId, Pair<Annotation, Type>> argsMap,
      TupleTag<?> mainTag,
      Type outputType) {

    List<BiFunction<Object[], KV<?, ?>, Object>> res = new ArrayList<>(argsMap.size());
    List<TypeDefinition> wrapperParamsIds =
        wrapperArgList.values().stream()
            .map(p -> p.getFirst() != null ? p.getFirst().getAnnotationType() : p.getSecond())
            .collect(Collectors.toList());
    for (Map.Entry<TypeId, Pair<Annotation, Type>> e : argsMap.entrySet()) {
      int wrapperArg = findArgIndex(wrapperArgList.keySet(), e.getKey());
      if (wrapperArg < 0) {
        if (e.getKey().isElement()) {
          res.add((args, elem) -> elem);
        } else if (e.getKey()
            .equals(
                TypeId.of(
                    TypeDescription.Generic.Builder.parameterizedType(
                            OutputReceiver.class, outputType)
                        .build()))) {
          // remap OutputReceiver to MultiOutputReceiver
          int outputPos =
              wrapperParamsIds.indexOf(
                  TypeDescription.ForLoadedType.of(DoFn.MultiOutputReceiver.class));
          Preconditions.checkState(outputPos >= 0);
          res.add(
              (args, elem) -> fromMultiOutput((DoFn.MultiOutputReceiver) args[outputPos], mainTag));
        } else {
          throw new IllegalStateException(
              "Missing argument "
                  + e.getKey()
                  + " in wrapper. Options are "
                  + wrapperArgList.keySet());
        }
      } else {
        res.add((args, elem) -> args[wrapperArg]);
      }
    }
    return res;
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
    Generic kvType = Generic.Builder.parameterizedType(KV.class, keyType, valueType).build();
    return kvType;
  }

  private static <T> OutputReceiver<T> fromMultiOutput(
      MultiOutputReceiver multiOutput, TupleTag<T> mainTag) {

    return new OutputReceiver<T>() {
      @Override
      public void output(T output) {
        multiOutput.get(mainTag).output(output);
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

  private MethodCallUtils() {}
}
