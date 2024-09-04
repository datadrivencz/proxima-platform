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
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

interface OnWindowParameterExpander {

  static OnWindowParameterExpander of(
      ParameterizedType inputType, Method processElement, @Nullable Method onWindowExpiration) {

    final LinkedHashMap<Type, Pair<Annotation, Type>> processArgs = extractArgs(processElement);
    final LinkedHashMap<Type, Pair<Annotation, Type>> onWindowArgs =
        extractArgs(onWindowExpiration);
    final List<Pair<Annotation, Type>> wrapperArgList =
        createWrapperArgList(processArgs, onWindowArgs);
    final List<Pair<AnnotationDescription, TypeDefinition>> wrapperArgs =
        createWrapperArgs(inputType, wrapperArgList);
    final List<BiFunction<Object[], KV<?, ?>, Object>> processArgsGenerators =
        projectArgs(wrapperArgList, processArgs);
    final List<BiFunction<Object[], KV<?, ?>, Object>> windowArgsGenerators =
        projectArgs(wrapperArgList, onWindowArgs);

    return new OnWindowParameterExpander() {
      @Override
      public List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs() {
        return wrapperArgs;
      }

      @Override
      public Object[] getProcessElementArgs(KV<?, ?> input, Object[] wrapperArgs) {
        return fromGenerators(input, processArgsGenerators, wrapperArgs);
      }

      @Override
      public Object[] getOnWindowExpirationArgs(Object[] wrapperArgs) {
        return fromGenerators(null, windowArgsGenerators, wrapperArgs);
      }

      private Object[] fromGenerators(
          @Nullable KV<?, ?> elem,
          List<BiFunction<Object[], KV<?, ?>, Object>> generators,
          Object[] wrapperArgs) {

        Object[] res = new Object[generators.size()];
        for (int i = 0; i < generators.size(); i++) {
          res[i] = generators.get(i).apply(wrapperArgs, elem);
        }
        return res;
      }
    };
  }

  static List<BiFunction<Object[], KV<?, ?>, Object>> projectArgs(
      List<Pair<Annotation, Type>> wrapperArgList, Map<Type, Pair<Annotation, Type>> argsMap) {

    List<BiFunction<Object[], KV<?, ?>, Object>> res = new ArrayList<>();
    return res;
  }

  static List<Pair<Annotation, Type>> createWrapperArgList(
      LinkedHashMap<Type, Pair<Annotation, Type>> processArgs,
      LinkedHashMap<Type, Pair<Annotation, Type>> onWindowArgs) {

    Set<Type> union = new HashSet<>(Sets.union(processArgs.keySet(), onWindowArgs.keySet()));
    // @Element is not supported by @OnWindowExpiration
    union.remove(DoFn.Element.class);
    return union.stream()
        .map(
            t -> {
              Pair<Annotation, Type> processPair = processArgs.get(t);
              Pair<Annotation, Type> windowPair = onWindowArgs.get(t);
              Type argType = MoreObjects.firstNonNull(processPair, windowPair).getSecond();
              Annotation processAnnotation =
                  Optional.ofNullable(processPair).map(Pair::getFirst).orElse(null);
              Annotation windowAnnotation =
                  Optional.ofNullable(windowPair).map(Pair::getFirst).orElse(null);
              Preconditions.checkState(
                  processPair == null
                      || windowPair == null
                      || processAnnotation == windowAnnotation
                      || processAnnotation.equals(windowAnnotation));
              return Pair.of(processAnnotation, argType);
            })
        .collect(Collectors.toList());
  }

  static List<Pair<AnnotationDescription, TypeDefinition>> createWrapperArgs(
      ParameterizedType inputType, List<Pair<Annotation, Type>> wrapperArgList) {

    List<Pair<? extends AnnotationDescription, ? extends TypeDefinition>> res =
        wrapperArgList.stream()
            .map(
                p ->
                    Pair.of(
                        p.getFirst() != null
                            ? AnnotationDescription.ForLoadedAnnotation.of(p.getFirst())
                            : null,
                        TypeDescription.Generic.Builder.of(p.getSecond()).build()))
            .collect(Collectors.toList());

    // add @StateId for buffer
    res.add(
        Pair.of(
            AnnotationDescription.Builder.ofType(DoFn.StateId.class)
                .define("value", ExternalStateExpander.EXPANDER_STATE_NAME)
                .build(),
            TypeDescription.Generic.Builder.parameterizedType(
                    TypeDescription.ForLoadedType.of(BagState.class),
                    ExternalStateExpander.getInputKvType(inputType))
                .build()));
    return (List) res;
  }

  private static LinkedHashMap<Type, Pair<Annotation, Type>> extractArgs(Method method) {
    LinkedHashMap<Type, Pair<Annotation, Type>> res = new LinkedHashMap<>();
    if (method != null) {
      for (int i = 0; i < method.getParameterCount(); i++) {
        Annotation[] annotations = method.getParameterAnnotations()[i];
        Type paramId =
            annotations.length > 0
                ? getSingleAnnotation(annotations)
                : method.getGenericParameterTypes()[i];
        res.put(
            paramId,
            Pair.of(
                annotations.length == 0 ? null : annotations[0],
                method.getGenericParameterTypes()[i]));
      }
    }
    return res;
  }

  static Class<?> getSingleAnnotation(Annotation[] annotations) {
    Preconditions.checkArgument(annotations.length == 1, Arrays.toString(annotations));
    return annotations[0].annotationType();
  }

  /**
   * Get arguments that must be declared by wrapper's call for both {@code @}ProcessElement and
   * {@code @}OnWindowExpiration be callable.
   */
  List<Pair<AnnotationDescription, TypeDefinition>> getWrapperArgs();

  /**
   * Get parameters that should be passed to {@code @}ProcessElement from wrapper's
   * {@code @}OnWindowExpiration
   */
  Object[] getProcessElementArgs(KV<?, ?> input, Object[] wrapperArgs);

  /** Get parameters that should be passed to {@code @}OnWindowExpiration from wrapper's call. */
  Object[] getOnWindowExpirationArgs(Object[] wrapperArgs);
}
