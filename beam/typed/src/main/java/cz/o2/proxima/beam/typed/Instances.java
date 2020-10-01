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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Value;

/** Helpers for easier work with Java instances. */
public class Instances {

  @Value
  public static class Parameter<T> {
    /** Type of the parameter. */
    final Class<T> type;

    /** Value of the parameter. */
    final T value;
  }

  /**
   * Instantiates class using declared constructor.
   *
   * @param clazz Class to instantiate.
   * @param parameters Parameters to pass to constructor.
   * @param <T> Class type.
   * @return New instance of the provided class.
   */
  public static <T> T create(Class<T> clazz, Parameter<?>... parameters) {
    final Map<Class<?>, Object> parameterMap = new HashMap<>();
    for (Parameter<?> parameter : parameters) {
      final Object previous = parameterMap.put(parameter.getType(), parameter.getValue());
      if (previous != null) {
        throw new UnsupportedOperationException(
            "Multiple parameters with the same type are not supported.");
      }
    }
    @SuppressWarnings("unchecked")
    final Constructor<T>[] constructors = (Constructor<T>[]) clazz.getConstructors();
    return findSuitableConstructor(constructors, parameterMap)
        .map(
            constructor -> {
              final Class<?>[] constructorParameterTypes = constructor.getParameterTypes();
              final Object[] constructorParameterInstances =
                  new Object[constructorParameterTypes.length];
              for (int i = 0; i < constructorParameterTypes.length; i++) {
                constructorParameterInstances[i] =
                    Objects.requireNonNull(parameterMap.get(constructorParameterTypes[i]));
              }
              try {
                return constructor.newInstance(constructorParameterInstances);
              } catch (Exception e) {
                throw new IllegalArgumentException(
                    String.format("Unable to instantiate class [%s].", clazz), e);
              }
            })
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Unable to find suitable constructor for class [%s].", clazz)));
  }

  /**
   * Find suitable constructor that can be called using provided parameters.
   *
   * @param candidates Candidates to check.
   * @param parameters Parameters that can be used for calling the constructor.
   * @param <T> Type that constructor creates.
   * @return Maybe constructor.
   */
  private static <T> Optional<Constructor<T>> findSuitableConstructor(
      Constructor<T>[] candidates, Map<Class<?>, Object> parameters) {
    for (Constructor<T> constructor : candidates) {
      if (isConstructorSuitable(constructor, parameters)) {
        return Optional.of(constructor);
      }
    }
    return Optional.empty();
  }

  /**
   * Check whether the provided constructor can be called using provided parameters.
   *
   * @param constructor Constructor to check.
   * @param parameters Parameters that can be used for calling the constructor.
   * @return True if constructor is suitable.
   */
  private static boolean isConstructorSuitable(
      Constructor<?> constructor, Map<Class<?>, Object> parameters) {
    if (Modifier.isPublic(constructor.getModifiers())) {
      for (Class<?> clazz : constructor.getParameterTypes()) {
        if (!parameters.containsKey(clazz)) {
          return false;
        }
      }
    }
    return true;
  }
}
