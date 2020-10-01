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

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Format {@link PCollection} of {@link TypedElement typed elements} to strings, so we can easily
 * assert it for testing.
 */
public class TypedElementFormatter<T>
    extends PTransform<PCollection<TypedElement<T>>, PCollection<String>> {

  /**
   * Construct a new {@link TypedElementFormatter}.
   *
   * @param toStringFn Function that converts typed element to string.
   * @param timeBase Baseline timestamp, so we can convert resulting timestamp to duration from the
   *     baseline.
   * @param <T> Type of typed element values.
   * @return Formatting transform.
   */
  public static <T> TypedElementFormatter<T> ofTypedElement(
      SerializableFunction<TypedElement<T>, String> toStringFn, Instant timeBase) {
    return new TypedElementFormatter<>(toStringFn, timeBase);
  }

  /**
   * Construct a new {@link TypedElementFormatter}.
   *
   * @param toStringFn Function that converts typed element value to string.
   * @param timeBase Baseline timestamp, so we can convert resulting timestamp to duration from the
   *     baseline.
   * @param <T> Type of typed element values.
   * @return Formatting transform.
   */
  public static <T> TypedElementFormatter<T> of(
      SerializableFunction<T, String> toStringFn, Instant timeBase) {
    return ofTypedElement(el -> toStringFn.apply(el.getValue()), timeBase);
  }

  private final SerializableFunction<TypedElement<T>, String> toStringFn;
  private final Instant timeBase;

  private TypedElementFormatter(
      SerializableFunction<TypedElement<T>, String> toStringFn, Instant timeBase) {
    this.toStringFn = toStringFn;
    this.timeBase = timeBase;
  }

  @Override
  public PCollection<String> expand(PCollection<TypedElement<T>> input) {
    return input
        .apply("Reify", Reify.timestamps())
        .apply(
            "Format",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    item -> {
                      final Duration duration = new Duration(timeBase, item.getTimestamp());
                      final TypedElement<T> el = item.getValue();
                      return String.format(
                          "%s@%s.%s [%s] at %s",
                          el.getKey(),
                          el.getAttributeDescriptor().getEntity(),
                          el.getAttribute(),
                          toStringFn.apply(el),
                          duration);
                    }));
  }
}
