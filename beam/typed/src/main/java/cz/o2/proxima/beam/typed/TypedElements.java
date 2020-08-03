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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Utilities for easier work with {@link TypedElement typed elements}. */
public class TypedElements {

  /** {@link #of(AttributeDescriptor)} */
  public static class FromStreamElement<T>
      extends PTransform<PCollection<StreamElement>, PCollection<TypedElement<T>>> {

    private final AttributeDescriptor<T> attributeDescriptor;

    FromStreamElement(AttributeDescriptor<T> attributeDescriptor) {
      this.attributeDescriptor = attributeDescriptor;
    }

    @Override
    public PCollection<TypedElement<T>> expand(PCollection<StreamElement> input) {
      return input.apply(
          MapElements.into(TypedElement.typeDescriptor(attributeDescriptor))
              .via(
                  el -> {
                    if (!attributeDescriptor.equals(el.getAttributeDescriptor())) {
                      throw new IllegalStateException(
                          String.format(
                              "Input element is from [%s.%s], expected [%s.%s].",
                              el.getAttributeDescriptor().getEntity(),
                              el.getAttributeDescriptor().getName(),
                              attributeDescriptor.getEntity(),
                              attributeDescriptor.getName()));
                    }
                    final TypedElement.Operation operation;
                    if (el.isDelete()) {
                      operation = TypedElement.Operation.DELETE;
                    } else if (el.isDeleteWildcard()) {
                      operation = TypedElement.Operation.DELETE_ALL;
                    } else {
                      operation = TypedElement.Operation.UPSERT;
                    }
                    final String attributeSuffix =
                        attributeDescriptor.isWildcard()
                            ? el.getAttribute()
                                .substring(attributeDescriptor.toAttributePrefix().length())
                            : null;
                    return TypedElement.of(
                        attributeDescriptor,
                        operation,
                        el.getKey(),
                        attributeSuffix,
                        el.getValue());
                  }));
    }
  }

  /** {@link #toStreamElement(Repository)} */
  private static class ToStreamElement<T>
      extends PTransform<PCollection<TypedElement<T>>, PCollection<StreamElement>> {

    private final RepositoryFactory repositoryFactory;

    ToStreamElement(Repository repository) {
      this.repositoryFactory = Objects.requireNonNull(repository).asFactory();
    }

    @Override
    public PCollection<StreamElement> expand(PCollection<TypedElement<T>> input) {
      return input.apply(
          ParDo.of(
              new DoFn<TypedElement<T>, StreamElement>() {

                @Override
                public TypeDescriptor<StreamElement> getOutputTypeDescriptor() {
                  return TypeDescriptor.of(StreamElement.class);
                }

                @ProcessElement
                public void processElement(ProcessContext ctx) {
                  final TypedElement<T> el = ctx.element();
                  switch (el.getOperation()) {
                    case DELETE:
                      ctx.output(
                          StreamElement.delete(
                              repositoryFactory
                                  .apply()
                                  .getEntity(el.getAttributeDescriptor().getEntity()),
                              el.getAttributeDescriptor(),
                              UUID.randomUUID().toString(),
                              el.getKey(),
                              el.getAttribute(),
                              ctx.timestamp().getMillis()));
                      break;
                    case DELETE_ALL:
                      ctx.output(
                          StreamElement.deleteWildcard(
                              repositoryFactory
                                  .apply()
                                  .getEntity(el.getAttributeDescriptor().getEntity()),
                              el.getAttributeDescriptor(),
                              UUID.randomUUID().toString(),
                              el.getKey(),
                              el.getAttribute(),
                              ctx.timestamp().getMillis()));
                      break;
                    case UPSERT:
                      ctx.output(
                          StreamElement.upsert(
                              repositoryFactory
                                  .apply()
                                  .getEntity(el.getAttributeDescriptor().getEntity()),
                              el.getAttributeDescriptor(),
                              UUID.randomUUID().toString(),
                              el.getKey(),
                              el.getAttribute(),
                              ctx.timestamp().getMillis(),
                              el.getPayload()));
                      break;
                    default:
                      throw new IllegalArgumentException(
                          String.format("Invalid operation [%s].", el.getOperation()));
                  }
                }
              }));
    }
  }

  /** {@link #values()} */
  public static class Values<T> extends PTransform<PCollection<TypedElement<T>>, PCollection<T>> {

    @Override
    public PCollection<T> expand(PCollection<TypedElement<T>> input) {
      final TypedElementCoder<T> inputCoder = (TypedElementCoder<T>) input.getCoder();
      return input.apply(
          FlatMapElements.into(inputCoder.getValueTypeDescriptor())
              .via(
                  el -> {
                    if (el.isUpsert()) {
                      return Collections.singletonList(el.getValue());
                    }
                    return Collections.emptyList();
                  }));
    }
  }

  /**
   * Create {@link PCollection} of {@link TypedElement typed elements} from {@link PCollection} of
   * {@link StreamElement untyped elements}.
   *
   * @param attributeDescriptor Attribute descriptor of stream elements in the input PCollection.
   * @param <T> Type of element value.
   * @return Transformation.
   */
  public static <T> FromStreamElement<T> of(AttributeDescriptor<T> attributeDescriptor) {
    return new FromStreamElement<>(attributeDescriptor);
  }

  /**
   * Create {@link PCollection} of {@link StreamElement untyped elements} from {@link PCollection}
   * of {@link TypedElement typed elements}.
   *
   * @param repository Repository that the input elements belong to.
   * @param <T> Type of element value.
   * @return Transformation.
   */
  public static <T> ToStreamElement<T> toStreamElement(Repository repository) {
    return new ToStreamElement<>(repository);
  }

  /**
   * Extract values from {@link PCollection} of {@link TypedElement typed elements}.
   *
   * @param <T> Type of element value.
   * @return Transformation.
   */
  public static <T> Values<T> values() {
    return new Values<>();
  }
}
