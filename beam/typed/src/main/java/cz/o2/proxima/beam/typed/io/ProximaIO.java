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
package cz.o2.proxima.beam.typed.io;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.beam.typed.TypedElement;
import cz.o2.proxima.beam.typed.TypedElementCoder;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import java.util.Objects;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

/** IO connector for Proxima platform. */
@Slf4j
public class ProximaIO {

  /**
   * Transformation that writes {@link StreamElement stream elements} into proxima using {@link
   * DirectDataOperator}.
   */
  public static class Write extends PTransform<PCollection<StreamElement>, PDone> {

    private final RepositoryFactory repositoryFactory;

    private Write(RepositoryFactory repositoryFactory) {
      this.repositoryFactory = repositoryFactory;
    }

    @Override
    public PDone expand(PCollection<StreamElement> input) {
      input.apply("Write", ParDo.of(new WriteFn(repositoryFactory)));
      return PDone.in(input.getPipeline());
    }

    public TypedWrite into(AttributeDescriptor<TypedElement<?>> attribute) {
      return new TypedWrite(repositoryFactory, attribute);
    }
  }

  public static class TypedWrite
      extends PTransform<PCollection<KV<String, TypedElement<?>>>, PDone> {

    private final RepositoryFactory repositoryFactory;
    private final AttributeDescriptor<TypedElement<?>> attributeDescriptor;

    TypedWrite(
        RepositoryFactory repositoryFactory,
        AttributeDescriptor<TypedElement<?>> attributeDescriptor) {
      this.repositoryFactory = repositoryFactory;
      this.attributeDescriptor = attributeDescriptor;
    }

    @Override
    public PDone expand(PCollection<KV<String, TypedElement<?>>> input) {
      input
          .apply(
              "NormalizeTimestamp",
              ParDo.of(new NormalizeTimestampFn(TypedElementCoder.of(attributeDescriptor))))
          .apply(
              "ToStreamElement",
              ParDo.of(new ToStreamElementFn(repositoryFactory, attributeDescriptor)))
          .apply("Write", ParDo.of(new WriteFn(repositoryFactory)));
      return PDone.in(input.getPipeline());
    }
  }

  @VisibleForTesting
  public static class NormalizeTimestampFn
      extends DoFn<KV<String, TypedElement<?>>, KV<String, TypedElement<?>>> {

    @VisibleForTesting
    @SuppressWarnings({"unchecked", "rawtypes"})
    public NormalizeTimestampFn(TypedElementCoder<?> typedElementAttributeDescriptor) {
      this.elementsBagState =
          StateSpecs.bag(
              KvCoder.of(
                  StringUtf8Coder.of(), (TypedElementCoder) typedElementAttributeDescriptor));
    }

    @StateId("elementsBagState")
    private final StateSpec<BagState<KV<String, TypedElement<?>>>> elementsBagState;

    @TimerId("bagStateElementTimer")
    private final TimerSpec bagStateElementTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(
        @Element KV<String, TypedElement<?>> element,
        @StateId("elementsBagState") BagState<KV<String, TypedElement<?>>> elementsBagState,
        @TimerId("bagStateElementTimer") Timer bagStateElementTimer) {

      if (elementsBagState.isEmpty().read()) {
        bagStateElementTimer.offset(Duration.millis(1)).setRelative();
      }
      elementsBagState.add(element);
    }

    @OnTimer("bagStateElementTimer")
    public void onTimer(
        OnTimerContext ctx,
        @StateId("elementsBagState") BagState<KV<String, TypedElement<?>>> elementsBagState) {
      Iterable<KV<String, TypedElement<?>>> currentElements = elementsBagState.read();
      for (KV<String, TypedElement<?>> el : currentElements) {
        ctx.outputWithTimestamp(el, ctx.fireTimestamp());
      }
      elementsBagState.clear();
    }
  }

  private static class WriteFn extends DoFn<StreamElement, Void> {

    private final RepositoryFactory repositoryFactory;

    private transient DirectDataOperator direct;

    private WriteFn(RepositoryFactory repositoryFactory) {
      this.repositoryFactory = repositoryFactory;
    }

    @Setup
    public void setUp() {
      direct = repositoryFactory.apply().getOrCreateOperator(DirectDataOperator.class);
    }

    @ProcessElement
    public void processElement(@Element StreamElement element) {
      direct
          .getWriter(element.getAttributeDescriptor())
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      String.format("Missing writer for [%s].", element.getAttributeDescriptor())))
          .write(
              element,
              (succ, error) -> {
                if (error != null) {
                  log.error(String.format("Unable to write element [%s].", element), error);
                }
              });
    }

    @Teardown
    public void tearDown() {
      if (direct != null) {
        direct.close();
      }
    }
  }

  private static class ToStreamElementFn extends DoFn<KV<String, TypedElement<?>>, StreamElement> {

    private final RepositoryFactory repositoryFactory;
    private final AttributeDescriptor<TypedElement<?>> attributeDescriptor;

    private transient EntityDescriptor entityDescriptor;

    private ToStreamElementFn(
        RepositoryFactory repositoryFactory,
        AttributeDescriptor<TypedElement<?>> attributeDescriptor) {
      this.repositoryFactory = repositoryFactory;
      this.attributeDescriptor = attributeDescriptor;
    }

    @Setup
    public void setUp() {
      entityDescriptor = repositoryFactory.apply().getEntity(attributeDescriptor.getName());
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      final KV<String, TypedElement<?>> element = ctx.element();
      Metrics.counter(
              "proxima-write", entityDescriptor.getName() + "." + attributeDescriptor.getName())
          .inc();
      ctx.output(
          StreamElement.upsert(
              entityDescriptor,
              attributeDescriptor,
              UUID.randomUUID().toString(),
              Objects.requireNonNull(element.getKey()),
              attributeDescriptor.getName(),
              ctx.timestamp().getMillis(),
              attributeDescriptor.getValueSerializer().serialize(element.getValue())));
    }
  }

  /**
   * Write {@link StreamElement stream elements} into proxima using {@link DirectDataOperator}.
   *
   * @param repositoryFactory Serializable factory for Proxima repository.
   * @return Write transform.
   */
  public static Write write(RepositoryFactory repositoryFactory) {
    return new Write(repositoryFactory);
  }
}
