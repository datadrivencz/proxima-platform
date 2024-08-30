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

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class ExternalStateExpanderTest {

  @Test
  public void testSimpleExpand() {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> inputs = pipeline.apply(Create.of("1", "2", "3"));
    PCollection<KV<Integer, String>> withKeys =
        inputs.apply(
            WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                .withKeyType(TypeDescriptors.integers()));
    PCollection<Long> count = withKeys.apply(ParDo.of(getSumFn()));
    PAssert.that(count).containsInAnyOrder(2L, 4L);
    ExternalStateExpander.expand(
        pipeline, Create.empty(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())), dummy());
    pipeline.run();
  }

  @Test
  public void testCompositeExpand() {
    PTransform<PCollection<String>, PCollection<Long>> transform =
        new PTransform<>() {
          @Override
          public PCollection<Long> expand(PCollection<String> input) {
            PCollection<KV<Integer, String>> withKeys =
                input.apply(
                    WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                        .withKeyType(TypeDescriptors.integers()));
            return withKeys.apply(ParDo.of(getSumFn()));
          }
        };
    Pipeline pipeline = Pipeline.create();
    PCollection<String> inputs = pipeline.apply(Create.of("1", "2", "3"));
    PCollection<Long> count = inputs.apply(transform);
    PAssert.that(count).containsInAnyOrder(2L, 4L);
    ExternalStateExpander.expand(
        pipeline, Create.empty(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())), dummy());
    pipeline.run();
  }

  @Test
  public void testSimpleExpandWithInitialState() throws CoderException {
    Pipeline pipeline = Pipeline.create();
    PCollection<String> inputs = pipeline.apply(Create.of("3", "4"));
    PCollection<KV<Integer, String>> withKeys =
        inputs.apply(
            WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                .withKeyType(TypeDescriptors.integers()));
    PCollection<Long> count = withKeys.apply("sum", ParDo.of(getSumFn()));
    PAssert.that(count).containsInAnyOrder(6L, 4L);
    VarIntCoder intCoder = VarIntCoder.of();
    VarLongCoder longCoder = VarLongCoder.of();
    ExternalStateExpander.expand(
        pipeline,
        Create.of(
                KV.of(
                    "sum",
                    new StateValue(
                        CoderUtils.encodeToByteArray(intCoder, 0),
                        "sum",
                        CoderUtils.encodeToByteArray(longCoder, 2L))),
                KV.of(
                    "sum",
                    new StateValue(
                        CoderUtils.encodeToByteArray(intCoder, 1),
                        "sum",
                        CoderUtils.encodeToByteArray(longCoder, 1L))))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())),
        dummy());
    pipeline.run();
  }

  private static DoFn<KV<Integer, String>, Long> getSumFn() {
    return new DoFn<KV<Integer, String>, Long>() {
      @StateId("sum")
      private final StateSpec<ValueState<Long>> spec = StateSpecs.value();

      @ProcessElement
      public void process(
          @Element KV<Integer, String> element, @StateId("sum") ValueState<Long> sum) {

        long current = MoreObjects.firstNonNull(sum.read(), 0L);
        sum.write(current + Integer.parseInt(element.getValue()));
      }

      @OnWindowExpiration
      public void onExpiration(@StateId("sum") ValueState<Long> sum, OutputReceiver<Long> output) {
        output.output(sum.read());
      }
    };
  }

  private PTransform<PCollection<KV<String, StateValue>>, PDone> dummy() {
    return new PTransform<>() {
      @Override
      public PDone expand(PCollection<KV<String, StateValue>> input) {
        return PDone.in(input.getPipeline());
      }
    };
  }
}
