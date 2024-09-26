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

import static org.junit.Assert.assertEquals;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import cz.o2.proxima.core.util.SerializableScopedValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ExternalStateExpanderTest {

  @Parameters
  public static List<Class<? extends PipelineRunner<?>>> params() {
    return Arrays.asList(DirectRunner.class, FlinkRunner.class);
  }

  @Parameter public Class<? extends PipelineRunner<?>> runner;

  @Test
  public void testSimpleExpand() {
    Pipeline pipeline = createPipeline();
    PCollection<String> inputs = pipeline.apply(Create.of("1", "2", "3"));
    PCollection<KV<Integer, String>> withKeys =
        inputs.apply(
            WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                .withKeyType(TypeDescriptors.integers()));
    PCollection<Long> count = withKeys.apply(ParDo.of(getSumFn()));
    PAssert.that(count).containsInAnyOrder(2L, 4L);
    Pipeline expanded =
        ExternalStateExpander.expand(
            pipeline,
            Create.empty(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())),
            new Instant(0),
            ign -> BoundedWindow.TIMESTAMP_MAX_VALUE,
            dummy());
    expanded.run();
  }

  @Test
  public void testSimpleExpandMultiOutput() {
    Pipeline pipeline = createPipeline();
    PCollection<String> inputs = pipeline.apply(Create.of("1", "2", "3"));
    PCollection<KV<Integer, String>> withKeys =
        inputs.apply(
            WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                .withKeyType(TypeDescriptors.integers()));
    TupleTag<Long> mainTag = new TupleTag<>();
    PCollection<Long> count =
        withKeys
            .apply(ParDo.of(getSumFn()).withOutputTags(mainTag, TupleTagList.empty()))
            .get(mainTag);
    PAssert.that(count).containsInAnyOrder(2L, 4L);
    Pipeline expanded =
        ExternalStateExpander.expand(
            pipeline,
            Create.empty(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())),
            new Instant(0),
            ign -> BoundedWindow.TIMESTAMP_MAX_VALUE,
            dummy());
    expanded.run();
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
    Pipeline pipeline = createPipeline();
    PCollection<String> inputs = pipeline.apply(Create.of("1", "2", "3"));
    PCollection<Long> count = inputs.apply(transform);
    PAssert.that(count).containsInAnyOrder(2L, 4L);
    Pipeline expanded =
        ExternalStateExpander.expand(
            pipeline,
            Create.empty(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())),
            new Instant(0),
            ign -> BoundedWindow.TIMESTAMP_MAX_VALUE,
            dummy());
    expanded.run();
  }

  @Test
  public void testSimpleExpandWithInitialState() throws CoderException {
    Pipeline pipeline = createPipeline();
    PCollection<String> inputs = pipeline.apply(Create.of("3", "4"));
    PCollection<KV<Integer, String>> withKeys =
        inputs.apply(
            WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                .withKeyType(TypeDescriptors.integers()));
    PCollection<Long> count = withKeys.apply("sum", ParDo.of(getSumFn()));
    PAssert.that(count).containsInAnyOrder(6L, 4L);
    VarIntCoder intCoder = VarIntCoder.of();
    VarLongCoder longCoder = VarLongCoder.of();
    Pipeline expanded =
        ExternalStateExpander.expand(
            pipeline,
            Create.of(
                    KV.of(
                        "sum/ParMultiDo(Anonymous)",
                        new StateValue(
                            CoderUtils.encodeToByteArray(intCoder, 0),
                            "sum",
                            CoderUtils.encodeToByteArray(longCoder, 2L))),
                    KV.of(
                        "sum/ParMultiDo(Anonymous)",
                        new StateValue(
                            CoderUtils.encodeToByteArray(intCoder, 1),
                            "sum",
                            CoderUtils.encodeToByteArray(longCoder, 1L))))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())),
            new Instant(0),
            current -> BoundedWindow.TIMESTAMP_MAX_VALUE,
            dummy());
    expanded.run();
  }

  @Test
  public void testSimpleExpandWithStateStore() {
    Pipeline pipeline = createPipeline();
    Instant now = new Instant(0);
    PCollection<String> inputs =
        pipeline.apply(
            Create.timestamped(TimestampedValue.of("1", now), TimestampedValue.of("2", now)));
    PCollection<KV<Integer, String>> withKeys =
        inputs.apply(
            WithKeys.<Integer, String>of(e -> Integer.parseInt(e) % 2)
                .withKeyType(TypeDescriptors.integers()));
    PCollection<Long> count = withKeys.apply("sum", ParDo.of(getSumFn()));
    PAssert.that(count).containsInAnyOrder(1L, 2L);
    Map<String, StateValue> states = new HashMap<>();
    Pipeline expanded =
        ExternalStateExpander.expand(
            pipeline,
            Create.empty(KvCoder.of(StringUtf8Coder.of(), StateValue.coder())),
            now,
            current -> current.equals(now) ? now.plus(1) : BoundedWindow.TIMESTAMP_MAX_VALUE,
            collectStates(states));
    expanded.run();
    assertEquals(1, states.size());
  }

  private static PTransform<PCollection<KV<String, StateValue>>, PDone> collectStates(
      Map<String, StateValue> states) {

    String id = UUID.randomUUID().toString();
    final SerializableScopedValue<String, Map<String, StateValue>> val =
        new SerializableScopedValue<>(id, states);
    return new PTransform<>() {
      @Override
      public PDone expand(PCollection<KV<String, StateValue>> input) {
        input.apply(
            MapElements.into(TypeDescriptors.voids())
                .via(
                    e -> {
                      Map<String, StateValue> m = val.get(id);
                      synchronized (m) {
                        m.put(e.getKey(), e.getValue());
                      }
                      return null;
                    }));
        return PDone.in(input.getPipeline());
      }
    };
  }

  private @NotNull Pipeline createPipeline() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(runner);
    return Pipeline.create(opts);
  }

  private static DoFn<KV<Integer, String>, Long> getSumFn() {
    return new DoFn<KV<Integer, String>, Long>() {
      @StateId("sum")
      private final StateSpec<ValueState<Long>> spec = StateSpecs.value();

      @ProcessElement
      public void process(
          OutputReceiver<Long> ignored,
          @Element KV<Integer, String> element,
          @StateId("sum") ValueState<Long> sum) {

        Preconditions.checkArgument(ignored instanceof OutputReceiver);
        long current = MoreObjects.firstNonNull(sum.read(), 0L);
        sum.write(current + Integer.parseInt(element.getValue()));
      }

      @OnWindowExpiration
      public void onExpiration(@StateId("sum") ValueState<Long> sum, OutputReceiver<Long> output) {
        Long value = sum.read();
        if (value != null) {
          output.output(value);
        }
      }
    };
  }

  private PTransform<PCollection<KV<String, StateValue>>, PDone> dummy() {
    return new PTransform<>() {
      @Override
      public PDone expand(PCollection<KV<String, StateValue>> input) {
        input.apply(MapElements.into(TypeDescriptors.voids()).via(a -> null));
        return PDone.in(input.getPipeline());
      }
    };
  }
}
