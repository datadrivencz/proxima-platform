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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class PipelineMaterializerTesterTest {

  private static final String TEST_CONFIG =
      "\n"
          + "entities {\n"
          + "  foo {\n"
          + "    attributes {\n"
          + "      article: { scheme: string }\n"
          + "      count: { scheme: long}\n"
          + "    }\n"
          + "  }\n"
          + "}\n"
          + "attributeFamilies {\n"
          + "  foo-commit-log {\n"
          + "    type: primary\n"
          + "    storage: \"inmem:///foo-commit-log\"\n"
          + "    entity: foo\n"
          + "    attributes: [\"*\"]\n"
          + "    access: commit-log\n"
          + "  }\n"
          + "}";

  private static class WordCountMaterializer implements SingleOutputPipelineMaterializer<Long> {

    private final AttributeDescriptor<String> article;
    private final AttributeDescriptor<Long> count;

    WordCountMaterializer(Repository repository) {
      this.article = repository.getEntity("foo").getAttribute("article");
      this.count = repository.getEntity("foo").getAttribute("count");
    }

    @Override
    public void registerInputs(RegisterInputsContext ctx) {
      ctx.registerInput(article);
    }

    @Override
    public void registerOutputs(RegisterOutputsContext ctx) {
      ctx.registerOutput(count);
    }

    @Override
    public PCollection<TypedElement<Long>> materialize(MaterializeContext ctx) {
      return ctx.getInput(article)
          .apply(
              FlatMapElements.into(TypeDescriptors.strings())
                  .via(el -> Arrays.asList(el.getValue().split(" "))))
          .apply(WithKeys.<String, String>of(x -> x).withKeyType(TypeDescriptors.strings()))
          .apply(
              Window.<KV<String, String>>into(new GlobalWindows())
                  .triggering(AfterWatermark.pastEndOfWindow())
                  .discardingFiredPanes()
                  .withTimestampCombiner(TimestampCombiner.LATEST)
                  .withAllowedLateness(Duration.ZERO)
                  .withOnTimeBehavior(OnTimeBehavior.FIRE_IF_NON_EMPTY))
          .apply(Count.perKey())
          .apply(
              MapElements.into(TypedElement.typeDescriptor(count))
                  .via(kv -> TypedElement.upsert(count, kv.getKey(), kv.getValue())));
    }
  }

  @Test
  public void test() {
    final Repository repository = ConfigRepository.of(ConfigFactory.parseString(TEST_CONFIG));
    final AttributeDescriptor<String> article = repository.getEntity("foo").getAttribute("article");
    final Instant now = Instant.now();
    PipelineMaterializerTester.of(repository, new WordCountMaterializer(repository))
        .withInput(
            article,
            testStream ->
                testStream
                    .addElements(
                        TimestampedValue.of(
                            TypedElement.upsert(article, "lukas", "second third fourth"), now),
                        TimestampedValue.of(
                            TypedElement.upsert(article, "david", "first second third"),
                            now.plus(Duration.standardSeconds(1))))
                    .advanceWatermarkToInfinity())
        .withOutputAssert(
            result ->
                PAssert.that(result.apply(TypedElementFormatter.of(Object::toString, now)))
                    .containsInAnyOrder(
                        "first@foo.count [1] at PT1S",
                        "second@foo.count [2] at PT1S",
                        "third@foo.count [2] at PT1S",
                        "fourth@foo.count [1] at PT0S"))
        .run();
  }
}
