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

import static org.joda.time.Instant.now;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.typed.TypedElement;
import cz.o2.proxima.beam.typed.TypedElementCoder;
import cz.o2.proxima.repository.Repository;
import cz.seznam.profile.model.ProfileModel;
import cz.seznam.profile.model.proto.features.Geohash;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;

public class ProximaIOTest {

  private final ProfileModel model =
      ProfileModel.ofTest(ConfigFactory.load().resolve(), Repository.Validate.NONE);

  @Test
  public void testTimestampNormalization() {
    Pipeline p = Pipeline.create();

    Instant now = now();
    TestStream<KV<String, TypedElement<Geohash>>> createEvents =
        TestStream.create(
                KvCoder.of(
                    StringUtf8Coder.of(),
                    TypedElementCoder.of(model.getUser().getFeatureGeohashDescriptor())))
            .advanceWatermarkTo(now)
            .addElements(newGeohash("user", "b", 101, 100L))
            .addElements(newGeohash("user", "b", 102, 102L))
            .addElements(newGeohash("user", "a", 103, 103L)) // 9223371950454775
            .addElements(newGeohash("user", "a", 103, 9223371950454775L))
            .advanceWatermarkTo(now.plus(1000000L))
            .addElements(newGeohash("user", "b", 150, now.getMillis()))
            .addElements(newGeohash("user", "c", 150, 101L))
            .advanceWatermarkToInfinity();

    @SuppressWarnings({"unchecked", "rawtypes"})
    TestStream<KV<String, TypedElement<?>>> createEventsCast = (TestStream) createEvents;

    PCollection<KV<String, TypedElement<?>>> normalizedElements =
        p.apply(createEventsCast)
            .apply(
                "NormalizeTimestamp",
                ParDo.of(
                    new ProximaIO.NormalizeTimestampFn(
                        TypedElementCoder.of(model.getUser().getFeatureGeohashDescriptor()))));

    PAssert.that(
            normalizedElements
                .apply("Reify", Reify.timestamps())
                .apply(
                    MapElements.into(
                            TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                        .via(
                            it -> {
                              KV<String, TypedElement<?>> el = it.getValue();
                              TypedElement<Geohash> te = (TypedElement) el.getValue();
                              return KV.of(
                                  it.getTimestamp().getMillis(),
                                  String.format(
                                      "user: %s, value: %s, geo: %s",
                                      el.getKey(), te.getAttribute(), te.findValue().get()));
                            })))
        .satisfies(
            it -> {
              it.forEach(
                  item -> {
                    long afterTime = now().plus(Duration.standardDays(1)).getMillis();
                    if (item.getKey() > afterTime) {
                      throw new AssertionError(
                          "Broken timestamp normalization! Elements have stamps in the future!");
                    }
                  });
              return null;
            });
    p.run();
  }

  /**
   * Create Geohash element.
   *
   * @param userId - rus id
   * @param geohash - geohash string
   * @param visitedCount - visited count for given geohash
   * @return Geohash typed element.
   */
  private TimestampedValue<KV<String, TypedElement<Geohash>>> newGeohash(
      String userId, String geohash, int visitedCount, long stamp) {

    TypedElement<Geohash> el =
        TypedElement.upsertWildcard(
            model.getUser().getFeatureGeohashDescriptor(),
            userId,
            geohash,
            model
                .getUser()
                .getFeatureGeohashDescriptor()
                .getValueSerializer()
                .getDefault()
                .toBuilder()
                .setVisitedCount(visitedCount)
                .build());

    return TimestampedValue.of(KV.of(userId, el), Instant.ofEpochMilli(stamp));
  }
}
