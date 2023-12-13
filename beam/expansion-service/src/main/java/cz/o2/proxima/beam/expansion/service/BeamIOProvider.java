/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.expansion.service;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.auto.service.AutoService;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.time.Watermarks;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(ExternalTransformRegistrar.class)
public class BeamIOProvider implements ExternalTransformRegistrar {

  static final String URM = "beam:transform:cz.o2.proxima.beam:read:v1";

  public static class Configuration {
    @Setter @Getter String type;
    @Setter @Getter String entity;
    @Setter @Getter List<String> attributes;
    @Setter @Getter Long startStamp = Watermarks.MIN_WATERMARK;
    @Setter @Getter Long endStamp = Watermarks.MAX_WATERMARK;
  }

  @Getter(AccessLevel.PACKAGE)
  private final Repository repo = Repository.of(ConfigProvider.get());

  private final BeamDataOperator op = repo.getOrCreateOperator(BeamDataOperator.class);

  @Override
  public @NonNull
      Map<@NonNull String, @NonNull ExternalTransformBuilder<@NonNull ?, @NonNull ?, @NonNull ?>>
          knownBuilderInstances() {

    return ImmutableMap.<String, ExternalTransformBuilder<?, ?, ?>>builder()
        .put(URM, builder())
        .build();
  }

  private ExternalTransformBuilder<Configuration, PBegin, PCollection<StreamElement>> builder() {
    return new ExternalTransformBuilder<>() {
      @Override
      public @NonNull PTransform<PBegin, PCollection<StreamElement>> buildExternal(
          Configuration configuration) {
        return new PTransform<>() {
          @Override
          public @NonNull PCollection<StreamElement> expand(@NonNull PBegin input) {
            return op.getBatchSnapshot(input.getPipeline(), attributes(configuration));
          }
        };
      }
    };
  }

  private AttributeDescriptor<?>[] attributes(Configuration configuration) {
    EntityDescriptor entity = repo.getEntity(configuration.getEntity());
    return configuration.getAttributes().stream()
        .map(entity::getAttribute)
        .toArray(AttributeDescriptor<?>[]::new);
  }
}
