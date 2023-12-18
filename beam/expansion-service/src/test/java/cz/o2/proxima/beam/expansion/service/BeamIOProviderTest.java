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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cz.o2.proxima.beam.expansion.service.BeamIOProvider.Configuration;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.POutput;
import org.junit.Test;

public class BeamIOProviderTest {
  @Test
  public void testProvider() {
    ConfigProvider.set(ConfigFactory.load("test-reference.conf").resolve());
    ServiceLoader<ExternalTransformRegistrar> loader =
        ServiceLoader.load(ExternalTransformRegistrar.class);
    List<ExternalTransformRegistrar> providers =
        loader.stream()
            .map(Provider::get)
            .filter(r -> r.getClass().getPackage().getName().startsWith("cz.o2"))
            .collect(Collectors.toList());
    assertEquals(providers.toString(), 1, providers.size());
    BeamIOProvider provider = (BeamIOProvider) providers.get(0);
    assertNotNull(provider.getRepo());
    assertEquals(1, provider.knownBuilderInstances().size());
    @SuppressWarnings("unchecked")
    ExternalTransformBuilder<BeamIOProvider.Configuration, ?, ?> builder =
        (ExternalTransformBuilder<Configuration, ?, ?>)
            provider.knownBuilderInstances().get(BeamIOProvider.URM);
    assertNotNull(builder);
    Configuration conf = new Configuration();
    conf.setType("batch-snapshot");
    conf.setEntity("gateway");
    conf.setAttributes(Collections.singletonList("status"));
    @SuppressWarnings("unchecked")
    PTransform<PBegin, ?> transform = (PTransform<PBegin, ?>) builder.buildExternal(conf);
    assertNotNull(transform);
    POutput expanded = transform.expand(Pipeline.create().begin());
    assertNotNull(expanded);
  }
}
