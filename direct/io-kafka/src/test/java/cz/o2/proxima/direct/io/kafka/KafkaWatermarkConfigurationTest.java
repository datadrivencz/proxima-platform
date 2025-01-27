/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.kafka;

import static org.junit.Assert.*;

import cz.o2.proxima.core.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.direct.core.time.BoundedOutOfOrdernessWatermarkEstimator;
import cz.o2.proxima.direct.core.time.SkewedProcessingTimeIdlePolicy;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class KafkaWatermarkConfigurationTest {

  @Test
  public void testConfigureDefault() {
    // Check backward compatibility with legacy behaviour
    Map<String, Object> cfg = ImmutableMap.of("timestamp-skew", 10L);
    KafkaWatermarkConfiguration configuration = new KafkaWatermarkConfiguration(cfg);
    WatermarkIdlePolicyFactory policyFactory = configuration.getWatermarkIdlePolicyFactory();
    configuration.getWatermarkEstimatorFactory().setup(cfg, policyFactory);
    BoundedOutOfOrdernessWatermarkEstimator estimator =
        (BoundedOutOfOrdernessWatermarkEstimator)
            configuration.getWatermarkEstimatorFactory().create();
    SkewedProcessingTimeIdlePolicy policy = (SkewedProcessingTimeIdlePolicy) policyFactory.create();

    assertNotNull(estimator);
    assertNotNull(policy);
    assertEquals(0L, estimator.getMaxOutOfOrderness());
    assertEquals(Watermarks.MIN_WATERMARK, estimator.getMinWatermark());
    assertEquals(10L, policy.getTimestampSkew());
  }
}
