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
package cz.o2.proxima.direct.time;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkEstimator;
import java.util.HashMap;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class PartitionedWatermarkEstimatorTest {

  private WatermarkEstimator estimator1;
  private WatermarkEstimator estimator2;
  private PartitionedWatermarkEstimator partitionedWatermarkEstimator;
  private ConfigRepository repo;

  @Before
  public void setup() {
    repo =
        ConfigRepository.Builder.of(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();

    estimator1 = mock(WatermarkEstimator.class);
    estimator2 = mock(WatermarkEstimator.class);
    partitionedWatermarkEstimator =
        new PartitionedWatermarkEstimator(
            new HashMap<Integer, WatermarkEstimator>() {
              {
                put(1, estimator1);
                put(2, estimator2);
              }
            });
  }

  @Test
  public void testUpdate() {
    StreamElement element1 = element();
    StreamElement element2 = element();
    partitionedWatermarkEstimator.update(1, element1);
    partitionedWatermarkEstimator.update(2, element2);

    verify(estimator1, times(1)).update(element1);
    verify(estimator1, never()).update(element2);
    verify(estimator2, times(1)).update(element2);
    verify(estimator2, never()).update(element1);
  }

  @Test
  public void testIdle() {
    partitionedWatermarkEstimator.idle(1);
    partitionedWatermarkEstimator.idle(2);

    verify(estimator1, times(1)).idle();
    verify(estimator2, times(1)).idle();
  }

  @Test
  public void testGetWatermark() {
    when(estimator1.getWatermark()).thenReturn(1000L);
    when(estimator2.getWatermark()).thenReturn(1100L);

    assertEquals(1000L, partitionedWatermarkEstimator.getWatermark());
  }

  private StreamElement element() {
    EntityDescriptor dummy = repo.getEntity("dummy");
    AttributeDescriptor<Object> data = dummy.getAttribute("data", true);
    return StreamElement.upsert(
        dummy, data, UUID.randomUUID().toString(), "key", "attr", System.currentTimeMillis(), null);
  }
}
