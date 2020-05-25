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

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkSupplier;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Watermark estimator wrapper for partitioned sources. Estimates watermark as a minimum across all
 * partition's watermarks. Uses processing time when none partitions are assigned. Update and idle
 * calls are delegated to estimator for a given partition.
 */
public class PartitionedWatermarkEstimator implements WatermarkSupplier {
  private static final long serialVersionUID = 1L;
  private final ConcurrentHashMap<Integer, WatermarkEstimator> estimators;

  public PartitionedWatermarkEstimator(Map<Integer, WatermarkEstimator> partitionsEstimators) {
    estimators = new ConcurrentHashMap<>(partitionsEstimators);
  }

  public static PartitionedWatermarkEstimator empty() {
    return new PartitionedWatermarkEstimator(Collections.emptyMap());
  }

  @Override
  public long getWatermark() {
    return estimators
        .values()
        .stream()
        .map(WatermarkEstimator::getWatermark)
        .min(Long::compare)
        .orElse(System.currentTimeMillis());
  }

  public void update(int partition, StreamElement element) {
    estimators.get(partition).update(element);
  }

  public void idle(int partition) {
    estimators.get(partition).idle();
  }
}
