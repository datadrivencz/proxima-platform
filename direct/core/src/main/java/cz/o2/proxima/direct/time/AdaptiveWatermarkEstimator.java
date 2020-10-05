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

import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;

import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.tdunning.math.stats.TDigest;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.AbstractWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

public class AdaptiveWatermarkEstimator extends AbstractWatermarkEstimator {

  public static final String MAX_OUT_OF_ORDERNESS_MS = "max-out-of-orderness";

  /** One hour. */
  public static final long DEFAULT_MAX_OUT_OF_ORDERNESS_MS = 3_600_000;

  private static TDigest newHistogram() {
    return TDigest.createMergingDigest(100.0);
  }

  public static class Factory implements WatermarkEstimatorFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public WatermarkEstimator create(
        Map<String, Object> cfg, WatermarkIdlePolicyFactory idlePolicyFactory) {
      final long maxOutOfOrderness =
          Optional.ofNullable(cfg.get(prefixedKey(MAX_OUT_OF_ORDERNESS_MS)))
              .map(v -> Long.valueOf(v.toString()))
              .orElse(DEFAULT_MAX_OUT_OF_ORDERNESS_MS);

      return new AdaptiveWatermarkEstimator(
          idlePolicyFactory.create(cfg), maxOutOfOrderness, 10, 100);
    }
  }

  private final long maxOutOfOrderness;
  private final EvictingQueue<TDigest> histograms;
  private final long rollInterval;

  private long minWatermark = Watermarks.MIN_WATERMARK;
  private long maxWatermark = Watermarks.MIN_WATERMARK;

  public AdaptiveWatermarkEstimator(
      WatermarkIdlePolicy idlePolicy,
      long maxOutOfOrderness,
      int numHistograms,
      long rollInterval) {
    super(idlePolicy);
    this.maxOutOfOrderness = maxOutOfOrderness;
    this.histograms = EvictingQueue.create(numHistograms);
    this.rollInterval = rollInterval;
  }

  @Override
  protected long estimateWatermark() {
    // We have seen full window so we try to adjust estimation.
    long currentLag = maxOutOfOrderness;
    if (histograms.size() >= 5) {
      final TDigest mergedDigest = TDigest.createMergingDigest(100.0);
      mergedDigest.add(new ArrayList<>(histograms));
      // First super naive approach...
      final long maxLateness = Math.min(0L, (long) Math.ceil(mergedDigest.getMax()));
      currentLag = Math.min(currentLag, maxLateness);
    }
    return Math.max(minWatermark, maxWatermark - currentLag);
  }

  @Override
  protected void updateWatermark(StreamElement element) {
    maxWatermark = Math.max(minWatermark, element.getStamp());
    // In ideal case all of the histogram updates would be zero.

    // Fraction of elements lower or equal to 0.
    TDigest currentHistogram = histograms.peek();
    if (currentHistogram == null) {
      currentHistogram = newHistogram();
      Preconditions.checkState(histograms.add(newHistogram()));
    }
    currentHistogram.add(maxWatermark - element.getStamp());

    // Rotate buffer if necessary.
    if (histograms.size() >= rollInterval) {
      Preconditions.checkState(histograms.add(newHistogram()));
    }
  }

  @Override
  public void setMinWatermark(long minWatermark) {
    this.minWatermark = minWatermark;
  }
}
