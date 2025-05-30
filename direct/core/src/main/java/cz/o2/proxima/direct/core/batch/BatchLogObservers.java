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
package cz.o2.proxima.direct.core.batch;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.time.WatermarkSupplier;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext;
import javax.annotation.Nullable;
import lombok.Value;

/** Utility class related to {@link BatchLogObserver BatchLogObservers}. */
@Internal
public class BatchLogObservers {

  @Value
  private static class SimpleOnNextContext implements OnNextContext {

    private static final long serialVersionUID = 1L;

    Partition partition;
    @Nullable Offset offset;
    long watermark;

    public Offset getOffset() {
      if (offset == null) {
        throw new UnsupportedOperationException(
            "Unable to calculate offset, because the underlying data store is not known to be immutable.");
      }
      return offset;
    }
  }

  /**
   * Create {@link OnNextContext} which holds watermark back on {@link Watermarks#MIN_WATERMARK}
   * until the end of data. This is the default behavior of batch readers when there is no way to
   * time-order data.
   *
   * @param partition the partition to create context for
   * @return a wrapped {@link OnNextContext} for given partition
   */
  public static OnNextContext defaultContext(Partition partition) {
    return new SimpleOnNextContext(partition, null, Watermarks.MIN_WATERMARK);
  }

  /**
   * Create {@link OnNextContext} which moves watermark according to given {@link
   * WatermarkSupplier}.
   *
   * @param partition the partition to create context for
   * @param offset offset of a current element
   * @param watermark {@link WatermarkSupplier} for watermark at any given time. The supplier can
   *     assume that each element gets consumed immediately after being passed to {@link
   *     BatchLogObserver#onNext}
   * @return a wrapped {@link OnNextContext} for given partition and given watermark supplier
   */
  public static OnNextContext withWatermarkSupplier(
      Partition partition, Offset offset, WatermarkSupplier watermark) {
    return new SimpleOnNextContext(partition, offset, watermark.getWatermark());
  }

  /**
   * Create {@link OnNextContext} which sets watermark ti given epoch millis.
   *
   * @param partition the partition to create context for
   * @param offset offset of a current element
   * @param watermark epoch millis to set the watermark to
   * @return a wrapped {@link OnNextContext} for given partition with given watermark
   */
  public static OnNextContext withWatermark(Partition partition, Offset offset, long watermark) {
    return new SimpleOnNextContext(partition, offset, watermark);
  }

  private BatchLogObservers() {}
}
