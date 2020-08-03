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

import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.POutput;

/** Pipeline options related to {@link Runner}. */
public interface RunnerPipelineOptions extends PipelineOptions {

  @Validation.Required
  @Description(
      "Whether to check for stable unique names on each transform. This is necessary to support updating of pipelines.")
  @Default.Enum("STREAM_FROM_CURRENT")
  InputMode getInputMode();

  void setInputMode(InputMode inputMode);

  /**
   * Get {@link PipelineMaterializer pipeline materializers} that {@link Runner} should use to
   * construct final pipeline.
   *
   * @return List of materializers.
   */
  @Validation.Required
  @Description("Pipeline materializers that runner should use to construct final pipeline.")
  List<Class<? extends PipelineMaterializer<? extends POutput>>> getMaterializers();

  void setMaterializers(
      List<Class<? extends PipelineMaterializer<? extends POutput>>> materializers);

  @Description("Allowed lateness for sort buffers.")
  @Default.Long(1000L)
  java.lang.Long getSortBufferAllowedLatenessMillis();

  void setSortBufferAllowedLatenessMillis(java.lang.Long sortBufferAllowedLatenessMillis);

  @Description("Allowed lateness for sort buffers in batch (high-latency) operations.")
  @Default.Long(3600000L)
  java.lang.Long getBatchSortBufferAllowedLatenessMillis();

  void setBatchSortBufferAllowedLatenessMillis(java.lang.Long sortBufferAllowedLatenessMillis);

  @Description(
      "Timestamp where we switch from batch to streaming source with bootstrap input mode.")
  java.lang.Long getBootstrapBarrierTimestamp();

  void setBootstrapBarrierTimestamp(java.lang.Long gdprBootstrapBarrierTimestamp);

  @Description(
      "Timestamp where we switch from batch to streaming source with bootstrap input mode for Gdpr command attribute.")
  java.lang.Long getGdprBootstrapBarrierTimestamp();

  void setGdprBootstrapBarrierTimestamp(java.lang.Long gdprBootstrapBarrierTimestamp);
}
