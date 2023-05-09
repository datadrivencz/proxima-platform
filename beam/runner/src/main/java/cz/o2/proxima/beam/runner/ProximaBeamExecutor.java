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
package cz.o2.proxima.beam.runner;

import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public class ProximaBeamExecutor {

  private final Pipeline pipeline;
  private final ProximaTranslationContext context;

  ProximaBeamExecutor(Pipeline pipeline, ProximaTranslationContext context) {
    this.pipeline = pipeline;
    this.context = context;
  }

  public PortablePipelineResult execute() {
    return asPipelineResult(context);
  }

  private PortablePipelineResult asPipelineResult(ProximaTranslationContext context) {
    return new PortablePipelineResult() {
      @Override
      public JobApi.MetricResults portableMetrics() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
      }

      @Override
      public @NonNull State getState() {
        return context.getState();
      }

      @Override
      public @NonNull State cancel() throws IOException {
        return context.cancel();
      }

      @Override
      public @NonNull State waitUntilFinish(@NonNull Duration duration) {
        return context.waitUntilFinish(duration);
      }

      @Override
      public @NonNull State waitUntilFinish() {
        return waitUntilFinish(Duration.ZERO);
      }

      @Override
      public @NonNull MetricResults metrics() {
        return context.getMetricsResult();
      }
    };
  }
}
