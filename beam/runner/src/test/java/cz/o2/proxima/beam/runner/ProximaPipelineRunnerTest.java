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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.Struct;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

public class ProximaPipelineRunnerTest {

  @Test
  public void testPipelineSubmit() throws Exception {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(Impulse.create());
    execute(pipeline);
  }

  private void execute(Pipeline pipeline) throws Exception {
    RunnerApi.Pipeline protoPipeline = PipelineTranslation.toProto(pipeline);
    ProximaRunner runner = new ProximaRunner(pipeline.getOptions());
    PortablePipelineResult res = runner.run(protoPipeline, asJobInfo(pipeline));
    res.waitUntilFinish();
  }

  private JobInfo asJobInfo(Pipeline pipeline) {
    return new JobInfo() {
      @Override
      public @NonNull String jobId() {
        return "fakeId";
      }

      @Override
      public @NonNull String jobName() {
        return "fakeName";
      }

      @Override
      public @NonNull String retrievalToken() {
        return "fakeToken";
      }

      @Override
      public @NonNull Struct pipelineOptions() {
        return Struct.newBuilder().build();
      }
    };
  }
}
