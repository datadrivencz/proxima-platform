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

import com.google.auto.service.AutoService;
import java.util.Collections;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.StreamingOptions;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface ProximaPipelineOptions extends PipelineOptions, StreamingOptions {

  @AutoService(PipelineOptionsRegistrar.class)
  class OptionsFactory implements PipelineOptionsRegistrar {

    @Override
    public @NonNull Iterable<@NonNull Class<@NonNull ? extends @NonNull PipelineOptions>>
        getPipelineOptions() {
      return Collections.singletonList(ProximaPipelineOptions.class);
    }
  }

  @Default.String("commit-log-family")
  String getCommitFamily();

  void setCommitFamily(String family);

  @Default.String("state-random-access-family")
  String getStateFamily();

  void setStateFamily(String family);

  @Default.String("shuffle-family")
  String getShuffleFamily();

  void setShuffleFamily(String family);
}
