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

/**
 * {@link InputMode} describes how we want to read data into Beam {@link
 * org.apache.beam.sdk.values.PCollection}.
 */
public enum InputMode {

  /**
   * Read input from commit-log in a streaming fashion from a timestamp <b>t</b>, replaying the full
   * history until <b>t</b> from historical data stored in batch storage.
   */
  BOOTSTRAP,

  /**
   * Read input from commit-log in a streaming fashion starting from the current offset (committed
   * offset with fallback to "latest").
   */
  STREAM_FROM_CURRENT,

  /**
   * Read input from commit-log in a streaming fashion starting from the oldest ("earliest") offset.
   */
  STREAM_FROM_OLDEST,

  /**
   * Read input from commit-log in a streaming fashion starting from the oldest ("earliest") offset,
   * stopping at current timestamp.
   */
  STREAM_FROM_OLDEST_BATCH,
}
