/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.flink.core;

import java.util.List;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ListTestSource<T> implements SourceFunction<T> {

  private final List<T> values;

  public ListTestSource(List<T> values) {
    this.values = values;
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    synchronized (ctx.getCheckpointLock()) {
      values.forEach(ctx::collect);
    }
  }

  @Override
  public void cancel() {}
}
