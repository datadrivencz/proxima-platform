/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;

@Slf4j
class Utils {

  static StreamElement update(
      EntityDescriptor entity, AttributeDescriptor<?> attr) {
    return StreamElement.update(
        entity, attr, "uuid", "key", attr.getName(),
        System.currentTimeMillis(), new byte[] {1, 2, 3});
  }

  static CompletableFuture<Throwable> startPipeline(Pipeline pipeline) {
    CompletableFuture<Throwable> result = new CompletableFuture<>();
    new Thread(() -> {
      try {
        pipeline.run().waitUntilFinish();
        result.complete(null);
      } catch (Throwable e) {
        log.error("Pipeline failed with exception", e);
        result.complete(e);
      }
    }).start();
    return result;
  }



}
