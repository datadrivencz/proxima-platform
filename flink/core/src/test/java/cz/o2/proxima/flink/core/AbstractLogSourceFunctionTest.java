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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

abstract class AbstractLogSourceFunctionTest {
  static <T> StreamElement createUpsertElement(
      EntityDescriptor entity,
      AttributeDescriptor<T> attribute,
      String key,
      Instant timestamp,
      T value) {
    return StreamElement.upsert(
        entity,
        attribute,
        UUID.randomUUID().toString(),
        key,
        attribute.getName(),
        timestamp.toEpochMilli(),
        attribute.getValueSerializer().serialize(value));
  }

  @FunctionalInterface
  interface RunReadTestSubtask {
    void run(List<AttributeDescriptor<?>> attributes, int expectedElements) throws Exception;
  }
}
