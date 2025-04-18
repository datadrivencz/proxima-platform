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
package cz.o2.proxima.direct.io.kafka;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;

/**
 * A parser that transforms KV from Kafka (byte[], byte[]) into {@link StreamElement}.
 *
 * @param <K> type of key read from Kafka
 * @param <V> type of value read from Kafka
 */
public interface ElementSerializer<K, V> extends Serializable {

  /**
   * Life-cycle method called after being instantiated to be able to parameterize itself from the
   * given {@link EntityDescriptor}.
   *
   * @param entityDescriptor the entity that this serializer will be used for
   */
  default void setup(EntityDescriptor entityDescriptor) {}

  /**
   * Parse the raw bytes from Kafka and return a {@link StreamElement}.
   *
   * @param record a {@link ConsumerRecord} to be parsed
   * @param entityDesc descriptor of entity being read
   * @return the {@link StreamElement} or null on parse error
   */
  @Nullable
  StreamElement read(ConsumerRecord<K, V> record, EntityDescriptor entityDesc);

  /**
   * Convert {@link StreamElement} into {@link ProducerRecord}.
   *
   * @param topic the target topic
   * @param partition the target partition
   * @param element the {@link StreamElement} to convert
   * @return the {@link ProducerRecord} to write to Kafka
   */
  ProducerRecord<K, V> write(String topic, int partition, StreamElement element);

  /**
   * Retrieve {@link Serde} for type K.
   *
   * @return {@link Serde} for key
   */
  Serde<K> keySerde();

  /**
   * Retrieve {@link Serde} for type V.
   *
   * @return {@link Serde} for value
   */
  Serde<V> valueSerde();

  /**
   * @return {@code true} if this serializer reads and writes sequential IDs of {@link
   *     StreamElement} (if any).
   */
  default boolean storesSequentialId() {
    return false;
  }
}
