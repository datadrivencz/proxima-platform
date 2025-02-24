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

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.ByteUtils;

/** Data read from a kafka partition. */
@Slf4j
public class KafkaStreamElement extends StreamElement {

  private static final long serialVersionUID = 1L;

  public static class KafkaStreamElementSerializer implements ElementSerializer<String, byte[]> {

    private static final long serialVersionUID = 1L;

    @Nullable
    @Override
    public StreamElement read(ConsumerRecord<String, byte[]> record, EntityDescriptor entityDesc) {
      String key = record.key();
      byte[] value = record.value();
      // in kafka, each entity attribute is separated by `#' from entity key
      int hashPos = key.lastIndexOf('#');
      if (hashPos < 0 || hashPos >= key.length()) {
        log.error("Invalid key in kafka topic: {}", key);
      } else {
        String entityKey = key.substring(0, hashPos);
        String attribute = key.substring(hashPos + 1);
        Optional<AttributeDescriptor<Object>> attr =
            entityDesc.findAttribute(attribute, true /* allow reading protected */);
        if (!attr.isPresent()) {
          log.error(
              "Invalid attribute {} in kafka key {} for entity {}", attribute, key, entityDesc);
        } else {
          @Nullable
          final Header sequentialIdHeader =
              record.headers().lastHeader(KafkaAccessor.SEQUENCE_ID_HEADER);
          final String uuid =
              Optional.ofNullable(record.headers().lastHeader(KafkaAccessor.UUID_HEADER))
                  .map(v -> new String(v.value(), StandardCharsets.UTF_8))
                  .filter(s -> !s.isEmpty())
                  .orElse(record.topic() + "#" + record.partition() + "#" + record.offset());
          if (sequentialIdHeader != null) {
            try {
              long seqId = asLong(sequentialIdHeader.value());
              return new KafkaStreamElement(
                  entityDesc,
                  attr.get(),
                  seqId,
                  entityKey,
                  attribute,
                  record.timestamp(),
                  value,
                  record.partition(),
                  record.offset());
            } catch (IOException ex) {
              log.warn("Failed to deserialize sequentialId from {}", sequentialIdHeader, ex);
            }
          }
          return new KafkaStreamElement(
              entityDesc,
              attr.get(),
              uuid,
              entityKey,
              attribute,
              record.timestamp(),
              value,
              record.partition(),
              record.offset());
        }
      }
      return null;
    }

    @Override
    public ProducerRecord<String, byte[]> write(String topic, int partition, StreamElement data) {
      List<Header> headers = new ArrayList<>();
      if (data.hasSequentialId()) {
        headers.add(
            new RecordHeader(KafkaAccessor.SEQUENCE_ID_HEADER, asBytes(data.getSequentialId())));
      } else {
        headers.add(
            new RecordHeader(
                KafkaAccessor.UUID_HEADER, data.getUuid().getBytes(StandardCharsets.UTF_8)));
      }
      return new ProducerRecord<>(
          topic,
          partition >= 0 ? partition : null,
          data.getStamp(),
          data.getKey() + "#" + data.getAttribute(),
          data.getValue(),
          headers);
    }

    @Override
    public Serde<String> keySerde() {
      return Serdes.String();
    }

    @Override
    public Serde<byte[]> valueSerde() {
      return Serdes.ByteArray();
    }

    @Override
    public boolean storesSequentialId() {
      return true;
    }

    byte[] asBytes(long sequentialId) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataOutputStream dout = new DataOutputStream(baos)) {
        ByteUtils.writeVarlong(sequentialId, dout);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      return baos.toByteArray();
    }

    long asLong(byte[] serializedSeqId) throws IOException {
      try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedSeqId);
          DataInputStream dis = new DataInputStream(bais)) {
        return ByteUtils.readVarlong(dis);
      }
    }
  }

  /** Partition in Kafka this element comes from. */
  @Getter private final int partition;

  /** Offset in the partition. */
  @Getter private final long offset;

  KafkaStreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      String uuid,
      String key,
      String attribute,
      long stamp,
      byte[] value,
      int partition,
      long offset) {

    super(
        entityDesc,
        attributeDesc,
        uuid,
        key,
        attribute,
        stamp,
        false /* not forced, is inferred from attribute descriptor name */,
        value);

    this.partition = partition;
    this.offset = offset;
  }

  KafkaStreamElement(
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> attributeDesc,
      long sequentialId,
      String key,
      String attribute,
      long stamp,
      byte[] value,
      int partition,
      long offset) {

    super(
        entityDesc,
        attributeDesc,
        sequentialId,
        key,
        attribute,
        stamp,
        false /* not forced, is inferred from attribute descriptor name */,
        value);

    this.partition = partition;
    this.offset = offset;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("entityDesc", getEntityDescriptor())
        .add("attributeDesc", getAttributeDescriptor())
        .add("uuid", getUuid())
        .add("key", getKey())
        .add("attribute", getAttribute())
        .add("stamp", getStamp())
        .add("value.length", getValue() == null ? -1 : getValue().length)
        .add("partition", partition)
        .add("offset", offset)
        .toString();
  }
}
