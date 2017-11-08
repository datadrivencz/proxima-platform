/**
 * Copyright 2017 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.storage.randomaccess;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader.Offset;
import java.util.Optional;
import lombok.Getter;

/**
 * {@code KeyValue} with {@code Offset}.
 */
public class KeyValue<T> {

  @SuppressWarnings("unchecked")
  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      Offset offset,
      byte[] valueBytes) {

    Optional<T> value = attrDesc.getValueSerializer().deserialize(valueBytes);

    if (!value.isPresent()) {
      throw new IllegalArgumentException("Cannot parse given bytes to value");
    }

    return new KeyValue<>(
        entityDesc,
        attrDesc,
        key,
        attribute,
        offset,
        value.get(),
        valueBytes);
  }


  public static <T> KeyValue<T> of(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      Offset offset,
      T value,
      byte[] valueBytes) {

    return new KeyValue<>(
        entityDesc,
        attrDesc,
        key,
        attribute,
        offset,
        value,
        valueBytes);
  }

  @Getter
  private final EntityDescriptor entityDescriptor;

  @Getter
  private final AttributeDescriptor<T> attrDescriptor;

  @Getter
  private final String key;

  @Getter
  private final String attribute;

  @Getter
  private final T value;

  @Getter
  private final byte[] valueBytes;

  @Getter
  private final Offset offset;


  KeyValue(
      EntityDescriptor entityDesc,
      AttributeDescriptor<T> attrDesc,
      String key,
      String attribute,
      Offset offset,
      T value,
      byte[] valueBytes) {

    this.entityDescriptor = entityDesc;
    this.attrDescriptor = attrDesc;
    this.key = key;
    this.attribute = attribute;
    this.value = value;
    this.valueBytes = valueBytes;
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "KeyValue("
        + "entityDesc=" + getEntityDescriptor()
        + ", attrDesc=" + getAttrDescriptor()
        + ", key=" + getKey()
        + ", attribute=" + getAttribute()
        + ", offset=" + getOffset()
        + ", value=" + getValue() + ")";
  }

}
